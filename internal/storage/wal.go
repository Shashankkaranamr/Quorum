package storage

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	raftv1 "github.com/Shashankkaranamr/Quorum/gen/quorum/raft/v1"
	"github.com/Shashankkaranamr/Quorum/internal/pbconv"
	"github.com/Shashankkaranamr/Quorum/raft"
)

// DefaultSegmentBytes is the size at which a segment is closed and the next one
// started. Segments exist so that the prefix a snapshot has made redundant can
// be deleted a file at a time, instead of rewriting a single ever-growing file.
const DefaultSegmentBytes = 16 << 20

const segmentSuffix = ".log"

// SnapshotStage names a point in SaveSnapshot's write sequence. It exists so a
// test can crash the log at exactly that point; see WALOptions.FailSnapshotAt.
type SnapshotStage uint8

const (
	// SnapshotStageNone injects nothing. It is the zero value.
	SnapshotStageNone SnapshotStage = iota

	// SnapshotStageFileWritten is after the .snap file is written and
	// fsynced, before the pointer record is appended.
	SnapshotStageFileWritten

	// SnapshotStagePointerWritten is after the pointer record is fsynced,
	// before any superseded segment or snapshot file is deleted.
	SnapshotStagePointerWritten
)

func (s SnapshotStage) String() string {
	switch s {
	case SnapshotStageNone:
		return "none"
	case SnapshotStageFileWritten:
		return "after-snapshot-file"
	case SnapshotStagePointerWritten:
		return "after-pointer"
	default:
		return fmt.Sprintf("stage(%d)", uint8(s))
	}
}

// ErrInjectedCrash is what SaveSnapshot returns when WALOptions.FailSnapshotAt
// stops it. The log is left exactly as a process killed at that point would
// leave it on disk, and refuses every further call; the test crashes it and
// reopens the directory.
var ErrInjectedCrash = errors.New("storage: injected crash")

// WALOptions configure a write-ahead log.
type WALOptions struct {
	// SegmentBytes is the roll threshold. Zero means DefaultSegmentBytes.
	// Tests set it small to exercise rolling without writing megabytes.
	SegmentBytes int64

	// SnapDir is where snapshot files go. Empty means a "snap" directory
	// beside the log directory, which gives the layout in DESIGN.md §2.
	SnapDir string

	// FailSnapshotAt stops SaveSnapshot at the named stage, as if the process
	// were killed there. It is fault injection for the crash-mid-snapshot
	// tests and must be left zero anywhere else.
	FailSnapshotAt SnapshotStage
}

// WALStats are the durability counters.
//
// They exist so that the fsync boundary can be audited from outside rather
// than taken on trust: TestOneFsyncPerReady asserts on Fsyncs, and phase 6
// uses the latency figures to tell a slow disk apart from a stalled loop.
type WALStats struct {
	// SyncCalls counts every Sync. The driver makes exactly one per Ready.
	SyncCalls uint64

	// Fsyncs counts File.Sync calls made by Sync -- the per-Ready fsyncs. A
	// Sync with nothing buffered makes none, because there is nothing to make
	// durable; every other Sync makes exactly one, however many entries it
	// carries.
	Fsyncs uint64

	// SnapshotFsyncs counts the fsyncs SaveSnapshot makes: the snapshot file
	// and the pointer record. They are counted apart from Fsyncs because they
	// are not per-Ready, and folding them in would make the one-per-Ready
	// invariant impossible to state.
	SnapshotFsyncs uint64

	// DirSyncs counts directory fsyncs, which happen only when a file is
	// created or deleted and only on platforms that support them.
	DirSyncs uint64

	Records         uint64
	EntriesWritten  uint64
	BytesWritten    uint64
	SegmentsCreated uint64

	SnapshotsSaved  uint64
	SegmentsDeleted uint64

	FsyncTotal time.Duration
	FsyncMax   time.Duration
}

// WALRecovery describes what Open found on disk.
type WALRecovery struct {
	Segments int
	Records  int

	// TruncatedBytes is how much of the last segment was discarded as a torn
	// tail. Non-zero means the previous process died mid-write -- expected
	// after a crash, and never a loss of anything that had been synced.
	TruncatedBytes int64

	// SnapshotIndex is the index of the snapshot recovery started from, or
	// zero if there was none.
	SnapshotIndex raft.Index

	// SupersededSegments counts segments found before the snapshot pointer
	// recovery started from. They are left over from a crash part-way through
	// deleting them, hold nothing the node still needs, and are removed.
	SupersededSegments int

	// OrphanSnapshots counts snapshot files no pointer names -- written by a
	// snapshot that crashed before its pointer was durable, or superseded by
	// a newer one. They are removed.
	OrphanSnapshots int
}

// WAL is the write-ahead log: the real Storage behind a node.
//
// Each Sync writes the Ready batch as one framed record and fsyncs it. One
// record per batch is what makes the batch the unit of atomicity: a crash
// mid-write leaves a torn record that fails its CRC, and recovery drops it
// whole. There is no state in which half a batch is durable.
//
// A WAL is owned by the single driver goroutine that owns the node, like
// everything else on the durability path. It takes no locks because it is
// never shared.
//
// After any write or fsync error the WAL refuses all further operations. A
// failed fsync leaves the kernel's view of the file unknowable -- on Linux the
// dirty pages may already have been dropped, so retrying can report success
// for data that is gone. The only safe recovery is to restart the process and
// rebuild state from what is actually on disk.
type WAL struct {
	dir     string
	snapDir string
	segSize int64
	failAt  SnapshotStage

	f    *os.File
	seq  uint64
	size int64

	// The durable state, kept in step with what is on disk. SaveSnapshot
	// needs it to write the log tail into the pointer record, and it is what
	// makes a batch recovery could not replay refusable before it is written.
	// It duplicates the core's copy of the log, bounded by compaction.
	hs   raft.HardState
	log  logState
	snap raft.Snapshot

	pendingEntries []raft.Entry
	pendingHS      *raft.HardState
	buf            []byte

	recovery WALRecovery
	stats    WALStats

	// recent is a ring of the latest per-Ready fsync latencies, for FsyncP99.
	recent [fsyncWindow]time.Duration

	// failed is set on the first write or fsync error and returned from
	// every call after it.
	failed error
	closed bool
}

var _ Storage = (*WAL)(nil)

var errWALClosed = errors.New("storage: write-ahead log is closed")

// OpenWAL opens the log in dir, creating it if it does not exist, and recovers
// the durable state from it.
//
// Recovery starts from the last segment that begins with a snapshot pointer, or
// from the first segment if none does, and scans forward. A torn or corrupt
// record at the end of the LAST segment is what a crash mid-write leaves
// behind, and is truncated away: everything before it was synced and survives,
// and nothing after it ever was. The same damage anywhere else is not a torn
// write -- segments are only closed once fully synced -- so it is reported as
// corruption and the node refuses to start, rather than silently dropping data
// it had promised to keep.
func OpenWAL(dir string, opts WALOptions) (*WAL, error) {
	w := &WAL{dir: dir, snapDir: opts.SnapDir, segSize: opts.SegmentBytes, failAt: opts.FailSnapshotAt}
	if w.segSize <= 0 {
		w.segSize = DefaultSegmentBytes
	}
	if w.snapDir == "" {
		w.snapDir = filepath.Join(filepath.Dir(dir), "snap")
	}

	for _, d := range []string{dir, w.snapDir} {
		created, err := ensureDir(d)
		if err != nil {
			return nil, err
		}
		if created {
			// The directory's own entry in its parent must be durable too, or
			// a crash can lose the whole log along with the directory holding
			// it.
			if err := w.syncDir(filepath.Dir(d)); err != nil {
				return nil, err
			}
		}
	}

	seqs, err := listSegments(dir)
	if err != nil {
		return nil, err
	}
	if len(seqs) == 0 {
		if err := w.removeOrphanSnapshots(""); err != nil {
			return nil, err
		}
		if err := w.createSegment(1); err != nil {
			return nil, err
		}
		return w, nil
	}

	if err := w.recover(seqs); err != nil {
		// Recovery opens the tail segment before it has finished judging the
		// log. A refusal after that point must still release the file: a
		// leaked handle outlives the failed open, and on Windows it stops the
		// directory being moved or deleted by whoever is investigating.
		if w.f != nil {
			_ = w.f.Close()
		}
		return nil, err
	}
	return w, nil
}

func ensureDir(dir string) (created bool, err error) {
	switch _, statErr := os.Stat(dir); {
	case statErr == nil:
		return false, nil
	case errors.Is(statErr, os.ErrNotExist):
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return false, fmt.Errorf("storage: create directory: %w", err)
		}
		return true, nil
	default:
		return false, fmt.Errorf("storage: stat directory: %w", statErr)
	}
}

func segmentName(seq uint64) string { return fmt.Sprintf("%06d%s", seq, segmentSuffix) }

func snapshotName(m raft.SnapshotMeta) string {
	return fmt.Sprintf("%020d-%020d.snap", m.Index, m.Term)
}

// listSegments returns the segment sequence numbers in dir, ascending. Whether
// they are consecutive is for recovery to judge: a gap is legal before a
// segment that begins with a snapshot pointer, and nowhere else.
func listSegments(dir string) ([]uint64, error) {
	des, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("storage: read wal directory: %w", err)
	}
	var seqs []uint64
	for _, de := range des {
		name := de.Name()
		if de.IsDir() || !strings.HasSuffix(name, segmentSuffix) {
			continue
		}
		seq, err := strconv.ParseUint(strings.TrimSuffix(name, segmentSuffix), 10, 64)
		if err != nil || seq == 0 {
			return nil, fmt.Errorf("storage: %s in %s is not a segment name this binary wrote; "+
				"move it out of the wal directory", name, dir)
		}
		seqs = append(seqs, seq)
	}
	slices.Sort(seqs)
	return seqs, nil
}

// startSegment picks where replay begins: the last segment whose first record
// is an intact snapshot pointer. Everything before it is superseded -- the
// pointer carries the log tail and hard state -- so a crash part-way through
// deleting older segments, which can leave any subset of them behind, is
// harmless.
//
// If no segment begins with a pointer, replay begins at the first segment, and
// that segment must be 000001: anything else means the start of the log is
// missing.
func (w *WAL) startSegment(seqs []uint64) (int, error) {
	for i := len(seqs) - 1; i >= 0; i-- {
		path := filepath.Join(w.dir, segmentName(seqs[i]))
		data, err := os.ReadFile(path)
		if err != nil {
			return 0, fmt.Errorf("storage: read %s: %w", path, err)
		}
		if rec, _, err := decodeRecord(data); err == nil && rec.typ == recordSnapshotPointer {
			return i, nil
		}
	}
	if seqs[0] != 1 {
		return 0, fmt.Errorf("storage: the wal in %s starts at %s, which does not begin with a "+
			"snapshot pointer; the start of the log is missing", w.dir, segmentName(seqs[0]))
	}
	return 0, nil
}

func (w *WAL) recover(seqs []uint64) error {
	start, err := w.startSegment(seqs)
	if err != nil {
		return err
	}
	superseded, seqs := seqs[:start], seqs[start:]
	for i := 1; i < len(seqs); i++ {
		if seqs[i] != seqs[i-1]+1 {
			return fmt.Errorf("storage: wal segments in %s jump from %s to %s; "+
				"a segment is missing, and replaying across the gap would corrupt the log",
				w.dir, segmentName(seqs[i-1]), segmentName(seqs[i]))
		}
	}

	var (
		hs      raft.HardState
		log     logState
		pointer *raftv1.WalSnapshotPointer
	)
	for i, seq := range seqs {
		path := filepath.Join(w.dir, segmentName(seq))
		data, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("storage: read %s: %w", path, err)
		}
		last := i == len(seqs)-1

		off := 0
		for off < len(data) {
			rec, n, err := decodeRecord(data[off:])
			if err != nil {
				torn := errors.Is(err, errShortRecord) || errors.Is(err, errCorruptRecord)
				if last && torn {
					break
				}
				return fmt.Errorf("storage: %s at offset %d: %w; this is not a torn write "+
					"from a crash, so the node will not start rather than discard data it "+
					"had made durable", path, off, err)
			}

			switch rec.typ {
			case recordEntryBatch:
				ents, err := pbconv.EntriesFromProto(rec.batch.GetEntries())
				if err != nil {
					return fmt.Errorf("storage: %s at offset %d: %w", path, off, err)
				}
				if err := log.append(ents); err != nil {
					return fmt.Errorf("storage: %s at offset %d: %w", path, off, err)
				}
				if p := rec.batch.GetHardState(); p != nil {
					hs = pbconv.HardStateFromProto(p)
				}
			case recordSnapshotPointer:
				if i != 0 || off != 0 {
					// SaveSnapshot always rolls to a fresh segment first. A
					// pointer anywhere else was not written by this code.
					return fmt.Errorf("storage: %s at offset %d: a snapshot pointer that is not "+
						"the first record of the first segment replayed", path, off)
				}
				pointer = rec.pointer
				if log, hs, err = stateFromPointer(pointer); err != nil {
					return fmt.Errorf("storage: %s: %w", path, err)
				}
			}
			off += n
			w.recovery.Records++
		}

		if last {
			if err := w.openTail(seq, path, int64(off), int64(len(data))); err != nil {
				return err
			}
		}
	}
	w.recovery.Segments = len(seqs)

	if pointer != nil {
		snap, err := w.readSnapshot(pointer)
		if err != nil {
			return err
		}
		w.snap = snap
		w.recovery.SnapshotIndex = snap.Meta.Index
	}

	// The commit index may sit below the snapshot -- a follower that
	// installed one persists its hard state afterwards -- but never beyond
	// what the node holds.
	if lastIndex := log.lastIndex(); hs.Commit > lastIndex {
		return fmt.Errorf("storage: recovered commit index %d is beyond the last recovered "+
			"entry %d; the log in %s is inconsistent", hs.Commit, lastIndex, w.dir)
	}
	w.hs, w.log = hs, log

	// Housekeeping, only once recovery has succeeded: nothing here is needed
	// any more, and leaving it would let it accumulate across crashes.
	for _, seq := range superseded {
		if err := os.Remove(filepath.Join(w.dir, segmentName(seq))); err != nil {
			return fmt.Errorf("storage: remove superseded segment: %w", err)
		}
		w.recovery.SupersededSegments++
		w.stats.SegmentsDeleted++
	}
	keep := ""
	if pointer != nil {
		keep = pointer.GetFilename()
	}
	return w.removeOrphanSnapshots(keep)
}

// stateFromPointer rebuilds the log and hard state a pointer record carries.
func stateFromPointer(p *raftv1.WalSnapshotPointer) (logState, raft.HardState, error) {
	meta := pbconv.SnapshotMetaFromProto(p.GetMetadata())
	if meta.Index == 0 {
		return logState{}, raft.HardState{}, errors.New("snapshot pointer names index 0")
	}
	ents, err := pbconv.EntriesFromProto(p.GetEntries())
	if err != nil {
		return logState{}, raft.HardState{}, err
	}
	log := logState{snap: meta}
	if err := log.append(ents); err != nil {
		return logState{}, raft.HardState{}, err
	}
	return log, pbconv.HardStateFromProto(p.GetHardState()), nil
}

// readSnapshot loads the file a durable pointer names and checks it against the
// pointer. A pointer is only ever written after its file is durable, so a file
// that is missing or does not match cannot be a crash artifact: it is damage,
// and the node refuses to start rather than restore a state machine from it.
func (w *WAL) readSnapshot(p *raftv1.WalSnapshotPointer) (raft.Snapshot, error) {
	name := p.GetFilename()
	if name != filepath.Base(name) || !strings.HasSuffix(name, ".snap") {
		return raft.Snapshot{}, fmt.Errorf("storage: snapshot pointer names %q, which is not a "+
			"snapshot file name this binary writes", name)
	}
	path := filepath.Join(w.snapDir, name)
	data, err := os.ReadFile(path)
	if err != nil {
		return raft.Snapshot{}, fmt.Errorf("storage: a durable snapshot pointer names %s, but it "+
			"cannot be read: %w; the pointer is only written once the file is durable, so this is "+
			"not a crash artifact", path, err)
	}
	if uint64(len(data)) != p.GetSizeBytes() || crc(data) != p.GetCrc32C() {
		return raft.Snapshot{}, fmt.Errorf("storage: snapshot %s is %d bytes with crc %08x, but its "+
			"pointer recorded %d bytes with crc %08x; the file is damaged",
			path, len(data), crc(data), p.GetSizeBytes(), p.GetCrc32C())
	}
	return raft.Snapshot{Meta: pbconv.SnapshotMetaFromProto(p.GetMetadata()), Data: data}, nil
}

// removeOrphanSnapshots deletes every snapshot file except keep.
func (w *WAL) removeOrphanSnapshots(keep string) error {
	des, err := os.ReadDir(w.snapDir)
	if err != nil {
		return fmt.Errorf("storage: read snapshot directory: %w", err)
	}
	for _, de := range des {
		name := de.Name()
		if de.IsDir() || name == keep || !strings.HasSuffix(name, ".snap") {
			continue
		}
		if err := os.Remove(filepath.Join(w.snapDir, name)); err != nil {
			return fmt.Errorf("storage: remove orphan snapshot: %w", err)
		}
		w.recovery.OrphanSnapshots++
	}
	return nil
}

// openTail opens the last segment for appending, first cutting off any torn
// tail at good.
//
// The truncation is fsynced before anything new is written. Otherwise a second
// crash could resurrect the torn bytes behind records written after them.
func (w *WAL) openTail(seq uint64, path string, good, size int64) error {
	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		return fmt.Errorf("storage: open %s for append: %w", path, err)
	}
	if good < size {
		if err := f.Truncate(good); err != nil {
			_ = f.Close()
			return fmt.Errorf("storage: truncate torn tail of %s at %d: %w", path, good, err)
		}
		if err := f.Sync(); err != nil {
			_ = f.Close()
			return fmt.Errorf("storage: sync truncation of %s: %w", path, err)
		}
		w.recovery.TruncatedBytes = size - good
	}
	if _, err := f.Seek(good, io.SeekStart); err != nil {
		_ = f.Close()
		return fmt.Errorf("storage: seek %s: %w", path, err)
	}
	w.f, w.seq, w.size = f, seq, good
	return nil
}

func (w *WAL) createSegment(seq uint64) error {
	path := filepath.Join(w.dir, segmentName(seq))
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return fmt.Errorf("storage: create segment %s: %w", path, err)
	}
	// The new file's directory entry must be durable before a record in it
	// can be, or a crash could keep the record's bytes and lose the file.
	if err := w.syncDir(w.dir); err != nil {
		_ = f.Close()
		return err
	}
	w.f, w.seq, w.size = f, seq, 0
	w.stats.SegmentsCreated++
	return nil
}

func (w *WAL) syncDir(dir string) error {
	synced, err := syncDir(dir)
	if err != nil {
		return fmt.Errorf("storage: sync directory %s: %w", dir, err)
	}
	if synced {
		w.stats.DirSyncs++
	}
	return nil
}

func (w *WAL) usable() error {
	switch {
	case w.failed != nil:
		return fmt.Errorf("storage: write-ahead log failed earlier and must be reopened: %w", w.failed)
	case w.closed:
		return errWALClosed
	default:
		return nil
	}
}

// Append implements Storage. It buffers; nothing is written until Sync.
func (w *WAL) Append(entries []raft.Entry) error {
	if err := w.usable(); err != nil {
		return err
	}
	w.pendingEntries = append(w.pendingEntries, entries...)
	return nil
}

// SetHardState implements Storage. It buffers; nothing is written until Sync.
func (w *WAL) SetHardState(hs raft.HardState) error {
	if err := w.usable(); err != nil {
		return err
	}
	copied := hs
	w.pendingHS = &copied
	return nil
}

// Sync implements Storage. This is the fsync.
//
// Everything buffered since the last Sync goes out as ONE record in ONE write,
// followed by ONE File.Sync. With nothing buffered it does nothing at all: a
// Ready that only carries messages or committed entries has nothing to make
// durable, and an fsync of an unchanged file would buy nothing but latency.
func (w *WAL) Sync() error {
	if err := w.usable(); err != nil {
		return err
	}
	w.stats.SyncCalls++
	if len(w.pendingEntries) == 0 && w.pendingHS == nil {
		return nil
	}

	// Reject a batch that recovery could not replay BEFORE writing it.
	// Writing it would make the log unrecoverable at the next restart, which
	// is a much worse place to find out.
	if err := w.log.check(w.pendingEntries); err != nil {
		return err
	}

	batch := &raftv1.WalEntryBatch{}
	var err error
	if batch.Entries, err = pbconv.EntriesToProto(w.pendingEntries); err != nil {
		return fmt.Errorf("storage: encode batch: %w", err)
	}
	if w.pendingHS != nil {
		batch.HardState = pbconv.HardStateToProto(*w.pendingHS)
	}
	if w.buf, err = encodeRecord(w.buf[:0], record{typ: recordEntryBatch, batch: batch}); err != nil {
		return err
	}

	// Roll before writing, never after, so a record is never split across
	// segments. A record bigger than a whole segment gets a segment to itself.
	if w.size > 0 && w.size+int64(len(w.buf)) > w.segSize {
		if err := w.roll(); err != nil {
			w.failed = err
			return err
		}
	}

	elapsed, err := w.writeAndSync(w.buf)
	if err != nil {
		return err
	}

	w.recent[w.stats.Fsyncs%fsyncWindow] = elapsed
	w.stats.Fsyncs++
	w.stats.FsyncTotal += elapsed
	w.stats.FsyncMax = max(w.stats.FsyncMax, elapsed)
	w.stats.EntriesWritten += uint64(len(w.pendingEntries))

	// Cannot fail: check passed above and nothing has changed since.
	_ = w.log.append(w.pendingEntries)
	if w.pendingHS != nil {
		w.hs = *w.pendingHS
	}
	w.pendingEntries = w.pendingEntries[:0]
	w.pendingHS = nil
	return nil
}

// writeAndSync writes one encoded record to the tail segment and fsyncs it. Any
// failure is fatal to the log.
func (w *WAL) writeAndSync(rec []byte) (time.Duration, error) {
	if _, err := w.f.Write(rec); err != nil {
		w.failed = fmt.Errorf("write %s: %w", segmentName(w.seq), err)
		return 0, w.failed
	}
	start := time.Now()
	if err := w.f.Sync(); err != nil {
		w.failed = fmt.Errorf("fsync %s: %w", segmentName(w.seq), err)
		return 0, w.failed
	}
	w.size += int64(len(rec))
	w.stats.Records++
	w.stats.BytesWritten += uint64(len(rec))
	return time.Since(start), nil
}

// SaveSnapshot implements Storage.
//
// The sequence, each step durable before the next begins:
//
//  1. Write and fsync the snapshot file.
//  2. Roll to a fresh segment, and write and fsync a pointer record naming
//     the file and carrying the log tail and hard state.
//  3. Delete every older segment and every other snapshot file.
//
// A crash before step 2 completes leaves a file no pointer names; recovery
// ignores and removes it, and the node comes back with its uncompacted log. A
// crash during step 3 leaves some superseded segments behind; recovery starts
// at the pointer and removes them. Either way the node recovers to a state it
// had durably reached, which TestCrashDuringSnapshot checks at each stage.
func (w *WAL) SaveSnapshot(snap raft.Snapshot) error {
	if err := w.usable(); err != nil {
		return err
	}
	if len(w.pendingEntries) > 0 || w.pendingHS != nil {
		return errUnsyncedSnapshot
	}
	if snap.Meta.Index <= w.log.snap.Index {
		return fmt.Errorf("storage: snapshot at %d is not newer than the current one at %d",
			snap.Meta.Index, w.log.snap.Index)
	}

	// 1. The payload.
	name := snapshotName(snap.Meta)
	if err := w.writeSnapshotFile(name, snap.Data); err != nil {
		w.failed = err
		return err
	}
	if w.failAt == SnapshotStageFileWritten {
		w.failed = ErrInjectedCrash
		return ErrInjectedCrash
	}

	// 2. The pointer, as the first record of a fresh segment.
	next := w.log.clone()
	next.applySnapshot(snap.Meta)
	pointer := &raftv1.WalSnapshotPointer{
		Metadata:  pbconv.SnapshotMetaToProto(snap.Meta),
		Filename:  name,
		Crc32C:    crc(snap.Data),
		SizeBytes: uint64(len(snap.Data)),
	}
	if !w.hs.IsEmpty() {
		pointer.HardState = pbconv.HardStateToProto(w.hs)
	}
	var err error
	if pointer.Entries, err = pbconv.EntriesToProto(next.entries); err != nil {
		return fmt.Errorf("storage: encode snapshot pointer: %w", err)
	}
	if w.buf, err = encodeRecord(w.buf[:0], record{typ: recordSnapshotPointer, pointer: pointer}); err != nil {
		return err
	}
	firstKept := w.seq + 1
	if err := w.roll(); err != nil {
		w.failed = err
		return err
	}
	if _, err := w.writeAndSync(w.buf); err != nil {
		return err
	}
	w.stats.SnapshotFsyncs++
	w.log, w.snap = next, raft.Snapshot{Meta: snap.Meta, Data: snap.Data}
	w.stats.SnapshotsSaved++
	if w.failAt == SnapshotStagePointerWritten {
		w.failed = ErrInjectedCrash
		return ErrInjectedCrash
	}

	// 3. Reclaim. Failing here loses nothing -- recovery would redo it -- but
	// it is reported, because a disk that refuses deletes will soon refuse
	// writes too.
	seqs, err := listSegments(w.dir)
	if err != nil {
		return err
	}
	for _, seq := range seqs {
		if seq >= firstKept {
			continue
		}
		if err := os.Remove(filepath.Join(w.dir, segmentName(seq))); err != nil {
			return fmt.Errorf("storage: delete superseded segment: %w", err)
		}
		w.stats.SegmentsDeleted++
	}
	if err := w.removeOrphanSnapshots(name); err != nil {
		return err
	}
	return w.syncDir(w.dir)
}

// writeSnapshotFile writes and fsyncs one snapshot file.
//
// It writes the final name directly rather than a temporary file and a rename:
// the rename buys nothing here, because the file is not trusted until a pointer
// names it, and a pointer is not written until this returns. SaveSnapshot never
// rewrites a file an existing pointer names, since it refuses any snapshot not
// newer than the current one.
func (w *WAL) writeSnapshotFile(name string, data []byte) error {
	path := filepath.Join(w.snapDir, name)
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("storage: create snapshot %s: %w", path, err)
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return fmt.Errorf("storage: write snapshot %s: %w", path, err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("storage: fsync snapshot %s: %w", path, err)
	}
	w.stats.SnapshotFsyncs++
	if err := f.Close(); err != nil {
		return fmt.Errorf("storage: close snapshot %s: %w", path, err)
	}
	return w.syncDir(w.snapDir)
}

func (w *WAL) roll() error {
	// The segment being closed was fsynced by the Sync that last wrote to it,
	// so closing it loses nothing.
	if err := w.f.Close(); err != nil {
		return fmt.Errorf("storage: close segment %s: %w", segmentName(w.seq), err)
	}
	return w.createSegment(w.seq + 1)
}

// InitialState implements Storage. It reports what recovery found when the log
// was opened, or what has been written since.
func (w *WAL) InitialState() (Recovered, error) {
	return Recovered{HardState: w.hs, Snapshot: w.snap, Entries: w.log.clone().entries}, nil
}

// fsyncWindow is how many recent fsyncs FsyncP99 looks at. Recent rather than
// lifetime, because the question it answers is "is the disk slow now", which
// is what tells a slow disk apart from a stalled loop.
const fsyncWindow = 512

// FsyncP99 is the 99th-percentile latency of the most recent per-Ready fsyncs,
// or zero if there have been none. It sorts a copy of the window, so it is
// meant for a status display a few times a second, not for every Ready.
func (w *WAL) FsyncP99() time.Duration {
	n := int(min(w.stats.Fsyncs, fsyncWindow))
	if n == 0 {
		return 0
	}
	window := slices.Clone(w.recent[:n])
	slices.Sort(window)
	return window[(n*99-1)/100]
}

// Recovery reports what Open found on disk.
func (w *WAL) Recovery() WALRecovery { return w.recovery }

// Stats returns a copy of the durability counters.
func (w *WAL) Stats() WALStats { return w.stats }

// Dir is the directory the log lives in.
func (w *WAL) Dir() string { return w.dir }

// SnapDir is the directory snapshot files live in.
func (w *WAL) SnapDir() string { return w.snapDir }

// Close releases the log. Anything buffered but not synced is discarded,
// exactly as a crash would discard it, and reported as an error because a
// clean shutdown should never have any: the driver syncs every Ready it
// buffers.
func (w *WAL) Close() error {
	if w.closed {
		return nil
	}
	pending := len(w.pendingEntries) > 0 || w.pendingHS != nil
	err := w.Crash()
	if err == nil && pending {
		err = errors.New("storage: closed with unsynced writes, which were discarded")
	}
	return err
}

// Crash abandons the log the way a killed process would: buffered writes are
// dropped, the file handle is released, and nothing is flushed on the way out.
// The simulator uses it to crash a node that is running on real disk; the node
// comes back by opening the directory again.
func (w *WAL) Crash() error {
	if w.closed {
		return nil
	}
	w.closed = true
	w.pendingEntries = nil
	w.pendingHS = nil
	if w.f == nil {
		return nil
	}
	if err := w.f.Close(); err != nil {
		return fmt.Errorf("storage: close segment %s: %w", segmentName(w.seq), err)
	}
	return nil
}
