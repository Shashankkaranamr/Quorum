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
// started. Segments exist so that phase 4 can delete the prefix a snapshot has
// made redundant a file at a time, instead of rewriting a single ever-growing
// file.
const DefaultSegmentBytes = 16 << 20

const segmentSuffix = ".log"

// WALOptions configure a write-ahead log.
type WALOptions struct {
	// SegmentBytes is the roll threshold. Zero means DefaultSegmentBytes.
	// Tests set it small to exercise rolling without writing megabytes.
	SegmentBytes int64
}

// WALStats are the durability counters.
//
// They exist so that the fsync boundary can be audited from outside rather
// than taken on trust: TestOneFsyncPerReady asserts on Fsyncs, and phase 6
// uses the latency figures to tell a slow disk apart from a stalled loop.
type WALStats struct {
	// SyncCalls counts every Sync. The driver makes exactly one per Ready.
	SyncCalls uint64

	// Fsyncs counts File.Sync calls on a segment -- the real fsyncs. A Sync
	// with nothing buffered makes none, because there is nothing to make
	// durable; every other Sync makes exactly one, however many entries it
	// carries.
	Fsyncs uint64

	// DirSyncs counts directory fsyncs, which happen only when a segment is
	// created and only on platforms that support them. They are counted apart
	// from Fsyncs because they are not per-Ready.
	DirSyncs uint64

	Records         uint64
	EntriesWritten  uint64
	BytesWritten    uint64
	SegmentsCreated uint64

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
	segSize int64

	f    *os.File
	seq  uint64
	size int64

	lastIndex raft.Index

	pendingEntries []raft.Entry
	pendingHS      *raft.HardState
	buf            []byte

	initHS      raft.HardState
	initEntries []raft.Entry
	recovery    WALRecovery

	stats WALStats

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
// Recovery scans every segment in order. A torn or corrupt record at the end of
// the LAST segment is what a crash mid-write leaves behind, and is truncated
// away: everything before it was synced and survives, and nothing after it
// ever was. The same damage anywhere else is not a torn write -- segments are
// only closed once fully synced -- so it is reported as corruption and the node
// refuses to start, rather than silently dropping data it had promised to keep.
func OpenWAL(dir string, opts WALOptions) (*WAL, error) {
	w := &WAL{dir: dir, segSize: opts.SegmentBytes}
	if w.segSize <= 0 {
		w.segSize = DefaultSegmentBytes
	}

	created, err := ensureDir(dir)
	if err != nil {
		return nil, err
	}
	if created {
		// The directory's own entry in its parent must be durable too, or a
		// crash can lose the whole log along with the directory holding it.
		if err := w.syncDir(filepath.Dir(dir)); err != nil {
			return nil, err
		}
	}

	seqs, err := listSegments(dir)
	if err != nil {
		return nil, err
	}
	if len(seqs) == 0 {
		if err := w.createSegment(1); err != nil {
			return nil, err
		}
		return w, nil
	}

	if err := w.recover(seqs); err != nil {
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
			return false, fmt.Errorf("storage: create wal directory: %w", err)
		}
		return true, nil
	default:
		return false, fmt.Errorf("storage: stat wal directory: %w", statErr)
	}
}

func segmentName(seq uint64) string { return fmt.Sprintf("%06d%s", seq, segmentSuffix) }

// listSegments returns the segment sequence numbers in dir, ascending, and
// fails if they are not consecutive. A gap means a segment was deleted or lost,
// and replaying across it would stitch together two logs that do not join.
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
	for i := 1; i < len(seqs); i++ {
		if seqs[i] != seqs[i-1]+1 {
			return nil, fmt.Errorf("storage: wal segments in %s jump from %s to %s; "+
				"a segment is missing, and replaying across the gap would corrupt the log",
				dir, segmentName(seqs[i-1]), segmentName(seqs[i]))
		}
	}
	return seqs, nil
}

func (w *WAL) recover(seqs []uint64) error {
	var (
		hs      raft.HardState
		entries []raft.Entry
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
				if entries, err = appendEntries(entries, ents); err != nil {
					return fmt.Errorf("storage: %s at offset %d: %w", path, off, err)
				}
				if p := rec.batch.GetHardState(); p != nil {
					hs = pbconv.HardStateFromProto(p)
				}
			default:
				return fmt.Errorf("storage: %s at offset %d: %s records are written by "+
					"snapshotting (phase 4), which this binary does not implement", path, off, rec.typ)
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

	lastIndex := raft.Index(len(entries))
	if hs.Commit > lastIndex {
		return fmt.Errorf("storage: recovered commit index %d is beyond the last recovered "+
			"entry %d; the log in %s is inconsistent", hs.Commit, lastIndex, w.dir)
	}

	w.initHS, w.initEntries, w.lastIndex = hs, entries, lastIndex
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

	newLast, err := w.checkContiguous()
	if err != nil {
		return err
	}

	batch := &raftv1.WalEntryBatch{}
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

	if _, err := w.f.Write(w.buf); err != nil {
		w.failed = fmt.Errorf("write %s: %w", segmentName(w.seq), err)
		return w.failed
	}
	start := time.Now()
	if err := w.f.Sync(); err != nil {
		w.failed = fmt.Errorf("fsync %s: %w", segmentName(w.seq), err)
		return w.failed
	}
	elapsed := time.Since(start)

	w.stats.Fsyncs++
	w.stats.FsyncTotal += elapsed
	w.stats.FsyncMax = max(w.stats.FsyncMax, elapsed)
	w.stats.Records++
	w.stats.EntriesWritten += uint64(len(w.pendingEntries))
	w.stats.BytesWritten += uint64(len(w.buf))

	w.size += int64(len(w.buf))
	w.lastIndex = newLast
	w.pendingEntries = w.pendingEntries[:0]
	w.pendingHS = nil
	return nil
}

// checkContiguous rejects a batch that recovery could not replay, BEFORE it is
// written. Writing it would make the log unrecoverable at the next restart,
// which is a much worse place to find out.
func (w *WAL) checkContiguous() (raft.Index, error) {
	last := w.lastIndex
	for _, e := range w.pendingEntries {
		if e.Index == 0 || e.Index > last+1 {
			return 0, fmt.Errorf("storage: non-contiguous entry at index %d, log ends at %d",
				e.Index, last)
		}
		last = e.Index
	}
	return last, nil
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
// was opened; the applied index is zero until phase 4's snapshots exist to
// record it.
func (w *WAL) InitialState() (raft.HardState, []raft.Entry, raft.Index, error) {
	return w.initHS, append([]raft.Entry(nil), w.initEntries...), 0, nil
}

// Recovery reports what Open found on disk.
func (w *WAL) Recovery() WALRecovery { return w.recovery }

// Stats returns a copy of the durability counters.
func (w *WAL) Stats() WALStats { return w.stats }

// Dir is the directory the log lives in.
func (w *WAL) Dir() string { return w.dir }

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
