package storage

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/Shashankkaranamr/Quorum/raft"
)

// durableState is what recovery must reproduce.
type durableState struct {
	HS      raft.HardState
	Snap    raft.SnapshotMeta
	Entries []raft.Entry
}

func (s durableState) String() string {
	return fmt.Sprintf("hs=%+v snap=%d@%d entries=%d", s.HS, s.Snap.Index, s.Snap.Term, len(s.Entries))
}

func stateOf(t *testing.T, s Storage) durableState {
	t.Helper()
	rec, err := s.InitialState()
	require.NoError(t, err)
	ents := rec.Entries
	if len(ents) == 0 {
		ents = nil
	}
	return durableState{HS: rec.HardState, Snap: rec.Snapshot.Meta, Entries: ents}
}

// batch is one Ready's worth of durable writes.
type batch struct {
	entries []raft.Entry
	hs      *raft.HardState
}

func ent(idx raft.Index, term raft.Term, data string) raft.Entry {
	return raft.Entry{Index: idx, Term: term, Type: raft.EntryNormal, Data: []byte(data)}
}

func hsp(term raft.Term, vote raft.NodeID, commit raft.Index) *raft.HardState {
	return &raft.HardState{Term: term, VotedFor: vote, Commit: commit}
}

// scriptedBatches is a history shaped like a real follower's: a vote, entries,
// commits, a leader overwriting an uncommitted suffix, and a batch whose hard
// state and entries arrive together. Every batch changes the durable state, so
// a recovery that lost any one of them is distinguishable from one that did not
// (TestTruncationCheckDistinguishesEveryPrefix holds this).
func scriptedBatches() []batch {
	return []batch{
		{hs: hsp(1, 2, 0)},
		{entries: []raft.Entry{{Index: 1, Term: 1, Type: raft.EntryNoOp}}},
		{entries: []raft.Entry{ent(2, 1, "a=1"), ent(3, 1, "b=2")}, hs: hsp(1, 2, 1)},
		{entries: []raft.Entry{ent(4, 1, "c=3")}},
		{hs: hsp(2, 0, 3)},
		{entries: []raft.Entry{ent(4, 2, "c=overwritten"), ent(5, 2, "d=4")}, hs: hsp(2, 3, 3)},
		{entries: []raft.Entry{ent(6, 2, "a longer value that spans more bytes than the others")}},
		{hs: hsp(2, 3, 6)},
	}
}

func applyBatch(t *testing.T, s Storage, b batch) {
	t.Helper()
	require.NoError(t, s.Append(b.entries))
	if b.hs != nil {
		require.NoError(t, s.SetHardState(*b.hs))
	}
	require.NoError(t, s.Sync())
}

func openWAL(t *testing.T, dir string, opts WALOptions) *WAL {
	t.Helper()
	w, err := OpenWAL(dir, opts)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Crash() })
	return w
}

// tornWALFixture writes scriptedBatches through a real WAL and returns the
// segment's bytes, the offset at which each record ends, and the durable state
// after each prefix of records. states[k] is the state after k records.
func tornWALFixture(t *testing.T) (data []byte, ends []int, states []durableState) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "wal")
	w := openWAL(t, dir, WALOptions{})
	model := NewMem()

	states = append(states, stateOf(t, model))
	for _, b := range scriptedBatches() {
		applyBatch(t, w, b)
		applyBatch(t, model, b)
		ends = append(ends, int(w.size))
		states = append(states, stateOf(t, model))
	}
	require.NoError(t, w.Close())

	data, err := os.ReadFile(filepath.Join(dir, segmentName(1)))
	require.NoError(t, err)
	require.Equal(t, ends[len(ends)-1], len(data))
	return data, ends, states
}

// completeRecords is how many whole records lie in the first off bytes.
func completeRecords(ends []int, off int) int {
	k := 0
	for k < len(ends) && ends[k] <= off {
		k++
	}
	return k
}

// checkRecoveredPrefix is the truncation test's verdict for one offset: the
// recovered state must be exactly the state after the last record that was
// wholly on disk. Not an earlier one (that loses a synced record) and not a
// later one (that invents a record from partial bytes).
func checkRecoveredPrefix(got durableState, states []durableState, ends []int, off int) error {
	k := completeRecords(ends, off)
	want := states[k]
	if fmt.Sprint(got) != fmt.Sprint(want) || !entriesEqual(got.Entries, want.Entries) {
		return fmt.Errorf("truncated at byte %d (%d whole records): recovered %v, want %v",
			off, k, got, want)
	}
	return nil
}

func entriesEqual(a, b []raft.Entry) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Index != b[i].Index || a[i].Term != b[i].Term || a[i].Type != b[i].Type ||
			!bytes.Equal(a[i].Data, b[i].Data) {
			return false
		}
	}
	return true
}

// TestWALTruncationAtEveryOffset is phase 3 acceptance criterion 2.
//
// A crash can stop a write at any byte. This writes a real WAL, then for EVERY
// byte offset from zero to the end truncates a copy there and recovers it. The
// recovered state must be exactly the prefix of whole records every time --
// never a partial record, never an error that throws away valid records before
// the tear. It then appends to the recovered log and recovers again, which is
// what proves the torn bytes were really cut off rather than left for the next
// write to land behind.
func TestWALTruncationAtEveryOffset(t *testing.T) {
	data, ends, states := tornWALFixture(t)
	t.Logf("%d records, %d bytes: recovering %d truncations", len(ends), len(data), len(data)+1)

	// Every offset, split across parallel workers: the cost is fsync latency,
	// not CPU, so running them concurrently changes the wall time and nothing
	// else. Each worker covers a disjoint set of offsets and together they
	// cover all of them.
	const workers = 8
	root := t.TempDir()
	for wk := range workers {
		t.Run(fmt.Sprintf("worker-%d", wk), func(t *testing.T) {
			t.Parallel()
			for off := wk; off <= len(data); off += workers {
				recoverTruncatedAt(t, filepath.Join(root, fmt.Sprintf("off-%05d", off)), data, ends, states, off)
			}
		})
	}
}

func recoverTruncatedAt(t *testing.T, dir string, data []byte, ends []int, states []durableState, off int) {
	t.Helper()
	require.NoError(t, os.MkdirAll(dir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, segmentName(1)), data[:off], 0o644))

	w, err := OpenWAL(dir, WALOptions{})
	require.NoErrorf(t, err, "truncated at byte %d: recovery refused a torn tail", off)
	if err := checkRecoveredPrefix(stateOf(t, w), states, ends, off); err != nil {
		_ = w.Crash()
		t.Fatal(err)
	}
	k := completeRecords(ends, off)
	wantCut := off
	if k > 0 {
		wantCut = off - ends[k-1]
	}
	require.EqualValues(t, wantCut, w.Recovery().TruncatedBytes, "truncated at byte %d", off)

	// Write after the tear, then recover again.
	before := states[k]
	next := batch{
		entries: []raft.Entry{ent(raft.Index(len(before.Entries))+1, before.HS.Term+1, "after")},
		hs:      hsp(before.HS.Term+1, 1, before.HS.Commit),
	}
	applyBatch(t, w, next)
	require.NoError(t, w.Close())

	w2, err := OpenWAL(dir, WALOptions{})
	require.NoErrorf(t, err, "truncated at byte %d, then appended: reopen failed", off)
	got := stateOf(t, w2)
	require.NoError(t, w2.Close())
	require.Zero(t, w2.Recovery().TruncatedBytes, "truncated at byte %d: the tear came back", off)
	require.Equal(t, *next.hs, got.HS)
	require.Truef(t, entriesEqual(append(append([]raft.Entry(nil), before.Entries...), next.entries...), got.Entries),
		"truncated at byte %d: log after append-and-reopen is %v", off, got.Entries)
}

// TestTruncationCheckDistinguishesEveryPrefix is the negative control for
// TestWALTruncationAtEveryOffset. Its check is only as good as the fixture: if
// two consecutive prefixes recovered to the same state, losing the record
// between them would be invisible. So every prefix must differ from every
// other, and the check must reject the state of the neighbouring prefix on
// either side at every offset.
func TestTruncationCheckDistinguishesEveryPrefix(t *testing.T) {
	data, ends, states := tornWALFixture(t)

	for i := range states {
		for j := i + 1; j < len(states); j++ {
			require.Falsef(t, fmt.Sprint(states[i]) == fmt.Sprint(states[j]) &&
				entriesEqual(states[i].Entries, states[j].Entries),
				"prefixes %d and %d recover to the same state, so losing a record between them would pass", i, j)
		}
	}

	for off := 0; off <= len(data); off++ {
		k := completeRecords(ends, off)
		require.NoError(t, checkRecoveredPrefix(states[k], states, ends, off))
		if k > 0 {
			require.Errorf(t, checkRecoveredPrefix(states[k-1], states, ends, off),
				"at byte %d the check accepted a recovery that lost record %d", off, k)
		}
		if k+1 < len(states) {
			require.Errorf(t, checkRecoveredPrefix(states[k+1], states, ends, off),
				"at byte %d the check accepted a recovery that invented record %d from partial bytes", off, k+1)
		}
	}
}

// TestWALRecoversThroughTrailingGarbage covers the other shape a crash leaves:
// not a short file but a long one, where the filesystem extended the file and
// the tail is zeros or junk.
func TestWALRecoversThroughTrailingGarbage(t *testing.T) {
	data, _, states := tornWALFixture(t)
	want := states[len(states)-1]

	tails := map[string][]byte{
		"zeros": make([]byte, 4096),
		"random": func() []byte {
			rng := rand.New(rand.NewPCG(1, 2))
			b := make([]byte, 4096)
			for i := range b {
				b[i] = byte(rng.Uint32())
			}
			return b
		}(),
	}
	for name, tail := range tails {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			require.NoError(t, os.WriteFile(filepath.Join(dir, segmentName(1)),
				append(bytes.Clone(data), tail...), 0o644))
			w := openWAL(t, dir, WALOptions{})
			got := stateOf(t, w)
			require.Equal(t, want.HS, got.HS)
			require.True(t, entriesEqual(want.Entries, got.Entries), "recovered %v, want %v", got, want)
			require.EqualValues(t, len(tail), w.Recovery().TruncatedBytes)
		})
	}
}

// TestWALRollsSegmentsAndRecoversAcrossThem writes enough to fill several
// small segments and checks that recovery stitches them back together.
func TestWALRollsSegmentsAndRecoversAcrossThem(t *testing.T) {
	dir := t.TempDir()
	opts := WALOptions{SegmentBytes: 256}
	w := openWAL(t, dir, opts)
	model := NewMem()

	for i := 1; i <= 60; i++ {
		b := batch{entries: []raft.Entry{ent(raft.Index(i), 1, fmt.Sprintf("value-%03d", i))}}
		if i%7 == 0 {
			b.hs = hsp(1, 1, raft.Index(i-3))
		}
		applyBatch(t, w, b)
		applyBatch(t, model, b)
	}
	require.Greater(t, w.Stats().SegmentsCreated, uint64(3), "the test must actually roll segments")
	require.NoError(t, w.Close())

	w2 := openWAL(t, dir, opts)
	require.Equal(t, int(w.Stats().SegmentsCreated), w2.Recovery().Segments)
	require.Equal(t, stateOf(t, model), stateOf(t, w2))
}

// TestWALRefusesDamageThatIsNotATornTail: segments are only closed after they
// are fully synced, so a bad record in any segment but the last cannot be a
// torn write. Truncating there would discard synced records in every later
// segment, so recovery must refuse instead.
func TestWALRefusesDamageThatIsNotATornTail(t *testing.T) {
	build := func(t *testing.T) string {
		dir := t.TempDir()
		w := openWAL(t, dir, WALOptions{SegmentBytes: 128})
		for i := 1; i <= 20; i++ {
			applyBatch(t, w, batch{entries: []raft.Entry{ent(raft.Index(i), 1, "0123456789")}})
		}
		require.NoError(t, w.Close())
		return dir
	}

	t.Run("corrupt byte in an earlier segment", func(t *testing.T) {
		dir := build(t)
		path := filepath.Join(dir, segmentName(1))
		b, err := os.ReadFile(path)
		require.NoError(t, err)
		b[len(b)-1] ^= 0xff
		require.NoError(t, os.WriteFile(path, b, 0o644))

		_, err = OpenWAL(dir, WALOptions{SegmentBytes: 128})
		require.ErrorContains(t, err, "not a torn write")
	})

	t.Run("missing segment", func(t *testing.T) {
		dir := build(t)
		require.NoError(t, os.Remove(filepath.Join(dir, segmentName(2))))
		_, err := OpenWAL(dir, WALOptions{SegmentBytes: 128})
		require.ErrorContains(t, err, "a segment is missing")
	})

	t.Run("unreadable record with a valid crc at the tail", func(t *testing.T) {
		dir := build(t)
		seqs, err := listSegments(dir)
		require.NoError(t, err)
		path := filepath.Join(dir, segmentName(seqs[len(seqs)-1]))
		f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
		require.NoError(t, err)
		// A snapshot pointer anywhere but the start of a segment is a record
		// this code never writes -- SaveSnapshot always rolls first -- and its
		// CRC is valid, so it must not be mistaken for a torn tail.
		rec, err := encodeRecord(nil, sampleRecords()[4])
		require.NoError(t, err)
		_, err = f.Write(rec)
		require.NoError(t, err)
		require.NoError(t, f.Close())

		_, err = OpenWAL(dir, WALOptions{SegmentBytes: 128})
		require.ErrorContains(t, err, "not the first record")
	})
}

// TestWALSyncIsOneRecordAndOneFsync checks the storage half of "one fsync per
// Ready": however many entries a batch carries, Sync writes one record and
// fsyncs once; with nothing buffered it fsyncs not at all.
func TestWALSyncIsOneRecordAndOneFsync(t *testing.T) {
	w := openWAL(t, t.TempDir(), WALOptions{})

	next := raft.Index(1)
	for _, n := range []int{1, 2, 10, 1000} {
		before := w.Stats()
		var ents []raft.Entry
		for range n {
			ents = append(ents, ent(next, 1, "payload"))
			next++
		}
		applyBatch(t, w, batch{entries: ents, hs: hsp(1, 1, next-1)})
		after := w.Stats()
		require.Equalf(t, before.Fsyncs+1, after.Fsyncs, "batch of %d entries", n)
		require.Equalf(t, before.Records+1, after.Records, "batch of %d entries", n)
		require.Equalf(t, before.SyncCalls+1, after.SyncCalls, "batch of %d entries", n)
	}

	before := w.Stats()
	require.NoError(t, w.Sync())
	after := w.Stats()
	require.Equal(t, before.SyncCalls+1, after.SyncCalls)
	require.Equal(t, before.Fsyncs, after.Fsyncs, "an empty Sync has nothing to make durable")
	require.Positive(t, after.FsyncTotal)
}

// TestWALRejectsANonContiguousBatchBeforeWritingIt: a batch recovery could not
// replay must be refused at Sync, while the log is still intact, rather than
// written and discovered at the next restart.
func TestWALRejectsANonContiguousBatchBeforeWritingIt(t *testing.T) {
	dir := t.TempDir()
	w := openWAL(t, dir, WALOptions{})
	applyBatch(t, w, batch{entries: []raft.Entry{ent(1, 1, "a")}})
	size := w.size

	require.NoError(t, w.Append([]raft.Entry{ent(3, 1, "gap")}))
	require.ErrorContains(t, w.Sync(), "non-contiguous")
	require.Equal(t, size, w.size, "nothing may be written for a rejected batch")
}

// TestWALAgreesWithMemStorage is a differential test: random batch sequences,
// including overwrites of uncommitted suffixes, applied to both the simulator's
// storage and the real log. After reopening from disk the two must agree
// exactly, so the simulator's crash model is the real one.
func TestWALAgreesWithMemStorage(t *testing.T) {
	for seed := range uint64(12) {
		rng := rand.New(rand.NewPCG(seed, 99))
		dir := t.TempDir()
		opts := WALOptions{SegmentBytes: int64(200 + rng.IntN(2000))}
		w := openWAL(t, dir, opts)
		model := NewMem()

		var last raft.Index
		var hs raft.HardState
		for range 40 + rng.IntN(40) {
			var b batch
			if rng.IntN(3) > 0 {
				from := last + 1
				if last > hs.Commit && rng.IntN(4) == 0 {
					from = hs.Commit + 1 + raft.Index(rng.IntN(int(last-hs.Commit))) // overwrite
				}
				for i := range rng.IntN(5) + 1 {
					b.entries = append(b.entries, ent(from+raft.Index(i), hs.Term+1, fmt.Sprintf("%d", rng.Int())))
				}
				last = from + raft.Index(len(b.entries)) - 1
			}
			if rng.IntN(2) == 0 {
				hs = raft.HardState{Term: hs.Term + raft.Term(rng.IntN(2)), VotedFor: raft.NodeID(rng.IntN(4)),
					Commit: min(last, hs.Commit+raft.Index(rng.IntN(3)))}
				b.hs = &hs
			}
			applyBatch(t, w, b)
			applyBatch(t, model, b)
		}
		require.NoError(t, w.Close())

		reopened := openWAL(t, dir, opts)
		require.Equalf(t, stateOf(t, model), stateOf(t, reopened), "seed %d", seed)
	}
}

// TestWALCrashLosesExactlyTheUnsynced: what Crash discards is exactly what was
// buffered since the last Sync -- the same contract MemStorage models.
func TestWALCrashLosesExactlyTheUnsynced(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWAL(dir, WALOptions{})
	require.NoError(t, err)
	applyBatch(t, w, batch{entries: []raft.Entry{ent(1, 1, "kept")}, hs: hsp(1, 1, 0)})

	require.NoError(t, w.Append([]raft.Entry{ent(2, 1, "lost")}))
	require.NoError(t, w.SetHardState(*hsp(2, 3, 1)))
	require.NoError(t, w.Crash())
	require.ErrorIs(t, w.Sync(), errWALClosed)

	w2 := openWAL(t, dir, WALOptions{})
	got := stateOf(t, w2)
	require.Equal(t, *hsp(1, 1, 0), got.HS)
	require.True(t, entriesEqual([]raft.Entry{ent(1, 1, "kept")}, got.Entries))
}
