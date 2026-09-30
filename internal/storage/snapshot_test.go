package storage

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/Shashankkaranamr/Quorum/raft"
)

// snapFixture is a WAL with a history long enough to span several segments,
// and the two states a snapshot of it can legitimately leave behind.
type snapFixture struct {
	dir  string
	opts WALOptions
	snap raft.Snapshot

	// before is the state with the snapshot never taken; after is the state
	// with it taken. A crash anywhere in SaveSnapshot must recover to one of
	// the two, and which one depends only on whether the pointer was durable.
	before, after durableState
}

func newSnapFixture(t *testing.T, failAt SnapshotStage) (*snapFixture, *WAL) {
	t.Helper()
	root := t.TempDir()
	f := &snapFixture{
		dir:  filepath.Join(root, "wal"),
		opts: WALOptions{SegmentBytes: 256, SnapDir: filepath.Join(root, "snap"), FailSnapshotAt: failAt},
		snap: raft.Snapshot{Meta: raft.SnapshotMeta{Index: 20, Term: 1}, Data: []byte("state-machine-image@20")},
	}
	w := openWAL(t, f.dir, f.opts)
	model := NewMem()
	for i := raft.Index(1); i <= 30; i++ {
		b := batch{entries: []raft.Entry{ent(i, 1, fmt.Sprintf("value-%02d", i))}}
		if i%5 == 0 {
			b.hs = hsp(1, 1, i-2)
		}
		applyBatch(t, w, b)
		applyBatch(t, model, b)
	}
	f.before = stateOf(t, model)
	require.NoError(t, model.SaveSnapshot(f.snap))
	f.after = stateOf(t, model)

	segs, err := listSegments(f.dir)
	require.NoError(t, err)
	require.Greater(t, len(segs), 3, "the fixture must span several segments, or there is nothing to reclaim")
	return f, w
}

// reopen opens the fixture's directory as a restarted process would, without
// fault injection.
func (f *snapFixture) reopen(t *testing.T) (*WAL, error) {
	t.Helper()
	opts := f.opts
	opts.FailSnapshotAt = SnapshotStageNone
	w, err := OpenWAL(f.dir, opts)
	if err == nil {
		t.Cleanup(func() { _ = w.Crash() })
	}
	return w, err
}

// checkConsistent is the verdict on a recovery: the log must come back as
// exactly the state before the snapshot or exactly the state after it, and in
// the second case the snapshot's bytes must be the ones that were saved.
func (f *snapFixture) checkConsistent(t *testing.T, w *WAL) (tookSnapshot bool, err error) {
	t.Helper()
	got := stateOf(t, w)
	switch {
	case statesEqual(got, f.before):
		return false, nil
	case statesEqual(got, f.after):
		rec, err := w.InitialState()
		require.NoError(t, err)
		if !bytes.Equal(rec.Snapshot.Data, f.snap.Data) {
			return true, fmt.Errorf("recovered snapshot data %q, saved %q", rec.Snapshot.Data, f.snap.Data)
		}
		return true, nil
	default:
		return false, fmt.Errorf("recovered %s, which is neither the state before the snapshot (%s) "+
			"nor after it (%s)", got, f.before, f.after)
	}
}

func statesEqual(a, b durableState) bool {
	return a.HS == b.HS && a.Snap == b.Snap && entriesEqual(a.Entries, b.Entries)
}

// keepsWorking appends after recovery and recovers again: a log that came back
// consistent but could not be written to, or lost the write, has not
// recovered.
func (f *snapFixture) keepsWorking(t *testing.T, w *WAL) {
	t.Helper()
	next := stateOf(t, w)
	b := batch{entries: []raft.Entry{ent(31, 2, "after-recovery")}, hs: hsp(2, 3, 28)}
	applyBatch(t, w, b)
	require.NoError(t, w.Close())

	again, err := f.reopen(t)
	require.NoError(t, err)
	got := stateOf(t, again)
	require.Equal(t, *b.hs, got.HS)
	require.Equal(t, next.Snap, got.Snap)
	require.True(t, entriesEqual(append(next.Entries, b.entries...), got.Entries),
		"the write made after recovery did not survive a second recovery: %s", got)
}

func snapFiles(t *testing.T, dir string) []string {
	t.Helper()
	des, err := os.ReadDir(dir)
	require.NoError(t, err)
	var out []string
	for _, de := range des {
		out = append(out, de.Name())
	}
	return out
}

// TestCrashDuringSnapshot is phase 4 acceptance criterion 5.
//
// The snapshot write sequence is: snapshot file, then pointer record, then
// deletion of what the pointer supersedes. A crash is injected at each point
// between those steps -- and at the two points inside them where a crash can
// leave a partial result -- and every one must recover to either exactly the
// state before the snapshot or exactly the state after it, and then keep
// working.
func TestCrashDuringSnapshot(t *testing.T) {
	type tc struct {
		name   string
		stage  SnapshotStage
		damage func(t *testing.T, f *snapFixture) // what else the crash left behind
		want   bool                               // whether recovery should hold the snapshot
	}
	for _, c := range []tc{
		{
			name:  "between the snapshot file and the pointer",
			stage: SnapshotStageFileWritten,
			want:  false,
		},
		{
			name:  "between the pointer and deleting superseded segments",
			stage: SnapshotStagePointerWritten,
			want:  true,
		},
		{
			// A crash during the pointer write itself: the record is torn.
			// Nothing had been deleted yet, so the old segments are all there.
			name:  "mid-way through writing the pointer",
			stage: SnapshotStagePointerWritten,
			damage: func(t *testing.T, f *snapFixture) {
				segs, err := listSegments(f.dir)
				require.NoError(t, err)
				tail := filepath.Join(f.dir, segmentName(segs[len(segs)-1]))
				info, err := os.Stat(tail)
				require.NoError(t, err)
				require.NoError(t, os.Truncate(tail, info.Size()-3))
			},
			want: false,
		},
		{
			// A crash part-way through deleting: deletions are not ordered on
			// disk, so any subset may have happened. Remove one from the
			// middle, which leaves a gap.
			name:  "part-way through deleting superseded segments",
			stage: SnapshotStagePointerWritten,
			damage: func(t *testing.T, f *snapFixture) {
				segs, err := listSegments(f.dir)
				require.NoError(t, err)
				require.NoError(t, os.Remove(filepath.Join(f.dir, segmentName(segs[len(segs)/2]))))
			},
			want: true,
		},
		{
			name:  "mid-way through writing the snapshot file",
			stage: SnapshotStageFileWritten,
			damage: func(t *testing.T, f *snapFixture) {
				path := filepath.Join(f.opts.SnapDir, snapshotName(f.snap.Meta))
				require.NoError(t, os.Truncate(path, 4))
			},
			want: false,
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			f, w := newSnapFixture(t, c.stage)
			require.ErrorIs(t, w.SaveSnapshot(f.snap), ErrInjectedCrash)
			require.Error(t, w.Sync(), "a log that crashed must refuse further writes")
			require.NoError(t, w.Crash())
			if c.damage != nil {
				c.damage(t, f)
			}

			r, err := f.reopen(t)
			require.NoError(t, err)
			took, err := f.checkConsistent(t, r)
			require.NoError(t, err)
			require.Equal(t, c.want, took)

			rec := r.Recovery()
			if took {
				require.Equal(t, f.snap.Meta.Index, rec.SnapshotIndex)
				require.Positive(t, rec.SupersededSegments,
					"the segments the pointer supersedes must be reclaimed on recovery")
				require.Equal(t, []string{snapshotName(f.snap.Meta)}, snapFiles(t, f.opts.SnapDir))
			} else {
				require.Empty(t, snapFiles(t, f.opts.SnapDir),
					"a snapshot file no durable pointer names must be removed, not trusted")
			}
			f.keepsWorking(t, r)
		})
	}
}

// TestSnapshotCrashCheckCatchesMisorderedWrites is the negative control for
// TestCrashDuringSnapshot. It builds, by hand, the disk a snapshot would leave
// if it deleted superseded segments BEFORE its pointer was durable, and
// crashed in between. The consistency check must reject it: the log's start is
// gone and nothing names the snapshot that was supposed to replace it.
func TestSnapshotCrashCheckCatchesMisorderedWrites(t *testing.T) {
	f, w := newSnapFixture(t, SnapshotStageFileWritten)
	require.ErrorIs(t, w.SaveSnapshot(f.snap), ErrInjectedCrash)
	require.NoError(t, w.Crash())

	segs, err := listSegments(f.dir)
	require.NoError(t, err)
	for _, seq := range segs[:len(segs)-1] {
		require.NoError(t, os.Remove(filepath.Join(f.dir, segmentName(seq))))
	}

	r, err := f.reopen(t)
	if err == nil {
		_, err = f.checkConsistent(t, r)
	}
	require.Error(t, err, "deleting before the pointer is durable loses the log, and the check must say so")
	t.Logf("caught: %v", err)
}

// TestWALRefusesAMissingOrDamagedSnapshot: a durable pointer is only written
// after its file is durable, so a pointer naming a file that is missing or does
// not match is damage, not a crash artifact, and the node must not start.
func TestWALRefusesAMissingOrDamagedSnapshot(t *testing.T) {
	for _, c := range []struct {
		name   string
		damage func(path string) error
		want   string
	}{
		{"missing", os.Remove, "cannot be read"},
		{"a flipped byte", func(path string) error {
			b, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			b[3] ^= 0x40
			return os.WriteFile(path, b, 0o644)
		}, "damaged"},
	} {
		t.Run(c.name, func(t *testing.T) {
			f, w := newSnapFixture(t, SnapshotStageNone)
			require.NoError(t, w.SaveSnapshot(f.snap))
			require.NoError(t, w.Close())
			require.NoError(t, c.damage(filepath.Join(f.opts.SnapDir, snapshotName(f.snap.Meta))))

			_, err := f.reopen(t)
			require.ErrorContains(t, err, c.want)
		})
	}
}

// TestRefusedRecoveryReleasesTheLog: when recovery refuses to start, it must not
// keep any file open. It opens the tail segment before it has finished judging
// the log, and it used to return the refusal with that handle still held --
// see BUGS.md, 2026-09-30. Deleting the directory afterwards is the check,
// because Windows refuses to delete a file that is open; on platforms that
// allow it, this passes either way.
func TestRefusedRecoveryReleasesTheLog(t *testing.T) {
	f, w := newSnapFixture(t, SnapshotStageNone)
	require.NoError(t, w.SaveSnapshot(f.snap))
	require.NoError(t, w.Close())
	require.NoError(t, os.Remove(filepath.Join(f.opts.SnapDir, snapshotName(f.snap.Meta))))

	_, err := f.reopen(t)
	require.Error(t, err, "recovery must refuse a pointer whose snapshot is missing")
	require.NoError(t, os.RemoveAll(f.dir), "the refused log still holds a file open")
}

// TestWALRefusesAnUnsafeSnapshot: saving a snapshot not newer than the current
// one would rewrite a file a durable pointer may name, and saving one with
// writes still buffered would make it durable ahead of them.
func TestWALRefusesAnUnsafeSnapshot(t *testing.T) {
	f, w := newSnapFixture(t, SnapshotStageNone)
	require.NoError(t, w.SaveSnapshot(f.snap))
	require.ErrorContains(t, w.SaveSnapshot(f.snap), "not newer")

	require.NoError(t, w.Append([]raft.Entry{ent(31, 1, "buffered")}))
	later := raft.Snapshot{Meta: raft.SnapshotMeta{Index: 25, Term: 1}, Data: []byte("x")}
	require.ErrorIs(t, w.SaveSnapshot(later), errUnsyncedSnapshot)
}

// TestWALSnapshotReclaimsAndRecovers: a completed snapshot deletes every older
// segment and snapshot file, and the node recovers from what is left alone.
func TestWALSnapshotReclaimsAndRecovers(t *testing.T) {
	f, w := newSnapFixture(t, SnapshotStageNone)
	before, err := listSegments(f.dir)
	require.NoError(t, err)

	require.NoError(t, w.SaveSnapshot(f.snap))
	after, err := listSegments(f.dir)
	require.NoError(t, err)
	require.Equal(t, []uint64{before[len(before)-1] + 1}, after,
		"every segment older than the pointer's must be deleted")
	require.Equal(t, uint64(len(before)), w.Stats().SegmentsDeleted)
	require.Equal(t, uint64(2), w.Stats().SnapshotFsyncs, "one fsync for the file, one for the pointer")
	require.NoError(t, w.Close())

	r, err := f.reopen(t)
	require.NoError(t, err)
	took, err := f.checkConsistent(t, r)
	require.NoError(t, err)
	require.True(t, took)
	f.keepsWorking(t, r)
}

// TestWALAgreesWithMemStorageThroughSnapshots extends the phase 3 differential
// test to snapshots: random batches, overwrites, snapshots the node took
// itself and snapshots "received" whose last entry the log may not hold, with
// the real log closed and reopened from disk at random points. The simulator's
// storage and the real log must agree after every reopen, including on the
// snapshot's bytes -- otherwise the simulator's crash tests would be testing a
// different storage from the one that ships.
func TestWALAgreesWithMemStorageThroughSnapshots(t *testing.T) {
	for seed := range uint64(12) {
		rng := rand.New(rand.NewPCG(seed, 404))
		root := t.TempDir()
		opts := WALOptions{SegmentBytes: int64(200 + rng.IntN(1500)), SnapDir: filepath.Join(root, "snap")}
		dir := filepath.Join(root, "wal")
		w := openWAL(t, dir, opts)
		model := NewMem()

		var hs raft.HardState
		snaps := 0
		for step := range 120 {
			cur := stateOf(t, model)
			last := cur.Snap.Index + raft.Index(len(cur.Entries))
			switch r := rng.IntN(10); {
			case r < 6: // a batch
				var b batch
				from := last + 1
				if last > hs.Commit && hs.Commit >= cur.Snap.Index && rng.IntN(4) == 0 {
					from = hs.Commit + 1 + raft.Index(rng.IntN(int(last-hs.Commit)))
				}
				for i := range rng.IntN(4) + 1 {
					b.entries = append(b.entries, ent(from+raft.Index(i), hs.Term+1, fmt.Sprintf("%d-%d", seed, step)))
				}
				last = from + raft.Index(len(b.entries)) - 1
				if rng.IntN(2) == 0 {
					hs = raft.HardState{Term: hs.Term + raft.Term(rng.IntN(2)), VotedFor: raft.NodeID(rng.IntN(4)),
						Commit: max(hs.Commit, min(last, hs.Commit+raft.Index(rng.IntN(4))))}
					b.hs = &hs
				}
				applyBatch(t, w, b)
				applyBatch(t, model, b)

			case r < 8 && hs.Commit > cur.Snap.Index: // a local snapshot of committed state
				at := cur.Snap.Index + 1 + raft.Index(rng.IntN(int(hs.Commit-cur.Snap.Index)))
				term := cur.Entries[at-cur.Snap.Index-1].Term
				s := raft.Snapshot{Meta: raft.SnapshotMeta{Index: at, Term: term}, Data: []byte(fmt.Sprintf("img-%d", at))}
				require.NoError(t, w.SaveSnapshot(s))
				require.NoError(t, model.SaveSnapshot(s))
				snaps++

			case r < 9: // a snapshot from a leader, which may not match the log
				at := max(hs.Commit, cur.Snap.Index) + 1 + raft.Index(rng.IntN(5))
				s := raft.Snapshot{Meta: raft.SnapshotMeta{Index: at, Term: hs.Term + raft.Term(rng.IntN(2))},
					Data: []byte(fmt.Sprintf("installed-%d", at))}
				require.NoError(t, w.SaveSnapshot(s))
				require.NoError(t, model.SaveSnapshot(s))
				hs.Commit = max(hs.Commit, at)
				snaps++

			default: // restart
				require.NoError(t, w.Close())
				w = openWAL(t, dir, opts)
				require.Equalf(t, stateOf(t, model), stateOf(t, w), "seed %d step %d", seed, step)
			}
		}
		require.NoError(t, w.Close())
		reopened := openWAL(t, dir, opts)
		require.Equalf(t, stateOf(t, model), stateOf(t, reopened), "seed %d", seed)
		wantSnap, err := model.InitialState()
		require.NoError(t, err)
		gotSnap, err := reopened.InitialState()
		require.NoError(t, err)
		require.Equal(t, string(wantSnap.Snapshot.Data), string(gotSnap.Snapshot.Data))
		require.Positive(t, snaps, "seed %d took no snapshots, so it tested nothing new", seed)
	}
}

// TestSnapshotStagesAreNamed keeps fault-injection output readable.
func TestSnapshotStagesAreNamed(t *testing.T) {
	for _, s := range []SnapshotStage{SnapshotStageNone, SnapshotStageFileWritten, SnapshotStagePointerWritten} {
		require.NotContains(t, s.String(), "stage(")
	}
	require.True(t, errors.Is(fmt.Errorf("wrapped: %w", ErrInjectedCrash), ErrInjectedCrash))
}
