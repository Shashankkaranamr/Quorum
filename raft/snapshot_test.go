package raft_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/Shashankkaranamr/Quorum/raft"
)

func entries(from, to raft.Index, term raft.Term) []raft.Entry {
	var out []raft.Entry
	for i := from; i <= to; i++ {
		out = append(out, raft.Entry{Index: i, Term: term, Type: raft.EntryNormal})
	}
	return out
}

func chunk(meta raft.SnapshotMeta, off uint64, data string, done bool) raft.Message {
	return raft.Message{
		Type: raft.MsgInstallSnapshot, From: 1, To: 2, Term: 1,
		SnapshotMeta: meta, SnapshotOffset: off, SnapshotData: []byte(data), SnapshotDone: done,
	}
}

// drain takes one Ready, acknowledges it, and returns it.
func drain(n *raft.Node) raft.Ready {
	rd := n.Ready()
	n.Advance()
	return rd
}

// TestCompactRefusesWhatIsNotApplied: a snapshot is the state machine as of
// some index, so the log may only be folded up to what has been applied.
// Dropping an entry the state machine never saw would leave the node unable to
// reach the state its own snapshot claims to describe.
func TestCompactRefusesWhatIsNotApplied(t *testing.T) {
	cfg := baseConfig(1, 3)
	cfg.Entries = entries(1, 6, 1)
	cfg.HardState = raft.HardState{Term: 1, Commit: 5}
	cfg.Applied = 3
	n := newNode(t, cfg)

	_, err := n.Compact(4, []byte("image@4"))
	require.ErrorContains(t, err, "only 3 is applied")

	snap, err := n.Compact(3, []byte("image@3"))
	require.NoError(t, err)
	require.Equal(t, raft.SnapshotMeta{Index: 3, Term: 1}, snap.Meta)

	st := n.Status()
	require.Equal(t, raft.Index(3), st.SnapshotIndex)
	require.Equal(t, raft.Index(6), st.LastLogIndex, "compaction must not touch entries above the snapshot")
	log := n.LogEntries()
	require.Len(t, log, 3)
	require.Equal(t, raft.Index(4), log[0].Index)

	_, err = n.Compact(3, nil)
	require.ErrorContains(t, err, "already compacted", "compacting to the same point twice must be refused")
}

// TestSnapshotChunksReassembleExactly: a snapshot sent in chunks, with a
// duplicate and a chunk from beyond a gap mixed in, must be reassembled
// byte-for-byte, and each out-of-place chunk answered with where the follower
// actually is so the leader resumes from there.
func TestSnapshotChunksReassembleExactly(t *testing.T) {
	n := newNode(t, baseConfig(2, 3))
	meta := raft.SnapshotMeta{Index: 9, Term: 1}

	step := func(m raft.Message) raft.Message {
		t.Helper()
		require.NoError(t, n.Step(m))
		rd := drain(n)
		require.Len(t, rd.Messages, 1)
		return rd.Messages[0]
	}

	resp := step(chunk(meta, 0, "abcd", false))
	require.Equal(t, raft.MsgInstallSnapshotResp, resp.Type)
	require.Equal(t, uint64(4), resp.SnapshotBytesReceived)

	resp = step(chunk(meta, 0, "abcd", false)) // duplicate
	require.Equal(t, uint64(4), resp.SnapshotBytesReceived, "a duplicate chunk must not be appended twice")

	resp = step(chunk(meta, 8, "ij", true)) // beyond a gap
	require.Equal(t, uint64(4), resp.SnapshotBytesReceived, "a chunk past a gap must be refused")

	resp = step(chunk(meta, 4, "efgh", false))
	require.Equal(t, uint64(8), resp.SnapshotBytesReceived)

	require.NoError(t, n.Step(chunk(meta, 8, "ij", true)))
	rd := n.Ready()
	require.NotNil(t, rd.Snapshot, "the final chunk must hand the snapshot to the driver to persist")
	require.Equal(t, "abcdefghij", string(rd.Snapshot.Data))
	require.Equal(t, meta, rd.Snapshot.Meta)
	require.Len(t, rd.Messages, 1)
	require.Equal(t, raft.MsgAppendEntriesResp, rd.Messages[0].Type,
		"the final chunk is acknowledged as an AppendEntries success, so replication resumes")
	require.True(t, rd.Messages[0].Success)
	require.Equal(t, raft.Index(9), rd.Messages[0].MatchIndex)
	require.Empty(t, rd.CommittedEntries, "nothing below the snapshot may be applied as entries")
	n.Advance()

	st := n.Status()
	require.Equal(t, raft.Index(9), st.SnapshotIndex)
	require.Equal(t, raft.Index(9), st.CommitIndex)
	require.Equal(t, raft.Index(9), st.LastApplied)
}

// TestInstalledSnapshotKeepsOnlyAMatchingSuffix is Figure 13's rule: entries
// after the snapshot survive only if the log holds the snapshot's last entry
// with the same term. Otherwise nothing in the log can be shown to agree with
// the leader's, and all of it goes.
func TestInstalledSnapshotKeepsOnlyAMatchingSuffix(t *testing.T) {
	for _, tc := range []struct {
		name     string
		meta     raft.SnapshotMeta
		wantLast raft.Index
	}{
		{"matching entry at the snapshot index", raft.SnapshotMeta{Index: 4, Term: 1}, 6},
		{"same index, different term", raft.SnapshotMeta{Index: 4, Term: 2}, 4},
		{"snapshot beyond the log", raft.SnapshotMeta{Index: 8, Term: 1}, 8},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := baseConfig(2, 3)
			cfg.Entries = entries(1, 6, 1)
			n := newNode(t, cfg)

			msg := chunk(tc.meta, 0, "img", true)
			msg.Term = 2
			require.NoError(t, n.Step(msg))
			rd := drain(n)
			require.NotNil(t, rd.Snapshot)

			st := n.Status()
			require.Equal(t, tc.wantLast, st.LastLogIndex)
			require.Equal(t, tc.meta.Index, st.SnapshotIndex)
			if tc.wantLast > tc.meta.Index {
				require.Equal(t, entries(5, 6, 1), n.LogEntries())
			} else {
				require.Empty(t, n.LogEntries())
			}
		})
	}
}

// TestStaleSnapshotIsNotInstalled: a snapshot that does not reach past the
// follower's commit index would move it backwards. It must be ignored, and
// answered with the commit index so the leader moves on.
func TestStaleSnapshotIsNotInstalled(t *testing.T) {
	cfg := baseConfig(2, 3)
	cfg.Entries = entries(1, 5, 1)
	cfg.HardState = raft.HardState{Term: 1, Commit: 5}
	cfg.Applied = 5
	n := newNode(t, cfg)

	require.NoError(t, n.Step(chunk(raft.SnapshotMeta{Index: 3, Term: 1}, 0, "old", true)))
	rd := drain(n)
	require.Nil(t, rd.Snapshot)
	require.Len(t, rd.Messages, 1)
	require.Equal(t, raft.MsgAppendEntriesResp, rd.Messages[0].Type)
	require.True(t, rd.Messages[0].Success)
	require.Equal(t, raft.Index(5), rd.Messages[0].MatchIndex)
	require.Equal(t, raft.Index(0), n.Status().SnapshotIndex)
	require.Len(t, n.LogEntries(), 5)
}

// TestRestoredLogMustContinueFromTheSnapshot: a restart that hands the core a
// snapshot and a log that do not join is a storage bug, and must be refused at
// construction rather than surface later as a mysterious gap.
func TestRestoredLogMustContinueFromTheSnapshot(t *testing.T) {
	cfg := baseConfig(1, 3)
	cfg.Snapshot = raft.Snapshot{Meta: raft.SnapshotMeta{Index: 5, Term: 1}}
	cfg.Entries = entries(7, 8, 1)
	_, err := raft.New(cfg)
	require.ErrorContains(t, err, "must continue directly from the snapshot")

	cfg.Entries = entries(6, 8, 1)
	n := newNode(t, cfg)
	st := n.Status()
	require.Equal(t, raft.Index(5), st.CommitIndex, "everything in a snapshot is committed")
	require.Equal(t, raft.Index(5), st.LastApplied, "and applied: the state machine is restored from it")
	require.Equal(t, raft.Index(8), st.LastLogIndex)
}
