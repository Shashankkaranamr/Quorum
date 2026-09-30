package testutil_test

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	kvv1 "github.com/Shashankkaranamr/Quorum/gen/quorum/kv/v1"
	"github.com/Shashankkaranamr/Quorum/internal/server"
	"github.com/Shashankkaranamr/Quorum/internal/statemachine"
	"github.com/Shashankkaranamr/Quorum/internal/storage"
	"github.com/Shashankkaranamr/Quorum/internal/testutil"
	"github.com/Shashankkaranamr/Quorum/internal/transport"
	"github.com/Shashankkaranamr/Quorum/internal/transport/inmem"
	"github.com/Shashankkaranamr/Quorum/raft"
)

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

// sendCounter counts every message handed to the transport, by sender,
// receiver and type. It is how a test proves a message was actually SENT,
// rather than inferring it from the receiver's eventual state.
type sendCounter struct {
	counts map[[2]raft.NodeID]map[raft.MessageType]int
}

func countSends(opts *testutil.Options) *sendCounter {
	sc := &sendCounter{counts: map[[2]raft.NodeID]map[raft.MessageType]int{}}
	prev := opts.WrapTransport
	opts.WrapTransport = func(id raft.NodeID, t transport.Transport) transport.Transport {
		if prev != nil {
			t = prev(id, t)
		}
		return transport.SendFunc(func(msgs []raft.Message) {
			for _, m := range msgs {
				k := [2]raft.NodeID{m.From, m.To}
				if sc.counts[k] == nil {
					sc.counts[k] = map[raft.MessageType]int{}
				}
				sc.counts[k][m.Type]++
			}
			t.Send(msgs)
		})
	}
	return sc
}

// to counts messages of one type sent to a node by anyone.
func (sc *sendCounter) to(id raft.NodeID, typ raft.MessageType) int {
	total := 0
	for k, byType := range sc.counts {
		if k[1] == id {
			total += byType[typ]
		}
	}
	return total
}

// waitApplied runs until every live node has applied index i.
func waitApplied(t *testing.T, c *testutil.Cluster, i raft.Index) {
	t.Helper()
	ok, _ := c.RunUntil(4*electionBoundTicks, func() bool {
		for _, id := range c.LiveIDs() {
			if c.Status(id).LastApplied < i {
				return false
			}
		}
		return true
	})
	require.True(t, ok, "not every node applied index %d:\n%s", i, c)
}

// proposeApplied proposes through the current leader and waits for every live
// node to apply it. One entry in flight at a time, like a client with one
// outstanding request.
func proposeApplied(t *testing.T, c *testutil.Cluster, typ raft.EntryType, data []byte) raft.Index {
	t.Helper()
	lead, ok := c.Leader()
	require.True(t, ok, "no leader:\n%s", c)
	idx, _, err := c.ProposeType(lead, typ, data)
	require.NoError(t, err)
	waitApplied(t, c, idx)
	return idx
}

// proposeMany proposes n distinct entries one at a time and returns the last
// index.
func proposeMany(t *testing.T, c *testutil.Cluster, n int, tag string) raft.Index {
	t.Helper()
	var last raft.Index
	for i := range n {
		last = proposeApplied(t, c, raft.EntryNormal, []byte(fmt.Sprintf("%s-%d", tag, i)))
	}
	return last
}

func hashOf(t *testing.T, c *testutil.Cluster, id raft.NodeID) [32]byte {
	t.Helper()
	h, ok := c.Replicas[id].SM.Inner.(testutil.Hasher)
	require.True(t, ok, "node %d's state machine cannot hash itself", id)
	sum, err := h.Hash()
	require.NoError(t, err)
	return sum
}

func nodeDir(root string, id raft.NodeID) string {
	return filepath.Join(root, fmt.Sprintf("node-%d", id))
}

func snapFilesOf(t *testing.T, root string, id raft.NodeID) []string {
	t.Helper()
	des, err := os.ReadDir(filepath.Join(nodeDir(root, id), "snap"))
	require.NoError(t, err)
	var out []string
	for _, de := range des {
		if strings.HasSuffix(de.Name(), ".snap") {
			out = append(out, de.Name())
		}
	}
	return out
}

// kvFleet gives every node a real key-value state machine and keeps hold of
// the current one, and every result it produced, so a test can inspect them.
type kvFleet struct {
	machines map[raft.NodeID]*statemachine.KV
	results  map[raft.NodeID]map[raft.Index]statemachine.Result

	// wrap, if set, interposes on each machine. The negative controls use it
	// to break snapshotting in one specific way.
	wrap func(*statemachine.KV) server.StateMachine
}

func newKVFleet(opts *testutil.Options, wrap func(*statemachine.KV) server.StateMachine) *kvFleet {
	f := &kvFleet{
		machines: map[raft.NodeID]*statemachine.KV{},
		results:  map[raft.NodeID]map[raft.Index]statemachine.Result{},
		wrap:     wrap,
	}
	opts.StateMachine = func(id raft.NodeID) server.StateMachine {
		kv := statemachine.New()
		if f.results[id] == nil {
			f.results[id] = map[raft.Index]statemachine.Result{}
		}
		kv.OnApply = func(r statemachine.Result) { f.results[id][r.Index] = r }
		f.machines[id] = kv
		if f.wrap != nil {
			return f.wrap(kv)
		}
		return kv
	}
	return f
}

func diskOptions(t *testing.T, n int, seed, threshold uint64) (testutil.Options, string) {
	t.Helper()
	root := t.TempDir()
	opts := testutil.DefaultOptions(n, seed)
	opts.SnapshotThreshold = threshold
	opts.Storage = testutil.WALStorageFactory(root, storage.WALOptions{SegmentBytes: 1 << 10})
	return opts, root
}

func crashAndRestartAll(t *testing.T, c *testutil.Cluster) {
	t.Helper()
	for _, id := range c.IDs {
		c.Crash(id)
	}
	for _, id := range c.IDs {
		require.NoError(t, c.Restart(id))
	}
}

// ---------------------------------------------------------------------------
// Criterion 1
// ---------------------------------------------------------------------------

// TestSnapshotAtThreshold is phase 4 acceptance criterion 1: crossing the
// snapshot threshold produces a snapshot file on disk and DELETES the
// write-ahead log segments it supersedes. Below the threshold, neither happens.
func TestSnapshotAtThreshold(t *testing.T) {
	const threshold = 40
	opts, root := diskOptions(t, 3, 1, threshold)
	c := newDiskCluster(t, opts)
	electLeader(t, c)

	firstSegment := func(id raft.NodeID) string {
		return filepath.Join(nodeDir(root, id), "wal", "000001.log")
	}

	proposeMany(t, c, threshold-10, "below")
	for _, id := range c.IDs {
		require.Zero(t, c.Status(id).SnapshotIndex, "node %d compacted below the threshold", id)
		require.Empty(t, snapFilesOf(t, root, id))
		require.FileExists(t, firstSegment(id))
	}

	proposeMany(t, c, 15, "above")
	for _, id := range c.IDs {
		st := c.Status(id)
		require.GreaterOrEqual(t, st.SnapshotIndex, raft.Index(threshold),
			"node %d applied %d entries without compacting", id, st.LastApplied)

		files := snapFilesOf(t, root, id)
		require.Len(t, files, 1, "node %d should hold exactly its latest snapshot", id)
		require.True(t, strings.HasPrefix(files[0], fmt.Sprintf("%020d-", st.SnapshotIndex)),
			"node %d's snapshot file %s does not match its snapshot index %d", id, files[0], st.SnapshotIndex)

		require.NoFileExists(t, firstSegment(id), "node %d kept the segment its snapshot supersedes", id)
		wal := c.Replicas[id].Store.(*storage.WAL)
		require.Positive(t, wal.Stats().SegmentsDeleted)
		t.Logf("node %d: snapshot at %d, %d segments deleted", id, st.SnapshotIndex, wal.Stats().SegmentsDeleted)
	}
	require.NoError(t, c.Err())
}

// ---------------------------------------------------------------------------
// Criterion 2
// ---------------------------------------------------------------------------

// restoreScenario builds a key-value history long enough to be compacted, with
// a tail of entries after the snapshot, then crashes every node and restarts
// it. Each node is isolated so that nothing new commits, and allowed exactly
// one step, in which it replays the committed tail from its log onto the
// restored snapshot. It returns an error if any node's state then differs from
// what it was before the crash.
func restoreScenario(t *testing.T, wrap func(*statemachine.KV) server.StateMachine) error {
	opts, _ := diskOptions(t, 3, 2, 30)
	fleet := newKVFleet(&opts, wrap)
	c := newDiskCluster(t, opts)
	electLeader(t, c)

	// The first 25 writes each go to their own key and are never touched
	// again, so they live only in the snapshot. The rest churn a few other
	// keys and form the log tail. If the tail rewrote every key, a restore
	// that lost data would be silently repaired by the replay and this check
	// could not fail -- which is how its first version behaved; see BUGS.md,
	// 2026-09-30.
	client := uint64(proposeApplied(t, c, raft.EntrySession, statemachine.EncodeRegister([]byte("r"))))
	for seq := range uint64(45) {
		key := fmt.Sprintf("key-%02d", seq)
		if seq >= 25 {
			key = fmt.Sprintf("hot-%d", seq%4)
		}
		data := statemachine.EncodePut(client, seq+1, key, []byte(fmt.Sprintf("v%d", seq)))
		if seq >= 25 && seq%5 == 0 {
			data = statemachine.EncodeDelete(client, seq+1, key)
		}
		proposeApplied(t, c, raft.EntryNormal, data)
	}

	type state struct {
		applied, snap raft.Index
		hash          [32]byte
	}
	before := map[raft.NodeID]state{}
	for _, id := range c.IDs {
		st := c.Status(id)
		h, err := fleet.machines[id].Hash()
		require.NoError(t, err)
		before[id] = state{applied: st.LastApplied, snap: st.SnapshotIndex, hash: h}
		require.Positive(t, st.SnapshotIndex, "node %d never compacted", id)
		require.Greater(t, st.LastApplied, st.SnapshotIndex,
			"node %d has no log tail after its snapshot, so this would test the snapshot alone", id)
	}

	crashAndRestartAll(t, c)
	c.Partition([]raft.NodeID{1}, []raft.NodeID{2}, []raft.NodeID{3})
	c.Tick()

	for _, id := range c.IDs {
		sm := c.Replicas[id].SM
		require.Equal(t, 1, sm.Restores, "node %d did not restart from its snapshot", id)
		require.Equal(t, before[id].snap, sm.RestoredAt)
		require.Equal(t, before[id].applied, c.Status(id).LastApplied,
			"node %d did not replay its committed tail in one step", id)

		h, err := fleet.machines[id].Hash()
		require.NoError(t, err)
		if h != before[id].hash {
			b := before[id].hash
			return fmt.Errorf("node %d: state hash %x before the restart, %x after restoring snapshot %d "+
				"and replaying to %d", id, b[:8], h[:8], before[id].snap, before[id].applied)
		}
	}
	return nil
}

// TestSnapshotRestoreIsIdentical is phase 4 acceptance criterion 2: restarting
// from snapshot plus write-ahead log tail reproduces the state machine exactly,
// sessions included, as a hash over its complete state.
func TestSnapshotRestoreIsIdentical(t *testing.T) {
	require.NoError(t, restoreScenario(t, nil))
}

// lossyRestore restores all but the last key. It stands in for a snapshot
// encoding or restore path that silently drops data.
type lossyRestore struct{ *statemachine.KV }

func (l lossyRestore) Restore(snap raft.Snapshot) error {
	var img kvv1.StateMachineSnapshot
	if err := proto.Unmarshal(snap.Data, &img); err != nil {
		return err
	}
	if n := len(img.Data); n > 0 {
		img.Data = img.Data[:n-1]
	}
	b, err := proto.Marshal(&img)
	if err != nil {
		return err
	}
	return l.KV.Restore(raft.Snapshot{Meta: snap.Meta, Data: b})
}

// TestRestoreCheckCatchesALossyRestore is the negative control for criterion
// 2: a restore that loses a single key must be caught by the hash comparison.
func TestRestoreCheckCatchesALossyRestore(t *testing.T) {
	err := restoreScenario(t, func(kv *statemachine.KV) server.StateMachine { return lossyRestore{kv} })
	require.Error(t, err, "a restore that dropped a key went unnoticed")
	t.Logf("caught: %v", err)
}

// ---------------------------------------------------------------------------
// Criterion 3
// ---------------------------------------------------------------------------

// farBehind isolates a follower, drives the rest of the cluster until the
// leader has compacted past everything the follower holds, heals, and waits
// for the follower to catch up. It returns the follower.
func farBehind(t *testing.T, c *testutil.Cluster) raft.NodeID {
	t.Helper()
	lead := electLeader(t, c)
	f := c.IDs[0]
	if f == lead {
		f = c.IDs[1]
	}
	c.Isolate(f)
	behind := c.Status(f).LastLogIndex

	others := []raft.NodeID{}
	for _, id := range c.IDs {
		if id != f {
			others = append(others, id)
		}
	}
	payload := 0
	ok, _ := c.RunUntil(5000, func() bool {
		l, ok := c.LeaderIn(others)
		if !ok {
			return false
		}
		if c.Status(l).SnapshotIndex > behind+1 {
			return true
		}
		payload++
		_, _, _ = c.Propose(l, []byte(fmt.Sprintf("while-%d-was-away-%d", f, payload)))
		return false
	})
	require.True(t, ok, "the leader never compacted past node %d's log:\n%s", f, c)

	c.Heal()
	ok, _ = c.RunUntil(20*electionBoundTicks, func() bool {
		target := c.MinCommitIndex(others)
		return c.Status(f).LastApplied >= target && target > behind+1
	})
	require.True(t, ok, "node %d never caught up:\n%s", f, c)
	return f
}

// TestFarBehindFollowerGetsSnapshot is phase 4 acceptance criterion 3. It
// asserts that InstallSnapshot messages were actually SENT to the follower,
// and that the follower actually installed one -- not merely that it
// eventually converged, which it could in principle have done some other way.
//
// The chunk size is set below the snapshot's size so the transfer takes
// several chunks, and the lossy variant makes chunks and acknowledgements get
// lost, duplicated and reordered on the way.
func TestFarBehindFollowerGetsSnapshot(t *testing.T) {
	for _, tc := range []struct {
		name  string
		fault inmem.Fault
	}{
		{"clean network", inmem.Fault{}},
		{"lossy network", inmem.Fault{DropRate: 0.1, DuplicateRate: 0.1, MaxDelayTicks: 2, ReorderDueBatch: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opts := testutil.DefaultOptions(3, 11)
			opts.Fault = tc.fault
			opts.SnapshotThreshold = 20
			opts.MaxSnapshotChunkBytes = 16 // a Digest snapshot is 40 bytes: three chunks
			sends := countSends(&opts)
			c := newCluster(t, opts)

			f := farBehind(t, c)

			chunks := sends.to(f, raft.MsgInstallSnapshot)
			require.GreaterOrEqual(t, chunks, 3,
				"node %d caught up, but fewer InstallSnapshot chunks were sent to it than one snapshot needs", f)
			require.Positive(t, c.Replicas[f].Driver.Metrics().SnapshotsInstalled)
			require.Positive(t, c.Replicas[f].SM.Restores, "node %d's state machine was never restored", f)
			t.Logf("node %d: %d InstallSnapshot chunks sent to it, %d acknowledgements sent back",
				f, chunks, sends.counts[[2]raft.NodeID{f, mustLeader(t, c)}][raft.MsgInstallSnapshotResp])

			// Converged state, not just converged indices.
			c.RunTicks(50)
			applied := c.Status(f).LastApplied
			for _, id := range c.IDs {
				require.Equal(t, applied, c.Status(id).LastApplied)
				require.Equal(t, hashOf(t, c, f), hashOf(t, c, id), "node %d and %d differ at %d", f, id, applied)
			}
			require.NoError(t, c.Err())
		})
	}
}

func mustLeader(t *testing.T, c *testutil.Cluster) raft.NodeID {
	t.Helper()
	id, ok := c.Leader()
	require.True(t, ok)
	return id
}

// TestSnapshotCountIsZeroForANearbyFollower is the negative control for
// criterion 3: a follower that falls behind by less than the leader has
// compacted catches up by AppendEntries, and the counter must stay at zero. If
// it did not, the assertion above would be counting something other than
// snapshot transfer.
func TestSnapshotCountIsZeroForANearbyFollower(t *testing.T) {
	opts := testutil.DefaultOptions(3, 11)
	opts.SnapshotThreshold = 20
	sends := countSends(&opts)
	c := newCluster(t, opts)
	lead := electLeader(t, c)
	f := c.IDs[0]
	if f == lead {
		f = c.IDs[1]
	}

	c.Isolate(f)
	for i := range 5 {
		_, _, err := c.Propose(lead, []byte(fmt.Sprintf("short-%d", i)))
		require.NoError(t, err)
		c.RunTicks(5)
	}
	require.Zero(t, c.Status(lead).SnapshotIndex)
	c.Heal()
	waitApplied(t, c, c.Status(lead).CommitIndex)

	require.Zero(t, sends.to(f, raft.MsgInstallSnapshot))
	require.Zero(t, c.Replicas[f].SM.Restores)
	require.NoError(t, c.Err())
}

// corruptingRestore flips one bit of every snapshot it restores. It stands in
// for a transfer or restore that corrupts data.
type corruptingRestore struct{ *testutil.Digest }

func (c corruptingRestore) Restore(snap raft.Snapshot) error {
	data := bytes.Clone(snap.Data)
	data[len(data)-1] ^= 0x01
	return c.Digest.Restore(raft.Snapshot{Meta: snap.Meta, Data: data})
}

// TestSnapshotFidelityCatchesACorruptTransfer is the negative control for the
// SnapshotFidelity checker: in the criterion-3 scenario, a follower whose
// restore corrupts one bit must be reported, and by that checker.
func TestSnapshotFidelityCatchesACorruptTransfer(t *testing.T) {
	opts := testutil.DefaultOptions(3, 11)
	opts.SnapshotThreshold = 20
	opts.StateMachine = func(raft.NodeID) server.StateMachine { return corruptingRestore{&testutil.Digest{}} }
	c := newCluster(t, opts)
	farBehind(t, c)

	require.True(t, c.Checker.Violated(testutil.SnapshotFidelity), "violations: %v", c.Checker.Violations())
	for _, v := range c.Checker.Violations() {
		require.Equal(t, testutil.SnapshotFidelity, v.Invariant, "a corrupt restore should trip only SnapshotFidelity")
	}
	t.Logf("caught: %s", c.Checker.Violations()[0])
}

// ---------------------------------------------------------------------------
// Criterion 4
// ---------------------------------------------------------------------------

// sessionScenario is phase 4 acceptance criterion 4 as a function, so the
// negative control can run it against a broken snapshot. A client writes, the
// write is compacted into every node's snapshot, the whole cluster restarts
// from those snapshots, and the client retries its last write with the same
// seq. Every node must recognize the retry from the session table it restored,
// and apply nothing.
func sessionScenario(t *testing.T, wrap func(*statemachine.KV) server.StateMachine) error {
	opts, _ := diskOptions(t, 3, 3, 10)
	fleet := newKVFleet(&opts, wrap)
	c := newDiskCluster(t, opts)
	electLeader(t, c)

	client := uint64(proposeApplied(t, c, raft.EntrySession, statemachine.EncodeRegister([]byte("a"))))
	var lastPut raft.Index
	for seq := uint64(1); seq <= 5; seq++ {
		lastPut = proposeApplied(t, c, raft.EntryNormal,
			statemachine.EncodePut(client, seq, "k", []byte(fmt.Sprintf("v%d", seq))))
	}
	// Another client's traffic pushes the compaction point past that write,
	// so the only record that it happened is in the snapshot's session table.
	other := uint64(proposeApplied(t, c, raft.EntrySession, statemachine.EncodeRegister([]byte("b"))))
	for seq := uint64(1); seq <= 15; seq++ {
		proposeApplied(t, c, raft.EntryNormal, statemachine.EncodePut(other, seq, "noise", []byte("x")))
	}
	for _, id := range c.IDs {
		require.Greater(t, c.Status(id).SnapshotIndex, lastPut,
			"node %d has not compacted past the write, so its log would still hold it", id)
	}

	crashAndRestartAll(t, c)
	for _, id := range c.IDs {
		require.Greater(t, c.Replicas[id].SM.RestoredAt, lastPut, "node %d did not restart from a snapshot", id)
	}
	electLeader(t, c)

	retry := proposeApplied(t, c, raft.EntryNormal, statemachine.EncodePut(client, 5, "k", []byte("RETRY")))
	for _, id := range c.IDs {
		res, ok := fleet.results[id][retry]
		require.True(t, ok, "node %d has no result for the retry at %d", id, retry)
		if !res.Duplicate {
			return fmt.Errorf("node %d applied the retry of (client %d, seq 5) at index %d as new "+
				"(status %s): its restored session table did not know the write", id, client, retry, res.Status)
		}
		if res.AppliedIndex != lastPut {
			return fmt.Errorf("node %d answered the retry with applied index %d, but the write applied at %d",
				id, res.AppliedIndex, lastPut)
		}
		if v, _ := fleet.machines[id].Get("k"); string(v) != "v5" {
			return fmt.Errorf("node %d holds k=%q after the retry, want %q", id, v, "v5")
		}
	}
	return nil
}

// TestSessionsSurviveSnapshot is phase 4 acceptance criterion 4.
func TestSessionsSurviveSnapshot(t *testing.T) {
	require.NoError(t, sessionScenario(t, nil))
}

// sessionlessSnapshot captures the key-value data but not the session table:
// what a snapshot would be if the sessions lived in process memory.
type sessionlessSnapshot struct{ *statemachine.KV }

func (s sessionlessSnapshot) Snapshot() ([]byte, raft.Index, error) {
	data, at, err := s.KV.Snapshot()
	if err != nil {
		return nil, 0, err
	}
	var img kvv1.StateMachineSnapshot
	if err := proto.Unmarshal(data, &img); err != nil {
		return nil, 0, err
	}
	img.Sessions = nil
	data, err = proto.Marshal(&img)
	return data, at, err
}

// TestSessionCheckCatchesASessionlessSnapshot is the negative control for
// criterion 4: with the session table left out of the snapshot, the retry is
// applied as a new write, and the scenario must say so.
func TestSessionCheckCatchesASessionlessSnapshot(t *testing.T) {
	err := sessionScenario(t, func(kv *statemachine.KV) server.StateMachine { return sessionlessSnapshot{kv} })
	require.Error(t, err, "a snapshot without sessions let a retry through unnoticed")
	t.Logf("caught: %v", err)
}

// ---------------------------------------------------------------------------
// Criterion 6
// ---------------------------------------------------------------------------

// TestWipedNodeCatchesUp is phase 4 acceptance criterion 6: a node whose data
// directory is deleted outright comes back empty and is brought up to date by
// snapshot transfer, ending with exactly the others' state.
//
// A caveat that belongs next to the test rather than buried: a wiped node has
// forgotten its vote, so in general it could vote a second time in a term it
// already voted in. That is safe here only because no election is in progress
// when it returns -- its old vote was for a term already decided. Real
// deployments treat a wiped node as a new member (DESIGN.md §9: membership
// changes are out of scope), and this test does not claim otherwise.
func TestWipedNodeCatchesUp(t *testing.T) {
	opts, root := diskOptions(t, 3, 4, 20)
	opts.MaxSnapshotChunkBytes = 16
	sends := countSends(&opts)
	c := newDiskCluster(t, opts)
	lead := electLeader(t, c)
	proposeMany(t, c, 60, "before")

	f := c.IDs[0]
	if f == lead {
		f = c.IDs[1]
	}
	c.Crash(f)
	require.NoError(t, os.RemoveAll(nodeDir(root, f)))
	require.NoDirExists(t, nodeDir(root, f))

	proposeMany(t, c, 30, "while-wiped")
	before := sends.to(f, raft.MsgInstallSnapshot)

	require.NoError(t, c.Restart(f))
	st := c.Status(f)
	require.Zero(t, st.LastLogIndex, "a wiped node must come back with nothing")
	require.Zero(t, st.Term)

	target := c.Status(lead).CommitIndex
	ok, _ := c.RunUntil(20*electionBoundTicks, func() bool { return c.Status(f).LastApplied >= target })
	require.True(t, ok, "wiped node %d never caught up to %d:\n%s", f, target, c)

	require.Greater(t, sends.to(f, raft.MsgInstallSnapshot), before, "node %d caught up without a snapshot", f)
	require.Positive(t, c.Replicas[f].SM.Restores)
	require.NotEmpty(t, snapFilesOf(t, root, f), "the wiped node did not persist the snapshot it received")

	c.RunTicks(50)
	for _, id := range c.IDs {
		require.Equal(t, c.Status(lead).LastApplied, c.Status(id).LastApplied)
		require.Equal(t, hashOf(t, c, lead), hashOf(t, c, id), "node %d does not match the leader", id)
	}
	require.NoError(t, c.Err())
}

// ---------------------------------------------------------------------------
// Everything at once
// ---------------------------------------------------------------------------

// TestRandomizedTrialsUpholdSafetyWithSnapshots runs the phase 2 randomized
// safety trials with compaction switched on and a tiny chunk size, so that
// snapshots are taken constantly and transferred in pieces through message
// loss, duplication, reordering, partitions and crashes. Every invariant --
// now including SnapshotFidelity -- is checked after every tick.
func TestRandomizedTrialsUpholdSafetyWithSnapshots(t *testing.T) {
	type variant struct {
		name   string
		trials int
		disk   bool
	}
	variants := []variant{{"mem", 60, false}, {"wal", 6, true}}
	if testing.Short() {
		variants = []variant{{"mem", 10, false}, {"wal", 2, true}}
	}
	for _, v := range variants {
		for _, n := range []int{3, 5} {
			t.Run(fmt.Sprintf("%s/n=%d", v.name, n), func(t *testing.T) {
				var committed raft.Index
				var taken, installed uint64
				for i := range v.trials {
					seed := uint64(i)*2654435761 + uint64(n) + 2_000_000
					st := runRandomizedTrial(t, n, seed, func(o *testutil.Options) {
						o.SnapshotThreshold = 15
						o.MaxSnapshotChunkBytes = 16
						if v.disk {
							o.Storage = testutil.WALStorageFactory(t.TempDir(), storage.WALOptions{SegmentBytes: 2 << 10})
						}
					})
					committed += st.committed
					taken += st.snapshotsTaken
					installed += st.snapshotsInstalled
				}
				t.Logf("%s n=%d: %d trials, %d committed, %d snapshots taken, %d installed from a leader",
					v.name, n, v.trials, committed, taken, installed)
				require.Positive(t, taken, "no trial compacted, so this tested nothing new")
				require.Positive(t, installed, "no trial transferred a snapshot, so this tested nothing new")
			})
		}
	}
}
