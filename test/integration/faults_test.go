package integration

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	adminv1 "github.com/Shashankkaranamr/Quorum/gen/quorum/admin/v1"
	"github.com/Shashankkaranamr/Quorum/internal/client"
	"github.com/Shashankkaranamr/Quorum/raft"
)

// put writes through cl, failing the test on error.
func put(t *testing.T, cl *client.Client, key, value string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := cl.Put(ctx, key, []byte(value))
	require.NoError(t, err)
}

func blockedPeers(st *adminv1.NodeStatus) []uint64 {
	var out []uint64
	for _, p := range st.GetPeers() {
		if p.GetBlockedByInjection() {
			out = append(out, p.GetNodeId())
		}
	}
	return out
}

// TestHarnessInflictsRealFaults is phase 6 acceptance criterion 1. Each fault
// the harness can inflict is applied to real processes and its effect checked
// directly -- not inferred from the cluster eventually recovering.
func TestHarnessInflictsRealFaults(t *testing.T) {
	c := startCluster(t, 3)
	lead, _ := c.waitLeader(electionBound)
	cl := c.client(client.Config{})
	put(t, cl, "seed", "1")

	t.Run("kill leaves no process, restart recovers the same data directory", func(t *testing.T) {
		f := c.others(c.leader())[0]
		before := c.mustStatus(f)
		c.kill(f) // asserts the PID is gone
		_, err := c.status(f)
		require.Error(t, err, "a killed node answered its admin API")

		c.start(f)
		after := c.mustStatus(f)
		require.GreaterOrEqual(t, after.GetLastLogIndex(), before.GetLastLogIndex(),
			"the restarted node came back without the log it had on disk")
		require.GreaterOrEqual(t, after.GetTerm(), before.GetTerm())
	})

	t.Run("a bidirectional partition stops one node's progress, and heals", func(t *testing.T) {
		lead = c.leader()
		f := c.others(lead)[0]
		rest := c.others(f)
		c.partition([]raft.NodeID{f}, rest)
		require.ElementsMatch(t, []uint64{uint64(rest[0]), uint64(rest[1])}, blockedPeers(c.mustStatus(f)))

		frozenAt := c.mustStatus(f).GetCommitIndex()
		for i := range 5 {
			put(t, cl, fmt.Sprintf("p%d", i), "v")
		}
		time.Sleep(300 * time.Millisecond)
		require.Equal(t, frozenAt, c.mustStatus(f).GetCommitIndex(), "a partitioned node learned of new commits")

		c.heal()
		require.Empty(t, blockedPeers(c.mustStatus(f)))
		target := c.mustStatus(c.leader(rest...)).GetCommitIndex()
		c.waitCaughtUp(target, 10*time.Second, f)
	})

	t.Run("a one-way partition cuts one direction only", func(t *testing.T) {
		lead, _ = c.waitLeader(electionBound)
		rest := c.others(lead)
		oldTerm := c.mustStatus(lead).GetTerm()
		// The leader can no longer be heard, but can still hear.
		c.block(lead, rest, false, true)
		for _, f := range rest {
			c.block(f, []raft.NodeID{lead}, true, false)
		}

		newLead, took := c.waitLeader(electionBound, rest...)
		t.Logf("followers elected node %d %s after they stopped hearing node %d", newLead, took, lead)

		// Proof the other direction still works: the old leader HEARS the
		// new term and steps down, though nobody can hear it.
		require.Eventually(t, func() bool {
			st := c.mustStatus(lead)
			return st.GetRole() != adminv1.Role_ROLE_LEADER && st.GetTerm() > oldTerm
		}, electionBound, 20*time.Millisecond, "the old leader never heard the new term")
		c.heal()
	})

	t.Run("freeze parks a node that stays alive, thaw resumes it", func(t *testing.T) {
		lead, _ = c.waitLeader(electionBound)
		f := c.others(lead)[0]
		_, err := c.admin(f).Freeze(context.Background(), &adminv1.FreezeRequest{})
		require.NoError(t, err)

		st := c.mustStatus(f) // answered while frozen
		require.True(t, st.GetFrozen())
		_, alive := c.sup.PID(f)
		require.True(t, alive, "freeze must not stop the process")
		frozenAt := st.GetCommitIndex()
		for i := range 5 {
			put(t, cl, fmt.Sprintf("f%d", i), "v")
		}
		time.Sleep(300 * time.Millisecond)
		require.Equal(t, frozenAt, c.mustStatus(f).GetCommitIndex(), "a frozen node processed messages")

		r, err := c.admin(f).Thaw(context.Background(), &adminv1.ThawRequest{})
		require.NoError(t, err)
		require.GreaterOrEqual(t, r.GetFrozenForMs(), uint64(300))
		require.Positive(t, r.GetTicksMissed())
		require.False(t, c.mustStatus(f).GetFrozen())
		c.waitCaughtUp(c.mustStatus(c.leader()).GetCommitIndex(), 10*time.Second, f)

		// A bounded freeze thaws itself.
		_, err = c.admin(f).Freeze(context.Background(), &adminv1.FreezeRequest{AutoThawAfterMs: 200})
		require.NoError(t, err)
		require.Eventually(t, func() bool { return !c.mustStatus(f).GetFrozen() },
			5*time.Second, 20*time.Millisecond, "auto-thaw never happened")
	})
}

// TestKilledNodeRecovers: a follower killed while writes continue comes back
// from its own data directory and converges on exactly the others' state.
func TestKilledNodeRecovers(t *testing.T) {
	c := startCluster(t, 3)
	lead, _ := c.waitLeader(electionBound)
	cl := c.client(client.Config{})
	for i := range 20 {
		put(t, cl, fmt.Sprintf("before-%d", i), "v")
	}
	f := c.others(lead)[0]
	c.kill(f)
	for i := range 20 {
		put(t, cl, fmt.Sprintf("during-%d", i), "v")
	}
	c.start(f)
	leadSt := c.mustStatus(c.leader())
	c.waitCaughtUp(leadSt.GetCommitIndex(), 10*time.Second, f)

	fs := c.mustStatus(f)
	ls := c.mustStatus(c.leader())
	require.GreaterOrEqual(t, fs.GetLastApplied(), leadSt.GetCommitIndex())
	require.Equal(t, ls.GetLastLogTerm(), fs.GetLastLogTerm())
	for i := range 20 {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		g, err := cl.Get(ctx, fmt.Sprintf("during-%d", i))
		cancel()
		require.NoError(t, err)
		require.True(t, g.Found)
	}
}

// acked is the set of writes that were acknowledged as successful.
type acked struct {
	mu   sync.Mutex
	kv   map[string]string
	lost int // writes whose outcome was not learned
}

func (a *acked) add(k, v string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.kv[k] = v
}

// TestAckedWritesSurviveLeaderKill is phase 6 acceptance criterion 2. Writers
// run continuously; the leader's process is killed mid-flight. A new leader
// must appear within a bound, and every write that received a success
// response -- before, during or after the kill -- must be readable
// afterwards, with its value.
func TestAckedWritesSurviveLeaderKill(t *testing.T) {
	c := startCluster(t, 3)
	lead, _ := c.waitLeader(electionBound)

	a := &acked{kv: map[string]string{}}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for g := range 4 {
		cl := c.client(client.Config{AttemptTimeout: time.Second})
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				k, v := fmt.Sprintf("w%d-%d", g, i), fmt.Sprintf("value-%d-%d", g, i)
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				_, err := cl.Put(ctx, k, []byte(v))
				cancel()
				if err == nil {
					a.add(k, v)
				} else {
					a.mu.Lock()
					a.lost++
					a.mu.Unlock()
				}
			}
		}()
	}

	time.Sleep(time.Second)
	c.kill(lead)
	newLead, took := c.waitLeader(electionBound, c.others(lead)...)
	t.Logf("node %d killed; node %d led %s later", lead, newLead, took)
	require.Less(t, took, electionBound)

	time.Sleep(1500 * time.Millisecond)
	close(stop)
	wg.Wait()
	c.start(lead)

	t.Logf("%d writes acknowledged, %d with unknown outcome", len(a.kv), a.lost)
	require.Greater(t, len(a.kv), 50, "too few writes to say anything")
	require.NoError(t, a.verify(c))

	// The hazard DESIGN.md §1 flags -- one goroutine owns ticks and fsyncs,
	// so a slow disk delays ticks -- is measured, not assumed. Log it.
	for _, id := range c.sup.IDs() {
		m := c.mustStatus(id).GetMetrics()
		t.Logf("node %d: worst tick lag %d ticks, fsync p99 %dus over %d fsyncs, %d elections started",
			id, m.GetTickLagTicks(), m.GetFsyncP99Micros(), m.GetFsyncCount(), m.GetElectionsStarted())
	}
}

// verify reads every acknowledged write back through a fresh client -- a
// linearizable read, so it sees the cluster's committed state -- and reports
// the first one that is missing or wrong.
func (a *acked) verify(c *realCluster) error {
	reader := c.client(client.Config{})
	a.mu.Lock()
	defer a.mu.Unlock()
	missing := 0
	var first string
	for k, v := range a.kv {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		g, err := reader.Get(ctx, k)
		cancel()
		if err != nil {
			return fmt.Errorf("reading acknowledged write %s: %w", k, err)
		}
		if !g.Found || string(g.Value) != v {
			missing++
			if first == "" {
				first = fmt.Sprintf("%s (found=%v value=%q, want %q)", k, g.Found, g.Value, v)
			}
		}
	}
	if missing > 0 {
		return fmt.Errorf("%d of %d acknowledged writes are missing or wrong, e.g. %s", missing, len(a.kv), first)
	}
	return nil
}

// TestAckedCheckCatchesLostData is the negative control for the acknowledged-
// writes check used by criteria 2 and 4. Writes are acknowledged, then every
// node is killed and its data directory deleted -- storage that "lost" what it
// had promised to keep -- and the cluster is restarted empty. The check must
// report the loss.
func TestAckedCheckCatchesLostData(t *testing.T) {
	c := startCluster(t, 3)
	c.waitLeader(electionBound)
	cl := c.client(client.Config{})
	a := &acked{kv: map[string]string{}}
	for i := range 20 {
		k := fmt.Sprintf("k%d", i)
		put(t, cl, k, k)
		a.add(k, k)
	}
	require.NoError(t, a.verify(c), "the check must pass before anything is lost")

	for _, id := range c.sup.IDs() {
		c.kill(id)
		require.NoError(t, os.RemoveAll(filepath.Dir(c.sup.PIDFile(id))))
	}
	for _, id := range c.sup.IDs() {
		c.start(id)
	}
	c.waitLeader(electionBound)
	err := a.verify(c)
	require.Error(t, err, "every acknowledged write was lost and the check did not notice")
	t.Logf("caught: %v", err)
}

// TestMinorityPartitionRefusesWrites is phase 6 acceptance criterion 3, on
// five real processes. The leader and one follower are cut off from the other
// three. The minority must refuse writes -- errors, never a false success --
// while the majority keeps committing. After healing, every node's log must
// match the majority's exactly, and the minority's refused write must be
// nowhere.
func TestMinorityPartitionRefusesWrites(t *testing.T) {
	c := startCluster(t, 5)
	lead, _ := c.waitLeader(electionBound)
	minority := []raft.NodeID{lead, c.others(lead)[0]}
	var majority []raft.NodeID
	for _, id := range c.sup.IDs() {
		if id != minority[0] && id != minority[1] {
			majority = append(majority, id)
		}
	}
	// Registered while the minority still has a quorum: registering is a write
	// too, and after the cut it could not commit.
	minClient := c.client(client.Config{AttemptTimeout: 500 * time.Millisecond}, minority...)
	c.partition(minority, majority)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	_, err := minClient.Put(ctx, "minority-write", []byte("must not happen"))
	cancel()
	require.Error(t, err, "the minority acknowledged a write it could not commit")
	t.Logf("minority write refused: %v", err)

	majLead, _ := c.waitLeader(electionBound, majority...)
	majClient := c.client(client.Config{}, majority...)
	for i := range 10 {
		put(t, majClient, fmt.Sprintf("majority-%d", i), "v")
	}
	t.Logf("majority (led by node %d) committed 10 writes", majLead)

	c.heal()
	var lastErr error
	require.Eventually(t, func() bool {
		var sts []*adminv1.NodeStatus
		for _, id := range c.sup.IDs() {
			st, err := c.status(id)
			if err != nil {
				lastErr = err
				return false
			}
			sts = append(sts, st)
		}
		lastErr = logsMatch(sts)
		return lastErr == nil
	}, 10*time.Second, 20*time.Millisecond, "the logs never converged after healing")
	require.NoError(t, lastErr)

	g, err := majClient.Get(context.Background(), "minority-write")
	require.NoError(t, err)
	require.False(t, g.Found, "the write the minority refused was applied after all")
}

// logsMatch reports whether every node holds the same, fully committed log.
// Equal last index and term imply identical logs, by Log Matching; the tails
// are compared entry by entry as well, rather than taking that on trust.
func logsMatch(sts []*adminv1.NodeStatus) error {
	ref := sts[0]
	for _, st := range sts {
		if st.GetLastLogIndex() != ref.GetLastLogIndex() || st.GetLastLogTerm() != ref.GetLastLogTerm() {
			return fmt.Errorf("node %d ends at %d@%d, node %d at %d@%d", st.GetNodeId(), st.GetLastLogIndex(),
				st.GetLastLogTerm(), ref.GetNodeId(), ref.GetLastLogIndex(), ref.GetLastLogTerm())
		}
		if st.GetCommitIndex() != st.GetLastLogIndex() {
			return fmt.Errorf("node %d has committed %d of %d entries", st.GetNodeId(), st.GetCommitIndex(), st.GetLastLogIndex())
		}
		if len(st.GetLogTail()) != len(ref.GetLogTail()) {
			return fmt.Errorf("node %d shows %d tail entries, node %d shows %d", st.GetNodeId(),
				len(st.GetLogTail()), ref.GetNodeId(), len(ref.GetLogTail()))
		}
		for i, e := range st.GetLogTail() {
			r := ref.GetLogTail()[i]
			if e.GetIndex() != r.GetIndex() || e.GetTerm() != r.GetTerm() {
				return fmt.Errorf("node %d has %d@%d where node %d has %d@%d", st.GetNodeId(), e.GetIndex(),
					e.GetTerm(), ref.GetNodeId(), r.GetIndex(), r.GetTerm())
			}
		}
	}
	return nil
}

// TestLogMatchCheckCatchesDivergence is the negative control for logsMatch:
// hand-built statuses that differ in a middle entry's term, in length, or in
// commitment must each be rejected; identical ones accepted.
func TestLogMatchCheckCatchesDivergence(t *testing.T) {
	node := func(id uint64, commit uint64, terms ...uint64) *adminv1.NodeStatus {
		st := &adminv1.NodeStatus{NodeId: id, CommitIndex: commit}
		for i, term := range terms {
			st.LogTail = append(st.LogTail, &adminv1.LogEntryView{Index: uint64(i + 1), Term: term})
			st.LastLogIndex, st.LastLogTerm = uint64(i+1), term
		}
		return st
	}
	require.NoError(t, logsMatch([]*adminv1.NodeStatus{node(1, 3, 1, 1, 2), node(2, 3, 1, 1, 2)}))
	require.Error(t, logsMatch([]*adminv1.NodeStatus{node(1, 3, 1, 1, 2), node(2, 3, 1, 2, 2)}), "a middle entry differs")
	require.Error(t, logsMatch([]*adminv1.NodeStatus{node(1, 3, 1, 1, 2), node(2, 2, 1, 1)}), "one log is shorter")
	require.Error(t, logsMatch([]*adminv1.NodeStatus{node(1, 3, 1, 1, 2), node(2, 2, 1, 1, 2)}), "not all committed")
}

// TestRollingRestart is phase 6 acceptance criterion 4. Every node is killed
// and restarted in turn while clients keep reading and writing. Every
// operation must succeed within its budget -- the cluster serves throughout --
// and every acknowledged write must survive.
func TestRollingRestart(t *testing.T) {
	c := startCluster(t, 3)
	c.waitLeader(electionBound)

	a := &acked{kv: map[string]string{}}
	var failures []string
	var fmu sync.Mutex
	var worst time.Duration
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for g := range 3 {
		cl := c.client(client.Config{AttemptTimeout: time.Second})
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				k := fmt.Sprintf("r%d-%d", g, i)
				start := time.Now()
				ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
				_, err := cl.Put(ctx, k, []byte(k))
				if err == nil {
					_, err = cl.Get(ctx, k)
				}
				cancel()
				took := time.Since(start)
				fmu.Lock()
				worst = max(worst, took)
				if err != nil {
					failures = append(failures, fmt.Sprintf("%s: %v", k, err))
				} else {
					a.add(k, k)
				}
				fmu.Unlock()
			}
		}()
	}

	for _, id := range c.sup.IDs() {
		time.Sleep(500 * time.Millisecond)
		c.kill(id)
		time.Sleep(300 * time.Millisecond)
		c.start(id)
		lead, _ := c.waitLeader(electionBound)
		c.waitCaughtUp(c.mustStatus(lead).GetCommitIndex(), 10*time.Second, id)
		t.Logf("node %d restarted and caught up", id)
	}
	time.Sleep(500 * time.Millisecond)
	close(stop)
	wg.Wait()

	t.Logf("%d writes acknowledged; slowest put+get %s", len(a.kv), worst)
	require.Empty(t, failures, "the cluster stopped serving during the rolling restart")
	require.Greater(t, len(a.kv), 50)
	require.NoError(t, a.verify(c))
}

// TestRestartedFollowerDoesNotDisrupt: restarting a follower must not cost the
// cluster an election. A restarted node that hears from the leader within its
// election timeout simply follows; one that does not campaigns, bumps the
// term and deposes a healthy leader for nothing. The first version of the
// transport let its reconnect backoff grow past the election timeout, so
// every restart did exactly that (BUGS.md, 2026-09-30).
func TestRestartedFollowerDoesNotDisrupt(t *testing.T) {
	c := startCluster(t, 3)
	lead := c.leader()
	time.Sleep(time.Second) // let startup elections settle
	term := c.mustStatus(lead).GetTerm()
	for round := range 3 {
		f := c.others(lead)[round%2]
		c.kill(f)
		time.Sleep(700 * time.Millisecond) // long enough for peers' backoff to grow
		c.start(f)
		time.Sleep(1500 * time.Millisecond)
		st := c.mustStatus(lead)
		require.Equal(t, adminv1.Role_ROLE_LEADER, st.GetRole(), "round %d: restarting node %d deposed leader %d", round, f, lead)
		require.Equal(t, term, st.GetTerm(), "round %d: restarting node %d forced an election (term %d -> %d)",
			round, f, term, st.GetTerm())
	}
}
