package node_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	kvv1 "github.com/Shashankkaranamr/Quorum/gen/quorum/kv/v1"
	"github.com/Shashankkaranamr/Quorum/internal/client"
	"github.com/Shashankkaranamr/Quorum/internal/node"
	"github.com/Shashankkaranamr/Quorum/internal/statemachine"
	"github.com/Shashankkaranamr/Quorum/raft"
)

// freshWrites counts a client's writes that a node applied as new, and those it
// recognized as retries.
func freshWrites(results []statemachine.Result, clientID uint64) (fresh, dup int) {
	for _, r := range results {
		if r.Type != raft.EntryNormal || r.ClientID != clientID || r.Status != kvv1.Status_STATUS_OK {
			continue
		}
		if r.Duplicate {
			dup++
		} else {
			fresh++
		}
	}
	return fresh, dup
}

// ambiguousScenario makes a leader commit and apply a client's Put and then die
// before answering, so the client cannot know whether the write happened. naive
// selects how the client recovers: the client library, which retries with the
// same seq, or a naive client that retries with a new one. It returns an error
// if the write was not applied exactly once.
func ambiguousScenario(t *testing.T, naive bool) error {
	var armed atomic.Bool
	victims := make(chan raft.NodeID, 1)
	c := newCluster(t, 3, func(o *node.Options) {
		id := o.ID
		o.Hooks.BeforeRespond = func(method string, _ raft.Index) error {
			if method == "Put" && armed.CompareAndSwap(true, false) {
				victims <- id
				return errors.New("injected: the leader died after the write committed, before answering")
			}
			return nil
		}
	})
	ctx := ctxFor(t, 20*time.Second)
	lead := c.leader()
	cl := c.client(ctx, client.Config{InitialLeader: lead})
	leadKV := rawKV(t, c.addrs[lead])

	type outcome struct {
		res client.PutResult
		err error
	}
	done := make(chan outcome, 1)
	armed.Store(true)
	go func() {
		if naive {
			_, err := leadKV.Put(ctx, &kvv1.PutRequest{ClientId: cl.ID(), Seq: 1, Key: "x", Value: []byte("1")})
			done <- outcome{err: err}
			return
		}
		r, err := cl.Put(ctx, "x", []byte("1"))
		done <- outcome{res: r, err: err}
	}()

	var victim raft.NodeID
	select {
	case victim = <-victims:
	case <-ctx.Done():
		t.Fatal("the write never reached the point of being applied")
	}
	require.Equal(t, lead, victim)
	c.kill(victim)
	o := <-done

	survivors := c.others(victim)
	var newLead raft.NodeID
	require.Eventually(t, func() bool {
		newLead = c.leaderNow(survivors...)
		return newLead != raft.None
	}, 5*time.Second, testTick)

	if naive {
		require.Error(t, o.err, "the response was supposed to be lost")
		// The naive retry: a new seq, so the state machine sees a new write.
		require.Eventually(t, func() bool {
			r, err := rawKV(t, c.addrs[c.leaderNow(survivors...)]).Put(ctx,
				&kvv1.PutRequest{ClientId: cl.ID(), Seq: 2, Key: "x", Value: []byte("1")})
			return err == nil && r.GetStatus() == kvv1.Status_STATUS_OK
		}, 5*time.Second, testTick)
	} else {
		require.NoError(t, o.err)
		require.Greater(t, cl.LastAttempts(), 1, "the client should have had to retry")
		if !o.res.Duplicate {
			return fmt.Errorf("the retry of seq 1 was answered as a fresh write, not a duplicate")
		}
	}

	// The node that answered the retry applied both the original entry and
	// the retry, in that order.
	results := c.results(c.leaderNow(survivors...))
	fresh, dup := freshWrites(results, cl.ID())
	if fresh != 1 {
		return fmt.Errorf("client %d's write was applied %d times (and recognized as a retry %d times)",
			cl.ID(), fresh, dup)
	}
	return nil
}

// TestAmbiguousRetryAppliesOnce is phase 5 acceptance criterion 4. The leader
// applies a Put and dies before responding. The client library retries with the
// same seq, reaches the new leader, is told duplicate = true, and the write
// has been applied exactly once.
func TestAmbiguousRetryAppliesOnce(t *testing.T) {
	require.NoError(t, ambiguousScenario(t, false))
}

// TestAppliedOnceCheckCatchesANaiveRetry is the negative control for criterion
// 4: a client that retries the ambiguous write under a NEW seq gets it applied
// twice, and the check must say so.
func TestAppliedOnceCheckCatchesANaiveRetry(t *testing.T) {
	err := ambiguousScenario(t, true)
	require.Error(t, err, "a retry under a new seq applied the write twice and the check missed it")
	t.Logf("caught: %v", err)
}

// TestNotLeaderRedirect is phase 5 acceptance criterion 5. A write sent to a
// follower is refused with NOT_LEADER and a hint naming the leader's address,
// nothing is applied, and the client library gets there in at most two
// attempts -- then one, once it has learned where the leader is.
func TestNotLeaderRedirect(t *testing.T) {
	c := newCluster(t, 3, nil)
	ctx := ctxFor(t, 10*time.Second)
	lead := c.leader()
	follower := c.others(lead)[0]

	cl := c.client(ctx, client.Config{InitialLeader: follower})
	require.Equal(t, 2, cl.LastAttempts(), "registration sent to a follower should cost exactly one redirect")

	resp, err := rawKV(t, c.addrs[follower]).Put(ctx,
		&kvv1.PutRequest{ClientId: cl.ID(), Seq: 99, Key: "never", Value: []byte("x")})
	require.NoError(t, err)
	require.Equal(t, kvv1.Status_STATUS_NOT_LEADER, resp.GetStatus())
	require.Equal(t, uint64(lead), resp.GetLeaderHint().GetNodeId())
	require.Equal(t, c.addrs[lead], resp.GetLeaderHint().GetGrpcAddr(), "the hint must be dialable")

	for i := range 20 {
		_, err := cl.Put(ctx, fmt.Sprintf("k%d", i), []byte("v"))
		require.NoError(t, err)
		require.Equal(t, 1, cl.LastAttempts(), "a client that knows the leader should not need a second attempt")
	}
	got, err := cl.Get(ctx, "never")
	require.NoError(t, err)
	require.False(t, got.Found, "the write refused with NOT_LEADER must not have been applied")

	fresh, _ := freshWrites(c.results(lead), cl.ID())
	require.Equal(t, 20, fresh)
}

// TestPinnedClientSeesTheMinorityRefuse: a client pinned to a leader that has
// been cut off from the majority must never be told its write succeeded -- it
// cannot commit there -- and a client pinned to a follower is told, definitely,
// that its write did not happen rather than being redirected.
func TestPinnedClientSeesTheMinorityRefuse(t *testing.T) {
	c := newCluster(t, 3, nil)
	lead := c.leader()
	ctx := ctxFor(t, 20*time.Second)
	cl := c.client(ctx, client.Config{AttemptTimeout: 300 * time.Millisecond})

	cl.Pin(c.others(lead)[0])
	_, err := cl.Put(ctx, "k", []byte("via a follower"))
	require.ErrorIs(t, err, client.ErrPinnedNotLeader)

	c.isolate(lead)
	cl.Pin(lead)
	short, cancel := context.WithTimeout(ctx, 1500*time.Millisecond)
	defer cancel()
	_, err = cl.Put(short, "k", []byte("via the cut-off leader"))
	require.ErrorIs(t, err, client.ErrUnknownOutcome, "a minority leader acknowledged a write it cannot commit")

	c.heal()
	cl.Pin(raft.None)
	got, err := cl.Get(ctx, "k")
	require.NoError(t, err)
	require.False(t, got.Found, "neither refused write may have been applied")
}

// TestClientEscapesAStaleMinority: the leader and one follower are cut off from
// the other three. A client whose last-known leader is the stale one must still
// get its write done through the majority. The trap is that the stale leader
// times out, and its follower -- honestly -- redirects back to it. The first
// version of the client followed that hint every time and never reached the
// majority (BUGS.md, 2026-09-30).
func TestClientEscapesAStaleMinority(t *testing.T) {
	c := newCluster(t, 5, nil)
	lead := c.leader()
	follower := c.others(lead)[0]
	var majority []raft.NodeID
	for id := range c.nodes {
		if id != lead && id != follower {
			majority = append(majority, id)
		}
	}
	ctx := ctxFor(t, 30*time.Second)
	cl := c.client(ctx, client.Config{InitialLeader: lead, AttemptTimeout: 300 * time.Millisecond})

	c.split([]raft.NodeID{lead, follower}, majority)
	require.Eventually(t, func() bool { return c.leaderNow(majority...) != raft.None },
		5*time.Second, testTick, "the majority never elected a leader")

	for i := range 3 {
		wctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		res, err := cl.Put(wctx, fmt.Sprintf("k%d", i), []byte("v"))
		cancel()
		require.NoError(t, err, "write %d never escaped the minority (%d attempts)", i, cl.LastAttempts())
		t.Logf("write %d committed at %d after %d attempts, via node %d", i, res.AppliedIndex, cl.LastAttempts(), cl.Leader())
		require.Contains(t, majority, cl.Leader())
	}
}
