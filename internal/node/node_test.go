package node_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	kvv1 "github.com/Shashankkaranamr/Quorum/gen/quorum/kv/v1"
	"github.com/Shashankkaranamr/Quorum/internal/client"
	"github.com/Shashankkaranamr/Quorum/internal/node"
	"github.com/Shashankkaranamr/Quorum/raft"
)

// rawKV is a KV client pinned to one node, with none of the client library's
// retrying or redirecting, for tests that need to see exactly what one node
// answers.
func rawKV(t *testing.T, addr string) kvv1.KVClient {
	t.Helper()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return kvv1.NewKVClient(conn)
}

func TestClusterServesWritesAndReads(t *testing.T) {
	c := newCluster(t, 3, nil)
	c.leader()
	ctx := ctxFor(t, 10*time.Second)
	cl := c.client(ctx, client.Config{})

	put, err := cl.Put(ctx, "k", []byte("v1"))
	require.NoError(t, err)
	require.False(t, put.Duplicate)

	got, err := cl.Get(ctx, "k")
	require.NoError(t, err)
	require.True(t, got.Found)
	require.Equal(t, "v1", string(got.Value))
	require.GreaterOrEqual(t, got.ReadIndex, put.AppliedIndex,
		"a read after a write must be linearized at or after it")

	del, err := cl.Delete(ctx, "k")
	require.NoError(t, err)
	require.True(t, del.Existed)
	got, err = cl.Get(ctx, "k")
	require.NoError(t, err)
	require.False(t, got.Found)
}

// staleReadScenario writes k=v1 through a leader, isolates that leader, lets
// the majority elect a new one and overwrite k=v2, and then asks the old
// leader -- which cannot hear anyone and so still believes it leads -- for k.
// It returns the old leader's answer.
func staleReadScenario(t *testing.T, mutation raft.Mutation) *kvv1.GetResponse {
	c := newCluster(t, 3, func(o *node.Options) { o.UnsafeMutation = mutation })
	ctx := ctxFor(t, 20*time.Second)
	old := c.leader()
	cl := c.client(ctx, client.Config{InitialLeader: old})
	_, err := cl.Put(ctx, "k", []byte("v1"))
	require.NoError(t, err)

	c.isolate(old)
	majority := c.others(old)
	require.Eventually(t, func() bool { return c.leaderNow(majority...) != raft.None },
		5*time.Second, testTick, "the majority never elected a new leader")
	cl2 := c.client(ctx, client.Config{}, majority...)
	_, err = cl2.Put(ctx, "k", []byte("v2"))
	require.NoError(t, err)

	st := c.nodes[old].Status()
	require.Equal(t, raft.Leader, st.Role,
		"the old leader must still believe it leads, or refusing the read proves nothing")

	resp, err := rawKV(t, c.addrs[old]).Get(ctx, &kvv1.GetRequest{Key: "k"})
	require.NoError(t, err)
	return resp
}

// TestPartitionedLeaderRefusesRead is phase 5 acceptance criterion 3. A leader
// partitioned from the majority, while the majority has moved on and
// overwritten the key, must refuse the read with NO_QUORUM. Answering with the
// old value fails the test.
func TestPartitionedLeaderRefusesRead(t *testing.T) {
	resp := staleReadScenario(t, raft.MutationNone)
	if resp.GetStatus() == kvv1.Status_STATUS_OK {
		t.Fatalf("the partitioned leader served a read: %q", resp.GetValue())
	}
	require.Equal(t, kvv1.Status_STATUS_NO_QUORUM, resp.GetStatus())
}

// TestStaleReadCheckCatchesAQuorumlessRead is the negative control for
// criterion 3: with the quorum confirmation removed from ReadIndex, the same
// scenario must produce exactly the stale read the test exists to catch.
func TestStaleReadCheckCatchesAQuorumlessRead(t *testing.T) {
	resp := staleReadScenario(t, raft.MutationReadWithoutQuorum)
	require.Equal(t, kvv1.Status_STATUS_OK, resp.GetStatus())
	require.Equal(t, "v1", string(resp.GetValue()),
		"without confirmation the old leader should have answered from its stale state")
	t.Logf("caught: the partitioned leader answered k=%q after the majority wrote v2", resp.GetValue())
}
