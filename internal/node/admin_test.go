package node_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	adminv1 "github.com/Shashankkaranamr/Quorum/gen/quorum/admin/v1"
)

func adminClient(t *testing.T, addr string) adminv1.AdminClient {
	t.Helper()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return adminv1.NewAdminClient(conn)
}

// TestWatchStatusStreamsLiveState: the status stream the visualizer will read
// keeps sending, keeps up with the cluster, and keeps answering while the node
// is frozen -- which is when an operator most needs to see it.
func TestWatchStatusStreamsLiveState(t *testing.T) {
	c := newCluster(t, 3, nil)
	lead := c.leader()
	a := adminClient(t, c.addrs[lead])
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stream, err := a.WatchStatus(ctx, &adminv1.WatchStatusRequest{LogTailLimit: 4, MinIntervalMs: 50})
	require.NoError(t, err)
	first, err := stream.Recv()
	require.NoError(t, err)
	require.Equal(t, adminv1.Role_ROLE_LEADER, first.GetRole())
	require.LessOrEqual(t, len(first.GetLogTail()), 4)
	require.Len(t, first.GetPeers(), 2)

	_, err = a.Freeze(ctx, &adminv1.FreezeRequest{AutoThawAfterMs: 300})
	require.NoError(t, err)
	var sawFrozen, sawThawed bool
	for !sawThawed {
		st, err := stream.Recv()
		require.NoError(t, err, "the stream stopped while the node was frozen")
		if st.GetFrozen() {
			sawFrozen = true
		} else if sawFrozen {
			sawThawed = true
		}
	}
	require.True(t, sawFrozen)
	require.Equal(t, uint64(lead), first.GetNodeId())
}
