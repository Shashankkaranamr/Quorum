package node_test

import (
	"context"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/Shashankkaranamr/Quorum/internal/client"
	"github.com/Shashankkaranamr/Quorum/internal/config"
	"github.com/Shashankkaranamr/Quorum/internal/node"
	"github.com/Shashankkaranamr/Quorum/internal/statemachine"
	"github.com/Shashankkaranamr/Quorum/raft"
)

// TestMain fails the package if any test leaves a goroutine running: the
// driver loop, the transport's peer goroutines and the gRPC servers must all
// exit when a node stops.
func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

// cluster is a set of real nodes in one process: real gRPC on localhost, real
// write-ahead logs in a temporary directory, real goroutines and clocks. Only
// the process boundary is missing, and test/integration covers that.
type cluster struct {
	t     *testing.T
	cfg   *config.Cluster
	addrs map[raft.NodeID]string
	nodes map[raft.NodeID]*node.Node
	opts  func(id raft.NodeID) node.Options

	mu      sync.Mutex
	applied map[raft.NodeID][]statemachine.Result // every result, every life
}

// testTick keeps real-time tests quick: elections take 100-200ms.
const testTick = 10 * time.Millisecond

func newCluster(t *testing.T, n int, tweak func(*node.Options)) *cluster {
	t.Helper()
	c := &cluster{
		t:       t,
		addrs:   map[raft.NodeID]string{},
		nodes:   map[raft.NodeID]*node.Node{},
		applied: map[raft.NodeID][]statemachine.Result{},
	}
	c.cfg = &config.Cluster{
		ClusterID: "test",
		Raft: config.RaftParams{
			TickMS:                   int(testTick / time.Millisecond),
			ElectionTimeoutMinTicks:  10,
			ElectionTimeoutMaxTicks:  20,
			HeartbeatTimeoutTicks:    3,
			SnapshotThresholdEntries: 200,
			MaxEntriesPerAppend:      64,
		},
		Storage: config.StorageParams{DataDir: t.TempDir(), WALSegmentBytes: 64 << 10},
	}

	listeners := map[raft.NodeID]net.Listener{}
	for i := 1; i <= n; i++ {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		id := raft.NodeID(i)
		listeners[id] = lis
		port := lis.Addr().(*net.TCPAddr).Port
		// Nothing serves HTTP yet (phase 7), but the config requires a
		// distinct port, so reserve a real one.
		httpLis, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		httpPort := httpLis.Addr().(*net.TCPAddr).Port
		require.NoError(t, httpLis.Close())
		c.cfg.Nodes = append(c.cfg.Nodes, config.Node{ID: config.NodeID(i), Host: "127.0.0.1", GRPCPort: port, HTTPPort: httpPort})
		c.addrs[id] = net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
	}
	require.NoError(t, c.cfg.Validate())

	c.opts = func(id raft.NodeID) node.Options {
		o := node.Options{
			Cluster:        c.cfg,
			ID:             id,
			RequestTimeout: 500 * time.Millisecond,
			OnApply: func(r statemachine.Result) {
				c.mu.Lock()
				defer c.mu.Unlock()
				c.applied[id] = append(c.applied[id], r)
			},
		}
		if tweak != nil {
			tweak(&o)
		}
		return o
	}
	for id, lis := range listeners {
		o := c.opts(id)
		o.Listener = lis
		nd, err := node.Start(o)
		require.NoError(t, err)
		c.nodes[id] = nd
	}
	t.Cleanup(func() {
		for _, nd := range c.nodes {
			if nd != nil {
				require.NoError(t, nd.Stop())
			}
		}
	})
	return c
}

// leader waits for a node that leads and has committed in its term, which is
// the point at which it can serve reads.
func (c *cluster) leader() raft.NodeID {
	c.t.Helper()
	var id raft.NodeID
	require.Eventually(c.t, func() bool {
		id = c.leaderNow()
		return id != raft.None
	}, 5*time.Second, testTick, "no leader emerged")
	return id
}

// leaderNow is the live node that leads in the highest term, if it has
// committed in that term.
func (c *cluster) leaderNow(among ...raft.NodeID) raft.NodeID {
	if len(among) == 0 {
		for id := range c.nodes {
			among = append(among, id)
		}
	}
	var best raft.NodeID
	var bestTerm raft.Term
	for _, id := range among {
		nd := c.nodes[id]
		if nd == nil {
			continue
		}
		s := nd.Status()
		if s.Role == raft.Leader && s.Term > bestTerm && s.CommitIndex > 0 {
			best, bestTerm = id, s.Term
		}
	}
	return best
}

func (c *cluster) others(id raft.NodeID) []raft.NodeID {
	var out []raft.NodeID
	for other := range c.nodes {
		if other != id {
			out = append(out, other)
		}
	}
	return out
}

// isolate cuts every link to and from id, in both directions, on both ends.
func (c *cluster) isolate(id raft.NodeID) {
	for other, nd := range c.nodes {
		if nd == nil {
			continue
		}
		if other == id {
			nd.Transport().Partition(c.others(id)...)
		} else {
			nd.Transport().Partition(id)
		}
	}
}

func (c *cluster) heal() {
	for _, nd := range c.nodes {
		if nd != nil {
			nd.Transport().Heal()
		}
	}
}

// kill crashes a node in-process. It stays down until restart.
func (c *cluster) kill(id raft.NodeID) {
	c.t.Helper()
	nd := c.nodes[id]
	require.NotNil(c.t, nd, "node %d is already down", id)
	c.nodes[id] = nil
	require.NoError(c.t, nd.Kill())
}

// restart brings a killed node back on the same address and data directory.
func (c *cluster) restart(id raft.NodeID) {
	c.t.Helper()
	var lis net.Listener
	require.Eventually(c.t, func() bool {
		var err error
		lis, err = net.Listen("tcp", c.addrs[id])
		return err == nil
	}, 5*time.Second, 20*time.Millisecond, "cannot rebind %s", c.addrs[id])
	o := c.opts(id)
	o.Listener = lis
	nd, err := node.Start(o)
	require.NoError(c.t, err)
	c.nodes[id] = nd
}

// client returns a registered client for the given nodes (all by default).
func (c *cluster) client(ctx context.Context, cfg client.Config, only ...raft.NodeID) *client.Client {
	c.t.Helper()
	cfg.Addrs = map[raft.NodeID]string{}
	if len(only) == 0 {
		for id := range c.addrs {
			only = append(only, id)
		}
	}
	for _, id := range only {
		cfg.Addrs[id] = c.addrs[id]
	}
	cl, err := client.Dial(cfg)
	require.NoError(c.t, err)
	c.t.Cleanup(func() { _ = cl.Close() })
	require.NoError(c.t, cl.Register(ctx))
	return cl
}

// results returns what one node's state machine reported applying, across all
// of its lives.
func (c *cluster) results(id raft.NodeID) []statemachine.Result {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]statemachine.Result(nil), c.applied[id]...)
}

func ctxFor(t *testing.T, d time.Duration) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}
