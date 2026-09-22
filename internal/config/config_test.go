package config_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/Shashankkaranamr/Quorum/internal/config"
)

// minimalYAML is the smallest configuration that should be accepted: three
// nodes and nothing else. Everything the consensus layer needs comes from
// defaults.
const minimalYAML = `
nodes:
  - {id: 1, host: 127.0.0.1, grpc_port: 7001, http_port: 8001}
  - {id: 2, host: 127.0.0.1, grpc_port: 7002, http_port: 8002}
  - {id: 3, host: 127.0.0.1, grpc_port: 7003, http_port: 8003}
`

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "cluster.yaml")
	require.NoError(t, os.WriteFile(path, []byte(body), 0o600))
	return path
}

func TestLoadRepoClusterYAML(t *testing.T) {
	// The cluster.yaml checked into the repo root is what `make run` uses and
	// what the README tells a reader to start from. If it stops being valid,
	// the documented quickstart is broken.
	c, err := config.Load(filepath.Join("..", "..", "cluster.yaml"))
	require.NoError(t, err)

	require.Len(t, c.Nodes, 3)
	require.Equal(t, 2, c.Quorum())
	require.Empty(t, c.Warnings())
	require.Equal(t, []config.NodeID{1, 2, 3}, c.IDs())
}

func TestLoadAppliesDefaults(t *testing.T) {
	c, err := config.Load(writeConfig(t, minimalYAML))
	require.NoError(t, err)

	require.Equal(t, "quorum-local", c.ClusterID)
	require.Equal(t, config.DefaultTickMS, c.Raft.TickMS)
	require.Equal(t, config.DefaultHeartbeatTimeoutTicks, c.Raft.HeartbeatTimeoutTicks)
	require.Equal(t, config.DefaultElectionTimeoutMinTicks, c.Raft.ElectionTimeoutMinTicks)
	require.Equal(t, config.DefaultElectionTimeoutMaxTicks, c.Raft.ElectionTimeoutMaxTicks)
	require.Equal(t, config.DefaultSnapshotThresholdEntries, c.Raft.SnapshotThresholdEntries)
	require.Equal(t, config.DefaultMaxEntriesPerAppend, c.Raft.MaxEntriesPerAppend)
	require.Equal(t, config.DefaultDataDir, c.Storage.DataDir)
	require.Equal(t, int64(config.DefaultWALSegmentBytes), c.Storage.WALSegmentBytes)
}

func TestAccessors(t *testing.T) {
	c, err := config.Load(writeConfig(t, minimalYAML))
	require.NoError(t, err)

	n, ok := c.Node(2)
	require.True(t, ok)
	require.Equal(t, "127.0.0.1:7002", n.GRPCAddr())
	require.Equal(t, "127.0.0.1:8002", n.HTTPAddr())

	_, ok = c.Node(99)
	require.False(t, ok, "unknown id must not resolve")

	peers := c.Peers(2)
	require.Equal(t, []config.NodeID{1, 3}, idsOf(peers))
	require.Equal(t, filepath.Join("data", "node-2"), c.DataDir(2))
}

func idsOf(nodes []config.Node) []config.NodeID {
	ids := make([]config.NodeID, len(nodes))
	for i, n := range nodes {
		ids[i] = n.ID
	}
	return ids
}

func TestQuorumSizes(t *testing.T) {
	// Quorum is the whole point of the system, so it gets an explicit table
	// rather than being trusted to the len/2+1 expression.
	for _, tc := range []struct {
		nodes, quorum, tolerates int
	}{
		{3, 2, 1},
		{4, 3, 1}, // even size buys nothing over 3
		{5, 3, 2},
	} {
		c := &config.Cluster{Nodes: make([]config.Node, tc.nodes)}
		require.Equal(t, tc.quorum, c.Quorum(), "quorum for %d nodes", tc.nodes)
		require.Equal(t, tc.tolerates, tc.nodes-c.Quorum(), "failures tolerated by %d nodes", tc.nodes)
	}
}

func TestEvenClusterSizeWarns(t *testing.T) {
	c := &config.Cluster{Nodes: make([]config.Node, 4)}
	require.Len(t, c.Warnings(), 1)
	require.Contains(t, c.Warnings()[0], "even")

	odd := &config.Cluster{Nodes: make([]config.Node, 5)}
	require.Empty(t, odd.Warnings())
}

func TestLoadRejectsBadConfigs(t *testing.T) {
	// Each case names the mistake it is guarding against. The point of strict
	// validation is that a bad config fails at startup instead of manifesting
	// later as erratic elections that look like a consensus bug.
	for _, tc := range []struct {
		name    string
		yaml    string
		wantMsg string
	}{
		{
			name:    "too few nodes",
			yaml:    "nodes:\n  - {id: 1, host: 127.0.0.1, grpc_port: 7001, http_port: 8001}\n",
			wantMsg: "between 3 and 5 nodes",
		},
		{
			name: "duplicate node id",
			yaml: strings.Replace(minimalYAML, "id: 3", "id: 2", 1),
			// nodes[2] collides with nodes[1] on both id and, first, nothing else.
			wantMsg: "duplicate node id 2",
		},
		{
			name:    "duplicate port across nodes",
			yaml:    strings.Replace(minimalYAML, "grpc_port: 7003", "grpc_port: 7001", 1),
			wantMsg: "already used by node 1",
		},
		{
			name:    "port out of range",
			yaml:    strings.Replace(minimalYAML, "grpc_port: 7003", "grpc_port: 70030", 1),
			wantMsg: "out of range",
		},
		{
			name:    "empty host",
			yaml:    strings.Replace(minimalYAML, "host: 127.0.0.1, grpc_port: 7002", `host: "", grpc_port: 7002`, 1),
			wantMsg: "host must not be empty",
		},
		{
			name:    "unknown field is a typo, not a default",
			yaml:    minimalYAML + "raft:\n  election_timout_min_ticks: 10\n",
			wantMsg: "field election_timout_min_ticks not found",
		},
		{
			name:    "empty election timeout range causes split votes",
			yaml:    minimalYAML + "raft:\n  election_timeout_min_ticks: 20\n  election_timeout_max_ticks: 20\n",
			wantMsg: "strictly less than max",
		},
		{
			name:    "election timeout too close to heartbeat",
			yaml:    minimalYAML + "raft:\n  heartbeat_timeout_ticks: 5\n  election_timeout_min_ticks: 6\n  election_timeout_max_ticks: 12\n",
			wantMsg: "must be at least 3x",
		},
		{
			name:    "negative tick",
			yaml:    minimalYAML + "raft:\n  tick_ms: -1\n",
			wantMsg: "tick_ms must be positive",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := config.Load(writeConfig(t, tc.yaml))
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.wantMsg)
		})
	}
}

func TestLoadMissingFileIsDistinguishable(t *testing.T) {
	_, err := config.Load(filepath.Join(t.TempDir(), "absent.yaml"))
	require.Error(t, err)
	require.ErrorIs(t, err, config.ErrNotFound,
		"callers need to tell 'no config' apart from 'bad config'")
}
