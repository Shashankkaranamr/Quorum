// Package config loads and validates the static cluster topology.
//
// Quorum uses static membership: every node learns about every peer from the
// same cluster.yaml, and the membership cannot change while the cluster is
// running. That is a deliberate scope decision documented in DESIGN.md, not an
// oversight.
//
// Validation is strict on purpose. A misconfigured election timeout does not
// produce an error at startup, it produces a cluster that elects leaders
// erratically under load and looks like a consensus bug. Catching it here is
// much cheaper than catching it in phase 6.
package config

import (
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"

	"go.yaml.in/yaml/v3"
)

// NodeID identifies a node. Zero is never a valid id, so the zero value of a
// NodeID is always distinguishable from a configured one.
type NodeID uint64

// Node is one member of the cluster.
type Node struct {
	ID   NodeID `yaml:"id"`
	Host string `yaml:"host"`
	// GRPCPort serves three services on one listener: the Raft peer transport,
	// the client-facing KV API, and the admin/fault-injection API.
	GRPCPort int `yaml:"grpc_port"`
	// HTTPPort serves status and metrics, and is what the visualizer scrapes.
	HTTPPort int `yaml:"http_port"`
}

// GRPCAddr is the dial target for this node's gRPC listener.
func (n Node) GRPCAddr() string {
	return net.JoinHostPort(n.Host, strconv.Itoa(n.GRPCPort))
}

// HTTPAddr is the dial target for this node's status/metrics listener.
func (n Node) HTTPAddr() string {
	return net.JoinHostPort(n.Host, strconv.Itoa(n.HTTPPort))
}

// RaftParams are the consensus timing and sizing knobs.
//
// Timing is expressed in logical ticks rather than milliseconds everywhere
// except TickMS itself. The consensus core has no clock: it counts ticks, and
// the driver decides how long a tick lasts. That is what lets the deterministic
// simulator run a sixty-second election scenario in microseconds.
type RaftParams struct {
	// TickMS is the wall-clock duration of one logical tick, used only by the
	// real driver. The simulator ignores it.
	TickMS int `yaml:"tick_ms"`
	// ElectionTimeoutMinTicks and ElectionTimeoutMaxTicks bound the randomized
	// election timeout. The range must be non-empty: identical timeouts across
	// nodes cause repeated split votes.
	ElectionTimeoutMinTicks int `yaml:"election_timeout_min_ticks"`
	ElectionTimeoutMaxTicks int `yaml:"election_timeout_max_ticks"`
	// HeartbeatTimeoutTicks is how often a leader sends AppendEntries.
	HeartbeatTimeoutTicks int `yaml:"heartbeat_timeout_ticks"`
	// SnapshotThresholdEntries is how many applied entries accumulate past the
	// last snapshot before a new one is taken.
	SnapshotThresholdEntries int `yaml:"snapshot_threshold_entries"`
	// MaxEntriesPerAppend caps the batch size of a single AppendEntries.
	MaxEntriesPerAppend int `yaml:"max_entries_per_append"`
}

// StorageParams configure the write-ahead log and snapshot files.
type StorageParams struct {
	// DataDir is the parent directory; each node gets DataDir/node-<id>.
	DataDir string `yaml:"data_dir"`
	// WALSegmentBytes is the size at which a WAL segment is rolled.
	WALSegmentBytes int64 `yaml:"wal_segment_bytes"`
}

// Cluster is the whole parsed configuration.
type Cluster struct {
	ClusterID string        `yaml:"cluster_id"`
	Nodes     []Node        `yaml:"nodes"`
	Raft      RaftParams    `yaml:"raft"`
	Storage   StorageParams `yaml:"storage"`
}

// Defaults are applied to any field left unset, so a minimal cluster.yaml that
// lists only nodes is valid and sensible.
const (
	DefaultTickMS                   = 50
	DefaultElectionTimeoutMinTicks  = 10
	DefaultElectionTimeoutMaxTicks  = 20
	DefaultHeartbeatTimeoutTicks    = 3
	DefaultSnapshotThresholdEntries = 10000
	DefaultMaxEntriesPerAppend      = 256
	DefaultDataDir                  = "./data"
	DefaultWALSegmentBytes          = 16 << 20 // 16 MiB

	// MinNodes and MaxNodes bound cluster size. The lower bound is a real
	// constraint: a two-node cluster has a quorum of two and therefore
	// tolerates zero failures, which defeats the purpose.
	MinNodes = 3
	MaxNodes = 5

	// MinElectionToHeartbeatRatio encodes Raft's broadcastTime << electionTimeout
	// requirement. If a leader can miss only one or two heartbeats before a
	// follower gives up, ordinary scheduling jitter triggers spurious elections.
	MinElectionToHeartbeatRatio = 3
)

// ErrNotFound is returned by Load when the config file does not exist, so a
// caller can distinguish "no config" from "bad config".
var ErrNotFound = errors.New("config file not found")

// Load reads and validates a cluster configuration from path.
//
// Decoding is strict: an unknown field is an error rather than a silently
// ignored typo, because a misspelled timeout that falls back to a default is
// exactly the kind of thing that gets blamed on the consensus implementation.
func Load(path string) (*Cluster, error) {
	f, err := os.Open(path) //nolint:gosec // operator-supplied config path is intentional
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("%w: %s", ErrNotFound, path)
		}
		return nil, fmt.Errorf("open config: %w", err)
	}
	defer func() { _ = f.Close() }()

	var c Cluster
	dec := yaml.NewDecoder(f)
	dec.KnownFields(true)
	if err := dec.Decode(&c); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}

	c.applyDefaults()
	if err := c.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config %s: %w", path, err)
	}
	return &c, nil
}

func (c *Cluster) applyDefaults() {
	if c.ClusterID == "" {
		c.ClusterID = "quorum-local"
	}
	if c.Raft.TickMS == 0 {
		c.Raft.TickMS = DefaultTickMS
	}
	if c.Raft.ElectionTimeoutMinTicks == 0 {
		c.Raft.ElectionTimeoutMinTicks = DefaultElectionTimeoutMinTicks
	}
	if c.Raft.ElectionTimeoutMaxTicks == 0 {
		c.Raft.ElectionTimeoutMaxTicks = DefaultElectionTimeoutMaxTicks
	}
	if c.Raft.HeartbeatTimeoutTicks == 0 {
		c.Raft.HeartbeatTimeoutTicks = DefaultHeartbeatTimeoutTicks
	}
	if c.Raft.SnapshotThresholdEntries == 0 {
		c.Raft.SnapshotThresholdEntries = DefaultSnapshotThresholdEntries
	}
	if c.Raft.MaxEntriesPerAppend == 0 {
		c.Raft.MaxEntriesPerAppend = DefaultMaxEntriesPerAppend
	}
	if c.Storage.DataDir == "" {
		c.Storage.DataDir = DefaultDataDir
	}
	if c.Storage.WALSegmentBytes == 0 {
		c.Storage.WALSegmentBytes = DefaultWALSegmentBytes
	}
}

// Validate reports the first thing wrong with the configuration, or nil.
func (c *Cluster) Validate() error {
	if c.ClusterID == "" {
		return errors.New("cluster_id must not be empty")
	}
	if len(c.Nodes) < MinNodes || len(c.Nodes) > MaxNodes {
		return fmt.Errorf("cluster must have between %d and %d nodes, got %d",
			MinNodes, MaxNodes, len(c.Nodes))
	}

	seenID := make(map[NodeID]bool, len(c.Nodes))
	seenPort := make(map[string]NodeID, len(c.Nodes)*2)
	for i, n := range c.Nodes {
		if n.ID == 0 {
			return fmt.Errorf("nodes[%d]: id must be greater than zero", i)
		}
		if seenID[n.ID] {
			return fmt.Errorf("nodes[%d]: duplicate node id %d", i, n.ID)
		}
		seenID[n.ID] = true

		if n.Host == "" {
			return fmt.Errorf("nodes[%d] (id %d): host must not be empty", i, n.ID)
		}
		// Iterated as a slice rather than a map so the error reported for a
		// node with two bad ports is deterministic.
		for _, p := range []struct {
			label string
			port  int
		}{{"grpc_port", n.GRPCPort}, {"http_port", n.HTTPPort}} {
			if p.port < 1 || p.port > 65535 {
				return fmt.Errorf("nodes[%d] (id %d): %s %d out of range 1-65535",
					i, n.ID, p.label, p.port)
			}
			key := net.JoinHostPort(n.Host, strconv.Itoa(p.port))
			if owner, dup := seenPort[key]; dup {
				return fmt.Errorf("nodes[%d] (id %d): %s %s already used by node %d",
					i, n.ID, p.label, key, owner)
			}
			seenPort[key] = n.ID
		}
	}

	return c.Raft.validate()
}

func (r RaftParams) validate() error {
	if r.TickMS <= 0 {
		return fmt.Errorf("raft.tick_ms must be positive, got %d", r.TickMS)
	}
	if r.HeartbeatTimeoutTicks < 1 {
		return fmt.Errorf("raft.heartbeat_timeout_ticks must be at least 1, got %d",
			r.HeartbeatTimeoutTicks)
	}
	if r.ElectionTimeoutMinTicks >= r.ElectionTimeoutMaxTicks {
		return fmt.Errorf(
			"raft.election_timeout_min_ticks (%d) must be strictly less than max (%d): "+
				"an empty randomization range causes repeated split votes",
			r.ElectionTimeoutMinTicks, r.ElectionTimeoutMaxTicks)
	}
	if want := r.HeartbeatTimeoutTicks * MinElectionToHeartbeatRatio; r.ElectionTimeoutMinTicks < want {
		return fmt.Errorf(
			"raft.election_timeout_min_ticks (%d) must be at least %dx "+
				"raft.heartbeat_timeout_ticks (%d), i.e. >= %d: "+
				"otherwise ordinary jitter triggers spurious elections",
			r.ElectionTimeoutMinTicks, MinElectionToHeartbeatRatio, r.HeartbeatTimeoutTicks, want)
	}
	if r.SnapshotThresholdEntries <= 0 {
		return fmt.Errorf("raft.snapshot_threshold_entries must be positive, got %d",
			r.SnapshotThresholdEntries)
	}
	if r.MaxEntriesPerAppend <= 0 {
		return fmt.Errorf("raft.max_entries_per_append must be positive, got %d",
			r.MaxEntriesPerAppend)
	}
	return nil
}

// Warnings returns advisory messages about configurations that are legal but
// probably not what the operator wanted. These never block startup.
func (c *Cluster) Warnings() []string {
	var w []string
	if len(c.Nodes)%2 == 0 {
		w = append(w, fmt.Sprintf(
			"cluster size %d is even: quorum is %d, so it tolerates %d failure(s), "+
				"the same as a cluster of %d. Prefer an odd size.",
			len(c.Nodes), c.Quorum(), len(c.Nodes)-c.Quorum(), len(c.Nodes)-1))
	}
	return w
}

// Quorum is the number of nodes that must agree for an entry to commit.
func (c *Cluster) Quorum() int { return len(c.Nodes)/2 + 1 }

// Node returns the node with the given id.
func (c *Cluster) Node(id NodeID) (Node, bool) {
	for _, n := range c.Nodes {
		if n.ID == id {
			return n, true
		}
	}
	return Node{}, false
}

// Peers returns every node except self, in configuration order.
func (c *Cluster) Peers(self NodeID) []Node {
	peers := make([]Node, 0, len(c.Nodes)-1)
	for _, n := range c.Nodes {
		if n.ID != self {
			peers = append(peers, n)
		}
	}
	return peers
}

// IDs returns every configured node id, in configuration order.
func (c *Cluster) IDs() []NodeID {
	ids := make([]NodeID, 0, len(c.Nodes))
	for _, n := range c.Nodes {
		ids = append(ids, n.ID)
	}
	return ids
}

// DataDir is where node id keeps its write-ahead log and snapshots.
func (c *Cluster) DataDir(id NodeID) string {
	return filepath.Join(c.Storage.DataDir, fmt.Sprintf("node-%d", id))
}
