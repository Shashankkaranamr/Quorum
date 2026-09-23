package testutil

import (
	"fmt"
	"math/rand/v2"
	"slices"

	"github.com/Shashankkaranamr/Quorum/internal/server"
	"github.com/Shashankkaranamr/Quorum/internal/storage"
	"github.com/Shashankkaranamr/Quorum/internal/transport/inmem"
	"github.com/Shashankkaranamr/Quorum/raft"
)

// Options configure a simulated cluster.
type Options struct {
	// N is the cluster size. Node ids are 1..N.
	N int

	// Seed drives everything nondeterministic: election jitter, message
	// delays, drops, duplication, reordering. A failing trial reproduces from
	// this value alone.
	Seed uint64

	Fault inmem.Fault

	ElectionTimeoutMinTicks int
	ElectionTimeoutMaxTicks int
	HeartbeatTimeoutTicks   int
	MaxEntriesPerAppend     int

	// SyncCostTicks makes a node's fsync take logical ticks, which is how the
	// simulator reproduces the head-of-line-blocking hazard: the driver cannot
	// process ticks while storage is busy, so tick lag becomes non-zero.
	SyncCostTicks int

	// UnsafeMutation deliberately breaks a Raft safety rule on every node. It
	// exists only so the invariant checkers can be shown to catch a specific
	// broken rule. See raft.Mutation.
	UnsafeMutation raft.Mutation

	// InitialEntries and InitialHardState construct a specific starting
	// history, which is what makes a scenario like Figure 8 buildable rather
	// than something one hopes a random trial stumbles into.
	InitialEntries   map[raft.NodeID][]raft.Entry
	InitialHardState map[raft.NodeID]raft.HardState
}

// DefaultOptions returns timings matching the shipped cluster.yaml, scaled so
// that trials are short: an election takes 10-20 ticks.
func DefaultOptions(n int, seed uint64) Options {
	return Options{
		N:                       n,
		Seed:                    seed,
		ElectionTimeoutMinTicks: 10,
		ElectionTimeoutMaxTicks: 20,
		HeartbeatTimeoutTicks:   3,
		MaxEntriesPerAppend:     64,
	}
}

// Replica is one simulated node: the core, its driver, its storage and its
// state machine.
type Replica struct {
	ID     raft.NodeID
	Driver *server.Driver
	Store  *storage.MemStorage
	SM     *Recorder

	// Down means the process is stopped: it receives nothing, ticks not at
	// all, and its buffered-but-unsynced storage is lost on restart.
	Down bool

	busyUntil uint64
	rng       *rand.Rand
}

// Node returns the underlying core.
func (r *Replica) Node() *raft.Node { return r.Driver.Node() }

// Recorder is a StateMachine that records what was applied, and reports every
// application to the checker so State Machine Safety can be verified.
type Recorder struct {
	id      raft.NodeID
	cluster *Cluster
	Applied []raft.Entry
}

// Apply implements server.StateMachine.
func (r *Recorder) Apply(entries []raft.Entry) error {
	for _, e := range entries {
		if n := len(r.Applied); n > 0 && e.Index != r.Applied[n-1].Index+1 {
			return fmt.Errorf("node %d applied index %d after %d: entries must arrive in order",
				r.id, e.Index, r.Applied[n-1].Index)
		}
		r.Applied = append(r.Applied, e)
		r.cluster.Checker.RecordApply(r.cluster.tick, r.id, e)
	}
	return nil
}

// Cluster is a deterministic simulated Raft cluster.
//
// It owns every node, steps them from a single goroutine, and checks the safety
// invariants after every tick. There is no concurrency and no wall clock
// anywhere in it: a scenario that would take a minute of real elections runs in
// microseconds, and a failure replays exactly from its seed.
type Cluster struct {
	Opts     Options
	IDs      []raft.NodeID
	Replicas map[raft.NodeID]*Replica
	Net      *inmem.Network
	Checker  *Checker

	tick uint64

	// cachedLog avoids copying every node's log on every tick. The core
	// reports a revision counter, so an unchanged log is detected in O(1).
	cachedLog map[raft.NodeID][]raft.Entry
	cachedRev map[raft.NodeID]uint64

	// describeCalls counts renders, so a test can assert none happen on a
	// passing run.
	describeCalls int

	// StepErrors collects errors returned by the core. Any entry here is a
	// bug: the simulator never generates a message the core cannot handle.
	StepErrors []error
}

// NewCluster builds a cluster and brings every node up as a follower.
func NewCluster(opts Options) (*Cluster, error) {
	if opts.N < 1 {
		return nil, fmt.Errorf("testutil: cluster size must be at least 1, got %d", opts.N)
	}

	ids := make([]raft.NodeID, opts.N)
	for i := range ids {
		ids[i] = raft.NodeID(i + 1)
	}

	c := &Cluster{
		Opts:      opts,
		IDs:       ids,
		Replicas:  make(map[raft.NodeID]*Replica, opts.N),
		Net:       inmem.New(opts.Seed, ids, opts.Fault),
		Checker:   NewChecker(),
		cachedLog: make(map[raft.NodeID][]raft.Entry, opts.N),
		cachedRev: make(map[raft.NodeID]uint64, opts.N),
	}

	for _, id := range ids {
		r, err := c.newReplica(id, storage.NewMem())
		if err != nil {
			return nil, err
		}
		c.Replicas[id] = r
	}
	return c, nil
}

// newReplica constructs one node. Each node gets its own random stream derived
// from the cluster seed and its id, so that adding or removing a draw elsewhere
// does not reshuffle a node's election timeouts.
func (c *Cluster) newReplica(id raft.NodeID, store *storage.MemStorage) (*Replica, error) {
	rng := rand.New(rand.NewPCG(c.Opts.Seed, uint64(id)*0x9e3779b97f4a7c15+1))

	hs, ents, applied, err := store.InitialState()
	if err != nil {
		return nil, err
	}
	if len(ents) == 0 {
		ents = c.Opts.InitialEntries[id]
	}
	if hs.IsEmpty() {
		hs = c.Opts.InitialHardState[id]
	}

	cfg := raft.Config{
		ID:                      id,
		Peers:                   c.IDs,
		ElectionTimeoutMinTicks: c.Opts.ElectionTimeoutMinTicks,
		ElectionTimeoutMaxTicks: c.Opts.ElectionTimeoutMaxTicks,
		HeartbeatTimeoutTicks:   c.Opts.HeartbeatTimeoutTicks,
		MaxEntriesPerAppend:     c.Opts.MaxEntriesPerAppend,
		Rand: func(n int) int {
			if n <= 0 {
				return 0
			}
			return rng.IntN(n)
		},
		HardState:      hs,
		Entries:        append([]raft.Entry(nil), ents...),
		Applied:        applied,
		UnsafeMutation: c.Opts.UnsafeMutation,
	}

	node, err := raft.New(cfg)
	if err != nil {
		return nil, err
	}

	store.SyncCostTicks = c.Opts.SyncCostTicks
	rec := &Recorder{id: id, cluster: c}
	r := &Replica{
		ID:     id,
		Driver: server.New(node, store, c.Net.TransportFor(id), rec),
		Store:  store,
		SM:     rec,
		rng:    rng,
	}
	return r, nil
}

// Tick advances the whole cluster by one logical tick.
//
// The order within a tick is fixed, because a simulator whose own scheduling
// varies is not reproducible:
//
//  1. logical time advances
//  2. due messages are delivered
//  3. every live node ticks and processes one Ready, in ascending id order
//  4. the invariant checkers run
func (c *Cluster) Tick() {
	c.tick++
	c.Net.Tick()

	for _, m := range c.Net.Deliver() {
		r := c.Replicas[m.To]
		if r == nil || r.Down {
			continue
		}
		c.guard(m.To, fmt.Sprintf("stepping %s", m), func() error { return r.Driver.Step(m) })
	}

	for _, id := range c.IDs {
		r := c.Replicas[id]
		if r.Down {
			continue
		}
		r.Driver.Tick()

		// A node whose storage is mid-fsync cannot process anything. Ticks
		// keep arriving and pile up, which is exactly the lag the metric is
		// there to measure.
		if r.busyUntil > c.tick {
			continue
		}

		before := r.Store.SyncCount
		c.guard(id, "processing ready", func() error { return r.Driver.Run() })
		if r.Store.SyncCount > before && c.Opts.SyncCostTicks > 0 {
			r.busyUntil = c.tick + uint64(c.Opts.SyncCostTicks)
		}
	}

	c.Check()
}

// guard runs one interaction with the core, turning a SafetyViolation panic
// into an attributed invariant violation.
//
// The core panics when it reaches a state Raft's safety argument says is
// unreachable -- a second leader in a term, or a leader asking it to overwrite
// a committed entry. Those are real detections, and the checkers should report
// them by name rather than the run dying with an anonymous stack trace. Any
// other panic is a genuine bug and is left to propagate.
func (c *Cluster) guard(id raft.NodeID, what string, fn func() error) {
	defer func() {
		r := recover()
		if r == nil {
			return
		}
		sv, ok := r.(raft.SafetyViolation)
		if !ok {
			panic(r)
		}
		c.Checker.fail(c.tick, Invariant(sv.Property),
			"node %d detected it while %s: %s", id, what, sv.Detail)
	}()
	if err := fn(); err != nil {
		c.StepErrors = append(c.StepErrors,
			fmt.Errorf("tick %d: node %d %s: %w", c.tick, id, what, err))
	}
}

// Check runs the invariant checkers against the current state.
func (c *Cluster) Check() {
	views := make(map[raft.NodeID]NodeView, len(c.IDs))
	live := make([]raft.NodeID, 0, len(c.IDs))
	for _, id := range c.IDs {
		r := c.Replicas[id]
		if r.Down {
			continue
		}
		st := r.Node().Status()
		log, ok := c.cachedLog[id]
		if !ok || c.cachedRev[id] != st.LogRevision {
			log = r.Node().LogEntries()
			c.cachedLog[id] = log
			c.cachedRev[id] = st.LogRevision
		}
		views[id] = NodeView{Status: st, Log: log}
		live = append(live, id)
	}
	c.Checker.Observe(c.tick, views, live)
}

// RunTicks advances the cluster by n ticks.
func (c *Cluster) RunTicks(n int) {
	for range n {
		c.Tick()
	}
}

// RunUntil advances the cluster until cond holds or maxTicks elapse. It
// reports whether cond became true, and how many ticks it took.
func (c *Cluster) RunUntil(maxTicks int, cond func() bool) (bool, int) {
	for i := range maxTicks {
		if cond() {
			return true, i
		}
		c.Tick()
	}
	return cond(), maxTicks
}

// Now is the current logical tick.
func (c *Cluster) Now() uint64 { return c.tick }

// Leader returns the single live leader, if there is exactly one.
//
// Two live nodes claiming leadership in DIFFERENT terms is normal and
// transient: a deposed leader that has not heard the news yet still believes it
// leads. Only the highest-term leader counts as current.
func (c *Cluster) Leader() (raft.NodeID, bool) {
	var best raft.NodeID
	var bestTerm raft.Term
	for _, id := range c.IDs {
		r := c.Replicas[id]
		if r.Down {
			continue
		}
		s := r.Node().Status()
		if s.Role != raft.Leader {
			continue
		}
		if best == raft.None || s.Term > bestTerm {
			best, bestTerm = id, s.Term
		}
	}
	return best, best != raft.None
}

// LeaderIn returns the leader among a subset of nodes.
func (c *Cluster) LeaderIn(ids []raft.NodeID) (raft.NodeID, bool) {
	var best raft.NodeID
	var bestTerm raft.Term
	for _, id := range ids {
		r := c.Replicas[id]
		if r == nil || r.Down {
			continue
		}
		s := r.Node().Status()
		if s.Role == raft.Leader && (best == raft.None || s.Term > bestTerm) {
			best, bestTerm = id, s.Term
		}
	}
	return best, best != raft.None
}

// Campaign forces a node to start an election immediately.
func (c *Cluster) Campaign(id raft.NodeID) {
	if r := c.Replicas[id]; r != nil && !r.Down {
		r.Node().Campaign()
	}
}

// Propose appends a command through a node, which must be the leader.
func (c *Cluster) Propose(id raft.NodeID, data []byte) (raft.Index, raft.Term, error) {
	r := c.Replicas[id]
	if r == nil || r.Down {
		return 0, 0, fmt.Errorf("testutil: node %d is not running", id)
	}
	return r.Driver.Propose(raft.EntryNormal, data)
}

// ProposeToLeader appends a command through whichever node currently leads.
func (c *Cluster) ProposeToLeader(data []byte) (raft.Index, raft.Term, error) {
	id, ok := c.Leader()
	if !ok {
		return 0, 0, fmt.Errorf("testutil: no leader")
	}
	return c.Propose(id, data)
}

// Crash stops a node abruptly.
//
// Anything buffered in storage but not yet synced is lost, exactly as it would
// be on a real node killed with SIGKILL. The node keeps its durable state so
// Restart can bring it back from it.
func (c *Cluster) Crash(id raft.NodeID) {
	r := c.Replicas[id]
	if r == nil || r.Down {
		return
	}
	r.Down = true
	r.Store.DropPending()
	delete(c.Checker.leaderLog, id)
}

// Restart brings a crashed node back from its durable state.
func (c *Cluster) Restart(id raft.NodeID) error {
	r := c.Replicas[id]
	if r == nil || !r.Down {
		return nil
	}
	fresh, err := c.newReplica(id, r.Store)
	if err != nil {
		return err
	}
	// The state machine is rebuilt from the log on restart, which in phase 2
	// means replaying everything. Phase 4's snapshots are what stop that being
	// the only option.
	fresh.SM.Applied = nil
	c.Replicas[id] = fresh
	delete(c.cachedLog, id)
	delete(c.cachedRev, id)
	return nil
}

// Partition splits the cluster. Messages between groups are dropped in both
// directions until Heal.
func (c *Cluster) Partition(groups ...[]raft.NodeID) { c.Net.Partition(groups...) }

// Heal removes every injected partition.
func (c *Cluster) Heal() { c.Net.Heal() }

// Isolate cuts every link to and from a node.
func (c *Cluster) Isolate(id raft.NodeID) { c.Net.Isolate(id) }

// Status returns a node's observable state.
func (c *Cluster) Status(id raft.NodeID) raft.Status { return c.Replicas[id].Node().Status() }

// Log returns a copy of a node's log.
func (c *Cluster) Log(id raft.NodeID) []raft.Entry { return c.Replicas[id].Node().LogEntries() }

// Applied returns what a node's state machine has applied.
func (c *Cluster) Applied(id raft.NodeID) []raft.Entry { return c.Replicas[id].SM.Applied }

// LiveIDs returns the ids of nodes that are running, in ascending order.
func (c *Cluster) LiveIDs() []raft.NodeID {
	out := make([]raft.NodeID, 0, len(c.IDs))
	for _, id := range c.IDs {
		if !c.Replicas[id].Down {
			out = append(out, id)
		}
	}
	return out
}

// MinCommitIndex is the lowest commit index across a set of nodes.
func (c *Cluster) MinCommitIndex(ids []raft.NodeID) raft.Index {
	var lowest raft.Index
	first := true
	for _, id := range ids {
		r := c.Replicas[id]
		if r == nil || r.Down {
			continue
		}
		ci := r.Node().Status().CommitIndex
		if first || ci < lowest {
			lowest, first = ci, false
		}
	}
	return lowest
}

// Err reports any invariant violation or internal error from the run.
func (c *Cluster) Err() error {
	if len(c.StepErrors) > 0 {
		return fmt.Errorf("simulator errors: %v", c.StepErrors)
	}
	return c.Checker.Err()
}

// String makes Cluster a fmt.Stringer so it can be passed directly as a
// failure-message argument.
//
// This matters more than it looks. Go evaluates a function call argument
// eagerly, so `require.Truef(t, ok, "...%s", c.Describe())` renders the whole
// cluster on every PASSING assertion too. Passing `c` instead defers the work
// to fmt, which only runs when the assertion actually fails.
func (c *Cluster) String() string { return c.Describe() }

// DescribeCalls counts how many times the cluster has been rendered. A passing
// test should never render it; see TestFailureMessagesAreLazy.
func (c *Cluster) DescribeCalls() int { return c.describeCalls }

// Describe renders the cluster state, for a failure message that a human has
// to diagnose from.
func (c *Cluster) Describe() string {
	c.describeCalls++
	out := fmt.Sprintf("cluster n=%d seed=%d tick=%d\n", c.Opts.N, c.Opts.Seed, c.tick)
	for _, id := range c.IDs {
		r := c.Replicas[id]
		if r.Down {
			out += fmt.Sprintf("  node %d: DOWN\n", id)
			continue
		}
		s := r.Node().Status()
		out += fmt.Sprintf("  node %d: %-9s term=%-3d commit=%-3d applied=%-3d last=%d@%d log=%v\n",
			id, s.Role, s.Term, s.CommitIndex, s.LastApplied, s.LastLogIndex, s.LastLogTerm,
			summarize(r.Node().LogEntries()))
	}
	out += fmt.Sprintf("  net: sent=%d delivered=%d dropped=%d dup=%d partitioned=%d inflight=%d\n",
		c.Net.Stats.Sent, c.Net.Stats.Delivered, c.Net.Stats.Dropped,
		c.Net.Stats.Duplicated, c.Net.Stats.Partitioned, c.Net.InFlight())
	return out
}

func summarize(ents []raft.Entry) []string {
	out := make([]string, 0, len(ents))
	for _, e := range ents {
		out = append(out, fmt.Sprintf("%d@%d", e.Index, e.Term))
	}
	if len(out) > 12 {
		return append(slices.Clone(out[:6]), append([]string{"..."}, out[len(out)-5:]...)...)
	}
	return out
}
