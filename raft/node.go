package raft

import "fmt"

// Node is a Raft replica: a pure state machine.
//
// It performs no I/O, reads no clock, starts no goroutine and holds no lock.
// Logical time advances only through Tick. Every effect it wants to have on the
// world -- persisting, sending, applying -- is described by Ready and carried
// out by the caller, which is what makes the deterministic simulator and the
// real deployment run the same code.
//
// A Node is not safe for concurrent use and is not meant to be. A single owner
// serializes every call, and that ownership is what replaces locking.
type Node struct {
	cfg Config

	id    NodeID
	peers []NodeID

	role     Role
	term     Term
	votedFor NodeID
	lead     NodeID

	log *raftLog

	// votes records responses to the current election. A node counts itself.
	votes map[NodeID]bool

	// progress is the leader's per-follower bookkeeping. It is rebuilt on
	// every election, because a new leader knows nothing about how far the
	// followers got under the previous one.
	progress map[NodeID]*Progress

	electionElapsed  int
	electionTimeout  int
	heartbeatElapsed int

	// msgs is the outbound queue drained by Ready.
	msgs []Message

	// prevHardState is the last state reported through Ready, used to decide
	// whether a new HardState needs persisting.
	prevHardState HardState

	// pendingReady guards against a caller taking two Readys without an
	// Advance between them, which would silently drop work.
	pendingReady bool
}

// New creates a node from cfg. Restored state in cfg (HardState, Entries,
// Applied) is adopted as-is, which is how phase 3 will bring a node back from
// its write-ahead log.
func New(cfg Config) (*Node, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	n := &Node{
		cfg:      cfg,
		id:       cfg.ID,
		peers:    append([]NodeID(nil), cfg.Peers...),
		log:      newLog(cfg.Entries, cfg.Applied),
		votes:    make(map[NodeID]bool, len(cfg.Peers)),
		progress: make(map[NodeID]*Progress, len(cfg.Peers)),
	}

	n.term = cfg.HardState.Term
	n.votedFor = cfg.HardState.VotedFor
	if c := cfg.HardState.Commit; c > 0 {
		n.log.commitTo(c)
	}
	if n.log.applied > n.log.committed {
		return nil, fmt.Errorf("raft: applied index %d exceeds committed %d",
			n.log.applied, n.log.committed)
	}
	n.prevHardState = n.hardState()

	n.becomeFollower(n.term, None)
	return n, nil
}

// ID returns this node's identity.
func (n *Node) ID() NodeID { return n.id }

func (n *Node) hardState() HardState {
	return HardState{Term: n.term, VotedFor: n.votedFor, Commit: n.log.committed}
}

func (n *Node) isLeader() bool { return n.role == Leader }

// resetElectionTimer picks a fresh randomized timeout.
//
// Randomization is what breaks split votes: if every node used the same
// timeout they would all campaign together, split the vote, and repeat. The
// range comes from Config and the draw comes from the injected Rand, so a
// failing trial replays exactly from its seed.
func (n *Node) resetElectionTimer() {
	span := n.cfg.ElectionTimeoutMaxTicks - n.cfg.ElectionTimeoutMinTicks
	n.electionTimeout = n.cfg.ElectionTimeoutMinTicks + n.cfg.Rand(span)
	n.electionElapsed = 0
}

// Tick advances logical time by one.
//
// This is the only way time moves. There is no clock in this package.
func (n *Node) Tick() {
	if n.isLeader() {
		n.heartbeatElapsed++
		if n.heartbeatElapsed >= n.cfg.HeartbeatTimeoutTicks {
			n.heartbeatElapsed = 0
			n.broadcastAppend()
		}
		return
	}

	n.electionElapsed++
	if n.electionElapsed >= n.electionTimeout {
		n.campaign()
	}
}

// Step delivers an inbound message.
//
// The term rule runs first and unconditionally: a message carrying a higher
// term forces an immediate step-down to follower whatever the current role,
// BEFORE the body is looked at. Processing the body first and adjusting the
// term afterwards is a classic way to let a stale leader act on one last
// message it had no right to act on.
func (n *Node) Step(m Message) error {
	switch {
	case m.Term == 0:
		// Local or malformed. Raft messages always carry a term.
		return fmt.Errorf("raft: message %s has no term", m.Type)

	case m.Term > n.term:
		lead := None
		// Only AppendEntries and InstallSnapshot identify a leader. A
		// RequestVote at a higher term means an election is in progress and
		// nobody has won it, so adopting the candidate as leader here would
		// be wrong.
		if m.Type == MsgAppendEntries || m.Type == MsgInstallSnapshot {
			lead = m.From
		}
		n.becomeFollower(m.Term, lead)

	case m.Term < n.term:
		// The sender is behind. Reply so it learns the current term and steps
		// down, rather than silently dropping and letting it keep campaigning.
		n.applyStaleMessageMutation(m)
		n.replyToStaleMessage(m)
		return nil
	}

	switch m.Type {
	case MsgRequestVote:
		n.handleRequestVote(m)
	case MsgRequestVoteResp:
		n.handleRequestVoteResponse(m)
	case MsgAppendEntries:
		n.handleAppendEntries(m)
	case MsgAppendEntriesResp:
		n.handleAppendEntriesResponse(m)
	case MsgInstallSnapshot, MsgInstallSnapshotResp:
		return fmt.Errorf("raft: %s is not implemented until phase 4", m.Type)
	default:
		return fmt.Errorf("raft: unknown message type %s", m.Type)
	}
	return nil
}

// applyStaleMessageMutation is a negative control and does nothing in any
// correct configuration.
//
// With MutationLeaderTruncatesOwnLog it makes a leader drop its own last
// uncommitted entry when a stale AppendEntries arrives -- something a correct
// leader never does, since it must ignore messages from an older term
// entirely. It removes exactly one entry above the commit index so that the
// only property broken is Leader Append-Only.
func (n *Node) applyStaleMessageMutation(m Message) {
	if n.cfg.UnsafeMutation != MutationLeaderTruncatesOwnLog {
		return
	}
	if !n.isLeader() || m.Type != MsgAppendEntries {
		return
	}
	if last := n.log.lastIndex(); last > n.log.committed {
		n.log.truncateFrom(last)
	}
}

// replyToStaleMessage tells a sender with an older term that it is behind.
//
// Only requests get a reply. Responding to a stale response would be an
// infinite exchange between two nodes that each think the other is behind.
func (n *Node) replyToStaleMessage(m Message) {
	switch m.Type {
	case MsgRequestVote:
		n.send(Message{Type: MsgRequestVoteResp, To: m.From, VoteGranted: false})
	case MsgAppendEntries:
		n.send(Message{Type: MsgAppendEntriesResp, To: m.From, Success: false})
	}
}

// send queues an outbound message, stamping it with our identity and term.
func (n *Node) send(m Message) {
	m.From = n.id
	if m.Term == 0 {
		m.Term = n.term
	}
	n.msgs = append(n.msgs, m)
}

func (n *Node) becomeFollower(term Term, lead NodeID) {
	if term > n.term {
		n.term = term
		n.votedFor = None
	}
	n.role = Follower
	n.lead = lead
	n.progress = make(map[NodeID]*Progress, len(n.peers))
	n.votes = make(map[NodeID]bool, len(n.peers))
	n.resetElectionTimer()
}

func (n *Node) becomeCandidate() {
	n.term++
	n.role = Candidate
	n.lead = None
	n.votedFor = n.id
	n.votes = map[NodeID]bool{n.id: true}
	n.progress = make(map[NodeID]*Progress, len(n.peers))
	n.resetElectionTimer()
}

func (n *Node) becomeLeader() {
	n.role = Leader
	n.lead = n.id
	n.heartbeatElapsed = 0

	n.progress = make(map[NodeID]*Progress, len(n.peers))
	next := n.log.lastIndex() + 1
	for _, p := range n.peers {
		n.progress[p] = &Progress{NextIndex: next, MatchIndex: 0}
	}
	// The leader's own match index starts at its durable point, not its last
	// index. See updateSelfProgress for why that distinction matters.
	n.progress[n.id].MatchIndex = n.log.stable

	// Append a no-op for this term. Raft forbids committing an entry from an
	// earlier term by counting replicas (§5.4.2), so without this a new leader
	// cannot learn its true commit index -- and phase 5's ReadIndex cannot
	// serve a linearizable read until it does.
	n.log.append(Entry{Term: n.term, Index: n.log.lastIndex() + 1, Type: EntryNoOp})
	n.broadcastAppend()
}

// campaign starts an election.
func (n *Node) campaign() {
	n.becomeCandidate()

	// A single-node cluster wins immediately; there is nobody to ask.
	if len(n.peers) == 1 {
		n.becomeLeader()
		return
	}

	lastIdx := n.log.lastIndex()
	lastTerm := n.log.lastTerm()
	for _, p := range n.peers {
		if p == n.id {
			continue
		}
		n.send(Message{
			Type:         MsgRequestVote,
			To:           p,
			LastLogIndex: lastIdx,
			LastLogTerm:  lastTerm,
		})
	}
}

// Propose appends a command to the log. Only a leader may propose.
func (n *Node) Propose(typ EntryType, data []byte) (Index, Term, error) {
	if !n.isLeader() {
		return 0, 0, ErrNotLeader
	}
	e := Entry{Term: n.term, Index: n.log.lastIndex() + 1, Type: typ, Data: data}
	n.log.append(e)
	n.broadcastAppend()
	return e.Index, e.Term, nil
}

// HasReady reports whether there is anything to persist, send or apply.
func (n *Node) HasReady() bool {
	if n.pendingReady {
		return false
	}
	return len(n.msgs) > 0 ||
		len(n.log.unstableEntries()) > 0 ||
		len(n.log.nextApplicable()) > 0 ||
		n.hardState() != n.prevHardState
}

// Ready returns everything that must now happen.
//
// The caller must process it in the order documented on Ready and then call
// Advance. Taking a second Ready without an Advance returns an empty one rather
// than silently duplicating work.
func (n *Node) Ready() Ready {
	if n.pendingReady {
		return Ready{}
	}
	n.pendingReady = true

	rd := Ready{
		Entries:          n.log.unstableEntries(),
		Messages:         n.msgs,
		CommittedEntries: n.log.nextApplicable(),
	}
	if hs := n.hardState(); hs != n.prevHardState {
		copied := hs
		rd.HardState = &copied
	}
	n.msgs = nil
	return rd
}

// Advance acknowledges that the previous Ready was fully processed: its entries
// and hard state are durable, its messages are sent, its committed entries are
// applied.
//
// Nothing in the core may depend on an entry being durable before this is
// called, which is exactly why the leader's own match index moves here.
func (n *Node) Advance() {
	if !n.pendingReady {
		return
	}
	n.pendingReady = false

	n.log.stable = n.log.lastIndex()
	if n.log.applied < n.log.committed {
		n.log.applied = n.log.committed
	}
	n.prevHardState = n.hardState()

	if n.isLeader() {
		n.updateSelfProgress()
	}
}

// updateSelfProgress advances the leader's own match index to its durable
// point.
//
// A leader counts itself in the commit quorum. If it counted entries it had
// merely appended in memory, a crash after commit but before fsync could lose
// an entry the cluster believed committed -- the quorum would have been one
// vote short all along. So the leader's match index tracks the stable index,
// not the last index, which costs one Ready cycle of latency and removes the
// window entirely.
//
// etcd historically counted at append time; this is a deliberate divergence,
// recorded in DESIGN.md.
func (n *Node) updateSelfProgress() {
	pr := n.progress[n.id]
	if pr == nil {
		return
	}
	if n.log.stable > pr.MatchIndex {
		pr.MatchIndex = n.log.stable
		pr.NextIndex = pr.MatchIndex + 1
		n.maybeCommit()
	}
}

// Status returns an immutable snapshot of observable state.
func (n *Node) Status() Status {
	s := Status{
		ID:              n.id,
		Role:            n.role,
		Term:            n.term,
		VotedFor:        n.votedFor,
		Leader:          n.lead,
		CommitIndex:     n.log.committed,
		LastApplied:     n.log.applied,
		LastLogIndex:    n.log.lastIndex(),
		LastLogTerm:     n.log.lastTerm(),
		StableIndex:     n.log.stable,
		LogRevision:     n.log.revision,
		ElectionElapsed: n.electionElapsed,
		ElectionTimeout: n.electionTimeout,
	}
	if len(n.progress) > 0 {
		s.Progress = make(map[NodeID]Progress, len(n.progress))
		for id, pr := range n.progress {
			s.Progress[id] = *pr
		}
	}
	return s
}

// LogEntries returns a copy of the whole log.
//
// It is O(n) and exists for inspection: the admin API's log tail and, above
// all, the invariant checkers, which cannot verify Log Matching without seeing
// both logs in full.
func (n *Node) LogEntries() []Entry {
	return append([]Entry(nil), n.log.entries...)
}

// Campaign starts an election immediately, without waiting for the election
// timer.
//
// Real deployments use this to hand leadership over deliberately or to bring a
// fresh cluster up without waiting out a timeout. Tests use it to drive a
// specific node to a specific term, which is what makes scenarios like Figure 8
// constructible rather than hoped for.
//
// It is a no-op on a node that is already leader.
func (n *Node) Campaign() {
	if n.isLeader() {
		return
	}
	n.campaign()
}
