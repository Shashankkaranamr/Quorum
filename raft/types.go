package raft

import (
	"fmt"
	"strconv"
)

// NodeID identifies a cluster member. Zero is never a valid id, so the zero
// value is always distinguishable from a configured one.
type NodeID uint64

// None is the absence of a node: no vote cast, no leader known.
const None NodeID = 0

func (id NodeID) String() string { return strconv.FormatUint(uint64(id), 10) }

// Term is a Raft term: a logical clock that increases monotonically and is
// never reused. Every message carries one.
type Term uint64

// Index is a position in the replicated log. The first real entry is at index
// 1, so index 0 means "before the log begins".
type Index uint64

// Role is the state a node is in. Raft's three roles are exhaustive: a node is
// always exactly one of them.
type Role uint8

// The three roles. A node is always in exactly one of them.
const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	default:
		return "role(" + strconv.Itoa(int(r)) + ")"
	}
}

// EntryType mirrors quorum.raft.v1.EntryType. The numeric values must stay in
// step with the proto: they are written to disk, and changing one makes every
// write-ahead log produced before the change unreadable.
type EntryType uint8

// Entry types. The numeric values are part of the on-disk format.
const (
	// EntryUnspecified is the zero value and is never a valid entry.
	EntryUnspecified EntryType = 0

	// EntryNormal carries a client command.
	EntryNormal EntryType = 1

	// EntryNoOp is the empty entry a leader appends on election. Raft forbids
	// committing an entry from an earlier term by counting replicas (§5.4.2),
	// so a new leader cannot learn its true commit index until it commits
	// something from its own term. This is how it finds out, and it is what
	// ReadIndex depends on in phase 5.
	EntryNoOp EntryType = 2

	// EntrySession registers a client session. The client id is the index
	// the entry lands at.
	EntrySession EntryType = 3

	// EntryConfig is reserved for membership changes and is not implemented.
	// Quorum uses static membership; see DESIGN.md §5.
	EntryConfig EntryType = 4
)

func (t EntryType) String() string {
	switch t {
	case EntryNormal:
		return "normal"
	case EntryNoOp:
		return "noop"
	case EntrySession:
		return "session"
	case EntryConfig:
		return "config"
	default:
		return "entry(" + strconv.Itoa(int(t)) + ")"
	}
}

// Entry is one slot in the replicated log.
//
// Index and Term together identify an entry uniquely across the cluster for all
// time. That pair is what the Log Matching property is stated in terms of, and
// what the AppendEntries consistency check compares.
type Entry struct {
	Term  Term
	Index Index
	Type  EntryType
	Data  []byte
}

func (e Entry) String() string {
	return fmt.Sprintf("%d@%d:%s", e.Index, e.Term, e.Type)
}

// HardState is the state that must be durable before the node acts on it.
// Losing any of it after a crash can violate safety directly: forgetting a vote
// lets a node vote twice in one term, and two leaders in one term follows.
type HardState struct {
	Term     Term
	VotedFor NodeID
	Commit   Index
}

// IsEmpty reports whether hs carries nothing worth persisting.
func (hs HardState) IsEmpty() bool {
	return hs.Term == 0 && hs.VotedFor == None && hs.Commit == 0
}

// MessageType identifies which Raft RPC a Message carries.
//
// The core uses a flat struct with a type tag rather than a sum type, mirroring
// the shape the generated protobuf gives us. internal/pbconv switches on this
// to build the proto oneof.
type MessageType uint8

// Message types.
const (
	// MsgUnspecified is the zero value and is never a valid message.
	MsgUnspecified MessageType = iota
	MsgRequestVote
	MsgRequestVoteResp
	MsgAppendEntries
	MsgAppendEntriesResp

	// MsgInstallSnapshot carries one chunk of the leader's snapshot to a
	// follower whose next entry the leader has already compacted away.
	MsgInstallSnapshot

	// MsgInstallSnapshotResp acknowledges a chunk that did not complete the
	// transfer. The chunk that does complete it is acknowledged with an
	// AppendEntriesResp instead, because from that moment the follower's log
	// agrees with the leader's up to the snapshot and ordinary replication
	// takes over.
	MsgInstallSnapshotResp
)

func (t MessageType) String() string {
	switch t {
	case MsgRequestVote:
		return "RequestVote"
	case MsgRequestVoteResp:
		return "RequestVoteResp"
	case MsgAppendEntries:
		return "AppendEntries"
	case MsgAppendEntriesResp:
		return "AppendEntriesResp"
	case MsgInstallSnapshot:
		return "InstallSnapshot"
	case MsgInstallSnapshotResp:
		return "InstallSnapshotResp"
	default:
		return "Msg(" + strconv.Itoa(int(t)) + ")"
	}
}

// Message is the envelope for every Raft RPC.
//
// Raft messages are one-way: a request and its response are two independent
// messages travelling over two independent directed links, not a call and a
// return. That is what makes one-way partitions expressible, and one-way
// partitions are where the subtler consensus bugs live.
//
// Term is on the envelope because every receiver applies the same rule to it
// before looking at the body: a greater term means step down and adopt it.
type Message struct {
	Type MessageType
	From NodeID
	To   NodeID
	Term Term

	// RequestVote: the candidate's last log entry, for the up-to-date check
	// in §5.4.1 that guarantees Leader Completeness.
	LastLogIndex Index
	LastLogTerm  Term

	// RequestVoteResp.
	VoteGranted bool

	// AppendEntries: the entry immediately preceding Entries. The receiver
	// refuses unless it has exactly this entry, which is the induction step
	// that gives Log Matching.
	PrevLogIndex Index
	PrevLogTerm  Term
	Entries      []Entry
	LeaderCommit Index

	// AppendEntriesResp.
	Success bool

	// MatchIndex is the highest index the follower now agrees with, set only
	// when Success. Sending it rather than inferring it from the request
	// avoids a class of bug where a stale or reordered response advances the
	// leader's view incorrectly -- and the simulator reorders on purpose.
	MatchIndex Index

	// ConflictTerm and ConflictIndex let a leader skip a whole conflicting
	// term per round trip instead of walking back one index at a time (the
	// optimization sketched at the end of §5.3).
	ConflictTerm  Term
	ConflictIndex Index

	// InstallSnapshot and its response. SnapshotMeta names which snapshot a
	// chunk or an acknowledgement belongs to, so a chunk of an older snapshot
	// can never be spliced into a newer one.
	SnapshotMeta   SnapshotMeta
	SnapshotOffset uint64
	SnapshotData   []byte
	SnapshotDone   bool

	// SnapshotBytesReceived is how much of SnapshotMeta's snapshot the
	// follower now holds contiguously. The leader resumes from exactly there,
	// which makes a lost, duplicated or reordered chunk cost a retry rather
	// than a corrupted image.
	SnapshotBytesReceived uint64

	// ReadSeq is the ReadIndex round an AppendEntries belongs to: the leader
	// stamps every AppendEntries with its latest read sequence number, and
	// the follower echoes it on its response. A response from a quorum
	// echoing s proves the leader was still leader after every read up to s
	// was registered. On the wire it is AppendEntries.read_context.
	ReadSeq uint64
}

func (m Message) String() string {
	switch m.Type {
	case MsgRequestVote:
		return fmt.Sprintf("%s %d->%d t%d last=%d@%d",
			m.Type, m.From, m.To, m.Term, m.LastLogIndex, m.LastLogTerm)
	case MsgRequestVoteResp:
		return fmt.Sprintf("%s %d->%d t%d granted=%v", m.Type, m.From, m.To, m.Term, m.VoteGranted)
	case MsgAppendEntries:
		return fmt.Sprintf("%s %d->%d t%d prev=%d@%d n=%d commit=%d read=%d",
			m.Type, m.From, m.To, m.Term, m.PrevLogIndex, m.PrevLogTerm, len(m.Entries), m.LeaderCommit, m.ReadSeq)
	case MsgAppendEntriesResp:
		return fmt.Sprintf("%s %d->%d t%d ok=%v match=%d conflict=%d@%d read=%d",
			m.Type, m.From, m.To, m.Term, m.Success, m.MatchIndex, m.ConflictIndex, m.ConflictTerm, m.ReadSeq)
	case MsgInstallSnapshot:
		return fmt.Sprintf("%s %d->%d t%d snap=%d@%d off=%d n=%d done=%v",
			m.Type, m.From, m.To, m.Term, m.SnapshotMeta.Index, m.SnapshotMeta.Term,
			m.SnapshotOffset, len(m.SnapshotData), m.SnapshotDone)
	case MsgInstallSnapshotResp:
		return fmt.Sprintf("%s %d->%d t%d snap=%d@%d received=%d",
			m.Type, m.From, m.To, m.Term, m.SnapshotMeta.Index, m.SnapshotMeta.Term, m.SnapshotBytesReceived)
	default:
		return fmt.Sprintf("%s %d->%d t%d", m.Type, m.From, m.To, m.Term)
	}
}

// SnapshotMeta identifies the point in the log a snapshot replaces: the last
// entry folded into it. Everything at or below Index is gone from the log and
// lives only in the snapshot.
type SnapshotMeta struct {
	Index Index
	Term  Term
}

// Snapshot is a state machine image and the log position it corresponds to.
//
// Data is opaque to the core. The core holds on to the latest snapshot only so
// that, as leader, it can send it to a follower that has fallen behind the
// compacted prefix; it never looks inside.
type Snapshot struct {
	Meta SnapshotMeta
	Data []byte
}

// IsEmpty reports whether s is the zero snapshot, i.e. nothing is compacted.
func (s Snapshot) IsEmpty() bool { return s.Meta.Index == 0 }

// ReadState is the outcome of a ReadIndex request.
//
// When Lost is false, leadership was confirmed by a quorum after the request
// was made, and the read may be served once the state machine has applied
// Index. When Lost is true the node stopped being leader before it could
// confirm, and the read must not be served here.
type ReadState struct {
	ID    uint64
	Index Index
	Lost  bool
}

// Ready is everything that must happen as a result of the steps taken since the
// last Ready: what to persist, what to send, and what to apply.
//
// The caller decides the ordering, and the ordering is a correctness
// requirement rather than a performance choice. See internal/server.
type Ready struct {
	// Snapshot is non-nil when this node accepted a snapshot from the leader.
	// It must be made durable before anything in Messages is sent -- the
	// follower's acknowledgement is a promise that it holds everything up to
	// the snapshot's index -- and restored into the state machine before
	// CommittedEntries, which follow on from it.
	Snapshot *Snapshot

	// HardState is non-nil when term, vote or commit changed and must be made
	// durable before anything in Messages leaves the process.
	HardState *HardState

	// Entries must be appended to stable storage and fsynced before Messages
	// are sent.
	Entries []Entry

	// Messages must not be sent until Entries and HardState are durable. A
	// granted vote and an accepted AppendEntries are both durable promises.
	Messages []Message

	// CommittedEntries are ready to hand to the state machine.
	CommittedEntries []Entry

	// ReadStates are ReadIndex requests that were confirmed or abandoned.
	// They promise nothing durable, so they may be acted on at any point in
	// the Ready.
	ReadStates []ReadState
}

// IsEmpty reports whether there is nothing to do.
func (rd Ready) IsEmpty() bool {
	return rd.Snapshot == nil &&
		rd.HardState == nil &&
		len(rd.Entries) == 0 &&
		len(rd.Messages) == 0 &&
		len(rd.CommittedEntries) == 0 &&
		len(rd.ReadStates) == 0
}

// SafetyViolation is panicked when the core reaches a state that Raft's safety
// argument says is unreachable: a second leader in one term, or a leader asking
// us to overwrite an entry we have already committed.
//
// Panicking is deliberate. By the time the core notices, a safety property has
// already been broken somewhere upstream, and continuing would turn a
// detectable fault into silent corruption. Property names the invariant that
// was broken so the simulator can attribute it rather than reporting an
// anonymous crash.
type SafetyViolation struct {
	Property string
	Detail   string
}

func (e SafetyViolation) Error() string {
	return "raft: safety violation (" + e.Property + "): " + e.Detail
}

// Property names carried by SafetyViolation. They match the invariant names the
// checkers use, so a panic and a checker failure are attributed identically.
const (
	PropertyElectionSafety            = "ElectionSafety"
	PropertyCommittedEntriesAreStable = "CommittedEntriesAreStable"
)

// Status is an immutable snapshot of a node's observable state.
//
// It is what the admin API serves and what the invariant checkers read. It
// deliberately carries no pointers into live state, so a reader can never
// observe a half-applied change.
type Status struct {
	ID       NodeID
	Role     Role
	Term     Term
	VotedFor NodeID
	Leader   NodeID

	CommitIndex  Index
	LastApplied  Index
	LastLogIndex Index
	LastLogTerm  Term
	StableIndex  Index

	// SnapshotIndex and SnapshotTerm are the last entry folded into this
	// node's snapshot. The log holds only what comes after SnapshotIndex.
	SnapshotIndex Index
	SnapshotTerm  Term

	// LogRevision increments on every log mutation, so an observer can detect
	// change without copying the log.
	LogRevision uint64

	// ElectionElapsed and ElectionTimeout are exposed so a test can assert
	// that the timer is reset only on the two events Raft permits, rather
	// than inferring it from behaviour.
	ElectionElapsed int
	ElectionTimeout int

	// Progress is the leader's per-follower bookkeeping, empty otherwise.
	Progress map[NodeID]Progress
}

// Progress is a leader's view of one follower.
type Progress struct {
	NextIndex  Index
	MatchIndex Index

	// PendingSnapshot is non-zero while the leader is transferring its
	// snapshot to this follower: it is the index of the snapshot being sent.
	// AppendEntries are withheld for the duration, because the follower has
	// nothing they could attach to.
	PendingSnapshot Index

	// SnapshotOffset is how many bytes of that snapshot the follower has
	// acknowledged, and so where the next chunk starts.
	SnapshotOffset uint64

	// SnapshotsSent counts InstallSnapshot chunks sent to this follower
	// during this leadership. It makes "the follower caught up by snapshot"
	// something a test can observe rather than infer.
	SnapshotsSent uint64
}
