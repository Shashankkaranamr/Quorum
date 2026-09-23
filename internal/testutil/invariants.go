package testutil

import (
	"fmt"

	"github.com/Shashankkaranamr/Quorum/raft"
)

// Invariant names one of the properties Raft must never violate. Mutation
// tests assert on these by name, so that "the checkers have teeth" means a
// specific checker caught a specific broken rule, not that something somewhere
// went wrong.
type Invariant string

const (
	// ElectionSafety holds when at most one leader is elected in a given
	// term (§5.2).
	ElectionSafety Invariant = "ElectionSafety"

	// LeaderAppendOnly holds when a leader never overwrites or deletes entries
	// in its own log, only appends to it (§5.3).
	LeaderAppendOnly Invariant = "LeaderAppendOnly"

	// LogMatching holds when, for any two logs containing an entry with the
	// same index and term, the logs are identical in all preceding
	// entries (§5.3).
	LogMatching Invariant = "LogMatching"

	// LeaderCompleteness holds when an entry committed in a term is present in
	// the log of every leader of every higher term (§5.4).
	LeaderCompleteness Invariant = "LeaderCompleteness"

	// StateMachineSafety holds when, once a node has applied an entry at an
	// index, no other node ever applies a different entry at that
	// index (§5.4.3).
	StateMachineSafety Invariant = "StateMachineSafety"

	// CommittedEntriesAreStable is not one of the five properties in the
	// paper; it is the direct, observable form of "a committed entry is never
	// altered". Two nodes whose commit index covers index i must believe the
	// same entry is committed there. It fires earlier and more loudly than
	// StateMachineSafety, because a node can disagree about what is committed
	// before it ever gets around to applying it.
	CommittedEntriesAreStable Invariant = "CommittedEntriesAreStable"

	// WellFormed catches internal contradictions that mean a bug regardless of
	// the safety properties: applied past committed, committed past the end of
	// the log.
	WellFormed Invariant = "WellFormed"
)

// Violation is a single invariant failure, with enough context to reproduce it.
type Violation struct {
	Invariant Invariant
	Tick      uint64
	Detail    string
}

func (v Violation) String() string {
	return fmt.Sprintf("[tick %d] %s: %s", v.Tick, v.Invariant, v.Detail)
}

// committedRecord is what the cluster was observed to believe about an index.
type committedRecord struct {
	entry raft.Entry
	// term is the term of the node that first reported this index committed.
	// It is an upper bound on the term in which the entry actually committed,
	// which makes the Leader Completeness check conservative: it may check
	// fewer leaders than it strictly could, never more.
	term raft.Term
}

type appliedRecord struct {
	entry raft.Entry
	by    raft.NodeID
}

// Checker accumulates the history of a run and verifies Raft's safety
// properties against it.
//
// Two of the properties are inherently historical -- Leader Completeness talks
// about all future leaders, State Machine Safety about all nodes ever -- so
// checking only the final state would miss most violations. The checker
// therefore runs after every step AND retains the history, so an end-of-trial
// pass can assert the properties that only make sense over time.
type Checker struct {
	violations []Violation

	// leaderByTerm is the observed leader of each term. A second, different
	// node claiming the same term is an Election Safety violation.
	leaderByTerm map[raft.Term]raft.NodeID

	// leaderLog snapshots a node's log while it holds leadership, so that a
	// later observation can confirm the log only grew.
	leaderLog map[raft.NodeID]leaderLogSnapshot

	// committed is what the cluster has been observed to believe is committed
	// at each index, and applied is what has actually reached a state machine.
	committed map[raft.Index]committedRecord
	applied   map[raft.Index]appliedRecord

	// leadersSeen records which (node, term) pairs have already had their
	// Leader Completeness check run, so it runs once per election rather than
	// once per tick.
	leadersChecked map[leaderTerm]bool

	// prevRev is each node's last-seen log revision, so the pairwise Log
	// Matching scan only runs for nodes whose log actually changed. Without
	// this the check is the dominant cost of a randomized trial.
	prevRev map[raft.NodeID]logRevision

	// committedSeen is the highest index already recorded for each node, so
	// recordCommitted walks only what is new rather than the whole prefix on
	// every tick.
	committedSeen map[raft.NodeID]raft.Index

	// MaxCommitted is the highest index ever observed committed anywhere.
	MaxCommitted raft.Index
}

type leaderTerm struct {
	node raft.NodeID
	term raft.Term
}

type leaderLogSnapshot struct {
	term raft.Term
	rev  logRevision
	log  []raft.Entry
}

// logRevision identifies a log version cheaply. The revision counter alone
// would do, except that a restarted node begins again from zero, so the last
// entry is carried along as a guard.
type logRevision struct {
	rev       uint64
	lastIndex raft.Index
	lastTerm  raft.Term
}

func revisionOf(s raft.Status) logRevision {
	return logRevision{rev: s.LogRevision, lastIndex: s.LastLogIndex, lastTerm: s.LastLogTerm}
}

// NewChecker returns an empty checker.
func NewChecker() *Checker {
	return &Checker{
		leaderByTerm:   make(map[raft.Term]raft.NodeID),
		leaderLog:      make(map[raft.NodeID]leaderLogSnapshot),
		committed:      make(map[raft.Index]committedRecord),
		applied:        make(map[raft.Index]appliedRecord),
		leadersChecked: make(map[leaderTerm]bool),
		prevRev:        make(map[raft.NodeID]logRevision),
		committedSeen:  make(map[raft.NodeID]raft.Index),
	}
}

// Violations returns every violation found so far.
func (c *Checker) Violations() []Violation { return c.violations }

// Err returns a non-nil error if anything was violated.
func (c *Checker) Err() error {
	if len(c.violations) == 0 {
		return nil
	}
	msg := fmt.Sprintf("%d invariant violation(s):", len(c.violations))
	for i, v := range c.violations {
		if i == 8 {
			msg += fmt.Sprintf("\n  ... and %d more", len(c.violations)-i)
			break
		}
		msg += "\n  " + v.String()
	}
	return fmt.Errorf("%s", msg)
}

// Violated reports whether a specific invariant was violated. Mutation tests
// use it to assert that the intended checker fired.
func (c *Checker) Violated(inv Invariant) bool {
	for _, v := range c.violations {
		if v.Invariant == inv {
			return true
		}
	}
	return false
}

func (c *Checker) fail(tick uint64, inv Invariant, format string, args ...any) {
	c.violations = append(c.violations, Violation{
		Invariant: inv,
		Tick:      tick,
		Detail:    fmt.Sprintf(format, args...),
	})
}

// NodeView is one node's observable state at one instant.
type NodeView struct {
	Status raft.Status
	Log    []raft.Entry
}

// Observe records the state of every live node at one tick and checks every
// property that can be checked from a single instant plus the history so far.
func (c *Checker) Observe(tick uint64, views map[raft.NodeID]NodeView, order []raft.NodeID) {
	for _, id := range order {
		v, ok := views[id]
		if !ok {
			continue
		}
		c.checkWellFormed(tick, id, v)
		c.checkElectionSafety(tick, id, v)
		c.checkLeaderAppendOnly(tick, id, v)
		c.recordCommitted(tick, id, v)
	}
	c.checkLogMatching(tick, views, order)

	// Leader Completeness is checked when a node becomes leader, because that
	// is the moment the property makes a claim: a new leader must already hold
	// everything committed in every earlier term.
	for _, id := range order {
		v, ok := views[id]
		if !ok || v.Status.Role != raft.Leader {
			continue
		}
		key := leaderTerm{id, v.Status.Term}
		if c.leadersChecked[key] {
			continue
		}
		c.leadersChecked[key] = true
		c.checkLeaderCompleteness(tick, id, v)
	}
}

func (c *Checker) checkWellFormed(tick uint64, id raft.NodeID, v NodeView) {
	s := v.Status
	if s.LastApplied > s.CommitIndex {
		c.fail(tick, WellFormed, "node %d applied %d past committed %d",
			id, s.LastApplied, s.CommitIndex)
	}
	if s.CommitIndex > s.LastLogIndex {
		c.fail(tick, WellFormed, "node %d committed %d past last log index %d",
			id, s.CommitIndex, s.LastLogIndex)
	}
}

func (c *Checker) checkElectionSafety(tick uint64, id raft.NodeID, v NodeView) {
	if v.Status.Role != raft.Leader {
		return
	}
	term := v.Status.Term
	if prev, ok := c.leaderByTerm[term]; ok && prev != id {
		c.fail(tick, ElectionSafety,
			"two leaders in term %d: node %d and node %d", term, prev, id)
		return
	}
	c.leaderByTerm[term] = id
}

func (c *Checker) checkLeaderAppendOnly(tick uint64, id raft.NodeID, v NodeView) {
	if v.Status.Role != raft.Leader {
		delete(c.leaderLog, id)
		return
	}

	prev, had := c.leaderLog[id]
	rev := revisionOf(v.Status)
	if had && prev.term == v.Status.Term && prev.rev == rev {
		// Nothing changed since the last look, so there is nothing to compare
		// and no reason to copy the log again.
		return
	}
	if had && prev.term == v.Status.Term {
		// Still leader in the same term: the old log must be a prefix of the
		// new one. Anything else means the leader overwrote or deleted its own
		// entries.
		if len(prev.log) > len(v.Log) {
			c.fail(tick, LeaderAppendOnly,
				"leader %d (term %d) log shrank from %d to %d entries",
				id, v.Status.Term, len(prev.log), len(v.Log))
		} else {
			for i, e := range prev.log {
				if v.Log[i].Index != e.Index || v.Log[i].Term != e.Term {
					c.fail(tick, LeaderAppendOnly,
						"leader %d (term %d) rewrote its own entry at position %d: was %s, now %s",
						id, v.Status.Term, i, e, v.Log[i])
					break
				}
			}
		}
	}

	c.leaderLog[id] = leaderLogSnapshot{
		term: v.Status.Term,
		rev:  rev,
		log:  append([]raft.Entry(nil), v.Log...),
	}
}

// checkLogMatching verifies the pairwise property.
//
// Stated as: if two logs hold an entry with the same index and term, every
// preceding entry is identical. The efficient equivalent used here is that
// beyond the longest common prefix of the two logs, no index may carry the same
// term in both -- if it did, the prefixes would have had to match and do not.
//
// Only nodes whose log changed since the last observation are re-checked
// against the rest. Logs change rarely relative to the tick rate, and without
// this the pairwise scan dominates the cost of a randomized trial.
func (c *Checker) checkLogMatching(tick uint64, views map[raft.NodeID]NodeView, order []raft.NodeID) {
	var changed []raft.NodeID
	for _, id := range order {
		v, ok := views[id]
		if !ok {
			continue
		}
		if rev := revisionOf(v.Status); c.prevRev[id] != rev {
			c.prevRev[id] = rev
			changed = append(changed, id)
		}
	}
	if len(changed) == 0 {
		return
	}

	for _, a := range changed {
		va := views[a]
		for _, b := range order {
			if a == b {
				continue
			}
			vb, ok := views[b]
			if !ok {
				continue
			}
			c.compareLogs(tick, a, va.Log, b, vb.Log)
		}
	}
}

func (c *Checker) compareLogs(tick uint64, a raft.NodeID, la []raft.Entry, b raft.NodeID, lb []raft.Entry) {
	n := min(len(la), len(lb))

	common := 0
	for common < n && la[common].Index == lb[common].Index && la[common].Term == lb[common].Term {
		common++
	}

	for i := common; i < n; i++ {
		if la[i].Index == lb[i].Index && la[i].Term == lb[i].Term {
			c.fail(tick, LogMatching,
				"nodes %d and %d agree at index %d term %d but diverge earlier "+
					"(first difference at position %d: %s vs %s)",
				a, b, la[i].Index, la[i].Term, common, la[common], lb[common])
			return
		}
	}
}

// recordCommitted notes what this node believes is committed, and fires if two
// nodes ever believe different entries are committed at the same index.
func (c *Checker) recordCommitted(tick uint64, id raft.NodeID, v NodeView) {
	from := c.committedSeen[id]
	if v.Status.CommitIndex <= from {
		return
	}
	c.committedSeen[id] = v.Status.CommitIndex

	for _, e := range v.Log {
		if e.Index <= from {
			continue
		}
		if e.Index > v.Status.CommitIndex {
			break
		}
		if e.Index > c.MaxCommitted {
			c.MaxCommitted = e.Index
		}
		prev, seen := c.committed[e.Index]
		if !seen {
			c.committed[e.Index] = committedRecord{entry: e, term: v.Status.Term}
			continue
		}
		if prev.entry.Term != e.Term {
			c.fail(tick, CommittedEntriesAreStable,
				"index %d was committed as term %d but node %d now has it committed as term %d",
				e.Index, prev.entry.Term, id, e.Term)
		}
		if v.Status.Term < prev.term {
			c.committed[e.Index] = committedRecord{entry: prev.entry, term: v.Status.Term}
		}
	}
}

func (c *Checker) checkLeaderCompleteness(tick uint64, id raft.NodeID, v NodeView) {
	byIndex := make(map[raft.Index]raft.Entry, len(v.Log))
	for _, e := range v.Log {
		byIndex[e.Index] = e
	}

	for idx, rec := range c.committed {
		if rec.term >= v.Status.Term {
			// Committed in this term or later as far as we can tell. The
			// property only constrains leaders of strictly higher terms.
			continue
		}
		got, ok := byIndex[idx]
		if !ok {
			c.fail(tick, LeaderCompleteness,
				"node %d became leader in term %d without entry %d (term %d), "+
					"which was already committed in term %d",
				id, v.Status.Term, idx, rec.entry.Term, rec.term)
			continue
		}
		if got.Term != rec.entry.Term {
			c.fail(tick, LeaderCompleteness,
				"node %d became leader in term %d holding term %d at index %d, "+
					"but term %d was committed there in term %d",
				id, v.Status.Term, got.Term, idx, rec.entry.Term, rec.term)
		}
	}
}

// RecordApply notes that a node applied an entry, and fires if two nodes ever
// apply different entries at the same index.
//
// This is State Machine Safety, and it is the strongest of the five: everything
// else exists to make it true.
func (c *Checker) RecordApply(tick uint64, id raft.NodeID, e raft.Entry) {
	prev, seen := c.applied[e.Index]
	if !seen {
		c.applied[e.Index] = appliedRecord{entry: e, by: id}
		return
	}
	if prev.entry.Term != e.Term || string(prev.entry.Data) != string(e.Data) {
		c.fail(tick, StateMachineSafety,
			"node %d applied %s at index %d, but node %d had already applied %s there",
			id, e, e.Index, prev.by, prev.entry)
	}
}

// AppliedEntry returns the entry applied at an index, if any.
func (c *Checker) AppliedEntry(i raft.Index) (raft.Entry, bool) {
	r, ok := c.applied[i]
	return r.entry, ok
}

// CommittedEntry returns what the cluster was observed to believe is committed
// at an index, if anything.
func (c *Checker) CommittedEntry(i raft.Index) (raft.Entry, bool) {
	r, ok := c.committed[i]
	return r.entry, ok
}

// LeaderOfTerm returns the observed leader of a term.
func (c *Checker) LeaderOfTerm(t raft.Term) (raft.NodeID, bool) {
	id, ok := c.leaderByTerm[t]
	return id, ok
}

// TermsWithLeaders is how many distinct terms elected a leader. A run where
// this is far larger than the number of deliberate failures suggests the
// cluster was churning through elections rather than making progress.
func (c *Checker) TermsWithLeaders() int { return len(c.leaderByTerm) }
