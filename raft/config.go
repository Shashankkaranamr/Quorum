package raft

import (
	"errors"
	"fmt"
	"slices"
)

// Mutation deliberately breaks one of Raft's safety rules.
//
// It exists for exactly one reason: a checker that has never been observed to
// fail is not evidence. The invariant checkers in internal/testutil are the
// primary deliverable of phase 2, and the only way to show they work is to run
// them against a Raft that is genuinely, specifically broken.
//
// Rules that keep this from becoming a liability:
//
//   - The zero value is MutationNone, so a Config built without thinking about
//     it is safe. TestZeroConfigIsUnmutated asserts this.
//   - Nothing outside a _test.go file may set it. TestNoProductionCallerSetsMutation
//     enforces that by scanning the source tree.
//   - Each value names the exact rule it removes, so a reviewer can see that
//     the negative control tests the thing it claims to test.
type Mutation uint8

const (
	// MutationNone is normal, correct Raft.
	MutationNone Mutation = iota

	// MutationCommitAnyTerm removes the Figure 8 rule: the leader advances
	// commitIndex on a majority matchIndex even when the entry belongs to an
	// earlier term. This is the subtle one. It looks correct -- a majority
	// really does hold the entry -- but such an entry can still be overwritten
	// by a legitimate later leader, so committing it is unsound.
	MutationCommitAnyTerm

	// MutationVoteTwicePerTerm removes the "one vote per term" rule, which
	// breaks Election Safety directly.
	MutationVoteTwicePerTerm

	// MutationSkipUpToDateCheck grants a vote to any candidate regardless of
	// whether its log is at least as up to date as the voter's, which breaks
	// Leader Completeness.
	MutationSkipUpToDateCheck

	// MutationLeaderTruncatesOwnLog makes a leader drop its own last entry
	// when a stale AppendEntries arrives, breaking Leader Append-Only.
	//
	// It is surgical on purpose: it removes one entry above the commit index
	// and nothing else, so the violation the checkers report is unambiguously
	// Leader Append-Only rather than a cascade.
	MutationLeaderTruncatesOwnLog
)

func (m Mutation) String() string {
	switch m {
	case MutationNone:
		return "none"
	case MutationCommitAnyTerm:
		return "commit-any-term"
	case MutationVoteTwicePerTerm:
		return "vote-twice-per-term"
	case MutationSkipUpToDateCheck:
		return "skip-up-to-date-check"
	case MutationLeaderTruncatesOwnLog:
		return "leader-truncates-own-log"
	default:
		return fmt.Sprintf("mutation(%d)", uint8(m))
	}
}

// Config constructs a Node.
//
// Everything the core needs is supplied here, because the core has no way to
// fetch anything: no clock, no random source, no storage. In particular Rand
// is injected rather than drawn from math/rand, which is what makes every run
// reproducible from a seed.
type Config struct {
	// ID is this node's identity. It must appear in Peers.
	ID NodeID

	// Peers is every member of the cluster including this node. Membership is
	// static for the lifetime of the cluster; see DESIGN.md §5.
	Peers []NodeID

	// ElectionTimeoutMinTicks and ElectionTimeoutMaxTicks bound the randomized
	// election timeout, in logical ticks. The range must be non-empty:
	// identical timeouts across nodes cause repeated split votes.
	ElectionTimeoutMinTicks int
	ElectionTimeoutMaxTicks int

	// HeartbeatTimeoutTicks is how often a leader sends AppendEntries. It must
	// be comfortably smaller than the election timeout, or ordinary jitter
	// triggers spurious elections (Raft's broadcastTime << electionTimeout).
	HeartbeatTimeoutTicks int

	// MaxEntriesPerAppend caps how many entries one AppendEntries carries.
	MaxEntriesPerAppend int

	// Rand returns a value in [0, n). It is the only source of nondeterminism
	// in the core, and injecting it is what lets a failing randomized trial be
	// reproduced from its seed alone.
	Rand func(n int) int

	// Restored state. Phase 3 populates these from the write-ahead log; until
	// then they are zero for a fresh node, or set directly by tests that need
	// to construct a specific history.
	HardState HardState
	Entries   []Entry
	Applied   Index

	// UnsafeMutation deliberately breaks a safety rule. Leave it zero. See
	// the Mutation documentation for why it exists at all.
	UnsafeMutation Mutation
}

var (
	// ErrNotLeader is returned by Propose on a node that is not the leader.
	// The caller should retry at the leader; the proposal was definitely not
	// accepted, which is the distinction phase 5's client contract rests on.
	ErrNotLeader = errors.New("raft: not leader")

	// ErrStopped is returned once a node has been stopped.
	ErrStopped = errors.New("raft: node stopped")
)

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("raft: Config.ID must not be zero")
	}
	if len(c.Peers) == 0 {
		return errors.New("raft: Config.Peers must not be empty")
	}
	if !slices.Contains(c.Peers, c.ID) {
		return fmt.Errorf("raft: Config.ID %d is not in Peers %v", c.ID, c.Peers)
	}
	seen := make(map[NodeID]bool, len(c.Peers))
	for _, p := range c.Peers {
		if p == None {
			return errors.New("raft: Config.Peers must not contain node id 0")
		}
		if seen[p] {
			return fmt.Errorf("raft: Config.Peers contains duplicate node id %d", p)
		}
		seen[p] = true
	}
	if c.HeartbeatTimeoutTicks < 1 {
		return fmt.Errorf("raft: HeartbeatTimeoutTicks must be at least 1, got %d",
			c.HeartbeatTimeoutTicks)
	}
	if c.ElectionTimeoutMinTicks >= c.ElectionTimeoutMaxTicks {
		return fmt.Errorf(
			"raft: ElectionTimeoutMinTicks (%d) must be strictly less than Max (%d): "+
				"an empty randomization range causes repeated split votes",
			c.ElectionTimeoutMinTicks, c.ElectionTimeoutMaxTicks)
	}
	if c.ElectionTimeoutMinTicks <= c.HeartbeatTimeoutTicks {
		return fmt.Errorf(
			"raft: ElectionTimeoutMinTicks (%d) must exceed HeartbeatTimeoutTicks (%d)",
			c.ElectionTimeoutMinTicks, c.HeartbeatTimeoutTicks)
	}
	if c.MaxEntriesPerAppend < 1 {
		return fmt.Errorf("raft: MaxEntriesPerAppend must be at least 1, got %d",
			c.MaxEntriesPerAppend)
	}
	if c.Rand == nil {
		return errors.New("raft: Config.Rand must be supplied; the core draws no randomness of its own")
	}
	return nil
}

// quorum is the number of nodes that must agree. It is computed from the static
// peer set, so it never changes for the lifetime of a node.
func (c *Config) quorum() int { return len(c.Peers)/2 + 1 }
