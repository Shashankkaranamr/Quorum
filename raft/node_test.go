package raft_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/Shashankkaranamr/Quorum/raft"
)

func baseConfig(id raft.NodeID, peers int) raft.Config {
	ps := make([]raft.NodeID, peers)
	for i := range ps {
		ps[i] = raft.NodeID(i + 1)
	}
	return raft.Config{
		ID:                      id,
		Peers:                   ps,
		ElectionTimeoutMinTicks: 10,
		ElectionTimeoutMaxTicks: 20,
		HeartbeatTimeoutTicks:   3,
		MaxEntriesPerAppend:     64,
		Rand:                    func(n int) int { return n / 2 },
	}
}

func newNode(t *testing.T, cfg raft.Config) *raft.Node {
	t.Helper()
	n, err := raft.New(cfg)
	require.NoError(t, err)
	return n
}

// TestUpToDateComparison covers §5.4.1, the single easiest thing in Raft to get
// backwards.
//
// "At least as up to date" compares the LAST ENTRY'S TERM first, and only on a
// tie, the index. A longer log whose last term is older is NOT more up to date:
// those extra entries belong to a term that lost and may legitimately be
// discarded. Getting the order wrong lets a candidate missing a committed entry
// win an election, which loses the entry.
func TestUpToDateComparison(t *testing.T) {
	// The voter holds [1@1, 2@2, 3@2]: last term 2, last index 3.
	voterLog := []raft.Entry{
		{Index: 1, Term: 1, Type: raft.EntryNormal},
		{Index: 2, Term: 2, Type: raft.EntryNormal},
		{Index: 3, Term: 2, Type: raft.EntryNormal},
	}

	for _, tc := range []struct {
		name        string
		lastIdx     raft.Index
		lastTerm    raft.Term
		wantGranted bool
		why         string
	}{
		{
			name: "identical log", lastIdx: 3, lastTerm: 2, wantGranted: true,
			why: "an equal log is 'at least as up to date'",
		},
		{
			name: "longer log, same term", lastIdx: 4, lastTerm: 2, wantGranted: true,
			why: "same last term, higher index wins",
		},
		{
			name: "shorter log, same term", lastIdx: 2, lastTerm: 2, wantGranted: false,
			why: "same last term, lower index loses",
		},
		{
			name: "higher last term, shorter log", lastIdx: 1, lastTerm: 3, wantGranted: true,
			why: "TERM IS COMPARED FIRST: a shorter log with a newer last term " +
				"is more up to date, because our extra entries belong to a term that lost",
		},
		{
			name: "lower last term, much longer log", lastIdx: 99, lastTerm: 1, wantGranted: false,
			why: "TERM IS COMPARED FIRST: length does not rescue an older last term. " +
				"If this grants, the comparison is backwards and Leader Completeness is gone",
		},
		{
			name: "empty candidate log", lastIdx: 0, lastTerm: 0, wantGranted: false,
			why: "a candidate with nothing cannot be at least as up to date as a non-empty log",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := baseConfig(1, 3)
			cfg.Entries = voterLog
			cfg.HardState = raft.HardState{Term: 5}
			n := newNode(t, cfg)

			require.NoError(t, n.Step(raft.Message{
				Type: raft.MsgRequestVote, From: 2, To: 1, Term: 5,
				LastLogIndex: tc.lastIdx, LastLogTerm: tc.lastTerm,
			}))

			rd := n.Ready()
			require.Len(t, rd.Messages, 1)
			require.Equal(t, raft.MsgRequestVoteResp, rd.Messages[0].Type)
			require.Equalf(t, tc.wantGranted, rd.Messages[0].VoteGranted, "%s", tc.why)
		})
	}
}

// TestVoteIsGrantedOncePerTerm covers §5.2: one vote per term is what makes
// Election Safety hold.
func TestVoteIsGrantedOncePerTerm(t *testing.T) {
	cfg := baseConfig(1, 5)
	cfg.HardState = raft.HardState{Term: 5}
	n := newNode(t, cfg)

	ask := func(from raft.NodeID) bool {
		require.NoError(t, n.Step(raft.Message{
			Type: raft.MsgRequestVote, From: from, To: 1, Term: 5,
		}))
		rd := n.Ready()
		n.Advance()
		require.Len(t, rd.Messages, 1)
		return rd.Messages[0].VoteGranted
	}

	require.True(t, ask(2), "first candidate in a fresh term should get the vote")
	require.False(t, ask(3), "a second candidate must be refused in the same term")
	require.True(t, ask(2), "a repeat request from the same candidate is idempotent")

	// A new term clears the vote.
	require.NoError(t, n.Step(raft.Message{
		Type: raft.MsgRequestVote, From: 3, To: 1, Term: 6,
	}))
	rd := n.Ready()
	require.True(t, rd.Messages[0].VoteGranted, "a higher term resets votedFor")
}

// TestElectionTimerResetDiscipline covers the rule the phase brief singles out.
//
// The timer resets on exactly two events: granting a vote, and receiving a
// valid AppendEntries from the current leader. Resetting on every message would
// mask a real liveness bug -- a node that keeps hearing chatter it refuses
// would never time out and campaign, so a cluster unable to make progress would
// still look healthy.
func TestElectionTimerResetDiscipline(t *testing.T) {
	log := []raft.Entry{
		{Index: 1, Term: 1, Type: raft.EntryNormal},
		{Index: 2, Term: 2, Type: raft.EntryNormal},
	}

	for _, tc := range []struct {
		name      string
		msg       raft.Message
		wantReset bool
		why       string
	}{
		{
			name: "granting a vote resets",
			msg: raft.Message{Type: raft.MsgRequestVote, From: 2, To: 1, Term: 5,
				LastLogIndex: 2, LastLogTerm: 2},
			wantReset: true,
			why:       "§5.2: a voter that just backed a candidate gives it time to win",
		},
		{
			name: "REFUSING a vote does not reset",
			msg: raft.Message{Type: raft.MsgRequestVote, From: 2, To: 1, Term: 5,
				LastLogIndex: 1, LastLogTerm: 1},
			wantReset: false,
			why: "a candidate we refused must not be able to keep us from campaigning; " +
				"resetting here is the classic way to hide a liveness bug",
		},
		{
			name: "a valid AppendEntries from the leader resets",
			msg: raft.Message{Type: raft.MsgAppendEntries, From: 2, To: 1, Term: 5,
				PrevLogIndex: 2, PrevLogTerm: 2},
			wantReset: true,
			why:       "§5.2: the leader is alive",
		},
		{
			name: "an AppendEntries that fails the consistency check STILL resets",
			msg: raft.Message{Type: raft.MsgAppendEntries, From: 2, To: 1, Term: 5,
				PrevLogIndex: 9, PrevLogTerm: 9},
			wantReset: true,
			why: "a log mismatch does not mean the sender is not the leader; " +
				"campaigning over it would disrupt a healthy cluster",
		},
		{
			name:      "an AppendEntriesResp does not reset",
			msg:       raft.Message{Type: raft.MsgAppendEntriesResp, From: 2, To: 1, Term: 5, Success: true},
			wantReset: false,
			why:       "a follower has no business resetting on a response it cannot act on",
		},
		{
			name:      "a RequestVoteResp does not reset",
			msg:       raft.Message{Type: raft.MsgRequestVoteResp, From: 2, To: 1, Term: 5},
			wantReset: false,
			why:       "we are not a candidate; this message is noise",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := baseConfig(1, 5)
			cfg.Entries = log
			cfg.HardState = raft.HardState{Term: 5}
			n := newNode(t, cfg)

			// Let the timer run part way so a reset is visible.
			for range 5 {
				n.Tick()
			}
			require.Equal(t, 5, n.Status().ElectionElapsed)

			require.NoError(t, n.Step(tc.msg))

			got := n.Status().ElectionElapsed
			if tc.wantReset {
				require.Zerof(t, got, "expected the timer to reset: %s", tc.why)
			} else {
				require.Equalf(t, 5, got, "expected the timer NOT to reset: %s", tc.why)
			}
		})
	}
}

// TestHigherTermStepsDownBeforeProcessing covers the term rule: a message with
// a greater term forces an immediate step-down whatever the current role,
// before the body is looked at.
func TestHigherTermStepsDownBeforeProcessing(t *testing.T) {
	for _, role := range []string{"candidate", "leader"} {
		t.Run(role, func(t *testing.T) {
			n := newNode(t, baseConfig(1, 3))
			n.Campaign()
			require.Equal(t, raft.Candidate, n.Status().Role)

			if role == "leader" {
				require.NoError(t, n.Step(raft.Message{
					Type: raft.MsgRequestVoteResp, From: 2, To: 1, Term: 1, VoteGranted: true,
				}))
				require.Equal(t, raft.Leader, n.Status().Role)
			}

			require.NoError(t, n.Step(raft.Message{
				Type: raft.MsgAppendEntries, From: 3, To: 1, Term: 9,
			}))

			s := n.Status()
			require.Equal(t, raft.Follower, s.Role, "a higher term must force a step-down")
			require.Equal(t, raft.Term(9), s.Term)
			require.Equal(t, raft.NodeID(3), s.Leader, "AppendEntries identifies the leader")
			require.Equal(t, raft.None, s.VotedFor, "a new term clears the vote")
		})
	}

	t.Run("a higher-term RequestVote does not make the candidate our leader", func(t *testing.T) {
		n := newNode(t, baseConfig(1, 3))
		require.NoError(t, n.Step(raft.Message{
			Type: raft.MsgRequestVote, From: 2, To: 1, Term: 9,
		}))
		s := n.Status()
		require.Equal(t, raft.Term(9), s.Term)
		require.Equal(t, raft.None, s.Leader,
			"an election in progress has no winner yet; adopting the candidate as leader would be wrong")
	})
}

// TestStaleMessageIsRejectedWithOurTerm covers the other half of the term rule.
func TestStaleMessageIsRejectedWithOurTerm(t *testing.T) {
	cfg := baseConfig(1, 3)
	cfg.HardState = raft.HardState{Term: 7}
	n := newNode(t, cfg)

	require.NoError(t, n.Step(raft.Message{
		Type: raft.MsgAppendEntries, From: 2, To: 1, Term: 3, PrevLogIndex: 0,
	}))

	rd := n.Ready()
	require.Len(t, rd.Messages, 1)
	require.False(t, rd.Messages[0].Success)
	require.Equal(t, raft.Term(7), rd.Messages[0].Term,
		"the reply must carry OUR term so the stale sender learns it and steps down")
	require.Equal(t, raft.Term(7), n.Status().Term, "our term must not move backwards")
}

// TestStaleAppendEntriesDoesNotTruncate is the reordering and duplication case.
//
// The simulator delays, duplicates and reorders messages on purpose, so a
// follower will receive an AppendEntries it has already applied. Truncating at
// prevLogIndex+1 unconditionally would delete entries it had already accepted,
// and if any were committed that is an immediate safety violation. §5.3 says to
// truncate only from the first genuinely conflicting entry.
func TestStaleAppendEntriesDoesNotTruncate(t *testing.T) {
	cfg := baseConfig(1, 3)
	cfg.Entries = []raft.Entry{
		{Index: 1, Term: 1, Type: raft.EntryNormal},
		{Index: 2, Term: 2, Type: raft.EntryNormal},
		{Index: 3, Term: 2, Type: raft.EntryNormal},
	}
	cfg.HardState = raft.HardState{Term: 2, Commit: 3}
	cfg.Applied = 3
	n := newNode(t, cfg)

	t.Run("a duplicate of an older message changes nothing", func(t *testing.T) {
		require.NoError(t, n.Step(raft.Message{
			Type: raft.MsgAppendEntries, From: 2, To: 1, Term: 2,
			PrevLogIndex: 1, PrevLogTerm: 1,
			Entries:      []raft.Entry{{Index: 2, Term: 2, Type: raft.EntryNormal}},
			LeaderCommit: 3,
		}))

		log := n.LogEntries()
		require.Len(t, log, 3,
			"a stale duplicate truncated the log; entries 2 and 3 were already committed")
		require.Equal(t, raft.Index(3), n.Status().CommitIndex)
	})

	t.Run("an overlapping message appends only what is new", func(t *testing.T) {
		require.NoError(t, n.Step(raft.Message{
			Type: raft.MsgAppendEntries, From: 2, To: 1, Term: 2,
			PrevLogIndex: 1, PrevLogTerm: 1,
			Entries: []raft.Entry{
				{Index: 2, Term: 2, Type: raft.EntryNormal},
				{Index: 3, Term: 2, Type: raft.EntryNormal},
				{Index: 4, Term: 2, Type: raft.EntryNormal},
			},
			LeaderCommit: 3,
		}))

		log := n.LogEntries()
		require.Len(t, log, 4)
		require.Equal(t, raft.Index(4), log[3].Index)
	})

	t.Run("a genuine conflict above the commit index does truncate", func(t *testing.T) {
		require.NoError(t, n.Step(raft.Message{
			Type: raft.MsgAppendEntries, From: 2, To: 1, Term: 3,
			PrevLogIndex: 3, PrevLogTerm: 2,
			Entries:      []raft.Entry{{Index: 4, Term: 3, Type: raft.EntryNormal}},
			LeaderCommit: 3,
		}))

		log := n.LogEntries()
		require.Len(t, log, 4)
		require.Equal(t, raft.Term(3), log[3].Term, "index 4 should have been replaced")
	})
}

// TestFollowerCommitIsClampedToLastNewEntry covers the Figure 2 rule
// commitIndex = min(leaderCommit, index of last new entry).
//
// Clamping to the entries THIS message carried, rather than to our own last
// index, is what stops a follower sitting on an abandoned branch from
// committing entries the leader never sent it.
func TestFollowerCommitIsClampedToLastNewEntry(t *testing.T) {
	cfg := baseConfig(1, 3)
	cfg.Entries = []raft.Entry{
		{Index: 1, Term: 1, Type: raft.EntryNormal},
		{Index: 2, Term: 1, Type: raft.EntryNormal},
		{Index: 3, Term: 1, Type: raft.EntryNormal},
	}
	cfg.HardState = raft.HardState{Term: 1}
	n := newNode(t, cfg)

	// The leader claims a commit index of 3 but its message only covers up to
	// index 1, so we may only commit index 1.
	require.NoError(t, n.Step(raft.Message{
		Type: raft.MsgAppendEntries, From: 2, To: 1, Term: 1,
		PrevLogIndex: 1, PrevLogTerm: 1,
		LeaderCommit: 3,
	}))
	require.Equal(t, raft.Index(1), n.Status().CommitIndex,
		"committed past the last entry this message covered")

	// A message that actually covers index 3 lets us commit it.
	require.NoError(t, n.Step(raft.Message{
		Type: raft.MsgAppendEntries, From: 2, To: 1, Term: 1,
		PrevLogIndex: 3, PrevLogTerm: 1,
		LeaderCommit: 3,
	}))
	require.Equal(t, raft.Index(3), n.Status().CommitIndex)
}

// TestConflictBackoffSkipsAWholeTerm covers the §5.3 optimization. A linear
// decrement would also be correct; this exists so that a far-behind follower
// does not cost one round trip per entry.
func TestConflictBackoffSkipsAWholeTerm(t *testing.T) {
	// The follower's log is entirely term 1; the leader's is term 5 above
	// index 1, so the conflict hint should send the leader back to index 2 in
	// one step rather than walking down from 6.
	cfg := baseConfig(1, 3)
	for i := 1; i <= 5; i++ {
		cfg.Entries = append(cfg.Entries, raft.Entry{Index: raft.Index(i), Term: 1, Type: raft.EntryNormal})
	}
	cfg.HardState = raft.HardState{Term: 1}
	n := newNode(t, cfg)

	require.NoError(t, n.Step(raft.Message{
		Type: raft.MsgAppendEntries, From: 2, To: 1, Term: 5,
		PrevLogIndex: 5, PrevLogTerm: 5,
	}))

	rd := n.Ready()
	require.Len(t, rd.Messages, 1)
	m := rd.Messages[0]
	require.False(t, m.Success)
	require.Equal(t, raft.Term(1), m.ConflictTerm)
	require.Equal(t, raft.Index(1), m.ConflictIndex,
		"the hint should name the first index of the conflicting term, not prevLogIndex")
}

// TestLeaderAppendsNoOpOnElection covers the entry ReadIndex will depend on in
// phase 5, and that Raft needs for the commit rule in the first place.
func TestLeaderAppendsNoOpOnElection(t *testing.T) {
	n := newNode(t, baseConfig(1, 3))
	n.Campaign()
	require.NoError(t, n.Step(raft.Message{
		Type: raft.MsgRequestVoteResp, From: 2, To: 1, Term: 1, VoteGranted: true,
	}))
	require.Equal(t, raft.Leader, n.Status().Role)

	log := n.LogEntries()
	require.Len(t, log, 1)
	require.Equal(t, raft.EntryNoOp, log[0].Type,
		"a new leader must append a no-op so it can learn its true commit index (§5.4.2)")
	require.Equal(t, raft.Term(1), log[0].Term)
}

// TestProposeRequiresLeadership covers the distinction phase 5's client
// contract rests on: a rejected proposal was definitely not accepted.
func TestProposeRequiresLeadership(t *testing.T) {
	n := newNode(t, baseConfig(1, 3))
	_, _, err := n.Propose(raft.EntryNormal, []byte("x"))
	require.ErrorIs(t, err, raft.ErrNotLeader)

	n.Campaign()
	_, _, err = n.Propose(raft.EntryNormal, []byte("x"))
	require.ErrorIs(t, err, raft.ErrNotLeader, "a candidate is not a leader either")
}

// TestSingleNodeClusterElectsItself is the degenerate case: there is nobody to
// ask, so a campaign wins immediately.
func TestSingleNodeClusterElectsItself(t *testing.T) {
	n := newNode(t, baseConfig(1, 1))
	n.Campaign()
	require.Equal(t, raft.Leader, n.Status().Role)

	idx, _, err := n.Propose(raft.EntryNormal, []byte("x"))
	require.NoError(t, err)

	// Commit requires durability even with a quorum of one.
	for range 4 {
		if !n.HasReady() {
			break
		}
		n.Ready()
		n.Advance()
	}
	require.GreaterOrEqual(t, n.Status().CommitIndex, idx)
}

func TestConfigValidation(t *testing.T) {
	for _, tc := range []struct {
		name string
		mut  func(*raft.Config)
		want string
	}{
		{"zero id", func(c *raft.Config) { c.ID = 0 }, "ID must not be zero"},
		{"id not in peers", func(c *raft.Config) { c.ID = 9 }, "is not in Peers"},
		{"empty peers", func(c *raft.Config) { c.Peers = nil }, "Peers must not be empty"},
		{"duplicate peer", func(c *raft.Config) { c.Peers = []raft.NodeID{1, 2, 2} }, "duplicate node id"},
		{"empty timeout range", func(c *raft.Config) { c.ElectionTimeoutMaxTicks = c.ElectionTimeoutMinTicks }, "strictly less than Max"},
		{"timeout under heartbeat", func(c *raft.Config) { c.HeartbeatTimeoutTicks = 15 }, "must exceed HeartbeatTimeoutTicks"},
		{"no rand", func(c *raft.Config) { c.Rand = nil }, "Rand must be supplied"},
		{"zero batch size", func(c *raft.Config) { c.MaxEntriesPerAppend = 0 }, "MaxEntriesPerAppend"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := baseConfig(1, 3)
			tc.mut(&cfg)
			_, err := raft.New(cfg)
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.want)
		})
	}

	t.Run("applied past committed", func(t *testing.T) {
		cfg := baseConfig(1, 3)
		cfg.Applied = 5
		_, err := raft.New(cfg)
		require.Error(t, err)
		require.Contains(t, err.Error(), "exceeds committed")
	})
}

func TestStringers(t *testing.T) {
	require.Equal(t, "follower", raft.Follower.String())
	require.Equal(t, "candidate", raft.Candidate.String())
	require.Equal(t, "leader", raft.Leader.String())
	require.Equal(t, "noop", raft.EntryNoOp.String())
	require.Equal(t, "AppendEntries", raft.MsgAppendEntries.String())
	require.Contains(t, raft.Entry{Index: 3, Term: 2, Type: raft.EntryNormal}.String(), "3@2")
	require.Contains(t, fmt.Sprint(raft.Message{Type: raft.MsgRequestVote, From: 1, To: 2, Term: 3}), "RequestVote")
	require.True(t, raft.HardState{}.IsEmpty())
	require.False(t, raft.HardState{Term: 1}.IsEmpty())
	require.True(t, raft.Ready{}.IsEmpty())
}
