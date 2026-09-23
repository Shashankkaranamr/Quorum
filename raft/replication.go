package raft

import (
	"fmt"
	"slices"
)

// broadcastAppend sends an AppendEntries to every peer.
func (n *Node) broadcastAppend() {
	for _, p := range n.peers {
		if p == n.id {
			continue
		}
		n.sendAppend(p)
	}
}

// sendAppend sends one peer whatever it is missing.
//
// A heartbeat is just an AppendEntries with no entries, carrying the same
// prevLogIndex/prevLogTerm. That is deliberate: the consistency check still
// runs, so a heartbeat to a follower that has diverged is rejected and starts
// the backtracking immediately, instead of waiting for the next real proposal.
func (n *Node) sendAppend(to NodeID) {
	pr := n.progress[to]
	if pr == nil {
		return
	}

	prevIndex := pr.NextIndex - 1
	prevTerm, ok := n.log.term(prevIndex)
	if !ok {
		// The entry the follower needs has been compacted away. Phase 4 sends
		// a snapshot here. Until snapshots exist this cannot happen, because
		// nothing compacts; rewinding to the start of the log keeps the node
		// correct if it ever does.
		pr.NextIndex = n.log.firstIndex()
		prevIndex = pr.NextIndex - 1
		prevTerm, _ = n.log.term(prevIndex)
	}

	ents := n.log.slice(pr.NextIndex, pr.NextIndex+Index(n.cfg.MaxEntriesPerAppend))

	n.send(Message{
		Type:         MsgAppendEntries,
		To:           to,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		Entries:      ents,
		LeaderCommit: n.log.committed,
	})
}

// handleAppendEntries is the follower side of replication (§5.3).
//
// By the time we get here m.Term equals our term: Step has already stepped us
// down if the leader was ahead, and rejected the message if it was behind.
func (n *Node) handleAppendEntries(m Message) {
	if n.role == Candidate {
		// Someone else won this term's election.
		n.becomeFollower(m.Term, m.From)
	}
	if n.role == Leader {
		// Two leaders in one term is impossible. Reaching here means the
		// safety argument already failed upstream, and quietly accepting a log
		// overwrite would turn a detectable fault into silent corruption.
		panic(SafetyViolation{
			Property: PropertyElectionSafety,
			Detail: fmt.Sprintf("node %d is leader of term %d but node %d sent AppendEntries for it",
				n.id, n.term, m.From),
		})
	}

	n.lead = m.From

	// Reset the election timer. This is the second and last place Raft permits
	// it: a valid AppendEntries from the leader of the current term. Note it
	// happens even when the consistency check below fails -- a log mismatch
	// does not mean the sender is not the leader, and treating it as such
	// would make a lagging follower campaign and disrupt a healthy cluster.
	n.resetElectionTimer()

	if !n.log.matchTerm(m.PrevLogIndex, m.PrevLogTerm) {
		ci, ct := n.log.conflictHint(m.PrevLogIndex)
		n.send(Message{
			Type:          MsgAppendEntriesResp,
			To:            m.From,
			Success:       false,
			ConflictIndex: ci,
			ConflictTerm:  ct,
		})
		return
	}

	// The entries we were sent may overlap what we already hold, because a
	// message can be delayed, duplicated or reordered. Truncating at
	// prevLogIndex+1 unconditionally would delete entries we already accepted,
	// possibly committed ones. Find the first genuine conflict instead.
	if len(m.Entries) > 0 {
		if conflict := n.log.findConflict(m.Entries); conflict != 0 {
			if conflict <= n.log.committed {
				// A leader is telling us to overwrite something we have
				// already committed. Committing it was the bug; this is only
				// where it becomes visible.
				panic(SafetyViolation{
					Property: PropertyCommittedEntriesAreStable,
					Detail: fmt.Sprintf("node %d asked to overwrite committed index %d (commit=%d) by leader %d term %d",
						n.id, conflict, n.log.committed, m.From, m.Term),
				})
			}
			n.log.truncateFrom(conflict)
			n.log.append(m.Entries[conflict-m.Entries[0].Index:]...)
		}
		// If there is no conflict, everything we were sent that we do not
		// already have is simply appended.
		if last := m.Entries[len(m.Entries)-1].Index; last > n.log.lastIndex() {
			from := n.log.lastIndex() + 1
			n.log.append(m.Entries[from-m.Entries[0].Index:]...)
		}
	}

	// Figure 2: commitIndex = min(leaderCommit, index of last new entry).
	// Clamping to the last entry THIS message carried, rather than to our own
	// last index, is what stops a follower that is ahead on some abandoned
	// branch from committing entries the leader never sent it.
	lastNew := m.PrevLogIndex + Index(len(m.Entries))
	if m.LeaderCommit > n.log.committed {
		n.log.commitTo(min(m.LeaderCommit, lastNew))
	}

	n.send(Message{
		Type:       MsgAppendEntriesResp,
		To:         m.From,
		Success:    true,
		MatchIndex: lastNew,
	})
}

// handleAppendEntriesResponse is the leader side.
func (n *Node) handleAppendEntriesResponse(m Message) {
	if !n.isLeader() {
		return
	}
	pr := n.progress[m.From]
	if pr == nil {
		return
	}

	if m.Success {
		// A response can arrive out of order, so only ever move forward.
		if m.MatchIndex > pr.MatchIndex {
			pr.MatchIndex = m.MatchIndex
			pr.NextIndex = pr.MatchIndex + 1
			if n.maybeCommit() {
				// Tell the followers the commit index moved, rather than
				// making them wait for the next heartbeat to learn it.
				n.broadcastAppend()
			}
		}
		return
	}

	// Rejected: back off and retry.
	next := n.backoffNextIndex(pr, m)
	if next >= pr.NextIndex {
		// Never move backwards into an infinite retry loop.
		return
	}
	pr.NextIndex = next
	n.sendAppend(m.From)
}

// backoffNextIndex computes where to retry after a rejection.
//
// The conflict hint lets a whole term be skipped in one round trip. A plain
// decrement by one is also correct and is what happens when there is no hint;
// it just costs a round trip per entry, which makes catching up a far-behind
// follower quadratic.
func (n *Node) backoffNextIndex(pr *Progress, m Message) Index {
	if m.ConflictTerm == 0 {
		// The follower's log simply ends before prevLogIndex. Resume from
		// where it actually ends.
		if m.ConflictIndex > 0 {
			return m.ConflictIndex
		}
		return max(pr.NextIndex-1, 1)
	}

	// If we have any entry in the conflicting term, resume just after the last
	// one: the follower will have the same prefix up to there.
	for i := n.log.lastIndex(); i >= n.log.firstIndex(); i-- {
		t, ok := n.log.term(i)
		if !ok {
			break
		}
		if t == m.ConflictTerm {
			return i + 1
		}
		if t < m.ConflictTerm {
			break
		}
	}
	// We have nothing from that term: skip past it entirely.
	if m.ConflictIndex > 0 {
		return m.ConflictIndex
	}
	return max(pr.NextIndex-1, 1)
}

// maybeCommit advances the leader's commit index, and is where Figure 8 lives.
//
// The quorum match index is the highest index replicated on a majority: sort
// every match index descending and take the element at position quorum-1.
//
// The rule that matters is the second condition. A leader may only commit an
// entry from ITS OWN TERM by counting replicas. An entry from an earlier term
// is committed indirectly, when a later current-term entry that sits above it
// commits.
//
// It is tempting to skip this, because a majority genuinely does hold the old
// entry. Figure 8 shows why that is unsound: an entry can be present on a
// majority and still be overwritten by a legitimately elected later leader
// whose log is more up to date. Committing it means telling a client a write
// succeeded and then losing it. TestFigure8CommitRule builds exactly that
// scenario and fails if this condition is removed.
func (n *Node) maybeCommit() bool {
	if !n.isLeader() {
		return false
	}

	matches := make([]Index, 0, len(n.peers))
	for _, p := range n.peers {
		if pr := n.progress[p]; pr != nil {
			matches = append(matches, pr.MatchIndex)
		}
	}
	if len(matches) < n.cfg.quorum() {
		return false
	}
	slices.SortFunc(matches, func(a, b Index) int {
		switch {
		case a > b:
			return -1
		case a < b:
			return 1
		default:
			return 0
		}
	})
	quorumIndex := matches[n.cfg.quorum()-1]

	if quorumIndex <= n.log.committed {
		return false
	}

	t, ok := n.log.term(quorumIndex)
	if !ok {
		return false
	}
	if t != n.term && n.cfg.UnsafeMutation != MutationCommitAnyTerm {
		// The Figure 8 rule. Removing this line is MutationCommitAnyTerm.
		return false
	}

	return n.log.commitTo(quorumIndex)
}
