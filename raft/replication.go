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

	if last := n.log.lastIndex(); pr.NextIndex > last+1 {
		// Only reachable when the leader's own log shrank under it, which a
		// correct leader never does (MutationLeaderTruncatesOwnLog does).
		// Clamping keeps the negative control running long enough for the
		// checker to report the violation by name.
		pr.NextIndex = last + 1
	}
	prevIndex := pr.NextIndex - 1
	prevTerm, ok := n.log.term(prevIndex)
	if !ok || pr.PendingSnapshot != 0 {
		// The entry the follower needs next has been compacted away, so no
		// AppendEntries can attach to its log: only the snapshot can bring it
		// forward. The same holds while a transfer is already under way.
		n.sendSnapshot(to, pr)
		return
	}

	ents := n.log.slice(pr.NextIndex, pr.NextIndex+Index(n.cfg.MaxEntriesPerAppend))

	n.send(Message{
		Type:         MsgAppendEntries,
		To:           to,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		Entries:      ents,
		LeaderCommit: n.log.committed,
		ReadSeq:      n.readSeq,
	})
}

// sendSnapshot sends a follower the next chunk of the leader's snapshot.
//
// The transfer is stop-and-wait: one chunk in flight, each acknowledged with
// the byte count the follower now holds, and the next chunk starting exactly
// there. A lost chunk or acknowledgement is recovered by the heartbeat, which
// resends the current chunk. If the leader compacts again mid-transfer, the
// next chunk belongs to the new snapshot and starts from zero; the follower
// discards the partial old one, because the two can never be spliced.
func (n *Node) sendSnapshot(to NodeID, pr *Progress) {
	snap := n.snapshot
	if snap.IsEmpty() {
		// Unreachable: an entry can only be missing below the compaction
		// point, and compacting is what produces a snapshot.
		panic(fmt.Sprintf("raft: node %d must send node %d a snapshot but holds none (log starts at %d)",
			n.id, to, n.log.firstIndex()))
	}
	if pr.PendingSnapshot != snap.Meta.Index {
		pr.PendingSnapshot = snap.Meta.Index
		pr.SnapshotOffset = 0
	}

	off := min(pr.SnapshotOffset, uint64(len(snap.Data)))
	end := min(off+uint64(n.cfg.chunkBytes()), uint64(len(snap.Data)))
	pr.SnapshotsSent++
	n.send(Message{
		Type:           MsgInstallSnapshot,
		To:             to,
		SnapshotMeta:   snap.Meta,
		SnapshotOffset: off,
		SnapshotData:   snap.Data[off:end],
		SnapshotDone:   end == uint64(len(snap.Data)),
	})
}

// handleInstallSnapshot is the follower side of snapshot transfer (§7).
//
// As with AppendEntries, Step has already applied the term rule, so m.Term
// equals ours and the sender is this term's leader.
func (n *Node) handleInstallSnapshot(m Message) {
	if n.role == Candidate {
		n.becomeFollower(m.Term, m.From)
	}
	if n.role == Leader {
		panic(SafetyViolation{
			Property: PropertyElectionSafety,
			Detail: fmt.Sprintf("node %d is leader of term %d but node %d sent InstallSnapshot for it",
				n.id, n.term, m.From),
		})
	}
	n.lead = m.From
	n.resetElectionTimer()

	meta := m.SnapshotMeta
	if meta.Index <= n.log.committed {
		// We already hold everything this snapshot covers: it is stale,
		// perhaps a retransmission that crossed our acknowledgement.
		// Installing it would move the node backwards. Report the commit
		// index instead -- every committed entry is in the leader's log
		// (Leader Completeness), so that much certainly matches -- and let
		// ordinary replication continue from there.
		n.incoming = incomingSnapshot{}
		n.send(Message{Type: MsgAppendEntriesResp, To: m.From, Success: true, MatchIndex: n.log.committed})
		return
	}

	if n.incoming.meta != meta {
		// A different snapshot from the one being assembled. Only its first
		// chunk can start a new assembly.
		n.incoming = incomingSnapshot{}
		if m.SnapshotOffset != 0 {
			n.send(Message{Type: MsgInstallSnapshotResp, To: m.From, SnapshotMeta: meta})
			return
		}
		n.incoming.meta = meta
	}
	if have := uint64(len(n.incoming.data)); m.SnapshotOffset != have {
		// A duplicate, or a chunk from beyond a gap. Either way, tell the
		// leader where we actually are.
		n.send(Message{Type: MsgInstallSnapshotResp, To: m.From, SnapshotMeta: meta, SnapshotBytesReceived: have})
		return
	}
	n.incoming.data = append(n.incoming.data, m.SnapshotData...)
	if !m.SnapshotDone {
		n.send(Message{Type: MsgInstallSnapshotResp, To: m.From, SnapshotMeta: meta,
			SnapshotBytesReceived: uint64(len(n.incoming.data))})
		return
	}

	snap := Snapshot{Meta: meta, Data: n.incoming.data}
	n.incoming = incomingSnapshot{}
	n.log.restore(meta)
	n.snapshot = snap
	n.pendingSnapshot = &snap

	// The acknowledgement is an AppendEntries success: from here on the
	// follower's log agrees with the leader's up to the snapshot, and
	// ordinary replication takes over. It is a durable promise like any
	// other, and the Ready that carries it persists the snapshot first.
	n.send(Message{Type: MsgAppendEntriesResp, To: m.From, Success: true, MatchIndex: meta.Index})
}

// handleInstallSnapshotResponse moves a snapshot transfer forward by one chunk.
func (n *Node) handleInstallSnapshotResponse(m Message) {
	if !n.isLeader() {
		return
	}
	pr := n.progress[m.From]
	if pr == nil || pr.PendingSnapshot == 0 || m.SnapshotMeta.Index != pr.PendingSnapshot {
		// An acknowledgement for a transfer no longer running.
		return
	}
	if m.SnapshotBytesReceived == pr.SnapshotOffset {
		// A duplicate of an acknowledgement already acted on. Resending on
		// it would double the traffic under duplication for nothing; the
		// heartbeat covers a genuinely lost chunk.
		return
	}
	pr.SnapshotOffset = m.SnapshotBytesReceived
	n.sendSnapshot(m.From, pr)
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

	if m.PrevLogIndex < n.log.snapIndex {
		// The leader is attaching below our snapshot: it has not yet heard
		// that we installed one, or we compacted past where it thinks we
		// are. Those entries are committed, so they match by Leader
		// Completeness, but we no longer have them to compare. Report our
		// commit index so the leader resumes from there. Rejecting instead
		// would hand it a conflict hint above its nextIndex, which it rightly
		// refuses to move forward on, and the two would stall.
		n.send(Message{Type: MsgAppendEntriesResp, To: m.From, Success: true, MatchIndex: n.log.committed,
			ReadSeq: m.ReadSeq})
		return
	}

	if !n.log.matchTerm(m.PrevLogIndex, m.PrevLogTerm) {
		ci, ct := n.log.conflictHint(m.PrevLogIndex)
		// Even a rejection echoes the read round: it proves this node still
		// recognizes the sender as leader of this term, which is all ReadIndex
		// asks.
		n.send(Message{
			Type:          MsgAppendEntriesResp,
			To:            m.From,
			Success:       false,
			ConflictIndex: ci,
			ConflictTerm:  ct,
			ReadSeq:       m.ReadSeq,
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
		ReadSeq:    m.ReadSeq,
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

	// Any response in our term, success or not, is this peer acknowledging us
	// as leader. Step has already discarded responses from older terms.
	if m.ReadSeq > n.readAcks[m.From] {
		n.readAcks[m.From] = m.ReadSeq
		n.confirmReads()
	}

	if m.Success {
		if pr.PendingSnapshot != 0 && m.MatchIndex >= n.log.snapIndex {
			// The follower now holds everything up to our compaction point
			// -- it installed the snapshot, or caught up some other way -- so
			// AppendEntries can attach again. Send what follows at once
			// rather than waiting out a heartbeat.
			pr.PendingSnapshot, pr.SnapshotOffset = 0, 0
			pr.MatchIndex = max(pr.MatchIndex, m.MatchIndex)
			pr.NextIndex = pr.MatchIndex + 1
			if n.maybeCommit() {
				n.broadcastAppend()
			} else {
				n.sendAppend(m.From)
			}
			return
		}
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

	if pr.PendingSnapshot != 0 {
		// A rejection of an AppendEntries sent before the transfer began.
		// The transfer supersedes it.
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

	if !n.log.commitTo(quorumIndex) {
		return false
	}
	if len(n.unrounded) > 0 && n.committedInTerm() {
		// The term's first commit: the leader now knows its commit index, so
		// reads that were waiting for it can start their round.
		ids := n.unrounded
		n.unrounded = nil
		n.startReadRound(ids...)
	}
	return true
}
