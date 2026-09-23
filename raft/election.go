package raft

// handleRequestVote decides whether to grant a vote.
//
// Two conditions, both required (§5.2 and §5.4.1):
//
//  1. We have not already voted in this term for someone else. One vote per
//     term is what makes Election Safety hold: two candidates cannot both
//     collect a majority out of the same set of single votes.
//  2. The candidate's log is at least as up to date as ours. This is what
//     makes Leader Completeness hold: a candidate missing a committed entry
//     cannot assemble a majority, because a majority of nodes hold that entry
//     and every one of them will refuse.
//
// By the time we get here the term rule in Step has already run, so m.Term
// equals our term.
func (n *Node) handleRequestVote(m Message) {
	canVote := n.votedFor == None || n.votedFor == m.From
	if n.cfg.UnsafeMutation == MutationVoteTwicePerTerm {
		// Negative control: pretend we never voted. Breaks Election Safety.
		canVote = true
	}

	upToDate := n.log.isUpToDate(m.LastLogIndex, m.LastLogTerm)
	if n.cfg.UnsafeMutation == MutationSkipUpToDateCheck {
		// Negative control: vote for anyone. Breaks Leader Completeness.
		upToDate = true
	}

	grant := canVote && upToDate
	if grant {
		n.votedFor = m.From

		// Reset the election timer ONLY here, and on a valid AppendEntries
		// from the current leader. Resetting on every received message would
		// hide a real liveness bug: a node that keeps hearing chatter from a
		// candidate it refuses would never time out and campaign itself, and
		// a cluster that cannot make progress would look healthy in a test
		// while stalling in the field.
		n.resetElectionTimer()
	}

	n.send(Message{Type: MsgRequestVoteResp, To: m.From, VoteGranted: grant})
}

// handleRequestVoteResponse tallies a vote.
func (n *Node) handleRequestVoteResponse(m Message) {
	if n.role != Candidate {
		// We already won, already lost, or stepped down. A late vote is not
		// wrong, it is simply no longer interesting.
		return
	}

	if _, seen := n.votes[m.From]; !seen {
		n.votes[m.From] = m.VoteGranted
	}

	granted, rejected := 0, 0
	for _, ok := range n.votes {
		if ok {
			granted++
		} else {
			rejected++
		}
	}

	switch {
	case granted >= n.cfg.quorum():
		n.becomeLeader()
	case rejected >= n.cfg.quorum():
		// A majority refused, so this election cannot be won. Step down and
		// wait out the timer rather than campaigning again immediately: an
		// instant retry at a higher term would disrupt whoever legitimately
		// wins, over and over.
		n.becomeFollower(n.term, None)
	}
}
