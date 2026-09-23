package raft

import "fmt"

// raftLog is the replicated log plus the three indices that move through it.
//
// Layout: entries[0] has index snapIndex+1. snapIndex is 0 until phase 4
// introduces snapshots, so for now entries[0] is index 1 and the log is a plain
// slice. The offset arithmetic is here from the start because retrofitting it
// after compaction exists means auditing every index expression again.
//
// Invariants, all asserted by tests:
//
//	snapIndex <= stable <= lastIndex()
//	applied   <= committed <= lastIndex()
//
// committed <= lastIndex() is why a follower can never commit an entry it does
// not have, no matter what a leader claims.
type raftLog struct {
	// snapIndex and snapTerm describe the last entry folded into a snapshot.
	// Zero means nothing has been compacted.
	snapIndex Index
	snapTerm  Term

	entries []Entry

	committed Index
	applied   Index

	// stable is the highest index known to be durable. The caller advances it
	// through Advance after its storage has fsynced. Nothing may depend on an
	// entry above this line having survived a crash -- which is why a leader
	// counts itself in the commit quorum only up to stable, not lastIndex.
	stable Index

	// revision increments on every mutation. It exists so an observer can tell
	// in O(1) whether the log changed since it last looked, instead of copying
	// and comparing it every tick -- which, at a few hundred ticks per trial
	// and a few hundred trials, is the dominant cost of the test suite.
	revision uint64
}

func newLog(entries []Entry, applied Index) *raftLog {
	l := &raftLog{
		entries: append([]Entry(nil), entries...),
		applied: applied,
	}
	l.stable = l.lastIndex()
	return l
}

func (l *raftLog) firstIndex() Index { return l.snapIndex + 1 }

func (l *raftLog) lastIndex() Index {
	if n := len(l.entries); n > 0 {
		return l.entries[n-1].Index
	}
	return l.snapIndex
}

// term returns the term of the entry at i. ok is false when i has been
// compacted away or is beyond the end of the log.
func (l *raftLog) term(i Index) (Term, bool) {
	switch {
	case i == l.snapIndex:
		return l.snapTerm, true
	case i < l.snapIndex || i > l.lastIndex():
		return 0, false
	default:
		return l.entries[i-l.snapIndex-1].Term, true
	}
}

func (l *raftLog) lastTerm() Term {
	t, _ := l.term(l.lastIndex())
	return t
}

// slice returns entries in [lo, hi), clamped to what the log actually holds.
func (l *raftLog) slice(lo, hi Index) []Entry {
	if lo < l.firstIndex() {
		lo = l.firstIndex()
	}
	if hi > l.lastIndex()+1 {
		hi = l.lastIndex() + 1
	}
	if lo >= hi {
		return nil
	}
	out := make([]Entry, hi-lo)
	copy(out, l.entries[lo-l.snapIndex-1:hi-l.snapIndex-1])
	return out
}

// matchTerm reports whether the log contains an entry at index i with term t.
// This is the AppendEntries consistency check.
func (l *raftLog) matchTerm(i Index, t Term) bool {
	got, ok := l.term(i)
	return ok && got == t
}

// isUpToDate implements the §5.4.1 voting restriction: a voter refuses a
// candidate whose log is behind its own.
//
// "At least as up to date" is defined by comparing the LAST ENTRY'S TERM first,
// and only if those are equal, the index. The ordering matters and is the
// single easiest thing in Raft to get backwards: a longer log with an older
// last term is NOT more up to date, because those extra entries belong to a
// term that lost, and they can legitimately be overwritten.
//
// This comparison is the entire mechanism behind Leader Completeness. If it is
// wrong, a candidate missing a committed entry can win, and the committed entry
// is lost.
func (l *raftLog) isUpToDate(lastIdx Index, lastTerm Term) bool {
	myTerm := l.lastTerm()
	if lastTerm != myTerm {
		return lastTerm > myTerm
	}
	return lastIdx >= l.lastIndex()
}

// append adds entries to the end of the log. The caller guarantees they are
// contiguous with lastIndex.
func (l *raftLog) append(ents ...Entry) {
	if len(ents) == 0 {
		return
	}
	if want := l.lastIndex() + 1; ents[0].Index != want {
		panic(fmt.Sprintf("raft: non-contiguous append: got index %d, want %d", ents[0].Index, want))
	}
	l.entries = append(l.entries, ents...)
	l.revision++
}

// truncateFrom deletes the entry at index i and everything after it.
//
// It also pulls stable back, because entries that no longer exist cannot be
// durable, and it refuses to cut below committed: an implementation that
// truncates a committed entry has already violated State Machine Safety, and
// failing loudly here turns a silent corruption into a crash with a stack
// trace. The randomized trials rely on that.
func (l *raftLog) truncateFrom(i Index) {
	if i <= l.committed {
		panic(fmt.Sprintf("raft: refusing to truncate committed index %d (committed=%d)", i, l.committed))
	}
	if i > l.lastIndex() {
		return
	}
	if i <= l.snapIndex {
		panic(fmt.Sprintf("raft: refusing to truncate compacted index %d (snapIndex=%d)", i, l.snapIndex))
	}
	l.entries = l.entries[:i-l.snapIndex-1]
	l.revision++
	if l.stable > l.lastIndex() {
		l.stable = l.lastIndex()
	}
}

// commitTo advances the commit index. It never moves backwards, and never past
// the end of the log.
func (l *raftLog) commitTo(i Index) bool {
	if i <= l.committed {
		return false
	}
	if last := l.lastIndex(); i > last {
		i = last
	}
	if i <= l.committed {
		return false
	}
	l.committed = i
	return true
}

// nextApplicable returns the entries that are committed but not yet applied.
func (l *raftLog) nextApplicable() []Entry {
	if l.applied >= l.committed {
		return nil
	}
	return l.slice(l.applied+1, l.committed+1)
}

// unstableEntries returns entries that have not yet been reported durable.
func (l *raftLog) unstableEntries() []Entry {
	if l.stable >= l.lastIndex() {
		return nil
	}
	return l.slice(l.stable+1, l.lastIndex()+1)
}

// findConflict locates where incoming entries first disagree with the log.
//
// It returns the index of the first entry whose term differs from what we hold,
// or 0 if every incoming entry we already have matches. Entries beyond our end
// are not conflicts, they are simply new.
//
// This is what makes a delayed or duplicated AppendEntries harmless. Blindly
// truncating at prevLogIndex+1 and appending would let a stale message that the
// simulator reordered delete entries the node had already accepted -- and if
// any of them were committed, that is an immediate safety violation. Raft §5.3
// is explicit: truncate only from the first genuinely conflicting entry.
func (l *raftLog) findConflict(ents []Entry) Index {
	for _, e := range ents {
		if !l.matchTerm(e.Index, e.Term) {
			return e.Index
		}
	}
	return 0
}

// conflictHint builds the backtracking hint for a rejected AppendEntries.
//
// A plain "decrement nextIndex by one" is correct but costs one round trip per
// entry, which makes a far-behind follower take thousands of rounds to catch
// up. Reporting the first index of the conflicting term lets the leader skip
// the whole term at once (§5.3).
func (l *raftLog) conflictHint(prevLogIndex Index) (conflictIndex Index, conflictTerm Term) {
	last := l.lastIndex()
	if prevLogIndex > last {
		// We simply do not have that far. Tell the leader where our log ends
		// so it can resume from there instead of probing downward one at a
		// time.
		return last + 1, 0
	}

	t, ok := l.term(prevLogIndex)
	if !ok {
		return last + 1, 0
	}

	// Walk back to the first index of the conflicting term.
	first := prevLogIndex
	for first > l.firstIndex() {
		pt, ok := l.term(first - 1)
		if !ok || pt != t {
			break
		}
		first--
	}
	return first, t
}
