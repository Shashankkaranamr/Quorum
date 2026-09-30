package storage

import (
	"fmt"

	"github.com/Shashankkaranamr/Quorum/raft"
)

// logState is a durable log as storage sees it: the snapshot it starts after,
// and the entries that follow.
//
// MemStorage, the write-ahead log's live state and its recovery all go through
// the methods here, so the simulator and a restarted real node cannot disagree
// about what a batch or a snapshot means.
type logState struct {
	snap    raft.SnapshotMeta
	entries []raft.Entry // entries[0].Index == snap.Index+1
}

func (l *logState) lastIndex() raft.Index {
	if n := len(l.entries); n > 0 {
		return l.entries[n-1].Index
	}
	return l.snap.Index
}

func (l *logState) term(i raft.Index) (raft.Term, bool) {
	switch {
	case i == l.snap.Index:
		return l.snap.Term, true
	case i < l.snap.Index || i > l.lastIndex():
		return 0, false
	default:
		return l.entries[i-l.snap.Index-1].Term, true
	}
}

// check reports whether batch could be appended, without appending it. The
// write-ahead log calls it before writing, because a batch that recovery could
// not replay is far better refused now than discovered at the next restart.
func (l *logState) check(batch []raft.Entry) error {
	last := l.lastIndex()
	for _, e := range batch {
		switch {
		case e.Index <= l.snap.Index:
			return fmt.Errorf("storage: entry at index %d is inside the snapshot, which ends at %d",
				e.Index, l.snap.Index)
		case e.Index > last+1:
			return fmt.Errorf("storage: non-contiguous entry at index %d, log ends at %d",
				e.Index, last)
		}
		last = e.Index
	}
	return nil
}

// append applies one batch. The rule is the follower's rule from §5.3: an entry
// at an index the log already holds replaces it and everything after it,
// because those later entries belonged to a branch that lost.
func (l *logState) append(batch []raft.Entry) error {
	if err := l.check(batch); err != nil {
		return err
	}
	for _, e := range batch {
		pos := int(e.Index - l.snap.Index - 1)
		// Clip the capacity so the append allocates rather than writing into
		// an array a caller may still be holding.
		l.entries = append(l.entries[:pos:pos], e)
	}
	return nil
}

// applySnapshot folds the log into a snapshot at m, by the rule in Raft's
// Figure 13: if the log holds m's entry with m's term, everything after it is
// still valid and is kept; otherwise the whole log is discarded. The consensus
// core applies the same rule to its own log (raft.raftLog.restore), which is
// what keeps the durable log and the in-memory one in agreement.
//
// For a snapshot the node took itself the entry is always there. For one
// received from the leader it may not be, and then nothing in the log can be
// shown to agree with the leader's.
func (l *logState) applySnapshot(m raft.SnapshotMeta) {
	if t, ok := l.term(m.Index); ok && t == m.Term && m.Index >= l.snap.Index {
		l.entries = append([]raft.Entry(nil), l.entries[m.Index-l.snap.Index:]...)
	} else {
		l.entries = nil
	}
	l.snap = m
}

func (l *logState) clone() logState {
	return logState{snap: l.snap, entries: append([]raft.Entry(nil), l.entries...)}
}
