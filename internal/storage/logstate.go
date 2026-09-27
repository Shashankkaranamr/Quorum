package storage

import (
	"fmt"

	"github.com/Shashankkaranamr/Quorum/raft"
)

// appendEntries applies one batch of entries to a durable log and returns the
// result.
//
// MemStorage's Sync and the write-ahead log's recovery both go through this, so
// the simulator and a restarted real node cannot disagree about what a batch
// means. The rule is the follower's rule from §5.3: an entry at an index the
// log already holds replaces it and everything after it, because those later
// entries belonged to a branch that lost.
//
// The input log is not modified; the caller keeps the returned slice.
func appendEntries(log, batch []raft.Entry) ([]raft.Entry, error) {
	for _, e := range batch {
		last := raft.Index(len(log))
		switch {
		case e.Index == 0:
			return nil, fmt.Errorf("storage: entry with index 0")
		case e.Index <= last:
			// Clip the capacity so the append below allocates rather than
			// writing into an array the caller may still be holding.
			log = append(log[:e.Index-1:e.Index-1], e)
		case e.Index == last+1:
			log = append(log, e)
		default:
			return nil, fmt.Errorf("storage: non-contiguous entry at index %d, log ends at %d",
				e.Index, last)
		}
	}
	return log, nil
}
