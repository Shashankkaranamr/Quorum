package storage

import (
	"errors"

	"github.com/Shashankkaranamr/Quorum/raft"
)

// Storage is the durability boundary.
//
// Append and SetHardState BUFFER. Only Sync makes anything durable. That split
// is the whole point of the interface: it puts the fsync at a single, named,
// auditable call rather than leaving it implicit in whatever a library does
// underneath.
//
// The contract the driver relies on:
//
//	SaveSnapshot(snap)    // durable when it returns
//	Append(entries)       // buffered, not durable
//	SetHardState(hs)      // buffered, not durable
//	Sync()                // everything buffered is now durable
//
// Nothing a node has promised another node may be sent before Sync returns. A
// granted vote and an accepted AppendEntries are both durable promises, and a
// crash in that window lets a node contradict itself -- two leaders in one term
// follows directly.
//
// MemStorage satisfies it for the simulator and WAL for real nodes. The
// simulator can run on either, which is how the crash tests exercise real
// recovery from disk.
type Storage interface {
	// Append buffers entries. Entries already present at those indices are
	// overwritten, which is how a follower accepts a leader's correction.
	Append(entries []raft.Entry) error

	// SetHardState buffers the term, vote and commit index.
	SetHardState(hs raft.HardState) error

	// Sync makes everything buffered durable. This is the fsync.
	Sync() error

	// SaveSnapshot makes snap durable and folds the log into it: entries at
	// or below its index are dropped, and those after it are kept only if
	// the log holds the snapshot's own last entry (raft's Figure 13 rule).
	//
	// Unlike Append it does not buffer. It is called with nothing buffered --
	// either between Readies, for a snapshot the node took itself, or first
	// thing in a Ready that installs one from the leader -- and it is durable
	// when it returns. A snapshot at or below the current one is refused:
	// rewriting the file a durable pointer names is how a crash would turn a
	// good snapshot into a torn one.
	SaveSnapshot(snap raft.Snapshot) error

	// InitialState returns what recovery found.
	InitialState() (Recovered, error)
}

// Recovered is a node's durable state as found at startup: everything needed
// to rebuild it.
//
// The state machine is restored from Snapshot, and the log continues from
// there with Entries. Snapshot is empty when nothing has been compacted.
type Recovered struct {
	HardState raft.HardState
	Snapshot  raft.Snapshot
	Entries   []raft.Entry
}

// errUnsyncedSnapshot is returned when SaveSnapshot is called with writes
// still buffered. Saving the snapshot first would make it durable ahead of
// entries that were appended before it, which is an order the log cannot
// express.
var errUnsyncedSnapshot = errors.New("storage: SaveSnapshot called with unsynced writes buffered; " +
	"Sync them first")
