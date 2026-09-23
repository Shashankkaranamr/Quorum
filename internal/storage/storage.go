package storage

import "github.com/Shashankkaranamr/Quorum/raft"

// Storage is the durability boundary.
//
// Append and SetHardState BUFFER. Only Sync makes anything durable. That split
// is the whole point of the interface: it puts the fsync at a single, named,
// auditable call rather than leaving it implicit in whatever a library does
// underneath.
//
// The contract the driver relies on:
//
//	Append(entries)       // buffered, not durable
//	SetHardState(hs)      // buffered, not durable
//	Sync()                // everything buffered is now durable
//
// Nothing a node has promised another node may be sent before Sync returns. A
// granted vote and an accepted AppendEntries are both durable promises, and a
// crash in that window lets a node contradict itself -- two leaders in one term
// follows directly.
//
// Phase 2 satisfies this with MemStorage. Phase 3 puts the real write-ahead log
// behind the same interface, and the ordering tests written against MemStorage
// keep working unchanged.
type Storage interface {
	// Append buffers entries. Entries already present at those indices are
	// overwritten, which is how a follower accepts a leader's correction.
	Append(entries []raft.Entry) error

	// SetHardState buffers the term, vote and commit index.
	SetHardState(hs raft.HardState) error

	// Sync makes everything buffered durable. This is the fsync.
	Sync() error

	// InitialState returns what recovery found: the persisted hard state, the
	// log, and how far the state machine had been applied. Phase 3 reads this
	// from the write-ahead log; phase 2 reads it from memory.
	InitialState() (raft.HardState, []raft.Entry, raft.Index, error)
}
