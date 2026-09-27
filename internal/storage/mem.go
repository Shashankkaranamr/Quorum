package storage

import "github.com/Shashankkaranamr/Quorum/raft"

// MemStorage is an in-memory Storage for the deterministic simulator.
//
// It is not a stub. It models the one thing about real storage that matters to
// correctness: Append and SetHardState buffer, and only Sync publishes. So a
// node that "crashes" before Sync loses exactly what a real node would lose.
// Batches are applied by the same function the write-ahead log's recovery
// uses, so the two cannot disagree about what a batch means.
//
// The cost of an fsync is modelled by the simulator rather than here
// (testutil.Options.SyncCostTicks), so it applies equally to the real
// write-ahead log when a simulated cluster runs on disk.
//
// It is not safe for concurrent use. Its owner is the single driver that owns
// the node.
type MemStorage struct {
	// durable state
	hs      raft.HardState
	entries []raft.Entry
	applied raft.Index

	// buffered, not yet durable
	pendingEntries []raft.Entry
	pendingHS      *raft.HardState

	// Counters.
	SyncCount      uint64
	AppendCount    uint64
	EntriesWritten uint64
}

var _ Storage = (*MemStorage)(nil)

// NewMem returns empty storage for a fresh node.
func NewMem() *MemStorage { return &MemStorage{} }

// Append implements Storage.
func (m *MemStorage) Append(entries []raft.Entry) error {
	if len(entries) == 0 {
		return nil
	}
	m.AppendCount++
	m.EntriesWritten += uint64(len(entries))
	m.pendingEntries = append(m.pendingEntries, entries...)
	return nil
}

// SetHardState implements Storage.
func (m *MemStorage) SetHardState(hs raft.HardState) error {
	copied := hs
	m.pendingHS = &copied
	return nil
}

// Sync implements Storage. This is the fsync.
func (m *MemStorage) Sync() error {
	m.SyncCount++

	entries, err := appendEntries(m.entries, m.pendingEntries)
	if err != nil {
		return err
	}
	m.entries = entries
	m.pendingEntries = m.pendingEntries[:0]

	if m.pendingHS != nil {
		m.hs = *m.pendingHS
		m.pendingHS = nil
	}
	return nil
}

// InitialState implements Storage.
func (m *MemStorage) InitialState() (raft.HardState, []raft.Entry, raft.Index, error) {
	return m.hs, append([]raft.Entry(nil), m.entries...), m.applied, nil
}

// SetApplied records how far the state machine has been applied. Phase 4 uses
// this to decide when to snapshot.
func (m *MemStorage) SetApplied(i raft.Index) { m.applied = i }

// DurableEntries returns the entries that survived the last Sync.
//
// This is what a crash test reads: everything appended but not yet synced is
// gone, exactly as it would be on a real node.
func (m *MemStorage) DurableEntries() []raft.Entry {
	return append([]raft.Entry(nil), m.entries...)
}

// DurableHardState returns the hard state that survived the last Sync.
func (m *MemStorage) DurableHardState() raft.HardState { return m.hs }

// PendingCount is how much is buffered but not durable. A crash loses exactly
// this much.
func (m *MemStorage) PendingCount() int { return len(m.pendingEntries) }

// Crash discards everything buffered but not yet synced.
//
// This is what a crash costs. A node killed with SIGKILL loses exactly the
// writes that had not reached the disk. The durable state stays, and the
// simulator restarts the node from it by calling InitialState again.
func (m *MemStorage) Crash() error {
	m.pendingEntries = m.pendingEntries[:0]
	m.pendingHS = nil
	return nil
}
