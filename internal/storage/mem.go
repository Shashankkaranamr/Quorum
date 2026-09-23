package storage

import (
	"fmt"

	"github.com/Shashankkaranamr/Quorum/raft"
)

// MemStorage is an in-memory Storage for the deterministic simulator.
//
// It is not a stub. It models the two things about real storage that matter to
// correctness:
//
//  1. Append and SetHardState buffer; only Sync publishes. So a node that
//     "crashes" before Sync loses exactly what a real node would lose, and the
//     phase 3 crash tests can be written against this before the real
//     write-ahead log exists.
//  2. Sync can be made to cost logical ticks, which is what lets the simulator
//     reproduce the head-of-line-blocking hazard DESIGN.md names: one goroutine
//     owns ticks and durability, so a slow fsync delays ticks and can stall an
//     election.
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

	// SyncCostTicks is how many logical ticks a Sync takes. Zero means
	// instant. The simulator withholds Ready processing for this many ticks,
	// which makes tick lag real rather than a counter that is always zero.
	SyncCostTicks int

	// Counters. SyncCount is what phase 3's "exactly one fsync per Ready"
	// test asserts on.
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

	for _, e := range m.pendingEntries {
		switch {
		case e.Index == 0:
			return fmt.Errorf("storage: entry with index 0")
		case e.Index <= raft.Index(len(m.entries)):
			// Overwrite: a leader corrected us. Truncate anything after it,
			// because those entries belonged to a branch that lost.
			m.entries = m.entries[:e.Index-1]
			m.entries = append(m.entries, e)
		case e.Index == raft.Index(len(m.entries))+1:
			m.entries = append(m.entries, e)
		default:
			return fmt.Errorf("storage: non-contiguous entry at index %d, log ends at %d",
				e.Index, len(m.entries))
		}
	}
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

// DropPending discards everything buffered but not yet synced.
//
// This is what a crash costs. A node killed with SIGKILL loses exactly the
// writes that had not reached the disk, and modelling that here is what lets
// the phase 3 crash tests be written against the simulator before the real
// write-ahead log exists.
func (m *MemStorage) DropPending() {
	m.pendingEntries = m.pendingEntries[:0]
	m.pendingHS = nil
}
