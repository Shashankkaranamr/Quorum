package storage

import (
	"fmt"

	"github.com/Shashankkaranamr/Quorum/raft"
)

// MemStorage is an in-memory Storage for the deterministic simulator.
//
// It is not a stub. It models the one thing about real storage that matters to
// correctness: Append and SetHardState buffer, and only Sync publishes. So a
// node that "crashes" before Sync loses exactly what a real node would lose.
// Batches and snapshots are applied by the same code the write-ahead log uses,
// so the two cannot disagree about what either means.
//
// The cost of an fsync is modelled by the simulator rather than here
// (testutil.Options.SyncCostTicks), so it applies equally to the real
// write-ahead log when a simulated cluster runs on disk.
//
// It is not safe for concurrent use. Its owner is the single driver that owns
// the node.
type MemStorage struct {
	// durable state
	hs   raft.HardState
	log  logState
	snap raft.Snapshot

	// buffered, not yet durable
	pendingEntries []raft.Entry
	pendingHS      *raft.HardState

	// Counters.
	SyncCount      uint64
	AppendCount    uint64
	EntriesWritten uint64
	SnapshotsSaved uint64
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

	if err := m.log.append(m.pendingEntries); err != nil {
		return err
	}
	m.pendingEntries = m.pendingEntries[:0]

	if m.pendingHS != nil {
		m.hs = *m.pendingHS
		m.pendingHS = nil
	}
	return nil
}

// SaveSnapshot implements Storage.
func (m *MemStorage) SaveSnapshot(snap raft.Snapshot) error {
	if len(m.pendingEntries) > 0 || m.pendingHS != nil {
		return errUnsyncedSnapshot
	}
	if snap.Meta.Index <= m.log.snap.Index {
		return fmt.Errorf("storage: snapshot at %d is not newer than the current one at %d",
			snap.Meta.Index, m.log.snap.Index)
	}
	m.log.applySnapshot(snap.Meta)
	m.snap = raft.Snapshot{Meta: snap.Meta, Data: append([]byte(nil), snap.Data...)}
	m.SnapshotsSaved++
	return nil
}

// InitialState implements Storage.
func (m *MemStorage) InitialState() (Recovered, error) {
	return Recovered{
		HardState: m.hs,
		Snapshot:  m.snap,
		Entries:   m.log.clone().entries,
	}, nil
}

// DurableEntries returns the entries after the snapshot that survived the last
// Sync.
//
// This is what a crash test reads: everything appended but not yet synced is
// gone, exactly as it would be on a real node.
func (m *MemStorage) DurableEntries() []raft.Entry { return m.log.clone().entries }

// DurableHardState returns the hard state that survived the last Sync.
func (m *MemStorage) DurableHardState() raft.HardState { return m.hs }

// DurableSnapshot returns the snapshot that survived, if any.
func (m *MemStorage) DurableSnapshot() raft.Snapshot { return m.snap }

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

// Wipe destroys everything, durable or not, as deleting a node's data
// directory would. The node comes back as if it had never run.
func (m *MemStorage) Wipe() {
	*m = MemStorage{}
}
