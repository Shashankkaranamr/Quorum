// Package storage provides crash-safe persistence for Raft state.
//
// Raft requires that currentTerm, votedFor and the log are durable before the
// node acts on them, because a granted vote and an accepted AppendEntries are
// both promises that must survive a crash. This package exists to make that
// fsync an explicit, auditable call rather than a side effect of some library.
//
// Layout, per node:
//
//	data/node-<id>/wal/000001.log          append-only segments
//	data/node-<id>/snap/<index>-<term>.snap
//
// HardState is written inside WAL records rather than to a separate file. That
// avoids atomic-rename, which is not portable to Windows.
//
// Record framing: [u32 length][u32 crc32c][u8 type][payload], with the CRC
// covering length, type and payload. Each Sync writes exactly one EntryBatch
// record holding that Ready's entries and, if it changed, its hard state, so a
// torn write can only ever lose a whole batch. SnapshotPointer is the other
// type: always the first record of its segment, naming a durable snapshot file
// and carrying the log tail and hard state, so recovery starts from the last
// one and never needs anything older.
//
// Invariants this package must uphold:
//
//   - Exactly one Sync per Ready batch, and at most one fsync: one if the batch
//     carries anything durable, none if it does not. The batch is the unit of
//     atomicity.
//   - Recovery from a torn tail always yields a prefix of the records that were
//     written, never a partial or corrupt one. Damage anywhere OTHER than the
//     tail of the last segment cannot be a torn write, and recovery refuses to
//     start rather than truncate away records that had been synced.
//   - After a failed write or fsync the log refuses every further call. What a
//     failed fsync left on disk is unknowable; only a restart that re-reads the
//     disk is safe.
//   - Snapshots are written payload-before-pointer: fsync the .snap file, then
//     roll to a fresh segment and append and fsync a SnapshotPointer record,
//     then delete every older segment and snapshot file. A crash at any point
//     in that sequence recovers to exactly the state before the snapshot or
//     exactly the state after it (TestCrashDuringSnapshot).
//
// Scope of the durability guarantee: Sync calls File.Sync, which is
// FlushFileBuffers on Windows and fsync on Unix. That protects against process
// and OS crashes. It does not protect against a drive with a volatile write
// cache that ignores flush barriers on power loss.
//
// MemStorage is the simulator's model of the same contract; WAL is the real
// thing. Both apply batches through one function, so they cannot disagree
// about what a batch or a snapshot means (TestWALAgreesWithMemStorage,
// TestWALAgreesWithMemStorageThroughSnapshots). Phase 3 built the WAL; phase 4
// added snapshots and segment reclamation.
package storage
