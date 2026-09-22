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
// HardState is written as a record type inside the WAL rather than to a
// separate file. That avoids atomic-rename and directory-fsync entirely,
// neither of which is portable to Windows.
//
// Record framing: [u32 length][u32 crc32c][u8 type][payload], where type is one
// of HardState, Entries, or SnapshotPointer.
//
// Invariants this package must uphold:
//
//   - Exactly one Sync per Ready batch. The batch is the unit of atomicity.
//   - Recovery from a torn tail always yields a prefix of the records that were
//     written, never a partial or corrupt one.
//   - Snapshots are written payload-before-pointer: fsync the .snap file, then
//     append and fsync a SnapshotPointer record, then delete superseded WAL
//     segments. A crash at any point in that sequence leaves a recoverable
//     state.
//
// Scope of the durability guarantee: Sync calls File.Sync, which is
// FlushFileBuffers on Windows and fsync on Unix. That protects against process
// and OS crashes. It does not protect against a drive with a volatile write
// cache that ignores flush barriers on power loss.
//
// Phase 3 fills this package in. It is currently a documented stub.
package storage
