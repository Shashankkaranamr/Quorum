// Package client is the Go client library for the Quorum KV service.
//
// It owns the three things that make a retry safe:
//
//   - A cluster-unique client_id obtained via RegisterClient, which is itself a
//     Raft log entry, so the id survives a leader change.
//   - A monotonic per-client sequence number. A retry after an ambiguous
//     failure reuses the same seq, which is what lets the state machine
//     recognize it as a duplicate and return the cached response.
//   - Leader discovery and redirect on NOT_LEADER, using the returned hint.
//
// The contract the caller sees: a call either succeeds, fails definitively, or
// is safe to retry verbatim. There is no fourth case in which a retry could
// apply a write twice.
//
// Phase 5 fills this package in. It is currently a documented stub.
package client
