// Package statemachine is the replicated key-value store that Raft entries are
// applied to, together with the client session table that makes retries safe.
//
// Apply must be deterministic: every replica applying the same entry at the
// same index must reach exactly the same state, including the session table.
// Nothing here may consult the clock, the network, or a random source.
//
// Session-based deduplication: each command carries (client_id, seq). The
// machine keeps sessions[client_id] = {last_seq, last_response}. On apply, a
// command whose seq is not greater than last_seq returns the cached response
// instead of being applied a second time. This is what makes a client retry
// after an ambiguous failure safe, and it is why the session table is part of
// the snapshot rather than process-local memory: a node that restarts from a
// snapshot must still recognize a duplicate.
//
// Known limits, carried into the docs rather than discovered later: one
// in-flight request per session (the response cache is a single slot), and
// session expiry can in principle break exactly-once delivery for a client that
// has been idle past the garbage-collection window.
//
// Snapshot renders the data and the session table together, sorted, so two
// replicas in the same state produce byte-identical snapshots and can be
// compared by hash. Restore replaces both.
//
// Phase 4 built the store, the session table and snapshotting, because
// "deduplication survives a snapshot" cannot be tested against anything less.
// Phase 5 serves it over gRPC. Session expiry is not implemented yet: a
// session is never garbage-collected, so STATUS_SESSION_EXPIRED is returned
// only for a client id that was never registered.
package statemachine
