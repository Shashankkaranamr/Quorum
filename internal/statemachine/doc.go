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
// Phase 5 fills this package in; phase 4 adds snapshot and restore. It is
// currently a documented stub.
package statemachine
