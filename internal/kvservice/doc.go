// Package kvservice implements the client-facing gRPC KV API on top of Raft.
//
// Writes are proposed as log entries, and the caller waits for the entry to be
// applied. If a different entry lands at the reserved index, which is what
// happens when leadership changes mid-flight, the waiter resolves as
// ErrLostLeadership and the client retries with the same sequence number.
// Deduplication is what makes that retry safe.
//
// Reads use ReadIndex, not a leader lease:
//
//  1. On election the leader commits a no-op entry. Raft requires this anyway
//     before prior-term entries can be committed by replica counting, and it is
//     what lets a new leader learn the true commit index.
//  2. Record readIndex = commitIndex.
//  3. Confirm leadership with a heartbeat round acknowledged by a quorum.
//  4. Wait until lastApplied >= readIndex, then serve from the state machine.
//
// A leader that has been silently partitioned fails step 3, so its read errors
// instead of returning a stale value. That is the specific claim the phase 5
// test suite has to prove, with a test that would fail if it were false.
//
// A leader lease fast path is documented as a possible optimization but is off
// by default, because it trades correctness for an assumption about bounded
// clock drift that this project does not want to quietly depend on.
//
// A non-leader returns NOT_LEADER together with a leader hint, so the client
// library can redirect without a full rediscovery round.
//
// Phase 5 fills this package in. It is currently a documented stub.
package kvservice
