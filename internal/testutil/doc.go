// Package testutil is the deterministic cluster harness and the invariant
// checkers.
//
// The harness owns N raft.Nodes, in-memory storage and the inmem transport, and
// steps them from a single goroutine. After every step it asserts the five Raft
// safety properties:
//
//	Election Safety      at most one leader is elected in a given term
//	Leader Append-Only   a leader never overwrites or deletes its own entries
//	Log Matching         same index and term implies identical preceding entries
//	Leader Completeness  a committed entry appears in every future leader's log
//	State Machine Safety no two nodes apply different commands at one index
//
// plus two of its own: CommittedEntriesAreStable, the direct form of "a
// committed entry is never altered", and SnapshotFidelity, which compares every
// replica's state-machine hash at every applied index so that a replica rebuilt
// from a snapshot is checked as strictly as one that applied every entry. The
// default state machine, Digest, is a running hash of everything applied, which
// makes that comparison cheap enough to run on every tick.
//
// A checker nobody has tested is worth very little, so the suite also carries
// negative controls: deliberately mutated Raft implementations, and
// deliberately broken storage and state machines, that each checker must
// catch. A checker that has never been shown to fail is not evidence of
// anything.
//
// Phase 5 adds the Porcupine model used to verify that recorded client
// histories are linearizable.
package testutil
