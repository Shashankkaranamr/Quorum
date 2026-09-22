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
// A checker nobody has tested is worth very little, so the suite also carries
// negative controls: deliberately mutated Raft implementations that each
// checker must catch. A checker that has never been shown to fail is not
// evidence of anything.
//
// This package also holds the Porcupine model used to verify that recorded
// client histories are linearizable.
//
// Phase 2 fills this package in. It is currently a documented stub.
package testutil
