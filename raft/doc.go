// Package raft implements the Raft consensus algorithm as a pure state machine.
//
// This is deliberately the only exported (non-internal) package in the
// repository, because its defining property is a claim that must be cheap for a
// reviewer to verify: it performs no I/O. It does not read the clock, touch the
// network, open a file, or start a goroutine. Logical time advances only when a
// caller invokes Tick. Randomness (for election timeout jitter) is injected
// through Config rather than drawn from math/rand.
//
// The interface is modelled on the Step/Ready design used by etcd:
//
//	Tick()                     advance logical time by one tick
//	Step(Message)              deliver an inbound message
//	Propose([]byte)            append a command (leader only)
//	Ready() Ready              what must now be persisted, sent, and applied
//	Advance()                  acknowledge that a Ready was fully processed
//
// Because the core neither blocks nor performs I/O, its caller decides the
// ordering of durability and transmission. That ordering is a correctness
// requirement, not a detail, and it lives in internal/server.
//
// Two consequences of this split are worth stating plainly:
//
//   - There is no shared mutable Raft state, and therefore no lock protecting
//     it. A single driver goroutine owns the Node exclusively.
//   - The deterministic test simulator and the real multi-process deployment
//     run identical consensus code. Only the clock and the I/O implementations
//     differ.
//
// Wire and disk encoding live outside this package (see internal/pbconv) so
// that the protobuf runtime is not a dependency of the consensus core. That is
// what keeps the no-I/O claim mechanically checkable; see purity_test.go.
//
// Phase 2 fills this package in. It is currently a documented stub.
package raft
