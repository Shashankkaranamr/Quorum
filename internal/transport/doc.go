// Package transport defines the message-passing boundary between Raft nodes.
//
// The interface exists so that the same consensus core can run in two modes
// without a second code path:
//
//	inmem  a deterministic, single-threaded simulator driven by logical ticks,
//	       with a partition matrix, drop probability and a delay queue. Used by
//	       the fast test suite: no goroutines, no sleeps, seeded randomness.
//	grpcx  real gRPC over localhost between separate OS processes. Used by the
//	       demo, the visualizer, and the end-to-end fault-injection suite.
//
// Raft messages are one-way, so the transport is a fire-and-forget Send plus an
// inbound stream. Delivery is explicitly best-effort: dropping, delaying,
// duplicating or reordering a message must never violate safety, only
// liveness. The deterministic simulator does all four on purpose.
//
// Phase 2 defines the interface and inmem; phase 5 adds grpcx. It is currently
// a documented stub.
package transport
