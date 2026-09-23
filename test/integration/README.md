# Integration tests

Reserved for **phase 6**. Currently empty.

This is where the end-to-end fault-injection suite lives: tests that start real
`quorum-node` processes over real localhost gRPC, then kill, partition, freeze
and restart them.

It is deliberately separate from the fast deterministic suite under
`internal/testutil/`. That one runs the consensus core in a single-threaded
simulator with logical time and finishes in milliseconds. This one exists to
cover the three things the simulator cannot:

1. the gRPC transport,
2. WAL file I/O,
3. the driver's goroutine and ticker wiring.

That list is exhaustive and is stated in
[DESIGN.md §3](../../DESIGN.md#3-transport-and-how-one-core-serves-two-test-modes).
Tests here should exercise something from it; anything that could be tested in
the simulator instead belongs there, where it is faster and reproducible from a
seed.

Acceptance criteria: [PLAN.md § Phase 6](../../PLAN.md#phase-6--fault-injection-suite-and-bug-log).
