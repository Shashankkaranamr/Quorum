// Package inmem is the deterministic in-process transport used by the fast test
// suite.
//
// Every scheduling decision derives from a test-supplied seed, so a failing run
// reproduces exactly from its seed alone. Messages are queued against logical
// tick deadlines rather than wall-clock timers, which is why a scenario that
// would take sixty seconds of real elections runs in microseconds.
//
// Supported faults: bidirectional and one-way partitions, uniform and targeted
// message drops, bounded and unbounded delays, duplication, and reordering.
//
// Phase 2 fills this package in. It is currently a documented stub.
package inmem
