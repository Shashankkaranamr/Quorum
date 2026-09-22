// Package grpcx is the real gRPC transport used between separate OS processes.
//
// Each node dials every peer and holds one long-lived unidirectional stream per
// directed link. A directed link maps 1:1 to a stream, which is what makes
// one-way partitions expressible, and one-way partitions are where the most
// interesting Raft bugs live.
//
// Fault injection is enforced here, below Raft and above TCP: a partitioned
// link has its stream closed and subsequent connections from the blocked peer
// rejected. Real sockets really close, and the consensus core cannot tell this
// from a severed cable.
//
// It is not, however, a kernel firewall rule. A reviewer should know the
// difference, so DESIGN.md and the README both say so rather than letting the
// demo imply something stronger than it does.
//
// Phase 5 fills this package in. It is currently a documented stub.
package grpcx
