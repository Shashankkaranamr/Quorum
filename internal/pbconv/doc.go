// Package pbconv converts between the consensus core's plain Go types and the
// generated protobuf types used on the wire and on disk.
//
// This package exists for one reason: so that package raft has no dependency on
// the protobuf runtime. That keeps the "the core does no I/O" claim
// mechanically checkable by inspecting one package's import graph, instead of
// being something a reader has to take on faith.
//
// The same encoding is used for the network and for the write-ahead log, so
// there is exactly one serialization format to get right and exactly one to
// fuzz.
//
// Phase 2 and phase 3 fill this package in. It is currently a documented stub.
package pbconv
