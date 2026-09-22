// Package admin exposes node introspection and fault injection.
//
// Two consumers share this surface deliberately: the visualizer's buttons and
// the automated fault-injection suite drive the same code, so what the demo
// shows cannot drift away from what the tests actually exercise.
//
// Introspection: role, term, commitIndex, lastApplied, log tail, peer
// connectivity, fsync count and tick lag. These are served from an immutable
// status snapshot published by the driver through an atomic pointer, so readers
// never block the consensus loop and cannot corrupt it.
//
// Fault injection: partition and heal a set of directed links, and freeze or
// thaw the node. Freeze parks the driver's event loop, so the process stays
// alive and the socket keeps accepting while nothing is processed. From the
// cluster's point of view that is a faithful freeze. It is cooperative rather
// than SIGSTOP, and the docs say so.
//
// Node kills are not here; they belong to the supervisor, because killing a
// process is not something the process can be asked to do to itself.
//
// Phase 6 and phase 7 fill this package in. It is currently a documented stub.
package admin
