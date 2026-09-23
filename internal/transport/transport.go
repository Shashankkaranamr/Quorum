package transport

import "github.com/Shashankkaranamr/Quorum/raft"

// Transport delivers Raft messages to peers. Send is fire-and-forget.
//
// # Why there is no Recv here
//
// DESIGN.md §3 originally sketched this interface with a
// `Recv() <-chan raft.Message` alongside Send. That does not survive contact
// with the deterministic simulator: a channel needs a goroutine to feed it, and
// the simulator is deliberately single-threaded with no goroutines and no wall
// clock, so that a failing trial replays exactly from its seed.
//
// Narrowing the interface to Send alone resolves it without weakening
// anything. Inbound delivery is the driver's concern, not the transport's:
//
//   - the simulator's scheduler pushes messages straight into Node.Step;
//   - the real gRPC transport (phase 5) accumulates inbound messages from its
//     stream handlers and exposes its own channel, which the real driver
//     selects on alongside its ticker.
//
// Both still run identical consensus code, which was the point of having one
// interface in the first place. This divergence is recorded in DESIGN.md.
//
// # Delivery semantics
//
// Delivery is best effort by design. Dropping, delaying, duplicating or
// reordering a message must never violate safety, only liveness. The
// simulator does all four on purpose to prove it.
type Transport interface {
	// Send queues messages for delivery. It must not block and must not fail
	// the caller: an unreachable peer is an ordinary condition in Raft, not an
	// error the consensus core knows how to handle.
	Send(msgs []raft.Message)
}

// SendFunc adapts a plain function to Transport.
type SendFunc func(msgs []raft.Message)

// Send implements Transport.
func (f SendFunc) Send(msgs []raft.Message) { f(msgs) }

// Discard is a Transport that drops everything. It stands in for a node whose
// links are all down.
var Discard Transport = SendFunc(func([]raft.Message) {})
