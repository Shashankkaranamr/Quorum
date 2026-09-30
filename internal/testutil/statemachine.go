package testutil

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"

	"github.com/Shashankkaranamr/Quorum/internal/server"
	"github.com/Shashankkaranamr/Quorum/raft"
)

// Hasher is a state machine that can summarize its whole state. The simulator
// uses it to check that every replica -- including one rebuilt from a snapshot
// -- holds exactly the same state at the same applied index.
type Hasher interface {
	Hash() ([32]byte, error)
}

// Digest is the simulator's default state machine. Its entire state is a
// running SHA-256 over every entry applied, in order.
//
// That makes it the strictest possible witness for snapshots: two replicas have
// equal digests at index i only if they applied identical entries at every
// index up to i, or restored a snapshot of a replica that did. A snapshot that
// lost, duplicated or reordered anything -- or was filed under the wrong index
// -- produces a digest no replica that applied the log could have, and
// SnapshotFidelity reports it. Unlike the key-value store it accepts any
// payload, so the randomized trials can propose arbitrary bytes.
type Digest struct {
	applied raft.Index
	sum     [32]byte
}

// Apply implements server.StateMachine.
func (d *Digest) Apply(entries []raft.Entry) error {
	for _, e := range entries {
		if e.Index != d.applied+1 {
			return fmt.Errorf("digest: applying index %d after %d", e.Index, d.applied)
		}
		h := sha256.New()
		h.Write(d.sum[:])
		var hdr [17]byte
		binary.LittleEndian.PutUint64(hdr[0:], uint64(e.Index))
		binary.LittleEndian.PutUint64(hdr[8:], uint64(e.Term))
		hdr[16] = byte(e.Type)
		h.Write(hdr[:])
		h.Write(e.Data)
		copy(d.sum[:], h.Sum(nil))
		d.applied = e.Index
	}
	return nil
}

// Snapshot implements server.StateMachine: the applied index and the digest.
func (d *Digest) Snapshot() ([]byte, raft.Index, error) {
	out := binary.LittleEndian.AppendUint64(nil, uint64(d.applied))
	return append(out, d.sum[:]...), d.applied, nil
}

// Restore implements server.StateMachine.
func (d *Digest) Restore(snap raft.Snapshot) error {
	if len(snap.Data) != 8+32 {
		return fmt.Errorf("digest: snapshot is %d bytes, want 40", len(snap.Data))
	}
	if at := raft.Index(binary.LittleEndian.Uint64(snap.Data)); at != snap.Meta.Index {
		return fmt.Errorf("digest: snapshot filed at %d describes the state at %d", snap.Meta.Index, at)
	}
	d.applied = snap.Meta.Index
	copy(d.sum[:], snap.Data[8:])
	return nil
}

// Hash implements Hasher.
func (d *Digest) Hash() ([32]byte, error) { return d.sum, nil }

// Recorder wraps a node's state machine. It reports every application to the
// checker, so State Machine Safety can be verified, and -- when the machine can
// hash itself -- the resulting state, so SnapshotFidelity can be.
type Recorder struct {
	id      raft.NodeID
	cluster *Cluster

	// Inner is the real state machine.
	Inner server.StateMachine

	// Applied is what this life of the node applied, in order, after
	// RestoredAt.
	Applied []raft.Entry

	// RestoredAt is the index of the last snapshot the machine was restored
	// from, or zero. Entries up to it were never applied by this machine; the
	// snapshot stands in for them.
	RestoredAt raft.Index

	// Restores counts snapshot restores, at startup and from a leader.
	Restores int
}

var _ server.StateMachine = (*Recorder)(nil)

// Apply implements server.StateMachine.
func (r *Recorder) Apply(entries []raft.Entry) error {
	for _, e := range entries {
		want := r.RestoredAt + 1
		if n := len(r.Applied); n > 0 {
			want = r.Applied[n-1].Index + 1
		}
		if e.Index != want {
			return fmt.Errorf("node %d applied index %d, expected %d: entries must arrive in order",
				r.id, e.Index, want)
		}
		if err := r.Inner.Apply([]raft.Entry{e}); err != nil {
			return err
		}
		r.Applied = append(r.Applied, e)
		r.cluster.Checker.RecordApply(r.cluster.tick, r.id, e)
		if err := r.recordHash(e.Index, "applying"); err != nil {
			return err
		}
	}
	return nil
}

// Snapshot implements server.StateMachine.
func (r *Recorder) Snapshot() ([]byte, raft.Index, error) { return r.Inner.Snapshot() }

// Restore implements server.StateMachine.
func (r *Recorder) Restore(snap raft.Snapshot) error {
	if err := r.Inner.Restore(snap); err != nil {
		return err
	}
	r.Applied = nil
	r.RestoredAt = snap.Meta.Index
	r.Restores++
	return r.recordHash(snap.Meta.Index, "restoring a snapshot")
}

// LastApplied is the index the machine is at.
func (r *Recorder) LastApplied() raft.Index {
	if n := len(r.Applied); n > 0 {
		return r.Applied[n-1].Index
	}
	return r.RestoredAt
}

func (r *Recorder) recordHash(i raft.Index, how string) error {
	h, ok := r.Inner.(Hasher)
	if !ok {
		return nil
	}
	sum, err := h.Hash()
	if err != nil {
		return err
	}
	r.cluster.Checker.RecordStateHash(r.cluster.tick, r.id, i, sum, how)
	return nil
}
