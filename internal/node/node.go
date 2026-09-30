// Package node assembles one Quorum replica from its parts: the write-ahead
// log, the state machine, the consensus core, the driver loop, the gRPC
// transport, the client-facing KV service and the admin service, all on one
// gRPC listener.
//
// It is what cmd/quorum-node runs, and what the in-process integration tests
// start several of. Having one assembly for both is the point: a test that
// passes against a Node exercises the wiring the binary ships, not a
// test-only imitation of it.
//
// Two ways to stop a node, and the difference matters:
//
//   - Stop is a clean shutdown: the listener closes, the loop finishes its
//     current Ready, and the write-ahead log is closed.
//   - Kill abandons the node the way a crash would: the listener closes and
//     the write-ahead log is dropped without a flush. In-process this is a
//     simulation of a crash. The real-process tests (and phase 6) kill actual
//     OS processes with TerminateProcess/SIGKILL.
package node

import (
	crand "crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"path/filepath"
	"time"

	"google.golang.org/grpc"

	"github.com/Shashankkaranamr/Quorum/internal/admin"
	"github.com/Shashankkaranamr/Quorum/internal/config"
	"github.com/Shashankkaranamr/Quorum/internal/kvservice"
	"github.com/Shashankkaranamr/Quorum/internal/server"
	"github.com/Shashankkaranamr/Quorum/internal/statemachine"
	"github.com/Shashankkaranamr/Quorum/internal/storage"
	"github.com/Shashankkaranamr/Quorum/internal/transport/grpcx"
	"github.com/Shashankkaranamr/Quorum/raft"
)

// Options configure one node.
type Options struct {
	Cluster *config.Cluster
	ID      raft.NodeID

	// Listener, if set, is used instead of listening on the configured gRPC
	// address. Tests bind port 0 first and build the config from the result.
	Listener net.Listener

	// RequestTimeout is kvservice's per-request bound. Zero means its
	// default.
	RequestTimeout time.Duration

	// Logf receives operational messages. Nil discards them.
	Logf func(format string, args ...any)

	// Fault injection and observation for tests. Leave zero elsewhere.
	Hooks          kvservice.Hooks
	OnApply        func(statemachine.Result)
	UnsafeMutation raft.Mutation
}

// Node is one running replica.
type Node struct {
	id    raft.NodeID
	wal   *storage.WAL
	loop  *server.Loop
	trans *grpcx.Transport
	srv   *grpc.Server
	lis   net.Listener
	serve chan error
}

// Start recovers the node's state from its data directory and starts serving.
func Start(opts Options) (*Node, error) {
	c := opts.Cluster
	self, ok := c.Node(config.NodeID(opts.ID))
	if !ok {
		return nil, fmt.Errorf("node: id %d is not in the cluster config", opts.ID)
	}
	logf := opts.Logf
	if logf == nil {
		logf = func(string, ...any) {}
	}

	wal, err := storage.OpenWAL(filepath.Join(c.DataDir(self.ID), "wal"),
		storage.WALOptions{SegmentBytes: c.Storage.WALSegmentBytes})
	if err != nil {
		return nil, err
	}
	rec, err := wal.InitialState()
	if err != nil {
		_ = wal.Close()
		return nil, err
	}

	// The state machine is rebuilt from the snapshot before the core exists,
	// so that the first Ready's committed entries continue from it.
	kv := statemachine.New()
	kv.OnApply = opts.OnApply
	if !rec.Snapshot.IsEmpty() {
		if err := kv.Restore(rec.Snapshot); err != nil {
			_ = wal.Close()
			return nil, fmt.Errorf("node: restore snapshot: %w", err)
		}
	}

	peers := make([]raft.NodeID, 0, len(c.Nodes))
	addrs := make(map[raft.NodeID]string, len(c.Nodes))
	for _, n := range c.Nodes {
		peers = append(peers, raft.NodeID(n.ID))
		addrs[raft.NodeID(n.ID)] = n.GRPCAddr()
	}

	// Election jitter must differ between processes and between restarts,
	// so the real node seeds from the operating system. The simulator seeds
	// deterministically instead; the core cannot tell the difference.
	var seed [16]byte
	if _, err := crand.Read(seed[:]); err != nil {
		_ = wal.Close()
		return nil, fmt.Errorf("node: seed election jitter: %w", err)
	}
	rng := rand.New(rand.NewPCG(binary.LittleEndian.Uint64(seed[:8]), binary.LittleEndian.Uint64(seed[8:])))

	core, err := raft.New(raft.Config{
		ID:                      opts.ID,
		Peers:                   peers,
		ElectionTimeoutMinTicks: c.Raft.ElectionTimeoutMinTicks,
		ElectionTimeoutMaxTicks: c.Raft.ElectionTimeoutMaxTicks,
		HeartbeatTimeoutTicks:   c.Raft.HeartbeatTimeoutTicks,
		MaxEntriesPerAppend:     c.Raft.MaxEntriesPerAppend,
		Rand:                    func(n int) int { return rng.IntN(max(n, 1)) },
		HardState:               rec.HardState,
		Snapshot:                rec.Snapshot,
		Entries:                 rec.Entries,
		UnsafeMutation:          opts.UnsafeMutation,
	})
	if err != nil {
		_ = wal.Close()
		return nil, err
	}

	trans, err := grpcx.New(opts.ID, addrs, grpcx.Options{
		Logf:       logf,
		MaxBackoff: time.Duration(c.Raft.TickMS*c.Raft.HeartbeatTimeoutTicks) * time.Millisecond,
	})
	if err != nil {
		_ = wal.Close()
		return nil, err
	}

	lis := opts.Listener
	if lis == nil {
		if lis, err = net.Listen("tcp", self.GRPCAddr()); err != nil {
			_ = trans.Close()
			_ = wal.Close()
			return nil, fmt.Errorf("node: listen on %s: %w", self.GRPCAddr(), err)
		}
	}

	loop := server.NewLoop(core, wal, trans, kv, trans.Inbound(), server.LoopOptions{
		Tick:              time.Duration(c.Raft.TickMS) * time.Millisecond,
		SnapshotThreshold: uint64(c.Raft.SnapshotThresholdEntries),
		Logf:              logf,
	})

	srv := grpc.NewServer()
	trans.Register(srv)
	kvservice.New(loop, kv, addrs, opts.RequestTimeout, opts.Hooks).Register(srv)
	admin.New(admin.Config{
		ID:             opts.ID,
		Peers:          peers,
		Tick:           time.Duration(c.Raft.TickMS) * time.Millisecond,
		ReachableTicks: c.Raft.ElectionTimeoutMinTicks,
	}, loop, trans).Register(srv)

	n := &Node{id: opts.ID, wal: wal, loop: loop, trans: trans, srv: srv, lis: lis, serve: make(chan error, 1)}
	go func() { n.serve <- srv.Serve(lis) }()
	return n, nil
}

// ID is this node's id.
func (n *Node) ID() raft.NodeID { return n.id }

// Addr is the address the node is serving on.
func (n *Node) Addr() string { return n.lis.Addr().String() }

// Status is the node's state as of its loop's last iteration.
func (n *Node) Status() raft.Status { return n.loop.Status() }

// Metrics is the node's driver counters.
func (n *Node) Metrics() server.Metrics { return n.loop.Metrics() }

// Snapshot is everything the node's loop last published.
func (n *Node) Snapshot() server.Snapshot { return n.loop.Snapshot() }

// Transport exposes fault injection on this node's links.
func (n *Node) Transport() *grpcx.Transport { return n.trans }

// Done is closed if the node's loop stops on its own, which happens only on a
// storage failure.
func (n *Node) Done() <-chan struct{} { return n.loop.Done() }

// Stop shuts the node down cleanly.
func (n *Node) Stop() error { return n.halt(false) }

// Kill abandons the node as a crash would: nothing buffered is flushed.
func (n *Node) Kill() error { return n.halt(true) }

func (n *Node) halt(crash bool) error {
	// The listener goes first, so no new request or message can arrive, then
	// the loop, which owns the log, then the log itself.
	n.srv.Stop()
	serveErr := <-n.serve
	if errors.Is(serveErr, grpc.ErrServerStopped) {
		serveErr = nil
	}
	n.loop.Stop()
	errs := []error{serveErr, n.trans.Close()}
	if crash {
		errs = append(errs, n.wal.Crash())
	} else {
		errs = append(errs, n.wal.Close(), n.loop.Err())
	}
	return errors.Join(errs...)
}
