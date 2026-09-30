package grpcx

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	raftv1 "github.com/Shashankkaranamr/Quorum/gen/quorum/raft/v1"
	"github.com/Shashankkaranamr/Quorum/internal/pbconv"
	"github.com/Shashankkaranamr/Quorum/internal/transport"
	"github.com/Shashankkaranamr/Quorum/raft"
)

const (
	// inboundCap and outboundCap bound the queues between the network and the
	// driver loop. When one fills, messages are dropped rather than blocking:
	// Raft tolerates loss by design, and a blocked sender would couple one
	// slow peer to every other link.
	inboundCap  = 4096
	outboundCap = 1024

	minBackoff = 10 * time.Millisecond

	// DefaultMaxBackoff caps the reconnect delay when Options leaves it zero.
	DefaultMaxBackoff = 100 * time.Millisecond
)

// Options configure a Transport.
type Options struct {
	// Logf receives operational messages. Nil discards them.
	Logf func(string, ...any)

	// MaxBackoff caps how long a peer's goroutine waits between reconnect
	// attempts. It must stay well under the election timeout: a node that
	// restarts and hears nothing from the leader for a full election timeout
	// campaigns and deposes it. The node sets it to one heartbeat interval,
	// which config validation keeps below a third of the election timeout.
	// The first version used a fixed 500ms, and every restart forced an
	// election (BUGS.md, 2026-09-30).
	MaxBackoff time.Duration
}

// Transport is the real Raft transport: one long-lived client stream per
// directed link, and the RaftTransport service for the streams peers open to
// us.
//
// Send never blocks. Each peer has a queue and a goroutine that owns the
// connection to it, reconnecting with backoff when the stream breaks; while a
// link is down its messages are dropped, which Raft treats like any other
// loss.
//
// Fault injection lives here, below Raft and above TCP. A blocked outbound
// link drops everything queued for it and holds no stream open. A blocked
// inbound link rejects the peer's stream the moment it delivers a message, so
// its socket really closes. The consensus core cannot tell either from a cut
// cable. It is not a kernel firewall rule; DESIGN.md §3 says what that does
// and does not exercise.
type Transport struct {
	raftv1.UnimplementedRaftTransportServer

	self    raft.NodeID
	inbound chan raft.Message
	peers   map[raft.NodeID]*peer
	logf    func(string, ...any)
	maxWait time.Duration
	closed  chan struct{}

	// mu guards the injected faults. It is the one lock in the transport,
	// listed in DESIGN.md §1: the fault state is read by the driver loop
	// (Send), by every peer goroutine and by every inbound stream handler,
	// and changed by whoever injects the fault.
	mu         sync.Mutex
	blockedOut map[raft.NodeID]bool
	blockedIn  map[raft.NodeID]bool
}

var _ transport.Transport = (*Transport)(nil)

// New creates a transport for self. addrs maps every OTHER node to its gRPC
// address. Outbound connections are made lazily, by each peer's goroutine.
func New(self raft.NodeID, addrs map[raft.NodeID]string, opts Options) (*Transport, error) {
	logf := opts.Logf
	if logf == nil {
		logf = func(string, ...any) {}
	}
	if opts.MaxBackoff <= 0 {
		opts.MaxBackoff = DefaultMaxBackoff
	}
	t := &Transport{
		self:       self,
		inbound:    make(chan raft.Message, inboundCap),
		peers:      make(map[raft.NodeID]*peer, len(addrs)),
		logf:       logf,
		maxWait:    max(opts.MaxBackoff, minBackoff),
		closed:     make(chan struct{}),
		blockedOut: map[raft.NodeID]bool{},
		blockedIn:  map[raft.NodeID]bool{},
	}
	for id, addr := range addrs {
		if id == self {
			continue
		}
		conn, err := grpc.NewClient(addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			// gRPC redials a lost server on its own schedule, starting at 1s
			// and growing to two minutes by default. Left alone, that alone
			// outlasts any election timeout, whatever this transport's own
			// backoff says. Both are capped at the same bound.
			grpc.WithConnectParams(grpc.ConnectParams{
				Backoff: backoff.Config{
					BaseDelay:  minBackoff,
					Multiplier: 1.6,
					Jitter:     0.2,
					MaxDelay:   t.maxWait,
				},
				MinConnectTimeout: time.Second,
			}))
		if err != nil {
			_ = t.closePeers()
			return nil, fmt.Errorf("grpcx: client for node %d at %s: %w", id, addr, err)
		}
		t.peers[id] = &peer{
			id:    id,
			t:     t,
			conn:  conn,
			queue: make(chan raft.Message, outboundCap),
			done:  make(chan struct{}),
		}
	}
	for _, p := range t.peers {
		go p.run()
	}
	return t, nil
}

// Register adds the RaftTransport service to a gRPC server.
func (t *Transport) Register(s grpc.ServiceRegistrar) { raftv1.RegisterRaftTransportServer(s, t) }

// Inbound is where messages from peers arrive, for the driver loop to select
// on.
func (t *Transport) Inbound() <-chan raft.Message { return t.inbound }

// Send implements transport.Transport. It never blocks.
func (t *Transport) Send(msgs []raft.Message) {
	for _, m := range msgs {
		p := t.peers[m.To]
		if p == nil || t.isBlocked(outbound, m.To) {
			continue
		}
		select {
		case p.queue <- m:
		default:
			// The peer is not keeping up. Dropping is safe; blocking the
			// driver loop on one slow link would not be.
		}
	}
}

// Stream implements raftv1.RaftTransportServer: it receives the messages one
// peer sends us over its directed link.
func (t *Transport) Stream(s grpc.ClientStreamingServer[raftv1.Message, raftv1.StreamSummary]) error {
	var received uint64
	for {
		pm, err := s.Recv()
		if errors.Is(err, io.EOF) {
			return s.SendAndClose(&raftv1.StreamSummary{MessagesReceived: received})
		}
		if err != nil {
			return err
		}
		m, err := pbconv.MessageFromProto(pm)
		if err != nil {
			return status.Errorf(codes.InvalidArgument, "grpcx: undecodable message: %v", err)
		}
		if m.To != t.self {
			return status.Errorf(codes.InvalidArgument, "grpcx: message for node %d delivered to node %d", m.To, t.self)
		}
		if t.isBlocked(inbound, m.From) {
			return status.Errorf(codes.Unavailable, "grpcx: link %d->%d is partitioned", m.From, t.self)
		}
		select {
		case t.inbound <- m:
			received++
		case <-t.closed:
			return status.Error(codes.Unavailable, "grpcx: transport closed")
		default:
			// The driver loop is behind. Drop, as the network would.
		}
	}
}

// Partition cuts this node's links to and from peers, in both directions.
func (t *Transport) Partition(peers ...raft.NodeID) {
	t.BlockOutbound(peers...)
	t.BlockInbound(peers...)
}

// BlockOutbound cuts the directed links from this node to peers. Used alone it
// makes a one-way partition: this node can hear them, they cannot hear it.
func (t *Transport) BlockOutbound(peers ...raft.NodeID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, p := range peers {
		t.blockedOut[p] = true
	}
}

// BlockInbound cuts the directed links from peers to this node.
func (t *Transport) BlockInbound(peers ...raft.NodeID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, p := range peers {
		t.blockedIn[p] = true
	}
}

// Blocked reports the peers whose links are cut, outbound and inbound, sorted.
func (t *Transport) Blocked() (out, in []raft.NodeID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for id := range t.blockedOut {
		out = append(out, id)
	}
	for id := range t.blockedIn {
		in = append(in, id)
	}
	slices.Sort(out)
	slices.Sort(in)
	return out, in
}

// Heal removes every injected fault.
func (t *Transport) Heal() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.blockedOut = map[raft.NodeID]bool{}
	t.blockedIn = map[raft.NodeID]bool{}
}

// direction selects which set of blocked links isBlocked consults.
type direction bool

const (
	outbound direction = false
	inbound  direction = true
)

// isBlocked reports whether the link to or from id is cut. The set is chosen
// inside the lock: Heal replaces the maps, so even reading which map to look
// in is a read of guarded state. The first version took the map as an
// argument, evaluated before the lock was held, and the race detector caught
// it (BUGS.md, 2026-09-30).
func (t *Transport) isBlocked(d direction, id raft.NodeID) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if d == inbound {
		return t.blockedIn[id]
	}
	return t.blockedOut[id]
}

// Close stops every peer goroutine and closes every outbound connection.
// Inbound streams end when the gRPC server they arrived on stops.
func (t *Transport) Close() error {
	select {
	case <-t.closed:
		return nil
	default:
	}
	close(t.closed)
	for _, p := range t.peers {
		<-p.done
	}
	return t.closePeers()
}

func (t *Transport) closePeers() error {
	var errs []error
	for _, p := range t.peers {
		if err := p.conn.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// peer owns the directed link from this node to one other.
type peer struct {
	id    raft.NodeID
	t     *Transport
	conn  *grpc.ClientConn
	queue chan raft.Message
	done  chan struct{}
}

func (p *peer) run() {
	defer close(p.done)
	backoff := minBackoff
	for {
		if p.t.isBlocked(outbound, p.id) {
			if !p.discardFor(minBackoff) {
				return
			}
			continue
		}
		sent, err := p.stream()
		if err == nil {
			return // closed
		}
		if sent > 0 {
			backoff = minBackoff
		}
		// Messages that arrive while the link is down are dropped, not
		// saved for later: a stale AppendEntries or vote delivered after a
		// reconnect is harmless but useless.
		if !p.discardFor(backoff) {
			return
		}
		backoff = min(2*backoff, p.t.maxWait)
	}
}

// stream opens one stream and sends on it until it fails, the link is blocked,
// or the transport closes (nil error).
func (p *peer) stream() (sent int, err error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, err := raftv1.NewRaftTransportClient(p.conn).Stream(ctx)
	if err != nil {
		return 0, err
	}
	for {
		select {
		case <-p.t.closed:
			_, _ = s.CloseAndRecv()
			return sent, nil
		case m := <-p.queue:
			if p.t.isBlocked(outbound, p.id) {
				return sent, errors.New("link blocked")
			}
			pm, err := pbconv.MessageToProto(m)
			if err != nil {
				p.t.logf("grpcx: cannot encode %s: %v", m, err)
				continue
			}
			if err := s.Send(pm); err != nil {
				return sent, err
			}
			sent++
		}
	}
}

// discardFor drops queued messages for d. It reports false if the transport
// closed meanwhile.
func (p *peer) discardFor(d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	for {
		select {
		case <-p.t.closed:
			return false
		case <-p.queue:
		case <-timer.C:
			return true
		}
	}
}
