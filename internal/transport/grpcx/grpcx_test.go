package grpcx

import (
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"google.golang.org/grpc"

	"github.com/Shashankkaranamr/Quorum/raft"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

// pair is two transports serving each other over localhost.
type pair struct {
	a, b *Transport
	stop func()
}

func newPair(t *testing.T) pair {
	t.Helper()
	la, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	lb, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addrs := map[raft.NodeID]string{1: la.Addr().String(), 2: lb.Addr().String()}

	a, err := New(1, addrs, nil)
	require.NoError(t, err)
	b, err := New(2, addrs, nil)
	require.NoError(t, err)
	sa, sb := grpc.NewServer(), grpc.NewServer()
	a.Register(sa)
	b.Register(sb)
	go func() { _ = sa.Serve(la) }()
	go func() { _ = sb.Serve(lb) }()
	return pair{a: a, b: b, stop: func() {
		sa.Stop()
		sb.Stop()
		require.NoError(t, a.Close())
		require.NoError(t, b.Close())
	}}
}

func heartbeat(from, to raft.NodeID) raft.Message {
	return raft.Message{Type: raft.MsgAppendEntries, From: from, To: to, Term: 1}
}

// deliveredWithin reports whether a message sent a->b arrives within d.
func deliveredWithin(p pair, d time.Duration) bool {
	deadline := time.After(d)
	for {
		p.a.Send([]raft.Message{heartbeat(1, 2)})
		select {
		case <-p.b.Inbound():
			return true
		case <-deadline:
			return false
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// TestPartitionIsDirectedAndHeals: blocking a->b stops delivery that way only,
// and healing restores it -- what makes one-way partitions expressible.
func TestPartitionIsDirectedAndHeals(t *testing.T) {
	p := newPair(t)
	defer p.stop()
	require.True(t, deliveredWithin(p, 5*time.Second), "a->b never connected")

	p.a.BlockOutbound(2)
	for len(p.b.Inbound()) > 0 {
		<-p.b.Inbound()
	}
	require.False(t, deliveredWithin(p, 300*time.Millisecond), "a blocked link delivered a message")

	p.b.Send([]raft.Message{heartbeat(2, 1)})
	select {
	case <-p.a.Inbound():
	case <-time.After(5 * time.Second):
		t.Fatal("blocking a->b also cut b->a")
	}

	p.a.Heal()
	require.True(t, deliveredWithin(p, 5*time.Second), "healing did not restore a->b")

	p.b.BlockInbound(1)
	for len(p.b.Inbound()) > 0 {
		<-p.b.Inbound()
	}
	require.False(t, deliveredWithin(p, 300*time.Millisecond), "an inbound block delivered a message")
}

// TestFaultStateIsSafeToChangeUnderTraffic changes the injected faults over
// and over while messages flow both ways. It is meant for the race detector,
// which is where it can fail: the fault state is read by the sender, by every
// peer goroutine and by every inbound stream while a test changes it. The
// first version read the fault maps outside the lock (BUGS.md, 2026-09-30).
func TestFaultStateIsSafeToChangeUnderTraffic(t *testing.T) {
	p := newPair(t)
	defer p.stop()

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := range 200 {
			p.a.Send([]raft.Message{heartbeat(1, 2)})
			p.b.Send([]raft.Message{heartbeat(2, 1)})
			if i%10 == 0 {
				time.Sleep(time.Millisecond)
			}
		}
	}()
	for i := range 100 {
		switch i % 3 {
		case 0:
			p.a.Partition(2)
		case 1:
			p.b.BlockInbound(1)
		default:
			p.a.Heal()
			p.b.Heal()
		}
		time.Sleep(time.Millisecond)
	}
	<-done
}
