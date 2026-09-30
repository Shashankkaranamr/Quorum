package server_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/Shashankkaranamr/Quorum/internal/server"
	"github.com/Shashankkaranamr/Quorum/internal/storage"
	"github.com/Shashankkaranamr/Quorum/internal/transport"
	"github.com/Shashankkaranamr/Quorum/raft"
)

// slowDisk makes every Sync that has something to write take a fixed time,
// the way a slow or contended disk makes fsync take.
type slowDisk struct {
	*storage.MemStorage
	delay   time.Duration
	pending bool
}

func (s *slowDisk) Append(e []raft.Entry) error {
	s.pending = s.pending || len(e) > 0
	return s.MemStorage.Append(e)
}

func (s *slowDisk) Sync() error {
	if s.pending {
		time.Sleep(s.delay)
		s.pending = false
	}
	return s.MemStorage.Sync()
}

// noopSM satisfies server.StateMachine for tests that only care about the
// loop.
type noopSM struct{ applied raft.Index }

func (m *noopSM) Apply(ents []raft.Entry) error {
	if n := len(ents); n > 0 {
		m.applied = ents[n-1].Index
	}
	return nil
}
func (m *noopSM) Snapshot() ([]byte, raft.Index, error) { return nil, m.applied, nil }
func (m *noopSM) Restore(s raft.Snapshot) error         { m.applied = s.Meta.Index; return nil }

func singleNodeLoop(t *testing.T, store storage.Storage, tick time.Duration) *server.Loop {
	t.Helper()
	core, err := raft.New(raft.Config{
		ID: 1, Peers: []raft.NodeID{1},
		ElectionTimeoutMinTicks: 4, ElectionTimeoutMaxTicks: 8, HeartbeatTimeoutTicks: 1,
		MaxEntriesPerAppend: 64, Rand: func(int) int { return 0 },
	})
	require.NoError(t, err)
	return server.NewLoop(core, store, transport.Discard, &noopSM{}, nil, server.LoopOptions{Tick: tick})
}

// TestTickLagSeesASlowDisk: the loop owns ticks, messages and fsyncs, so a slow
// fsync delays ticks -- the hazard DESIGN.md §1 says is measured rather than
// assumed. This proves the measurement works in the real loop: with every
// fsync taking five ticks, the loop must report ticks piling up.
//
// The first version of the loop could not: it counted ticks as they arrived
// from a time.Ticker, which drops ticks its receiver is too busy to take, so
// the backlog it was meant to measure never reached it (BUGS.md, 2026-09-30).
func TestTickLagSeesASlowDisk(t *testing.T) {
	defer goleak.VerifyNone(t)
	const tick = 5 * time.Millisecond
	l := singleNodeLoop(t, &slowDisk{MemStorage: storage.NewMem(), delay: 5 * tick}, tick)
	defer l.Stop() // before goleak's check, which was deferred first

	require.Eventually(t, func() bool { return l.Status().Role == raft.Leader }, 5*time.Second, tick)
	for range 10 {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, err := l.Propose(ctx, raft.EntryNormal, []byte("x"))
		cancel()
		require.NoError(t, err)
	}
	m := l.Metrics()
	t.Logf("worst tick lag %d ticks, %d ticks processed", m.MaxTickLagTicks, m.TicksProcessed)
	require.GreaterOrEqual(t, m.MaxTickLagTicks, uint64(3),
		"five-tick fsyncs left no visible tick lag: the metric cannot see the hazard it exists for")
}

// TestTickLagIsZeroOnAFastDisk is the other half: without a slow disk the loop
// keeps up, and the metric must say so rather than report lag regardless.
func TestTickLagIsZeroOnAFastDisk(t *testing.T) {
	defer goleak.VerifyNone(t)
	const tick = 20 * time.Millisecond
	l := singleNodeLoop(t, storage.NewMem(), tick)
	defer l.Stop()
	require.Eventually(t, func() bool { return l.Status().Role == raft.Leader }, 5*time.Second, tick)
	time.Sleep(10 * tick)
	require.LessOrEqual(t, l.Metrics().MaxTickLagTicks, uint64(1))
}
