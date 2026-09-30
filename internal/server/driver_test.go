package server_test

import (
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/Shashankkaranamr/Quorum/internal/server"
	"github.com/Shashankkaranamr/Quorum/internal/storage"
	"github.com/Shashankkaranamr/Quorum/internal/testutil"
	"github.com/Shashankkaranamr/Quorum/internal/transport"
	"github.com/Shashankkaranamr/Quorum/internal/transport/inmem"
	"github.com/Shashankkaranamr/Quorum/raft"
)

// ---------------------------------------------------------------------------
// The ordering recorder
// ---------------------------------------------------------------------------

// durabilityAudit sits on both sides of one node's driver -- between it and its
// storage, and between it and its transport -- and checks every outbound
// message against what is durable at the instant the message is handed over.
//
// It checks two things, because either alone can be fooled:
//
//   - Structure: no message is sent while a Sync is in progress or while
//     Append/SetHardState writes are buffered and unsynced.
//   - Substance: the promise the message makes is already durable. A granted
//     vote requires the vote on disk; an AppendEntries success requires the
//     acknowledged entries on disk; any message requires its term on disk.
//
// A promise stamped with term T is also covered once a term GREATER than T is
// durable. One Ready can span several steps -- grant a vote in term 14, then
// see term 18 and vote again -- and only the final state is written. That is
// safe because the whole batch is durable before any of it is sent, so the
// disk holds exactly what a node that persisted after every single step would
// hold, and a node whose disk says term 18 can never act in term 14 again. The
// first version of this audit did not know that; see BUGS.md, 2026-09-27.
//
// The substance check is what catches a driver that sends BEFORE it even
// starts buffering the batch: at that moment nothing is pending, so the
// structural check alone would pass it.
type durabilityAudit struct {
	id raft.NodeID

	// What the wrapped storage has made durable, modelled independently of it.
	term  raft.Term
	vote  raft.NodeID
	snap  raft.SnapshotMeta
	terms []raft.Term // terms[i] is the term of durable entry snap.Index+i+1

	pendingEntries []raft.Entry
	pendingHS      *raft.HardState
	syncing        bool

	sends      int
	snapshots  int
	violations []string
}

func newAudit(id raft.NodeID, s storage.Storage) (*durabilityAudit, error) {
	rec, err := s.InitialState()
	if err != nil {
		return nil, err
	}
	a := &durabilityAudit{id: id, term: rec.HardState.Term, vote: rec.HardState.VotedFor,
		snap: rec.Snapshot.Meta}
	for _, e := range rec.Entries {
		a.terms = append(a.terms, e.Term)
	}
	return a, nil
}

func (a *durabilityAudit) violate(m raft.Message, format string, args ...any) {
	a.violations = append(a.violations,
		fmt.Sprintf("node %d sent %s: %s", a.id, m, fmt.Sprintf(format, args...)))
}

func (a *durabilityAudit) durableLast() raft.Index { return a.snap.Index + raft.Index(len(a.terms)) }

func (a *durabilityAudit) durableTermAt(i raft.Index) (raft.Term, bool) {
	switch {
	case i == a.snap.Index:
		return a.snap.Term, true
	case i < a.snap.Index || i > a.durableLast():
		return 0, false
	default:
		return a.terms[i-a.snap.Index-1], true
	}
}

// checkSend is the verdict on one outbound message.
func (a *durabilityAudit) checkSend(m raft.Message) {
	a.sends++
	if a.syncing {
		a.violate(m, "while Sync was still in progress")
	}
	if len(a.pendingEntries) > 0 || a.pendingHS != nil {
		a.violate(m, "with %d entries and hard state=%v buffered but not synced",
			len(a.pendingEntries), a.pendingHS != nil)
	}

	if m.Term > a.term {
		a.violate(m, "advertising term %d when only term %d is durable", m.Term, a.term)
	}
	if m.Term < a.term {
		return // superseded by a durable later term, as above
	}
	switch m.Type {
	case raft.MsgRequestVote:
		if a.term != m.Term || a.vote != m.From {
			a.violate(m, "campaigning without its own vote durable (durable term=%d vote=%d)", a.term, a.vote)
		}
	case raft.MsgRequestVoteResp:
		if m.VoteGranted && (a.term != m.Term || a.vote != m.To) {
			a.violate(m, "granting a vote that is not durable (durable term=%d vote=%d)", a.term, a.vote)
		}
	case raft.MsgAppendEntriesResp:
		if m.Success {
			if m.MatchIndex > a.durableLast() {
				a.violate(m, "acknowledging index %d with only %d entries durable", m.MatchIndex, a.durableLast())
			}
		}
	case raft.MsgAppendEntries:
		for _, e := range m.Entries {
			if t, ok := a.durableTermAt(e.Index); !ok || t != e.Term {
				a.violate(m, "replicating entry %d@%d that is not durable locally", e.Index, e.Term)
				break
			}
		}
	case raft.MsgInstallSnapshot:
		if m.SnapshotMeta.Index > a.snap.Index {
			a.violate(m, "sending snapshot %d@%d when only snapshot %d is durable locally",
				m.SnapshotMeta.Index, m.SnapshotMeta.Term, a.snap.Index)
		}
	}
}

func (a *durabilityAudit) err() error {
	if len(a.violations) == 0 {
		return nil
	}
	return fmt.Errorf("%d durability-ordering violations, first: %s", len(a.violations), a.violations[0])
}

// auditedStorage forwards to real storage and keeps the audit's model of what
// is durable in step with it.
type auditedStorage struct {
	inner storage.Storage
	audit *durabilityAudit
}

func (s *auditedStorage) Append(entries []raft.Entry) error {
	s.audit.pendingEntries = append(s.audit.pendingEntries, entries...)
	return s.inner.Append(entries)
}

func (s *auditedStorage) SetHardState(hs raft.HardState) error {
	s.audit.pendingHS = &hs
	return s.inner.SetHardState(hs)
}

func (s *auditedStorage) Sync() error {
	a := s.audit
	a.syncing = true
	err := s.inner.Sync()
	a.syncing = false
	if err != nil {
		return err
	}
	// Only now, with Sync returned, does the model treat the batch as durable.
	for _, e := range a.pendingEntries {
		pos := e.Index - a.snap.Index - 1
		a.terms = append(a.terms[:pos], e.Term)
	}
	if hs := a.pendingHS; hs != nil {
		a.term, a.vote = hs.Term, hs.VotedFor
	}
	a.pendingEntries, a.pendingHS = nil, nil
	return nil
}

// SaveSnapshot is durable when it returns, so the model folds its log at once,
// by the same keep-the-suffix-if-it-matches rule storage applies.
func (s *auditedStorage) SaveSnapshot(snap raft.Snapshot) error {
	if err := s.inner.SaveSnapshot(snap); err != nil {
		return err
	}
	a, m := s.audit, snap.Meta
	a.snapshots++
	if t, ok := a.durableTermAt(m.Index); ok && t == m.Term && m.Index >= a.snap.Index {
		a.terms = append([]raft.Term(nil), a.terms[m.Index-a.snap.Index:]...)
	} else {
		a.terms = nil
	}
	a.snap = m
	return nil
}

func (s *auditedStorage) InitialState() (storage.Recovered, error) {
	return s.inner.InitialState()
}

func (s *auditedStorage) Crash() error {
	s.audit.pendingEntries, s.audit.pendingHS = nil, nil
	return s.inner.(testutil.Crasher).Crash()
}

func (s *auditedStorage) Close() error {
	if c, ok := s.inner.(io.Closer); ok {
		return c.Close()
	}
	return nil
}

// auditedTransport checks each message before it leaves.
type auditedTransport struct {
	inner transport.Transport
	audit *durabilityAudit
}

func (t auditedTransport) Send(msgs []raft.Message) {
	for _, m := range msgs {
		t.audit.checkSend(m)
	}
	t.inner.Send(msgs)
}

// withAudit wires audits into a cluster's options. A restarted node gets a
// fresh audit seeded from what its storage recovered; the returned slice keeps
// every audit from every life, so a violation before a crash is not lost when
// the node comes back.
func withAudit(opts *testutil.Options) *[]*durabilityAudit {
	audits := map[raft.NodeID]*durabilityAudit{}
	var all []*durabilityAudit
	base := opts.Storage
	if base == nil {
		base = testutil.MemStorageFactory()
	}
	opts.Storage = func(id raft.NodeID) (storage.Storage, error) {
		inner, err := base(id)
		if err != nil {
			return nil, err
		}
		a, err := newAudit(id, inner)
		if err != nil {
			return nil, err
		}
		audits[id] = a
		all = append(all, a)
		return &auditedStorage{inner: inner, audit: a}, nil
	}
	opts.WrapTransport = func(id raft.NodeID, t transport.Transport) transport.Transport {
		return auditedTransport{inner: t, audit: audits[id]}
	}
	return &all
}

// ---------------------------------------------------------------------------
// A randomized schedule to run the audits under
// ---------------------------------------------------------------------------

// runFaultySchedule drives a cluster through elections, proposals, message
// loss, partitions and crash/restart cycles, so every kind of message a node
// can send is sent many times, including by nodes that just recovered.
func runFaultySchedule(t *testing.T, c *testutil.Cluster, seed uint64, ticks int) {
	t.Helper()
	rng := rand.New(rand.NewPCG(seed, 7))
	n := len(c.IDs)
	down := map[raft.NodeID]bool{}
	for i := range ticks {
		if rng.IntN(100) < 2 {
			id := c.IDs[rng.IntN(n)]
			if down[id] {
				require.NoError(t, c.Restart(id))
				delete(down, id)
			} else if n-len(down) > n/2+1 {
				c.Crash(id)
				down[id] = true
			}
		}
		if rng.IntN(100) < 2 {
			c.Heal()
			if rng.IntN(2) == 0 {
				c.Isolate(c.IDs[rng.IntN(n)])
			}
		}
		if id, ok := c.Leader(); ok && rng.IntN(100) < 30 {
			_, _, _ = c.Propose(id, []byte(fmt.Sprintf("v%d", i)))
		}
		c.Tick()
	}
	c.Heal()
	for id := range down {
		require.NoError(t, c.Restart(id))
	}
	c.RunTicks(100)
	require.NoError(t, c.Err())
}

func faultyOptions(n int, seed uint64) testutil.Options {
	opts := testutil.DefaultOptions(n, seed)
	opts.Fault = inmem.Fault{DropRate: 0.05, DuplicateRate: 0.05, MaxDelayTicks: 2, ReorderDueBatch: true}
	return opts
}

func newTestCluster(t *testing.T, opts testutil.Options) *testutil.Cluster {
	t.Helper()
	c, err := testutil.NewCluster(opts)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, c.Close())
		if t.Failed() {
			t.Logf("final state:\n%s", c)
		}
	})
	return c
}

// ---------------------------------------------------------------------------
// Acceptance criteria
// ---------------------------------------------------------------------------

// TestNoSendBeforeSync is phase 3 acceptance criterion 3.
//
// Every message every node sends, across randomized schedules with crashes and
// restarts, is checked at the moment it is handed to the transport against
// what that node's storage had made durable at that moment. It runs mostly on
// the simulator's storage for breadth, and on the real write-ahead log for a
// few seeds so that the ordering is also checked against the storage that
// actually ships.
func TestNoSendBeforeSync(t *testing.T) {
	// The snapshot variants compact every few entries, so the audit also sees
	// snapshots saved between Readies, snapshots installed from a leader, and
	// the acknowledgements a follower sends for them.
	type variant struct {
		name  string
		seeds int
		disk  bool
		snap  bool
	}
	variants := []variant{
		{"mem", 40, false, false}, {"wal", 3, true, false},
		{"mem+snapshots", 40, false, true}, {"wal+snapshots", 3, true, true},
	}
	if testing.Short() {
		variants = []variant{{"mem", 8, false, false}, {"wal", 1, true, false}, {"mem+snapshots", 8, false, true}}
	}

	for _, v := range variants {
		for _, n := range []int{3, 5} {
			t.Run(fmt.Sprintf("%s/n=%d", v.name, n), func(t *testing.T) {
				totalSends, totalSnaps := 0, 0
				for seed := range uint64(v.seeds) {
					opts := faultyOptions(n, seed)
					if v.disk {
						opts.Storage = testutil.WALStorageFactory(t.TempDir(), storage.WALOptions{})
					}
					if v.snap {
						opts.SnapshotThreshold = 10
					}
					audits := withAudit(&opts)
					c := newTestCluster(t, opts)
					runFaultySchedule(t, c, seed, 400)

					require.Greater(t, len(*audits), len(c.IDs), "seed %d never restarted a node", seed)
					for _, a := range *audits {
						require.NoErrorf(t, a.err(), "seed %d", seed)
						totalSends += a.sends
						totalSnaps += a.snapshots
					}
					require.NotZero(t, c.Checker.MaxCommitted, "seed %d committed nothing", seed)
				}
				t.Logf("%d messages audited, %d snapshots saved", totalSends, totalSnaps)
				require.NotZero(t, totalSends)
				if v.snap {
					require.NotZero(t, totalSnaps, "the snapshot variant saved no snapshots")
				}
			})
		}
	}
}

// TestDurabilityAuditCatchesMisorderedDrivers is the negative control for
// TestNoSendBeforeSync. It processes a real Ready from a real node in two
// deliberately wrong orders, and the audit must flag both:
//
//   - send, then persist: nothing is buffered yet when the message leaves, so
//     only the substance check can see that the vote it grants is not durable.
//   - persist, send, then sync: the structural check sees the buffered writes.
func TestDurabilityAuditCatchesMisorderedDrivers(t *testing.T) {
	type order func(rd raft.Ready, s storage.Storage, tr transport.Transport)
	persist := func(rd raft.Ready, s storage.Storage) {
		require.NoError(t, s.Append(rd.Entries))
		if rd.HardState != nil {
			require.NoError(t, s.SetHardState(*rd.HardState))
		}
	}
	orders := map[string]order{
		"send before persisting": func(rd raft.Ready, s storage.Storage, tr transport.Transport) {
			tr.Send(rd.Messages)
			persist(rd, s)
			require.NoError(t, s.Sync())
		},
		"send between buffering and sync": func(rd raft.Ready, s storage.Storage, tr transport.Transport) {
			persist(rd, s)
			tr.Send(rd.Messages)
			require.NoError(t, s.Sync())
		},
		"correct order": func(rd raft.Ready, s storage.Storage, tr transport.Transport) {
			persist(rd, s)
			require.NoError(t, s.Sync())
			tr.Send(rd.Messages)
		},
	}

	for name, process := range orders {
		t.Run(name, func(t *testing.T) {
			node := newVoter(t, 2, storage.NewMem())
			mem := storage.NewMem()
			a, err := newAudit(2, mem)
			require.NoError(t, err)
			s := &auditedStorage{inner: mem, audit: a}
			tr := auditedTransport{inner: transport.Discard, audit: a}

			require.NoError(t, node.Step(voteRequest(1, 2, 1)))
			rd := node.Ready()
			require.NotNil(t, rd.HardState, "the vote must produce a hard state to persist")
			process(rd, s, tr)
			node.Advance()

			require.Equal(t, 1, a.sends)
			if name == "correct order" {
				require.NoError(t, a.err())
			} else {
				require.Error(t, a.err(), "the audit missed a %q driver", name)
			}
		})
	}

	// The other half of a negative control: a correct driver must not be
	// flagged. One Ready can span two terms -- the node grants candidate 1 in
	// term 1, then hears candidate 3 in term 2 before the Ready is processed.
	// The batch makes term 2 durable, and the term-1 grant is covered by it:
	// a node that has durably moved past term 1 can never vote in term 1
	// again. See BUGS.md, 2026-09-27.
	t.Run("correct order, one Ready spanning two terms", func(t *testing.T) {
		node := newVoter(t, 2, storage.NewMem())
		mem := storage.NewMem()
		a, err := newAudit(2, mem)
		require.NoError(t, err)

		require.NoError(t, node.Step(voteRequest(1, 2, 1)))
		require.NoError(t, node.Step(voteRequest(3, 2, 2)))
		rd := node.Ready()
		require.Equal(t, raft.HardState{Term: 2, VotedFor: 3}, *rd.HardState)
		require.Len(t, rd.Messages, 2, "both grants must be in the one batch")

		orders["correct order"](rd, &auditedStorage{inner: mem, audit: a},
			auditedTransport{inner: transport.Discard, audit: a})
		require.NoError(t, a.err())
	})
}

// TestOneFsyncPerReady is phase 3 acceptance criterion 4, against the real
// write-ahead log.
//
// Every Sync the driver makes is measured individually: a Ready that carries
// entries or hard state costs exactly one fsync, however many entries it
// holds, and a Ready that carries neither costs none. The driver makes exactly
// one Sync per Ready. Together: at most one fsync per Ready, never two, and
// never zero when there was something to make durable.
//
// PLAN.md words this as "exactly once per processed Ready batch". A Ready that
// carries only messages or committed entries -- a leader's heartbeat, say --
// has nothing to make durable, and fsyncing an unchanged file for it would add
// latency to every heartbeat for no durability at all. That refinement is
// recorded in DESIGN.md §10.
// TestDurabilityAuditCatchesAnEarlySnapshotAck is the negative control for the
// snapshot half of TestNoSendBeforeSync. A follower that completes a snapshot
// transfer acknowledges it with "I hold everything up to its index"; sending
// that before the snapshot is durable is the same broken promise as
// acknowledging entries that are not. The audit must catch it, and must not
// flag the correct order.
func TestDurabilityAuditCatchesAnEarlySnapshotAck(t *testing.T) {
	for _, early := range []bool{false, true} {
		t.Run(fmt.Sprintf("ack before snapshot is durable=%v", early), func(t *testing.T) {
			node := newVoter(t, 2, storage.NewMem())
			mem := storage.NewMem()
			a, err := newAudit(2, mem)
			require.NoError(t, err)
			s := &auditedStorage{inner: mem, audit: a}
			tr := auditedTransport{inner: transport.Discard, audit: a}

			require.NoError(t, node.Step(raft.Message{
				Type: raft.MsgInstallSnapshot, From: 1, To: 2, Term: 1,
				SnapshotMeta: raft.SnapshotMeta{Index: 9, Term: 1}, SnapshotData: []byte("image"), SnapshotDone: true,
			}))
			rd := node.Ready()
			require.NotNil(t, rd.Snapshot)

			if !early {
				require.NoError(t, s.SaveSnapshot(*rd.Snapshot))
			}
			require.NoError(t, s.Append(rd.Entries))
			if rd.HardState != nil {
				require.NoError(t, s.SetHardState(*rd.HardState))
			}
			require.NoError(t, s.Sync())
			tr.Send(rd.Messages)
			if early {
				require.NoError(t, s.SaveSnapshot(*rd.Snapshot))
			}
			node.Advance()

			if early {
				require.Error(t, a.err(), "the audit missed an acknowledgement sent before its snapshot was durable")
				t.Logf("caught: %v", a.err())
			} else {
				require.NoError(t, a.err())
			}
		})
	}
}

func TestOneFsyncPerReady(t *testing.T) {
	type fsyncs struct{ durable, empty, maxBatch int }
	counts := map[raft.NodeID]*fsyncs{}
	var instances []*countingWAL

	opts := faultyOptions(3, 5)
	walFactory := testutil.WALStorageFactory(t.TempDir(), storage.WALOptions{SegmentBytes: 64 << 10})
	opts.Storage = func(id raft.NodeID) (storage.Storage, error) {
		s, err := walFactory(id)
		if err != nil {
			return nil, err
		}
		if counts[id] == nil {
			counts[id] = &fsyncs{}
		}
		cw := &countingWAL{WAL: s.(*storage.WAL), t: t, tally: func(entries int, durable bool) {
			c := counts[id]
			if durable {
				c.durable++
				c.maxBatch = max(c.maxBatch, entries)
			} else {
				c.empty++
			}
		}}
		instances = append(instances, cw)
		return cw, nil
	}
	c := newTestCluster(t, opts)
	runFaultySchedule(t, c, 5, 300)

	// One Ready with a very large batch: many proposals land in the leader's
	// log before it next processes a Ready.
	leader, ok := c.Leader()
	require.True(t, ok)
	for i := range 500 {
		_, _, err := c.Propose(leader, []byte(fmt.Sprintf("bulk-%d", i)))
		require.NoError(t, err)
	}
	c.RunTicks(50)
	require.NoError(t, c.Err())

	for _, id := range c.IDs {
		r := c.Replicas[id]
		cw := r.Store.(*countingWAL)
		m := r.Driver.Metrics()
		require.Equalf(t, m.ReadyBatches, m.Syncs, "node %d: driver synced %d times for %d Readies",
			id, m.Syncs, m.ReadyBatches)
		require.Equalf(t, m.ReadyBatches, cw.Stats().SyncCalls,
			"node %d: storage saw %d Syncs for %d Readies", id, cw.Stats().SyncCalls, m.ReadyBatches)
	}
	for id, n := range counts {
		t.Logf("node %d: %d Readies with durable state (1 fsync each), %d without (0 fsyncs), largest batch %d entries",
			id, n.durable, n.empty, n.maxBatch)
		require.Positive(t, n.durable)
		require.Positive(t, n.empty, "no message-only Ready was exercised")
	}
	require.GreaterOrEqual(t, counts[leader].maxBatch, 500, "the bulk batch did not arrive as one Ready")
	require.Greater(t, len(instances), len(c.IDs), "the schedule never restarted a node onto a reopened log")
}

// countingWAL checks each Sync's fsync cost individually, at the moment it
// happens.
type countingWAL struct {
	*storage.WAL
	t        *testing.T
	tally    func(entries int, durable bool)
	buffered int
	hs       bool
}

func (w *countingWAL) Append(entries []raft.Entry) error {
	w.buffered += len(entries)
	return w.WAL.Append(entries)
}

func (w *countingWAL) SetHardState(hs raft.HardState) error {
	w.hs = true
	return w.WAL.SetHardState(hs)
}

func (w *countingWAL) Sync() error {
	before := w.Stats().Fsyncs
	err := w.WAL.Sync()
	fsyncs := w.Stats().Fsyncs - before
	durable := w.buffered > 0 || w.hs
	if durable {
		require.EqualValuesf(w.t, 1, fsyncs, "a Ready with %d entries (hard state: %v) cost %d fsyncs",
			w.buffered, w.hs, fsyncs)
	} else {
		require.Zerof(w.t, fsyncs, "a Ready with nothing to persist cost %d fsyncs", fsyncs)
	}
	w.tally(w.buffered, durable)
	w.buffered, w.hs = 0, false
	return err
}

// ---------------------------------------------------------------------------
// Restart and votes
// ---------------------------------------------------------------------------

func newVoter(t *testing.T, id raft.NodeID, s storage.Storage) *raft.Node {
	t.Helper()
	rec, err := s.InitialState()
	require.NoError(t, err)
	n, err := raft.New(raft.Config{
		ID:                      id,
		Peers:                   []raft.NodeID{1, 2, 3},
		ElectionTimeoutMinTicks: 10,
		ElectionTimeoutMaxTicks: 20,
		HeartbeatTimeoutTicks:   3,
		MaxEntriesPerAppend:     64,
		Rand:                    func(int) int { return 0 },
		HardState:               rec.HardState,
		Snapshot:                rec.Snapshot,
		Entries:                 rec.Entries,
	})
	require.NoError(t, err)
	return n
}

func voteRequest(from, to raft.NodeID, term raft.Term) raft.Message {
	return raft.Message{Type: raft.MsgRequestVote, From: from, To: to, Term: term}
}

// failingSync crashes the node inside Sync: the write never becomes durable
// and Sync returns an error, so the driver stops before sending anything.
type failingSync struct{ storage.Storage }

func (f failingSync) Sync() error {
	if err := f.Storage.(testutil.Crasher).Crash(); err != nil {
		return err
	}
	return errors.New("simulated crash during fsync")
}

// forgetsHardState is storage that loses the vote across a restart -- what a
// node would look like if it kept its vote only in memory.
type forgetsHardState struct{ storage.Storage }

func (f forgetsHardState) InitialState() (storage.Recovered, error) {
	rec, err := f.Storage.InitialState()
	rec.HardState = raft.HardState{}
	return rec, err
}

// voteAcrossRestart has node 2 receive a vote request from candidate 1 in term
// 1, crash at the given point, restart from whatever its storage kept, and then
// receive a request from candidate 3 in the SAME term. It returns every vote
// node 2 granted, across both lives.
func voteAcrossRestart(t *testing.T, open func() storage.Storage, crashInSync bool) []raft.Message {
	t.Helper()
	var sent []raft.Message
	record := transport.SendFunc(func(msgs []raft.Message) { sent = append(sent, msgs...) })

	// First life.
	s := open()
	driven := s
	if crashInSync {
		driven = failingSync{s}
	}
	d := server.New(newVoter(t, 2, s), driven, record, nil)
	require.NoError(t, d.Step(voteRequest(1, 2, 1)))
	err := d.Run()
	if crashInSync {
		require.Error(t, err)
	} else {
		require.NoError(t, err)
		require.NoError(t, s.(testutil.Crasher).Crash())
	}

	// Second life, same term, a different candidate with an equally
	// up-to-date log.
	s = open()
	t.Cleanup(func() { _ = s.(testutil.Crasher).Crash() })
	d = server.New(newVoter(t, 2, s), s, record, nil)
	require.NoError(t, d.Step(voteRequest(3, 2, 1)))
	require.NoError(t, d.Run())

	var grants []raft.Message
	for _, m := range sent {
		if m.Type == raft.MsgRequestVoteResp && m.VoteGranted {
			grants = append(grants, m)
		}
	}
	return grants
}

// checkOneVotePerTerm is the verdict: a node may grant at most one candidate
// per term, counted across every life it has had.
func checkOneVotePerTerm(grants []raft.Message) error {
	voted := map[raft.Term]raft.NodeID{}
	for _, g := range grants {
		if prev, ok := voted[g.Term]; ok && prev != g.To {
			return fmt.Errorf("node %d voted for both %d and %d in term %d", g.From, prev, g.To, g.Term)
		}
		voted[g.Term] = g.To
	}
	return nil
}

func walOpener(t *testing.T) func() storage.Storage {
	dir := t.TempDir()
	return func() storage.Storage {
		w, err := storage.OpenWAL(dir, storage.WALOptions{})
		require.NoError(t, err)
		return w
	}
}

// TestRestartDoesNotDoubleVote is phase 3 acceptance criterion 5, on the real
// write-ahead log.
//
// The node grants a vote, crashes before the term advances, and restarts from
// disk. A second candidate in the same term must be refused. It also covers
// the other crash point: dying inside the fsync, where the vote never became
// durable -- and, by the ordering rule, was therefore never sent, so granting
// the second candidate is correct and still only one vote left the node.
func TestRestartDoesNotDoubleVote(t *testing.T) {
	t.Run("crash after the vote was sent", func(t *testing.T) {
		grants := voteAcrossRestart(t, walOpener(t), false)
		require.NoError(t, checkOneVotePerTerm(grants))
		require.Len(t, grants, 1, "the first vote must have been granted, or nothing was tested")
		require.Equal(t, raft.NodeID(1), grants[0].To)
	})
	t.Run("crash inside the fsync", func(t *testing.T) {
		grants := voteAcrossRestart(t, walOpener(t), true)
		require.NoError(t, checkOneVotePerTerm(grants))
		require.Len(t, grants, 1)
		require.Equal(t, raft.NodeID(3), grants[0].To, "the unsynced vote for 1 must never have been sent")
	})
}

// TestDoubleVoteCheckCatchesAmnesia is the negative control for
// TestRestartDoesNotDoubleVote: the same scenario on storage that forgets the
// hard state across a restart must produce a double vote, and the check must
// report it. Without this, a scenario that never reached the second vote at
// all would pass for the wrong reason.
func TestDoubleVoteCheckCatchesAmnesia(t *testing.T) {
	open := walOpener(t)
	grants := voteAcrossRestart(t, func() storage.Storage {
		return amnesiacWAL{forgetsHardState{open()}}
	}, false)
	require.Len(t, grants, 2)
	require.Error(t, checkOneVotePerTerm(grants))
}

// amnesiacWAL keeps Crash reachable through the forgetting wrapper.
type amnesiacWAL struct{ forgetsHardState }

func (a amnesiacWAL) Crash() error { return a.Storage.(testutil.Crasher).Crash() }
