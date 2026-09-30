package server

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/Shashankkaranamr/Quorum/internal/storage"
	"github.com/Shashankkaranamr/Quorum/internal/transport"
	"github.com/Shashankkaranamr/Quorum/raft"
)

// ResultReporter is a state machine that can say what applying its last entry
// produced. The loop hands that value to whoever proposed the entry, without
// knowing what it means.
type ResultReporter interface {
	LastResult() any
}

// Applied is the outcome of a proposal that committed and was applied.
type Applied struct {
	Index  raft.Index
	Term   raft.Term
	Result any
}

// NotLeaderError is returned when a request reached a node that is not the
// leader. Nothing was proposed, so the request definitely did not happen here.
type NotLeaderError struct {
	// Leader is who this node believes leads, or raft.None if it does not
	// know.
	Leader raft.NodeID
}

func (e NotLeaderError) Error() string {
	return fmt.Sprintf("server: not leader (leader hint: %d)", e.Leader)
}

var (
	// ErrLostLeadership means a proposal was accepted into the log but this
	// node stopped leading before learning whether it committed. Whether it
	// happened is unknown; a retry with the same (client, seq) is safe
	// because the state machine deduplicates it.
	ErrLostLeadership = errors.New("server: leadership lost before the entry was known to commit")

	// ErrReadNotConfirmed means a read could not be confirmed by a quorum
	// before this node stopped leading. Serving it could have returned a
	// stale value, so it was not served.
	ErrReadNotConfirmed = errors.New("server: leadership could not be confirmed for the read")

	// ErrStopped means the loop is not running.
	ErrStopped = errors.New("server: loop stopped")
)

// LoopOptions configure a Loop.
type LoopOptions struct {
	// Tick is the wall-clock length of one logical tick.
	Tick time.Duration

	// SnapshotThreshold is Driver.SetSnapshotThreshold.
	SnapshotThreshold uint64

	// Logf reports conditions worth a human's attention -- a message the core
	// rejected, the loop stopping on a storage failure. Nil discards them.
	Logf func(format string, args ...any)

	// LogTail is how many recent log entries each published snapshot carries,
	// for status displays. Zero means 32.
	LogTail int
}

// Snapshot is what the loop publishes after every iteration: everything an
// observer may want, captured at one instant, never modified afterwards. That
// last property is what makes reading it from another goroutine, without a
// lock, safe.
type Snapshot struct {
	Status  raft.Status
	Metrics Metrics
	LogTail []raft.Entry

	// Frozen is true while the loop is parked by Freeze.
	Frozen bool

	MessagesReceived  uint64
	ProposalsAccepted uint64
	ProposalsRejected uint64

	// Fsyncs and FsyncP99 come from the storage, when it reports them.
	Fsyncs   uint64
	FsyncP99 time.Duration

	PublishedAt time.Time
}

// StorageStats is storage that reports its fsync behaviour. The write-ahead
// log does.
type StorageStats interface {
	Stats() storage.WALStats
	FsyncP99() time.Duration
}

// ThawReport says what a freeze cost.
type ThawReport struct {
	FrozenFor   time.Duration
	TicksMissed uint64
}

// Loop drives a raft.Node from one goroutine against the real clock.
//
// It is the loop DESIGN.md §1 describes: a single select over the ticker, the
// inbound message channel and requests from RPC handlers, followed by Ready
// processing through the same Driver the simulator uses. The goroutine owns
// the node, the driver, the storage, the state machine and every pending
// request. RPC handlers never touch any of them: they send a request on a
// channel and wait for the reply on another. That is why the pending-proposal
// registry, listed in DESIGN.md §1 as one of three places allowed a lock, turns
// out to need none.
//
// The single remaining piece of shared state is the status snapshot, an
// immutable raft.Status published through an atomic pointer, so observers can
// read it without ever blocking the loop.
type Loop struct {
	d       *Driver
	inbound <-chan raft.Message
	opts    LoopOptions

	proposals chan *proposeReq
	reads     chan *readReq
	control   chan controlReq
	stop      chan struct{}
	done      chan struct{}
	err       error // written by the loop before done closes

	status atomic.Pointer[Snapshot]
	store  storage.Storage

	// Owned by the loop goroutine.
	pending    map[raft.Index]*proposeReq
	readWait   map[uint64]*readReq
	confirmed  []*readReq
	nextReadID uint64
	wasLeader  bool
	leaderTerm raft.Term

	received, accepted, rejected uint64
	p99                          time.Duration
	p99At                        time.Time
	frozen                       bool
}

// controlReq asks the loop to freeze (freeze true) or thaw.
type controlReq struct {
	freeze   bool
	autoThaw time.Duration
	reply    chan ThawReport
}

type proposeReq struct {
	ctx   context.Context
	typ   raft.EntryType
	data  []byte
	term  raft.Term
	reply chan proposeReply
}

type proposeReply struct {
	applied Applied
	err     error
}

type readReq struct {
	ctx   context.Context
	fn    func() any
	index raft.Index
	reply chan readReply
}

type readReply struct {
	index raft.Index
	value any
	err   error
}

// NewLoop builds the driver around node and starts the loop goroutine.
// Inbound messages for the node arrive on inbound.
func NewLoop(node *raft.Node, store storage.Storage, trans transport.Transport, sm StateMachine,
	inbound <-chan raft.Message, opts LoopOptions) *Loop {
	if opts.Tick <= 0 {
		opts.Tick = 50 * time.Millisecond
	}
	if opts.LogTail <= 0 {
		opts.LogTail = 32
	}
	l := &Loop{
		inbound:   inbound,
		opts:      opts,
		proposals: make(chan *proposeReq),
		reads:     make(chan *readReq),
		control:   make(chan controlReq),
		store:     store,
		stop:      make(chan struct{}),
		done:      make(chan struct{}),
		pending:   map[raft.Index]*proposeReq{},
		readWait:  map[uint64]*readReq{},
	}
	l.d = New(node, store, trans, resolvingSM{inner: sm, loop: l})
	l.d.SetSnapshotThreshold(opts.SnapshotThreshold)
	l.publish()
	go l.run()
	return l
}

// Propose submits an entry and waits until it is applied here.
//
// The error distinguishes the outcomes the client contract depends on:
// NotLeaderError (definitely not proposed), ErrLostLeadership (proposed,
// outcome unknown), and the context's error (proposed or perhaps not, outcome
// unknown). Only a nil error means the entry committed.
func (l *Loop) Propose(ctx context.Context, typ raft.EntryType, data []byte) (Applied, error) {
	req := &proposeReq{ctx: ctx, typ: typ, data: data, reply: make(chan proposeReply, 1)}
	select {
	case l.proposals <- req:
	case <-ctx.Done():
		return Applied{}, ctx.Err()
	case <-l.done:
		return Applied{}, ErrStopped
	}
	select {
	case r := <-req.reply:
		return r.applied, r.err
	case <-ctx.Done():
		return Applied{}, ctx.Err()
	case <-l.done:
		return Applied{}, ErrStopped
	}
}

// Read performs a linearizable read. It runs fn on the loop goroutine -- where
// it may safely look at the state machine -- once a quorum has confirmed this
// node's leadership and the state machine has caught up to the read index.
func (l *Loop) Read(ctx context.Context, fn func() any) (raft.Index, any, error) {
	req := &readReq{ctx: ctx, fn: fn, reply: make(chan readReply, 1)}
	select {
	case l.reads <- req:
	case <-ctx.Done():
		return 0, nil, ctx.Err()
	case <-l.done:
		return 0, nil, ErrStopped
	}
	select {
	case r := <-req.reply:
		return r.index, r.value, r.err
	case <-ctx.Done():
		return 0, nil, ctx.Err()
	case <-l.done:
		return 0, nil, ErrStopped
	}
}

// Status is the node's state as of the loop's last iteration. It never blocks.
func (l *Loop) Status() raft.Status { return l.status.Load().Status }

// Metrics is the driver's counters as of the loop's last iteration.
func (l *Loop) Metrics() Metrics { return l.status.Load().Metrics }

// Snapshot is everything published by the loop's last iteration. It never
// blocks, including while the loop is frozen.
func (l *Loop) Snapshot() Snapshot { return *l.status.Load() }

// Freeze parks the loop: no ticks, no messages, no requests are processed
// until Thaw, or until autoThaw passes if it is non-zero. The process stays up
// and its listener keeps accepting; requests simply wait, and inbound messages
// back up and are dropped, as they would for a process that stopped being
// scheduled. It is cooperative -- the loop parks itself -- which is the honest
// way to describe it: it is not SIGSTOP.
//
// It returns once the loop is parked. Freezing a frozen loop does nothing.
func (l *Loop) Freeze(ctx context.Context, autoThaw time.Duration) error {
	_, err := l.sendControl(ctx, controlReq{freeze: true, autoThaw: autoThaw})
	return err
}

// Thaw resumes a frozen loop and reports how long it was parked and how many
// ticks it missed. The missed ticks are not replayed: a process that was not
// scheduled did not see that time pass either, and replaying it would fire
// every timer at once.
func (l *Loop) Thaw(ctx context.Context) (ThawReport, error) {
	return l.sendControl(ctx, controlReq{})
}

func (l *Loop) sendControl(ctx context.Context, c controlReq) (ThawReport, error) {
	c.reply = make(chan ThawReport, 1)
	select {
	case l.control <- c:
	case <-ctx.Done():
		return ThawReport{}, ctx.Err()
	case <-l.done:
		return ThawReport{}, ErrStopped
	}
	select {
	case r := <-c.reply:
		return r, nil
	case <-ctx.Done():
		return ThawReport{}, ctx.Err()
	case <-l.done:
		return ThawReport{}, ErrStopped
	}
}

// park is the frozen state. It returns false if the loop was told to stop
// while frozen.
func (l *Loop) park(req controlReq, ticker *time.Ticker) bool {
	start := time.Now()
	var missed uint64
	var auto <-chan time.Time
	if req.autoThaw > 0 {
		timer := time.NewTimer(req.autoThaw)
		defer timer.Stop()
		auto = timer.C
	}
	l.frozen = true
	l.publish()
	req.reply <- ThawReport{}
	defer func() {
		l.frozen = false
		l.publish()
	}()
	for {
		select {
		case <-l.stop:
			return false
		case <-ticker.C:
			missed++
		case <-auto:
			l.logf("auto-thawed after %s, %d ticks missed", time.Since(start), missed)
			return true
		case c := <-l.control:
			report := ThawReport{FrozenFor: time.Since(start), TicksMissed: missed}
			if c.freeze {
				// Already frozen: acknowledge and stay parked.
				c.reply <- ThawReport{}
				continue
			}
			c.reply <- report
			return true
		}
	}
}

// Done is closed when the loop has exited.
func (l *Loop) Done() <-chan struct{} { return l.done }

// Err is why the loop exited, or nil if it was stopped.
func (l *Loop) Err() error {
	<-l.done
	return l.err
}

// Stop ends the loop and waits for it. Every waiting request is answered with
// ErrStopped. It does not close the storage or the transport; their owner does.
func (l *Loop) Stop() {
	select {
	case <-l.stop:
	default:
		close(l.stop)
	}
	<-l.done
}

func (l *Loop) logf(format string, args ...any) {
	if l.opts.Logf != nil {
		l.opts.Logf(format, args...)
	}
}

// inboundBatch bounds how many queued messages one iteration steps before
// processing a Ready, so a flood of traffic cannot starve the ticker.
const inboundBatch = 64

func (l *Loop) run() {
	defer close(l.done)
	defer l.failAll(ErrStopped)

	ticker := time.NewTicker(l.opts.Tick)
	defer ticker.Stop()

	// next is when the next logical tick falls due. Ticks are counted from
	// the wall clock, not from ticker events: a time.Ticker drops the ticks
	// its receiver is too busy to take, so after a slow fsync it delivers
	// one tick however many were missed. Counting from the ticker made the
	// tick-lag metric structurally zero, and made the core's clock run slow
	// whenever the loop was busy (BUGS.md, 2026-09-30). The ticker only wakes
	// the loop; the clock decides how many ticks that is.
	next := time.Now().Add(l.opts.Tick)

	for {
		select {
		case <-l.stop:
			return
		case <-ticker.C:
			for now := time.Now(); !next.After(now); next = next.Add(l.opts.Tick) {
				l.d.Tick()
			}
			l.sweepAbandoned()
		case m := <-l.inbound:
			l.step(m)
		drain:
			for range inboundBatch - 1 {
				select {
				case m = <-l.inbound:
					l.step(m)
				default:
					break drain
				}
			}
		case p := <-l.proposals:
			l.propose(p)
		case r := <-l.reads:
			l.read(r)
		case c := <-l.control:
			if !c.freeze {
				c.reply <- ThawReport{} // not frozen; nothing to thaw
				continue
			}
			if !l.park(c, ticker) {
				return
			}
			// Time spent frozen is deliberately not replayed; see Thaw.
			next = time.Now().Add(l.opts.Tick)
		}

		if err := l.process(); err != nil {
			l.err = err
			l.logf("loop stopping: %v", err)
			return
		}
	}
}

func (l *Loop) step(m raft.Message) {
	l.received++
	if err := l.d.Step(m); err != nil {
		l.logf("rejected %s: %v", m, err)
	}
}

func (l *Loop) propose(p *proposeReq) {
	idx, term, err := l.d.Propose(p.typ, p.data)
	if err != nil {
		l.rejected++
		_, _, lead := l.d.node.SoftState()
		p.reply <- proposeReply{err: NotLeaderError{Leader: lead}}
		return
	}
	l.accepted++
	p.term = term
	l.pending[idx] = p
}

func (l *Loop) read(r *readReq) {
	l.nextReadID++
	id := l.nextReadID
	if err := l.d.ReadIndex(id); err != nil {
		_, _, lead := l.d.node.SoftState()
		r.reply <- readReply{err: NotLeaderError{Leader: lead}}
		return
	}
	l.readWait[id] = r
}

// process runs Readies until there is nothing left to do, then settles every
// request whose outcome is now known.
func (l *Loop) process() error {
	if err := l.d.Run(); err != nil {
		return err
	}
	for l.d.node.HasReady() {
		if err := l.d.ProcessReady(); err != nil {
			return err
		}
	}

	for _, rs := range l.d.TakeReadStates() {
		r, ok := l.readWait[rs.ID]
		if !ok {
			continue
		}
		delete(l.readWait, rs.ID)
		if rs.Lost {
			r.reply <- readReply{err: ErrReadNotConfirmed}
			continue
		}
		r.index = rs.Index
		l.confirmed = append(l.confirmed, r)
	}
	applied := l.d.node.Applied()
	kept := l.confirmed[:0]
	for _, r := range l.confirmed {
		if r.index <= applied {
			r.reply <- readReply{index: r.index, value: r.fn()}
		} else {
			kept = append(kept, r)
		}
	}
	l.confirmed = kept

	role, term, _ := l.d.node.SoftState()
	isLeader := role == raft.Leader
	if l.wasLeader && (!isLeader || term != l.leaderTerm) {
		// Every proposal made while leading is now of unknown fate. It may
		// yet commit under the next leader; the client's retry with the same
		// seq will find out, and deduplication makes that retry safe.
		for idx, p := range l.pending {
			p.reply <- proposeReply{err: ErrLostLeadership}
			delete(l.pending, idx)
		}
	}
	l.wasLeader, l.leaderTerm = isLeader, term

	l.publish()
	return nil
}

// resolve answers the proposal waiting on e's index, if any. An entry of a
// different term at that index means ours was overwritten: it never committed.
// That is still reported as ErrLostLeadership rather than a definite failure,
// because the client cannot distinguish it from the step-down case and does
// not need to -- the retry is the same.
func (l *Loop) resolve(e raft.Entry, result any) {
	p, ok := l.pending[e.Index]
	if !ok {
		return
	}
	delete(l.pending, e.Index)
	if p.term != e.Term {
		p.reply <- proposeReply{err: ErrLostLeadership}
		return
	}
	p.reply <- proposeReply{applied: Applied{Index: e.Index, Term: e.Term, Result: result}}
}

// sweepAbandoned forgets requests whose caller has given up. A leader cut off
// from the majority never learns its proposals' fate or confirms its reads,
// and without this their waiters would accumulate for as long as it
// mistakenly believes it leads.
func (l *Loop) sweepAbandoned() {
	for idx, p := range l.pending {
		if p.ctx.Err() != nil {
			delete(l.pending, idx)
		}
	}
	for id, r := range l.readWait {
		if r.ctx.Err() != nil {
			delete(l.readWait, id)
		}
	}
	kept := l.confirmed[:0]
	for _, r := range l.confirmed {
		if r.ctx.Err() == nil {
			kept = append(kept, r)
		}
	}
	l.confirmed = kept
}

func (l *Loop) failAll(err error) {
	for idx, p := range l.pending {
		p.reply <- proposeReply{err: err}
		delete(l.pending, idx)
	}
	for id, r := range l.readWait {
		r.reply <- readReply{err: err}
		delete(l.readWait, id)
	}
	for _, r := range l.confirmed {
		r.reply <- readReply{err: err}
	}
	l.confirmed = nil
}

// p99Every bounds how often the loop recomputes the fsync percentile, which
// sorts a window of latencies: often enough for a display, rarely enough to
// cost the loop nothing measurable.
const p99Every = 250 * time.Millisecond

func (l *Loop) publish() {
	now := time.Now()
	s := &Snapshot{
		Status:            l.d.node.Status(),
		Metrics:           l.d.Metrics(),
		LogTail:           l.d.node.LogTail(l.opts.LogTail),
		Frozen:            l.frozen,
		MessagesReceived:  l.received,
		ProposalsAccepted: l.accepted,
		ProposalsRejected: l.rejected,
		PublishedAt:       now,
	}
	if st, ok := l.store.(StorageStats); ok {
		s.Fsyncs = st.Stats().Fsyncs
		if now.Sub(l.p99At) >= p99Every {
			l.p99, l.p99At = st.FsyncP99(), now
		}
		s.FsyncP99 = l.p99
	}
	l.status.Store(s)
}

// resolvingSM applies entries one at a time so each one's result can be handed
// to the proposal waiting on it, on the loop goroutine, before the next entry
// changes the state machine's "last result".
type resolvingSM struct {
	inner StateMachine
	loop  *Loop
}

func (s resolvingSM) Apply(entries []raft.Entry) error {
	for _, e := range entries {
		if err := s.inner.Apply([]raft.Entry{e}); err != nil {
			return err
		}
		var result any
		if rr, ok := s.inner.(ResultReporter); ok {
			result = rr.LastResult()
		}
		s.loop.resolve(e, result)
	}
	return nil
}

func (s resolvingSM) Snapshot() ([]byte, raft.Index, error) { return s.inner.Snapshot() }
func (s resolvingSM) Restore(snap raft.Snapshot) error      { return s.inner.Restore(snap) }
