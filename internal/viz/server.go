package viz

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"strconv"
	"strings"
	"time"

	adminv1 "github.com/Shashankkaranamr/Quorum/gen/quorum/admin/v1"
	"github.com/Shashankkaranamr/Quorum/internal/client"
	"github.com/Shashankkaranamr/Quorum/internal/supervisor"
	"github.com/Shashankkaranamr/Quorum/raft"
)

// Refresh is how often the aggregated picture is rebuilt and, if it changed,
// pushed to browsers. With each node streaming its status at the same
// interval, a change reaches the page in well under the 500ms phase 7 allows.
const Refresh = 100 * time.Millisecond

// Server is the visualizer backend for one cluster.
type Server struct {
	sup    *supervisor.Supervisor
	assets fs.FS
	logf   func(string, ...any)

	updates  chan nodeUpdate
	subs     chan chan State
	unsubs   chan chan State
	snapshot chan chan State
	notes    chan string
	loadSet  chan bool // control goroutine -> aggregator: the load switch changed
	loadCtl  chan bool // SetLoad -> control goroutine
	controls chan control
	stop     chan struct{}

	// kv is the client used to write. Only the control goroutine touches it.
	kv   *client.Client
	done chan struct{}
}

type nodeUpdate struct {
	id     uint64
	status *adminv1.NodeStatus // nil when the stream failed
}

// control is one button press, run by the control goroutine.
type control struct {
	run   func(ctx context.Context) (string, error)
	reply chan error
}

// New starts a visualizer backend for the cluster sup manages. assets is the
// web frontend. It watches every node until Close.
func New(sup *supervisor.Supervisor, assets fs.FS, logf func(string, ...any)) *Server {
	if logf == nil {
		logf = func(string, ...any) {}
	}
	s := &Server{
		sup: sup, assets: assets, logf: logf,
		updates:  make(chan nodeUpdate, 64),
		subs:     make(chan chan State),
		unsubs:   make(chan chan State),
		snapshot: make(chan chan State),
		notes:    make(chan string, 16),
		loadSet:  make(chan bool),
		loadCtl:  make(chan bool),
		controls: make(chan control),
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
	ctx, cancel := context.WithCancel(context.Background())
	watchersDone := make(chan struct{})
	go func() {
		defer close(watchersDone)
		done := make(chan struct{}, len(sup.IDs()))
		for _, id := range sup.IDs() {
			go func() { s.watch(ctx, id); done <- struct{}{} }()
		}
		for range sup.IDs() {
			<-done
		}
	}()
	controlsDone := make(chan struct{})
	go func() { defer close(controlsDone); s.runControls(ctx) }()
	go func() {
		defer close(s.done)
		s.aggregate()
		cancel()
		<-watchersDone
		<-controlsDone
	}()
	return s
}

// Close stops the backend. It leaves the cluster's processes running.
func (s *Server) Close() {
	select {
	case <-s.stop:
	default:
		close(s.stop)
	}
	<-s.done
}

// watch follows one node's status stream, reconnecting whenever it breaks. A
// broken stream is reported at once, which is how a killed node shows as
// unreachable within one refresh rather than after a timeout.
func (s *Server) watch(ctx context.Context, id raft.NodeID) {
	a, conn, err := s.sup.Admin(id)
	if err != nil {
		s.logf("viz: node %d: %v", id, err)
		return
	}
	defer func() { _ = conn.Close() }()
	for ctx.Err() == nil {
		stream, err := a.WatchStatus(ctx, &adminv1.WatchStatusRequest{LogTailLimit: 16, MinIntervalMs: uint32(Refresh.Milliseconds())})
		for err == nil {
			var st *adminv1.NodeStatus
			if st, err = stream.Recv(); err == nil {
				s.send(nodeUpdate{id: uint64(id), status: st})
			}
		}
		s.send(nodeUpdate{id: uint64(id)})
		select {
		case <-ctx.Done():
		case <-time.After(Refresh):
		}
	}
}

func (s *Server) send(u nodeUpdate) {
	select {
	case s.updates <- u:
	case <-s.stop:
	}
}

// aggregate is the one goroutine that owns the picture.
func (s *Server) aggregate() {
	nodes := make([]*nodeRecord, 0, len(s.sup.IDs()))
	byID := map[uint64]*nodeRecord{}
	for _, id := range s.sup.IDs() {
		n := &nodeRecord{id: uint64(id), addr: s.sup.Addr(id)}
		nodes = append(nodes, n)
		byID[n.id] = n
	}
	var (
		events  []Event
		seq     uint64
		load    bool
		subs    = map[chan State]bool{}
		current = build(s.sup.Cluster().ClusterID, seq, nodes, events, load)
		dirty   = true
	)
	note := func(kind, text string) {
		events = append(events, Event{At: time.Now().UnixMilli(), Kind: kind, Text: text})
		if len(events) > maxEvents {
			events = events[len(events)-maxEvents:]
		}
		dirty = true
	}
	ticker := time.NewTicker(Refresh)
	defer ticker.Stop()
	for {
		select {
		case <-s.stop:
			return
		case u := <-s.updates:
			n := byID[u.id]
			if u.status == nil {
				dirty = dirty || n.reachable
				n.reachable = false
				continue
			}
			n.last, n.reachable, dirty = u.status, true, true
		case text := <-s.notes:
			kind, msg, _ := strings.Cut(text, "|")
			note(kind, msg)
		case on := <-s.loadSet:
			load, dirty = on, true
		case ch := <-s.subs:
			subs[ch] = true
			ch <- current
		case ch := <-s.unsubs:
			delete(subs, ch)
		case ch := <-s.snapshot:
			ch <- current
		case <-ticker.C:
			// Process liveness comes from the operating system, not from
			// whether the node answers: "down" means there is no process.
			for _, n := range nodes {
				pid, ok := s.sup.PID(raft.NodeID(n.id))
				if ok != n.running || pid != n.pid {
					n.running, n.pid, dirty = ok, pid, true
				}
			}
			if !dirty {
				continue
			}
			seq++
			next := build(s.sup.Cluster().ClusterID, seq, nodes, events, load)
			for _, d := range diff(current, next) {
				kind, msg, _ := strings.Cut(d, "|")
				note(kind, msg)
			}
			next.Events = append([]Event(nil), events...)
			current, dirty = next, false
			for ch := range subs {
				select {
				case ch <- current:
				default:
					// A slow browser skips a frame rather than holding
					// everyone else up.
				}
			}
		}
	}
}

// State returns the current picture.
func (s *Server) State() State {
	ch := make(chan State, 1)
	select {
	case s.snapshot <- ch:
		return <-ch
	case <-s.done:
		return State{}
	}
}

// Subscribe returns a channel that receives each new picture, starting with
// the current one, and a function to stop.
func (s *Server) Subscribe() (<-chan State, func()) {
	ch := make(chan State, 1)
	select {
	case s.subs <- ch:
	case <-s.done:
		close(ch)
		return ch, func() {}
	}
	return ch, func() {
		select {
		case s.unsubs <- ch:
		case <-s.done:
		}
	}
}

// runControls executes button presses one at a time, so two controls can
// never race over the same process. It also owns the KV client used to write,
// and the background writer the "load" switch runs.
func (s *Server) runControls(ctx context.Context) {
	defer func() {
		if s.kv != nil {
			_ = s.kv.Close()
		}
	}()
	loadTick := time.NewTicker(250 * time.Millisecond)
	defer loadTick.Stop()
	load, n := false, 0
	for {
		select {
		case <-ctx.Done():
			return
		case c := <-s.controls:
			text, err := c.run(ctx)
			if err == nil && text != "" {
				s.noteAsync(text)
			}
			c.reply <- err
		case on := <-s.loadCtl:
			load = on
			select {
			case s.loadSet <- on:
			case <-ctx.Done():
				return
			}
		case <-loadTick.C:
			if !load {
				continue
			}
			cl, err := s.client(ctx)
			if err != nil {
				continue
			}
			n++
			wctx, cancel := context.WithTimeout(ctx, 2*time.Second)
			_, _ = cl.Put(wctx, fmt.Sprintf("load-%d", n%8), []byte(strconv.Itoa(n)))
			cancel()
		}
	}
}

// SetLoad turns the background writer on or off. It writes a few keys over and
// over through the ordinary KV API, so entries can be watched replicating.
func (s *Server) SetLoad(ctx context.Context, on bool) error {
	select {
	case s.loadCtl <- on:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-s.done:
		return errors.New("viz: stopped")
	}
}

func (s *Server) noteAsync(text string) {
	select {
	case s.notes <- text:
	default:
	}
}

// client returns the control goroutine's KV client, dialing and registering it
// on first use. Call it only from the control goroutine.
func (s *Server) client(ctx context.Context) (*client.Client, error) {
	if s.kv != nil {
		return s.kv, nil
	}
	cl, err := s.dialClient(ctx)
	if err == nil {
		s.kv = cl
	}
	return cl, err
}

func (s *Server) dialClient(ctx context.Context) (*client.Client, error) {
	addrs := map[raft.NodeID]string{}
	for _, id := range s.sup.IDs() {
		addrs[id] = s.sup.Addr(id)
	}
	cl, err := client.Dial(client.Config{Addrs: addrs, AttemptTimeout: time.Second})
	if err != nil {
		return nil, err
	}
	rctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := cl.Register(rctx); err != nil {
		_ = cl.Close()
		return nil, err
	}
	return cl, nil
}

// do runs one control on the control goroutine and waits for it.
func (s *Server) do(ctx context.Context, run func(ctx context.Context) (string, error)) error {
	c := control{run: run, reply: make(chan error, 1)}
	select {
	case s.controls <- c:
	case <-ctx.Done():
		return ctx.Err()
	case <-s.done:
		return errors.New("viz: stopped")
	}
	select {
	case err := <-c.reply:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
