// Package viz is the visualizer's backend: it watches every node's admin
// status stream, aggregates the cluster into one picture, streams that picture
// to browsers over server-sent events, and turns the UI's buttons into
// supervisor and admin calls.
//
// It cannot corrupt the cluster, and that is structural rather than a matter
// of care. The nodes are separate processes; this package reaches them only
// through the supervisor (processes) and the admin and KV APIs (everything
// inside a process). TestVizCannotReachRaftState fails if it ever imports a
// package that holds node state, and TestRoutesAreExactlyTheDocumentedControls
// fails if an HTTP route appears that is not one of the listed controls.
//
// It takes no locks. One goroutine owns the aggregated state and another runs
// control actions one at a time; everything else talks to them over channels.
package viz

import (
	"fmt"
	"strings"
	"time"

	adminv1 "github.com/Shashankkaranamr/Quorum/gen/quorum/admin/v1"
)

// State is the whole cluster as the UI sees it. It is rebuilt, never mutated,
// so a copy handed to a browser stream is never touched again.
type State struct {
	ClusterID string     `json:"clusterId"`
	Seq       uint64     `json:"seq"`
	UpdatedAt int64      `json:"updatedAt"` // unix ms
	Leader    uint64     `json:"leader"`    // 0 if no reachable node leads
	Term      uint64     `json:"term"`      // the highest term any reachable node reports
	Load      bool       `json:"load"`      // whether background writes are running
	Nodes     []NodeView `json:"nodes"`
	Links     []Link     `json:"links"`
	Events    []Event    `json:"events"`
}

// NodeView is one node. Process and Reachable are what the visualizer itself
// observes -- is there a process, does its admin stream answer -- and the rest
// is what the node last said about itself. When a node stops answering, its
// last report is kept, marked Stale, so the UI can show what it was doing when
// it went away.
type NodeView struct {
	ID        uint64 `json:"id"`
	Addr      string `json:"addr"`
	PID       int    `json:"pid"`
	Process   string `json:"process"` // "running" or "down"
	Reachable bool   `json:"reachable"`
	Stale     bool   `json:"stale"`

	Role       string     `json:"role"`
	Term       uint64     `json:"term"`
	Leader     uint64     `json:"leader"`
	Commit     uint64     `json:"commit"`
	Applied    uint64     `json:"applied"`
	LastIndex  uint64     `json:"lastIndex"`
	LastTerm   uint64     `json:"lastTerm"`
	SnapIndex  uint64     `json:"snapIndex"`
	Frozen     bool       `json:"frozen"`
	Log        []LogEntry `json:"log"`
	Metrics    Metrics    `json:"metrics"`
	ObservedAt int64      `json:"observedAt"` // unix ms, when the node reported this
}

// LogEntry is one entry of a node's log tail.
type LogEntry struct {
	Index     uint64 `json:"index"`
	Term      uint64 `json:"term"`
	Summary   string `json:"summary"`
	Committed bool   `json:"committed"`
	Applied   bool   `json:"applied"`
}

// Metrics are the node's counters the UI shows.
type Metrics struct {
	TickLag   uint64 `json:"tickLag"`
	FsyncP99  uint64 `json:"fsyncP99Us"`
	Fsyncs    uint64 `json:"fsyncs"`
	Elections uint64 `json:"elections"`
	Snapshots uint64 `json:"snapshots"`
}

// Link is one directed link. Injected is true when the fault injector has cut
// it -- the case the UI draws as a deliberate partition. Up is false when it is
// cut or when either end is not reachable at all.
type Link struct {
	From     uint64 `json:"from"`
	To       uint64 `json:"to"`
	Up       bool   `json:"up"`
	Injected bool   `json:"injected"`
}

// Event is one line of the timeline: an election, a node going down or coming
// back, a fault injected or healed.
type Event struct {
	At   int64  `json:"at"` // unix ms
	Kind string `json:"kind"`
	Text string `json:"text"`
}

// maxEvents bounds the timeline.
const maxEvents = 60

// nodeRecord is what the aggregator knows about one node.
type nodeRecord struct {
	id        uint64
	addr      string
	pid       int
	running   bool
	reachable bool
	last      *adminv1.NodeStatus
}

func roleName(r adminv1.Role) string {
	return strings.ToLower(strings.TrimPrefix(r.String(), "ROLE_"))
}

// view renders one node.
func (n *nodeRecord) view() NodeView {
	v := NodeView{ID: n.id, Addr: n.addr, PID: n.pid, Process: "down", Reachable: n.reachable}
	if n.running {
		v.Process = "running"
	}
	st := n.last
	if st == nil {
		v.Role = "unknown"
		return v
	}
	v.Stale = !n.reachable
	v.Role = roleName(st.GetRole())
	v.Term, v.Leader = st.GetTerm(), st.GetLeaderId()
	v.Commit, v.Applied = st.GetCommitIndex(), st.GetLastApplied()
	v.LastIndex, v.LastTerm, v.SnapIndex = st.GetLastLogIndex(), st.GetLastLogTerm(), st.GetSnapshotIndex()
	v.Frozen = st.GetFrozen()
	v.ObservedAt = st.GetObservedAtUnixMs()
	m := st.GetMetrics()
	v.Metrics = Metrics{TickLag: m.GetTickLagTicks(), FsyncP99: m.GetFsyncP99Micros(), Fsyncs: m.GetFsyncCount(),
		Elections: m.GetElectionsStarted(), Snapshots: m.GetSnapshotsTaken()}
	for _, e := range st.GetLogTail() {
		v.Log = append(v.Log, LogEntry{Index: e.GetIndex(), Term: e.GetTerm(), Summary: e.GetSummary(),
			Committed: e.GetCommitted(), Applied: e.GetApplied()})
	}
	return v
}

// build assembles the whole picture from the node records.
func build(clusterID string, seq uint64, nodes []*nodeRecord, events []Event, load bool) State {
	s := State{ClusterID: clusterID, Seq: seq, UpdatedAt: time.Now().UnixMilli(), Load: load,
		Events: append([]Event(nil), events...)}
	var leaderTerm uint64
	for _, n := range nodes {
		v := n.view()
		s.Nodes = append(s.Nodes, v)
		if !n.reachable || n.last == nil {
			continue
		}
		s.Term = max(s.Term, v.Term)
		if v.Role == "leader" && v.Term >= leaderTerm {
			s.Leader, leaderTerm = v.ID, v.Term
		}
	}
	// A leader that has not heard of the current term is a deposed leader
	// that does not know it yet, not the leader.
	if leaderTerm < s.Term {
		s.Leader = 0
	}

	byID := map[uint64]*nodeRecord{}
	for _, n := range nodes {
		byID[n.id] = n
	}
	blocked := func(on, peer uint64, outbound bool) bool {
		n := byID[on]
		if n == nil || n.last == nil || !n.reachable {
			return false
		}
		for _, p := range n.last.GetPeers() {
			if p.GetNodeId() == peer {
				if outbound {
					return p.GetBlockedOutbound()
				}
				return p.GetBlockedInbound()
			}
		}
		return false
	}
	for _, a := range nodes {
		for _, b := range nodes {
			if a.id == b.id {
				continue
			}
			// a -> b is cut if a will not send to b, or b will not accept
			// from a. Either end's injection is enough.
			injected := blocked(a.id, b.id, true) || blocked(b.id, a.id, false)
			up := !injected && a.reachable && b.reachable
			s.Links = append(s.Links, Link{From: a.id, To: b.id, Up: up, Injected: injected})
		}
	}
	return s
}

// diff compares two pictures and describes what changed, for the timeline. It
// is where an election becomes legible: a new leader in a higher term is an
// event, and so is a node dropping out or a link being cut.
func diff(prev, next State) []string {
	var out []string
	if next.Leader != 0 && (next.Leader != prev.Leader || next.Term != prev.Term) {
		out = append(out, fmt.Sprintf("election|term %d: node %d is leader", next.Term, next.Leader))
	} else if next.Leader == 0 && prev.Leader != 0 {
		out = append(out, fmt.Sprintf("election|no leader (node %d was, in term %d)", prev.Leader, prev.Term))
	}
	old := map[uint64]NodeView{}
	for _, n := range prev.Nodes {
		old[n.ID] = n
	}
	for _, n := range next.Nodes {
		o, ok := old[n.ID]
		if !ok {
			continue
		}
		switch {
		case o.Process == "running" && n.Process == "down":
			out = append(out, fmt.Sprintf("process|node %d: process gone (pid %d)", n.ID, o.PID))
		case o.Process == "down" && n.Process == "running":
			out = append(out, fmt.Sprintf("process|node %d: process started (pid %d)", n.ID, n.PID))
		case o.Reachable && !n.Reachable && n.Process == "running":
			out = append(out, fmt.Sprintf("process|node %d: not answering", n.ID))
		}
		if !o.Frozen && n.Frozen {
			out = append(out, fmt.Sprintf("fault|node %d frozen", n.ID))
		} else if o.Frozen && !n.Frozen && n.Reachable {
			out = append(out, fmt.Sprintf("fault|node %d thawed", n.ID))
		}
	}
	oldLinks := map[[2]uint64]bool{}
	for _, l := range prev.Links {
		oldLinks[[2]uint64{l.From, l.To}] = l.Injected
	}
	for _, l := range next.Links {
		was, ok := oldLinks[[2]uint64{l.From, l.To}]
		if !ok || was == l.Injected {
			continue
		}
		if l.Injected {
			out = append(out, fmt.Sprintf("fault|link %d -> %d cut", l.From, l.To))
		} else {
			out = append(out, fmt.Sprintf("fault|link %d -> %d restored", l.From, l.To))
		}
	}
	return out
}
