package integration

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	adminv1 "github.com/Shashankkaranamr/Quorum/gen/quorum/admin/v1"
	"github.com/Shashankkaranamr/Quorum/internal/supervisor"
	"github.com/Shashankkaranamr/Quorum/internal/viz"
	"github.com/Shashankkaranamr/Quorum/raft"
	"github.com/Shashankkaranamr/Quorum/web"
)

// vizUpdateBound is phase 7's "within 500ms of a change".
const vizUpdateBound = 500 * time.Millisecond

// vizSession is a visualizer over a real cluster, driven only over HTTP --
// the same requests the browser makes -- and read only from its SSE stream.
type vizSession struct {
	t      *testing.T
	c      *realCluster
	url    string
	states chan viz.State
}

func startViz(t *testing.T, c *realCluster) *vizSession {
	t.Helper()
	s := viz.New(c.sup, web.Assets, t.Logf)
	hs := httptest.NewServer(s.Handler())
	ctx, cancel := context.WithCancel(context.Background())
	v := &vizSession{t: t, c: c, url: hs.URL, states: make(chan viz.State, 256)}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, hs.URL+"/api/events", nil)
	require.NoError(t, err)
	res, err := http.DefaultClient.Do(req) //nolint:bodyclose // the reader goroutine below closes it
	require.NoError(t, err)
	require.Equal(t, "text/event-stream", res.Header.Get("Content-Type"))
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		defer func() { _ = res.Body.Close() }()
		sc := bufio.NewScanner(res.Body)
		sc.Buffer(make([]byte, 1<<20), 1<<22)
		for sc.Scan() {
			line, ok := strings.CutPrefix(sc.Text(), "data: ")
			if !ok {
				continue
			}
			var st viz.State
			if json.Unmarshal([]byte(line), &st) == nil {
				select {
				case v.states <- st:
				default: // the test is not reading; drop, as a slow browser would
				}
			}
		}
	}()
	t.Cleanup(func() {
		cancel()
		<-readerDone
		hs.Close()
		s.Close()
	})
	return v
}

// post presses a button.
func (v *vizSession) post(path string, body any) {
	v.t.Helper()
	b, _ := json.Marshal(body)
	req, err := http.NewRequest(http.MethodPost, v.url+path, bytes.NewReader(b))
	require.NoError(v.t, err)
	req.Header.Set(viz.ControlHeader, "1")
	req.Header.Set("Content-Type", "application/json")
	res, err := http.DefaultClient.Do(req)
	require.NoError(v.t, err)
	defer func() { _ = res.Body.Close() }()
	var msg bytes.Buffer
	_, _ = msg.ReadFrom(res.Body)
	require.Equal(v.t, http.StatusOK, res.StatusCode, "%s: %s", path, msg.String())
}

// await reads states from the stream until cond holds, and returns that state
// and how long it took. It fails the test after within.
func (v *vizSession) await(within time.Duration, what string, cond func(viz.State) bool) (viz.State, time.Duration) {
	v.t.Helper()
	start := time.Now()
	deadline := time.After(within)
	for {
		select {
		case st := <-v.states:
			if cond(st) {
				return st, time.Since(start)
			}
		case <-deadline:
			v.t.Fatalf("the visualizer did not show %s within %s", what, within)
			return viz.State{}, 0
		}
	}
}

// drain discards states already queued, so the next await sees only what
// happens after this point.
func (v *vizSession) drain() {
	for {
		select {
		case <-v.states:
		default:
			return
		}
	}
}

func node(st viz.State, id uint64) viz.NodeView {
	for _, n := range st.Nodes {
		if n.ID == id {
			return n
		}
	}
	return viz.NodeView{}
}

func link(st viz.State, from, to uint64) viz.Link {
	for _, l := range st.Links {
		if l.From == from && l.To == to {
			return l
		}
	}
	return viz.Link{}
}

// TestVizShowsLiveState is phase 7 acceptance criterion 1, and with its 5-node
// run the live half of criterion 6. A write made through the page must appear,
// committed, in every node's log tail on the stream within 500ms of the write
// completing -- entries visibly replicating -- along with each node's role,
// term, indices and leader.
func TestVizShowsLiveState(t *testing.T) {
	for _, n := range []int{3, 5} {
		t.Run(fmt.Sprintf("%d nodes", n), func(t *testing.T) {
			c := startCluster(t, n)
			c.leader()
			v := startViz(t, c)
			st, _ := v.await(5*time.Second, "a leader", func(s viz.State) bool { return s.Leader != 0 })
			require.Len(t, st.Nodes, n)
			require.Len(t, st.Links, n*(n-1))

			for i := range 3 {
				key := fmt.Sprintf("live-%d", i)
				v.post("/api/put", map[string]string{"key": key, "value": "v"})
				v.drain()
				_, took := v.await(vizUpdateBound, "the write on every node", func(s viz.State) bool {
					for _, nd := range s.Nodes {
						found := false
						for _, e := range nd.Log {
							found = found || (e.Committed && strings.Contains(e.Summary, "put "+key+" "))
						}
						if !found {
							return false
						}
					}
					return true
				})
				t.Logf("%s: committed on all %d nodes and shown %s after the write returned", key, n, took)
			}

			st, _ = v.await(time.Second, "every node's details", func(viz.State) bool { return true })
			for _, nd := range st.Nodes {
				require.Equal(t, "running", nd.Process)
				require.True(t, nd.Reachable)
				require.Contains(t, []string{"leader", "follower"}, nd.Role)
				require.Equal(t, st.Term, nd.Term, "node %d", nd.ID)
				require.Equal(t, st.Leader, nd.Leader, "node %d", nd.ID)
				require.Positive(t, nd.Commit)
				require.NotEmpty(t, nd.Log)
			}
		})
	}
}

// TestVizKillIsRealAndRestartRecovers is phase 7 acceptance criterion 2. The
// page's "kill" must terminate the operating-system process -- checked by the
// PID's absence, not by what the page shows -- and "start" must bring the node
// back from its existing data directory.
func TestVizKillIsRealAndRestartRecovers(t *testing.T) {
	c := startCluster(t, 3)
	lead := c.leader()
	v := startViz(t, c)
	f := uint64(c.others(lead)[0])
	st, _ := v.await(5*time.Second, "node details", func(s viz.State) bool { return node(s, f).PID != 0 && node(s, f).LastIndex > 0 })
	pid, lastIndex := node(st, f).PID, node(st, f).LastIndex
	require.True(t, supervisor.ProcessAlive(pid))

	v.post(fmt.Sprintf("/api/nodes/%d/kill", f), nil)
	require.False(t, supervisor.ProcessAlive(pid), "the page's kill left process %d running", pid)
	v.await(vizUpdateBound, "the node as down", func(s viz.State) bool { return node(s, f).Process == "down" })

	v.post(fmt.Sprintf("/api/nodes/%d/start", f), nil)
	st, _ = v.await(10*time.Second, "the node back", func(s viz.State) bool {
		n := node(s, f)
		return n.Process == "running" && n.Reachable && !n.Stale
	})
	back := node(st, f)
	require.NotEqual(t, pid, back.PID, "a restart is a new process")
	require.GreaterOrEqual(t, back.LastIndex, lastIndex, "the node came back without the log on its disk")
}

// TestVizShowsDirectedPartitions is phase 7 acceptance criterion 3. A one-way
// cut made from the page must be a real transport-level cut -- checked on the
// node's own admin API, not on the page -- and the page must show exactly that
// directed link down. A two-way cut shows both directions.
func TestVizShowsDirectedPartitions(t *testing.T) {
	c := startCluster(t, 3)
	c.leader()
	v := startViz(t, c)
	v.await(5*time.Second, "a leader", func(s viz.State) bool { return s.Leader != 0 })

	v.post("/api/partition", map[string]any{"a": []uint64{1}, "b": []uint64{2}, "oneway": true})
	var one *adminv1.PeerView
	for _, p := range c.mustStatus(1).GetPeers() {
		if p.GetNodeId() == 2 {
			one = p
		}
	}
	require.True(t, one.GetBlockedOutbound(), "node 1's transport is not refusing to send to node 2")
	require.False(t, one.GetBlockedInbound(), "a one-way cut also blocked the other direction")
	v.await(vizUpdateBound, "1 -> 2 cut and 2 -> 1 up", func(s viz.State) bool {
		return link(s, 1, 2).Injected && !link(s, 2, 1).Injected && link(s, 2, 1).Up
	})

	v.post("/api/heal", nil)
	v.post("/api/partition", map[string]any{"a": []uint64{3}, "b": []uint64{1, 2}})
	v.await(vizUpdateBound, "node 3 cut off both ways", func(s viz.State) bool {
		for _, p := range [][2]uint64{{3, 1}, {1, 3}, {3, 2}, {2, 3}} {
			if !link(s, p[0], p[1]).Injected {
				return false
			}
		}
		return !link(s, 1, 2).Injected && !link(s, 2, 1).Injected
	})

	v.post("/api/heal", nil)
	v.await(vizUpdateBound, "every link restored", func(s viz.State) bool {
		for _, l := range s.Links {
			if l.Injected || !l.Up {
				return false
			}
		}
		return true
	})
}

// TestVizMakesElectionsLegible is phase 7 acceptance criterion 4. Killing the
// leader from the page must show the old leader as down within one refresh,
// then a new leader in a higher term as soon as one is elected, with the
// election on the timeline.
func TestVizMakesElectionsLegible(t *testing.T) {
	c := startCluster(t, 3)
	c.leader()
	v := startViz(t, c)
	st, _ := v.await(5*time.Second, "a leader", func(s viz.State) bool { return s.Leader != 0 })
	old, oldTerm := st.Leader, st.Term

	v.post(fmt.Sprintf("/api/nodes/%d/kill", old), nil)
	killed := time.Now()
	_, took := v.await(vizUpdateBound, "the old leader as down", func(s viz.State) bool {
		n := node(s, old)
		return n.Process == "down" && !n.Reachable
	})
	t.Logf("old leader %d shown down %s after the kill returned", old, took)

	st, _ = v.await(electionBound, "a new leader in a higher term", func(s viz.State) bool {
		return s.Leader != 0 && s.Leader != old && s.Term > oldTerm
	})
	elected := c.leaderAmong(c.others(raft.NodeID(old))...)
	t.Logf("node %d leads term %d, shown %s after the kill; the cluster reports node %d",
		st.Leader, st.Term, time.Since(killed), elected)
	require.Equal(t, "down", node(st, old).Process, "the old leader must still be shown down")
	require.True(t, node(st, old).Stale, "the old leader's last report is kept, marked stale")

	found := false
	for _, e := range st.Events {
		found = found || (e.Kind == "election" && strings.Contains(e.Text, fmt.Sprintf("term %d: node %d is leader", st.Term, st.Leader)))
	}
	require.True(t, found, "the election is not on the timeline: %v", st.Events)
}
