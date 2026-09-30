package viz

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	adminv1 "github.com/Shashankkaranamr/Quorum/gen/quorum/admin/v1"
	"github.com/Shashankkaranamr/Quorum/internal/supervisor"
	"github.com/Shashankkaranamr/Quorum/web"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

const module = "github.com/Shashankkaranamr/Quorum/"

// holdsNodeState are the packages that hold, or can reach, a running node's
// Raft state. A process importing them could build a node of its own; the
// visualizer must not be able to.
var holdsNodeState = []string{
	"internal/node", "internal/server", "internal/storage", "internal/statemachine",
	"internal/kvservice", "internal/transport", "internal/testutil",
}

// forbiddenDeps lists the packages in holdsNodeState that pkg depends on,
// directly or not.
func forbiddenDeps(t *testing.T, pkg string) []string {
	t.Helper()
	out, err := exec.Command("go", "list", "-deps", module+pkg).Output()
	require.NoError(t, err, "go list -deps %s", pkg)
	var bad []string
	for _, dep := range strings.Fields(string(out)) {
		for _, f := range holdsNodeState {
			if dep == module+f || strings.HasPrefix(dep, module+f+"/") {
				bad = append(bad, strings.TrimPrefix(dep, module))
			}
		}
	}
	return bad
}

// TestVizCannotReachRaftState is phase 7 acceptance criterion 5, made
// structural. The nodes are separate processes, so the visualizer can reach
// their state only through what it imports: the supervisor (processes), the
// admin API (fault injection and status) and the client library (writes).
// If it ever depends on a package that holds node state, this fails.
//
// Package raft itself is allowed: the supervisor's API names nodes with
// raft.NodeID. Importing the core's types does not give a process access to
// another process's node.
func TestVizCannotReachRaftState(t *testing.T) {
	for _, pkg := range []string{"internal/viz", "cmd/quorum-viz", "web"} {
		require.Empty(t, forbiddenDeps(t, pkg), "%s can reach node state", pkg)
	}
}

// TestNodeStateCheckFlagsANodeAssembly is the negative control: the same check
// run on internal/node, which really does hold node state, must object.
func TestNodeStateCheckFlagsANodeAssembly(t *testing.T) {
	bad := forbiddenDeps(t, "internal/node")
	require.Contains(t, bad, "internal/server")
	require.Contains(t, bad, "internal/storage")
}

// TestRoutesAreExactlyTheDocumentedControls pins the whole HTTP surface. Every
// control that changes anything is a POST, and each says which supervisor,
// admin or KV call it makes; a new route cannot appear without this test being
// changed deliberately.
func TestRoutesAreExactlyTheDocumentedControls(t *testing.T) {
	var got []string
	for _, r := range routes() {
		got = append(got, r.Pattern)
		if strings.HasPrefix(r.Pattern, "POST ") {
			require.True(t, strings.Contains(r.Does, "supervisor.") || strings.Contains(r.Does, "admin ") ||
				strings.Contains(r.Does, "KV Put"), "%s does not say which API it goes through: %q", r.Pattern, r.Does)
		} else {
			require.True(t, strings.HasPrefix(r.Pattern, "GET "), "%s", r.Pattern)
		}
	}
	require.Equal(t, []string{
		"GET /", "GET /api/state", "GET /api/events",
		"POST /api/nodes/{id}/kill", "POST /api/nodes/{id}/start",
		"POST /api/nodes/{id}/freeze", "POST /api/nodes/{id}/thaw",
		"POST /api/partition", "POST /api/heal", "POST /api/put", "POST /api/load",
	}, got)
}

// offlineServer is a visualizer for a cluster whose nodes are not running:
// enough to test the HTTP surface and the shape of the state.
func offlineServer(t *testing.T, configPath string) (*Server, *httptest.Server) {
	t.Helper()
	sup, err := supervisor.New(filepath.Join(t.TempDir(), "quorum-node"), configPath)
	require.NoError(t, err)
	s := New(sup, web.Assets, nil)
	hs := httptest.NewServer(s.Handler())
	t.Cleanup(func() {
		hs.Close()
		s.Close()
	})
	return s, hs
}

func writeConfig(t *testing.T, n int) string {
	t.Helper()
	var b strings.Builder
	b.WriteString("nodes:\n")
	for i := 1; i <= n; i++ {
		// Ports nothing listens on: the nodes are "down".
		fmt.Fprintf(&b, "  - {id: %d, host: 127.0.0.1, grpc_port: %d}\n", i, 1+i)
	}
	fmt.Fprintf(&b, "storage:\n  data_dir: %q\n", filepath.ToSlash(filepath.Join(t.TempDir(), "data")))
	p := filepath.Join(t.TempDir(), "cluster.yaml")
	require.NoError(t, os.WriteFile(p, []byte(b.String()), 0o644))
	return p
}

// TestControlsRefuseCrossSiteRequests: a page on another origin must not be
// able to press the buttons. Reads are open; every control needs the control
// header, and a foreign Origin is refused even with it.
func TestControlsRefuseCrossSiteRequests(t *testing.T) {
	_, hs := offlineServer(t, writeConfig(t, 3))
	post := func(header, origin string) int {
		req, err := http.NewRequest(http.MethodPost, hs.URL+"/api/heal", nil)
		require.NoError(t, err)
		if header != "" {
			req.Header.Set(ControlHeader, header)
		}
		if origin != "" {
			req.Header.Set("Origin", origin)
		}
		res, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		_ = res.Body.Close()
		return res.StatusCode
	}
	require.Equal(t, http.StatusForbidden, post("", ""), "no control header")
	require.Equal(t, http.StatusForbidden, post("1", "http://evil.example"), "foreign origin")
	require.NotEqual(t, http.StatusForbidden, post("1", hs.URL), "same origin with the header must get through")

	res, err := http.Get(hs.URL + "/api/state")
	require.NoError(t, err)
	_ = res.Body.Close()
	require.Equal(t, http.StatusOK, res.StatusCode)
}

// TestFrontendIsServedFromTheBinary: the page and its script come from the
// embedded assets, and the script listens to the state stream.
func TestFrontendIsServedFromTheBinary(t *testing.T) {
	_, hs := offlineServer(t, writeConfig(t, 3))
	for path, want := range map[string]string{"/": "app.js", "/app.js": "/api/events", "/style.css": "--leader"} {
		res, err := http.Get(hs.URL + path)
		require.NoError(t, err)
		b, err := io.ReadAll(res.Body)
		_ = res.Body.Close()
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, res.StatusCode, path)
		require.Contains(t, string(b), want, path)
	}
}

// TestShapeFollowsTheConfig is the configuration half of phase 7 criterion 6:
// the repository's own cluster.yaml and cluster-5.yaml give three and five
// nodes and every directed link between them, with no code change. (The live
// half runs real clusters of both sizes in test/integration.)
func TestShapeFollowsTheConfig(t *testing.T) {
	for file, n := range map[string]int{"cluster.yaml": 3, "cluster-5.yaml": 5} {
		t.Run(file, func(t *testing.T) {
			s, _ := offlineServer(t, filepath.Join("..", "..", file))
			var st State
			require.Eventually(t, func() bool { st = s.State(); return st.Seq > 0 }, 5*time.Second, 10*time.Millisecond)
			require.Len(t, st.Nodes, n)
			require.Len(t, st.Links, n*(n-1))
			for _, node := range st.Nodes {
				require.Equal(t, "down", node.Process, "nothing is running in this test")
			}
		})
	}
}

func status(role adminv1.Role, term uint64, blockedOut, blockedIn []uint64, peers ...uint64) *adminv1.NodeStatus {
	st := &adminv1.NodeStatus{Role: role, Term: term}
	for _, p := range peers {
		st.Peers = append(st.Peers, &adminv1.PeerView{NodeId: p,
			BlockedOutbound: slices.Contains(blockedOut, p), BlockedInbound: slices.Contains(blockedIn, p)})
	}
	return st
}

// TestPictureShowsDirectionAndElections checks the aggregation itself: a
// one-way cut is one directed link, not two; a new leader in a higher term is a
// timeline event; and a deposed leader that has not heard of the new term is
// not shown as the leader.
func TestPictureShowsDirectionAndElections(t *testing.T) {
	n1 := &nodeRecord{id: 1, running: true, reachable: true, last: status(adminv1.Role_ROLE_LEADER, 3, []uint64{2}, nil, 2, 3)}
	n2 := &nodeRecord{id: 2, running: true, reachable: true, last: status(adminv1.Role_ROLE_FOLLOWER, 3, nil, nil, 1, 3)}
	n3 := &nodeRecord{id: 3, running: true, reachable: true, last: status(adminv1.Role_ROLE_FOLLOWER, 3, nil, nil, 1, 2)}
	before := build("c", 1, []*nodeRecord{n1, n2, n3}, nil, false)
	require.Equal(t, uint64(1), before.Leader)
	cut := map[[2]uint64]bool{}
	for _, l := range before.Links {
		if l.Injected {
			cut[[2]uint64{l.From, l.To}] = true
		}
	}
	require.Equal(t, map[[2]uint64]bool{{1, 2}: true}, cut, "a one-way cut must be exactly one directed link")

	// Node 1 is killed; node 2 wins term 4.
	n1.running, n1.reachable = false, false
	n2.last = status(adminv1.Role_ROLE_LEADER, 4, nil, nil, 1, 3)
	after := build("c", 2, []*nodeRecord{n1, n2, n3}, nil, false)
	require.Equal(t, uint64(2), after.Leader)
	require.Equal(t, uint64(4), after.Term)
	events := strings.Join(diff(before, after), "\n")
	require.Contains(t, events, "term 4: node 2 is leader")
	require.Contains(t, events, "node 1: process gone")
	require.Equal(t, "down", after.Nodes[0].Process)
	require.True(t, after.Nodes[0].Stale, "the dead node's last report is kept, and marked stale")

	// A reachable old leader still claiming term 3 while term 4 exists is not
	// the leader.
	n1.running, n1.reachable = true, true
	n2.last = status(adminv1.Role_ROLE_FOLLOWER, 4, nil, nil, 1, 3)
	stale := build("c", 3, []*nodeRecord{n1, n2, n3}, nil, false)
	require.Zero(t, stale.Leader)
}
