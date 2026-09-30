// Package integration runs Quorum as real, separate OS processes talking over
// localhost gRPC, started and killed by the supervisor and broken through the
// admin API -- the same paths quorumctl uses.
package integration

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	adminv1 "github.com/Shashankkaranamr/Quorum/gen/quorum/admin/v1"
	"github.com/Shashankkaranamr/Quorum/internal/client"
	"github.com/Shashankkaranamr/Quorum/internal/supervisor"
	"github.com/Shashankkaranamr/Quorum/raft"
)

// Binaries built once for the whole package, in TestMain.
var nodeBin, ctlBin string

func TestMain(m *testing.M) {
	flag.Parse()
	if testing.Short() {
		// Every test here builds and runs real processes.
		os.Exit(m.Run())
	}
	dir, err := os.MkdirTemp("", "quorum-integration-bin-")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	code := func() int {
		defer func() { _ = os.RemoveAll(dir) }()
		if nodeBin, ctlBin, err = build(dir); err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		return m.Run()
	}()
	os.Exit(code)
}

// build compiles quorum-node and quorumctl, so the tests run exactly what
// `make build` would produce.
func build(dir string) (node, ctl string, err error) {
	goBin, err := exec.LookPath("go")
	if err != nil {
		return "", "", fmt.Errorf("the go tool must be on PATH to build the binaries under test: %w", err)
	}
	exe := ""
	if runtime.GOOS == "windows" {
		exe = ".exe"
	}
	node, ctl = filepath.Join(dir, "quorum-node"+exe), filepath.Join(dir, "quorumctl"+exe)
	for out, pkg := range map[string]string{node: "./cmd/quorum-node", ctl: "./cmd/quorumctl"} {
		args := []string{"build", "-o", out}
		if raceEnabled {
			// Under `make race`, the nodes run under the race detector too,
			// so the real multi-process system is checked, not only the
			// test harness. Reports land in each node's log; see
			// startCluster's cleanup.
			args = append(args, "-race")
		}
		cmd := exec.Command(goBin, append(args, pkg)...)
		cmd.Dir = filepath.Join("..", "..")
		if b, err := cmd.CombinedOutput(); err != nil {
			return "", "", fmt.Errorf("go build %s: %w\n%s", pkg, err, b)
		}
	}
	return node, ctl, nil
}

func skipShort(t *testing.T) {
	t.Helper()
	if testing.Short() {
		t.Skip("builds and runs real processes")
	}
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = l.Close() }()
	return l.Addr().(*net.TCPAddr).Port
}

// testTickMS keeps elections quick: 200-400ms at the default 10-20 ticks.
const testTickMS = 20

// electionBound is how long a new leader may take to appear after the old
// one is lost. The election timeout is at most 400ms; this allows several
// split votes and a slow machine.
const electionBound = 5 * time.Second

func writeConfig(t *testing.T, dir string, n int) string {
	t.Helper()
	var b strings.Builder
	fmt.Fprintf(&b, "cluster_id: integration\nnodes:\n")
	for id := 1; id <= n; id++ {
		fmt.Fprintf(&b, "  - {id: %d, host: 127.0.0.1, grpc_port: %d}\n", id, freePort(t))
	}
	fmt.Fprintf(&b, "raft:\n  tick_ms: %d\n  snapshot_threshold_entries: 500\n", testTickMS)
	fmt.Fprintf(&b, "storage:\n  data_dir: %q\n", filepath.ToSlash(filepath.Join(dir, "data")))
	path := filepath.Join(dir, "cluster.yaml")
	require.NoError(t, os.WriteFile(path, []byte(b.String()), 0o644))
	return path
}

// realCluster is n quorum-node processes under a supervisor.
type realCluster struct {
	t      *testing.T
	config string
	sup    *supervisor.Supervisor
}

func startCluster(t *testing.T, n int) *realCluster {
	t.Helper()
	skipShort(t)
	cfg := writeConfig(t, t.TempDir(), n)
	sup, err := supervisor.New(nodeBin, cfg)
	require.NoError(t, err)
	c := &realCluster{t: t, config: cfg, sup: sup}
	t.Cleanup(func() {
		for _, id := range sup.IDs() {
			if _, ok := sup.PID(id); ok {
				_ = sup.Kill(id)
			}
		}
		for _, id := range sup.IDs() {
			b, _ := os.ReadFile(sup.LogFile(id))
			if strings.Contains(string(b), "WARNING: DATA RACE") {
				t.Errorf("node %d reported a data race:\n%s", id, tail(string(b), 6000))
			}
		}
		if t.Failed() {
			for _, id := range sup.IDs() {
				b, _ := os.ReadFile(sup.LogFile(id))
				t.Logf("node %d log (last 3000 bytes):\n%s", id, tail(string(b), 3000))
			}
		}
	})
	for _, id := range sup.IDs() {
		c.start(id)
	}
	return c
}

func tail(s string, n int) string {
	if len(s) > n {
		return s[len(s)-n:]
	}
	return s
}

func (c *realCluster) start(id raft.NodeID) {
	c.t.Helper()
	_, err := c.sup.Start(id)
	require.NoError(c.t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	require.NoError(c.t, c.sup.WaitReady(ctx, id))
}

// kill terminates a node's process and proves it: the operating system must
// no longer have a process with that id.
func (c *realCluster) kill(id raft.NodeID) {
	c.t.Helper()
	pid, ok := c.sup.PID(id)
	require.True(c.t, ok, "node %d is not running", id)
	// The check below must be able to fail: the process is alive now.
	require.True(c.t, supervisor.ProcessAlive(pid), "node %d's process %d is not alive before the kill", id, pid)
	require.NoError(c.t, c.sup.Kill(id))
	require.False(c.t, supervisor.ProcessAlive(pid), "node %d's process %d survived the kill", id, pid)
}

func (c *realCluster) admin(id raft.NodeID) adminv1.AdminClient {
	c.t.Helper()
	a, conn, err := c.sup.Admin(id)
	require.NoError(c.t, err)
	c.t.Cleanup(func() { _ = conn.Close() })
	return a
}

// status asks a node for its state. A node that is down or wedged returns an
// error rather than failing the test.
func (c *realCluster) status(id raft.NodeID) (*adminv1.NodeStatus, error) {
	a, conn, err := c.sup.Admin(id)
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	return a.GetStatus(ctx, &adminv1.GetStatusRequest{LogTailLimit: 32})
}

func (c *realCluster) mustStatus(id raft.NodeID) *adminv1.NodeStatus {
	c.t.Helper()
	st, err := c.status(id)
	require.NoError(c.t, err)
	return st
}

// leaderAmong is the node in ids that leads in the highest term and has
// committed in it, or None.
func (c *realCluster) leaderAmong(ids ...raft.NodeID) raft.NodeID {
	if len(ids) == 0 {
		ids = c.sup.IDs()
	}
	var best raft.NodeID
	var bestTerm uint64
	for _, id := range ids {
		st, err := c.status(id)
		if err != nil || st.GetFrozen() {
			continue
		}
		if st.GetRole() == adminv1.Role_ROLE_LEADER && st.GetTerm() > bestTerm && st.GetCommitIndex() > 0 {
			best, bestTerm = id, st.GetTerm()
		}
	}
	return best
}

// waitLeader waits for a leader among ids and reports how long it took.
func (c *realCluster) waitLeader(within time.Duration, ids ...raft.NodeID) (raft.NodeID, time.Duration) {
	c.t.Helper()
	start := time.Now()
	for time.Since(start) < within {
		if id := c.leaderAmong(ids...); id != raft.None {
			return id, time.Since(start)
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatalf("no leader among %v within %s", ids, within)
	return raft.None, 0
}

// leader waits for a leader among ids (all by default). Leadership can be in
// flux for a moment after any fault, and a status call can time out on a
// loaded machine, so code that needs a leader must wait for one rather than
// take leaderAmong's momentary answer.
func (c *realCluster) leader(ids ...raft.NodeID) raft.NodeID {
	c.t.Helper()
	id, _ := c.waitLeader(electionBound, ids...)
	return id
}

func (c *realCluster) others(id raft.NodeID, of ...raft.NodeID) []raft.NodeID {
	if len(of) == 0 {
		of = c.sup.IDs()
	}
	var out []raft.NodeID
	for _, o := range of {
		if o != id {
			out = append(out, o)
		}
	}
	return out
}

// block cuts node on's links to peers, in the directions req says.
func (c *realCluster) block(on raft.NodeID, peers []raft.NodeID, inboundOnly, outboundOnly bool) {
	c.t.Helper()
	req := &adminv1.BlockLinksRequest{InboundOnly: inboundOnly, OutboundOnly: outboundOnly}
	for _, p := range peers {
		req.PeerIds = append(req.PeerIds, uint64(p))
	}
	_, err := c.admin(on).BlockLinks(context.Background(), req)
	require.NoError(c.t, err)
}

// partition cuts every link between groups a and b, in both directions, on
// both ends.
func (c *realCluster) partition(a, b []raft.NodeID) {
	c.t.Helper()
	for _, x := range a {
		c.block(x, b, false, false)
	}
	for _, y := range b {
		c.block(y, a, false, false)
	}
}

// heal removes every injected fault on every running node.
func (c *realCluster) heal() {
	c.t.Helper()
	for _, id := range c.sup.IDs() {
		if _, ok := c.sup.PID(id); !ok {
			continue
		}
		_, err := c.admin(id).Heal(context.Background(), &adminv1.HealRequest{})
		require.NoError(c.t, err)
	}
}

func (c *realCluster) addrs(only ...raft.NodeID) map[raft.NodeID]string {
	if len(only) == 0 {
		only = c.sup.IDs()
	}
	out := map[raft.NodeID]string{}
	for _, id := range only {
		out[id] = c.sup.Addr(id)
	}
	return out
}

// client returns a registered client that talks to the given nodes (all by
// default).
func (c *realCluster) client(cfg client.Config, only ...raft.NodeID) *client.Client {
	c.t.Helper()
	cfg.Addrs = c.addrs(only...)
	cl, err := client.Dial(cfg)
	require.NoError(c.t, err)
	c.t.Cleanup(func() { _ = cl.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	require.NoError(c.t, cl.Register(ctx))
	return cl
}

// waitCaughtUp waits until every listed node has applied at least index i.
func (c *realCluster) waitCaughtUp(i uint64, within time.Duration, ids ...raft.NodeID) {
	c.t.Helper()
	if len(ids) == 0 {
		ids = c.sup.IDs()
	}
	require.Eventually(c.t, func() bool {
		for _, id := range ids {
			st, err := c.status(id)
			if err != nil || st.GetLastApplied() < i {
				return false
			}
		}
		return true
	}, within, 20*time.Millisecond, "nodes %v did not all apply index %d", ids, i)
}

// TestNodesAreRaceCheckedUnderRace keeps the data-race log scan honest: under
// the race detector, the node binary must really have been built with it, or
// scanning its logs for reports would pass however racy it was.
func TestNodesAreRaceCheckedUnderRace(t *testing.T) {
	skipShort(t)
	out, err := exec.Command("go", "version", "-m", nodeBin).CombinedOutput()
	require.NoError(t, err, "%s", out)
	require.Equal(t, raceEnabled, strings.Contains(string(out), "-race=true"),
		"race detector on for the tests: %v; build info of the node binary:\n%s", raceEnabled, out)
}
