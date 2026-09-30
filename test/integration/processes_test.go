package integration

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// quorumctl runs the real CLI against config and returns its stdout.
func quorumctl(ctx context.Context, config string, args ...string) (string, error) {
	full := append([]string{args[0], "-config", config}, args[1:]...)
	cmd := exec.CommandContext(ctx, ctlBin, full...)
	var out, errOut bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errOut
	if err := cmd.Run(); err != nil {
		return out.String(), fmt.Errorf("quorumctl %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(errOut.String()))
	}
	return out.String(), nil
}

// eventually retries fn until it succeeds or d passes.
func eventually(t *testing.T, d time.Duration, fn func() error) {
	t.Helper()
	deadline := time.Now().Add(d)
	var err error
	for time.Now().Before(deadline) {
		if err = fn(); err == nil {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("still failing after %s: %v", d, err)
}

// TestProcessesServeReadsAndWrites is phase 5 acceptance criterion 1: three
// separate quorum-node processes over localhost gRPC, driven by the real
// quorumctl binary. `put k v` then `get k` returns v.
//
// It then goes one step past the criterion: every process is killed outright
// and restarted from its data directory, and the value must still be there --
// the write-ahead log doing its job across a real process kill, not a
// simulated one.
func TestProcessesServeReadsAndWrites(t *testing.T) {
	c := startCluster(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	eventually(t, 20*time.Second, func() error {
		_, err := quorumctl(ctx, c.config, "put", "-timeout", "3s", "greeting", "hello from three processes")
		return err
	})
	out, err := quorumctl(ctx, c.config, "get", "greeting")
	require.NoError(t, err)
	require.Equal(t, "hello from three processes\n", out)

	_, err = quorumctl(ctx, c.config, "get", "no-such-key")
	require.ErrorContains(t, err, "key not found", "a missing key must fail loudly, not print nothing")

	for _, id := range c.sup.IDs() {
		c.kill(id)
	}
	for _, id := range c.sup.IDs() {
		c.start(id)
	}
	eventually(t, 20*time.Second, func() error {
		out, err = quorumctl(ctx, c.config, "get", "-timeout", "3s", "greeting")
		return err
	})
	require.Equal(t, "hello from three processes\n", out, "the write did not survive killing every process")
}

// TestQuorumctlDrivesTheCluster runs a whole operator session through the
// real quorumctl binary: up, status, put, kill, start, partition, heal,
// freeze, thaw, get, down. Every subcommand must succeed and say what it did;
// the fault-injection tests exercise the same supervisor and admin paths in
// depth.
func TestQuorumctlDrivesTheCluster(t *testing.T) {
	skipShort(t)
	cfg := writeConfig(t, t.TempDir(), 3)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	run := func(args ...string) string {
		t.Helper()
		// The node binary sits beside quorumctl, which is where up and
		// start look for it by default.
		out, err := quorumctl(ctx, cfg, args...)
		require.NoError(t, err)
		return out
	}
	t.Cleanup(func() { _, _ = quorumctl(context.Background(), cfg, "down") })

	out := run("up")
	require.Contains(t, out, "node 3 started")
	require.Contains(t, out, "NODE")

	eventually(t, 20*time.Second, func() error {
		_, err := quorumctl(ctx, cfg, "put", "-timeout", "3s", "k", "v1")
		return err
	})

	require.Contains(t, run("kill", "2"), "the process is gone")
	require.Regexp(t, `(?m)^2\s+-\s+down`, run("status"))
	require.Contains(t, run("start", "2"), "node 2 started")

	require.Contains(t, run("partition", "1", "|", "2,3"), "cut [1] <-> [2 3]")
	require.Regexp(t, `(?m)^1\s+\d+\s+\w+.*\s2,3\s*$`, run("status"), "node 1 should report 2 and 3 blocked")
	require.Contains(t, run("heal"), "healed")
	require.Contains(t, run("partition", "-oneway", "3", "|", "1"), "cut [3] -> [1]")
	run("heal")

	require.Contains(t, run("freeze", "3"), "node 3 frozen")
	require.Regexp(t, `(?m)^3\s+\d+.*\strue\s`, run("status"), "status must answer, and say frozen, while the loop is parked")
	require.Regexp(t, `node 3 thawed after \d+ms, \d+ ticks missed`, run("thaw", "3"))

	eventually(t, 20*time.Second, func() error {
		_, err := quorumctl(ctx, cfg, "put", "-timeout", "3s", "k", "v2")
		return err
	})
	require.Equal(t, "v2\n", run("get", "k"))

	out = run("down")
	for id := 1; id <= 3; id++ {
		require.Contains(t, out, fmt.Sprintf("node %d killed", id))
	}
	require.Contains(t, run("status"), "down")
}
