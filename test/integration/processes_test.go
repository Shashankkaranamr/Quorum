// Package integration holds tests that run Quorum as real, separate OS
// processes talking over localhost gRPC.
package integration

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// buildBinaries compiles quorum-node and quorumctl into a temporary directory,
// so the test runs exactly what `make build` would produce.
func buildBinaries(t *testing.T) (node, ctl string) {
	t.Helper()
	goBin, err := exec.LookPath("go")
	require.NoError(t, err, "the go tool must be on PATH to build the binaries under test")
	dir := t.TempDir()
	exe := ""
	if runtime.GOOS == "windows" {
		exe = ".exe"
	}
	node, ctl = filepath.Join(dir, "quorum-node"+exe), filepath.Join(dir, "quorumctl"+exe)
	for out, pkg := range map[string]string{node: "./cmd/quorum-node", ctl: "./cmd/quorumctl"} {
		cmd := exec.Command(goBin, "build", "-o", out, pkg)
		cmd.Dir = filepath.Join("..", "..")
		b, err := cmd.CombinedOutput()
		require.NoError(t, err, "go build %s: %s", pkg, b)
	}
	return node, ctl
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = l.Close() }()
	return l.Addr().(*net.TCPAddr).Port
}

// writeConfig writes a three-node cluster.yaml on free ports with its data
// under dir.
func writeConfig(t *testing.T, dir string) string {
	t.Helper()
	var b strings.Builder
	fmt.Fprintf(&b, "cluster_id: integration\nnodes:\n")
	for id := 1; id <= 3; id++ {
		fmt.Fprintf(&b, "  - {id: %d, host: 127.0.0.1, grpc_port: %d, http_port: %d}\n", id, freePort(t), freePort(t))
	}
	fmt.Fprintf(&b, "raft:\n  tick_ms: 20\nstorage:\n  data_dir: %q\n", filepath.ToSlash(filepath.Join(dir, "data")))
	path := filepath.Join(dir, "cluster.yaml")
	require.NoError(t, os.WriteFile(path, []byte(b.String()), 0o644))
	return path
}

// syncBuffer collects a child process's output, which exec writes from its
// own goroutine.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// processes is a running cluster of quorum-node processes.
type processes struct {
	t      *testing.T
	bin    string
	config string
	procs  map[int]*exec.Cmd
	logs   map[int]*syncBuffer
}

func (p *processes) start(id int) {
	p.t.Helper()
	cmd := exec.Command(p.bin, "-id", fmt.Sprint(id), "-config", p.config)
	buf := p.logs[id]
	if buf == nil {
		buf = &syncBuffer{}
		p.logs[id] = buf
	}
	cmd.Stdout, cmd.Stderr = buf, buf
	require.NoError(p.t, cmd.Start())
	p.procs[id] = cmd
}

// kill terminates a node's process abruptly: TerminateProcess on Windows,
// SIGKILL elsewhere. No shutdown code runs.
func (p *processes) kill(id int) {
	p.t.Helper()
	cmd := p.procs[id]
	require.NoError(p.t, cmd.Process.Kill())
	_ = cmd.Wait()
	delete(p.procs, id)
}

func startCluster(t *testing.T) (*processes, string) {
	t.Helper()
	node, ctl := buildBinaries(t)
	p := &processes{t: t, bin: node, config: writeConfig(t, t.TempDir()), procs: map[int]*exec.Cmd{}, logs: map[int]*syncBuffer{}}
	for id := 1; id <= 3; id++ {
		p.start(id)
	}
	t.Cleanup(func() {
		for id := range p.procs {
			p.kill(id)
		}
		if t.Failed() {
			for id, l := range p.logs {
				t.Logf("node %d output:\n%s", id, l)
			}
		}
	})
	return p, ctl
}

// quorumctl runs the real CLI and returns its stdout.
func quorumctl(ctx context.Context, ctl, config string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, ctl, append([]string{args[0], "-config", config, "-timeout", "3s"}, args[1:]...)...)
	var out, errOut bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errOut
	err := cmd.Run()
	if err != nil {
		return out.String(), fmt.Errorf("%w: %s", err, strings.TrimSpace(errOut.String()))
	}
	return out.String(), nil
}

// eventually retries fn until it succeeds or the deadline passes; the first
// command against a fresh cluster waits for its first election.
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
// which is the write-ahead log doing its job across a real process kill, not a
// simulated one.
func TestProcessesServeReadsAndWrites(t *testing.T) {
	if testing.Short() {
		t.Skip("builds and runs real processes")
	}
	p, ctl := startCluster(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	eventually(t, 20*time.Second, func() error {
		_, err := quorumctl(ctx, ctl, p.config, "put", "greeting", "hello from three processes")
		return err
	})
	out, err := quorumctl(ctx, ctl, p.config, "get", "greeting")
	require.NoError(t, err)
	require.Equal(t, "hello from three processes\n", out)

	_, err = quorumctl(ctx, ctl, p.config, "get", "no-such-key")
	require.ErrorContains(t, err, "key not found", "a missing key must fail loudly, not print nothing")

	for id := 1; id <= 3; id++ {
		p.kill(id)
	}
	for id := 1; id <= 3; id++ {
		p.start(id)
	}
	eventually(t, 20*time.Second, func() error {
		out, err = quorumctl(ctx, ctl, p.config, "get", "greeting")
		return err
	})
	require.Equal(t, "hello from three processes\n", out, "the write did not survive killing every process")
}
