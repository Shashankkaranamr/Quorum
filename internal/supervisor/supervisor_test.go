package supervisor

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func newTestSupervisor(t *testing.T) *Supervisor {
	t.Helper()
	dir := t.TempDir()
	cfg := filepath.Join(dir, "cluster.yaml")
	yaml := fmt.Sprintf("nodes:\n"+
		"  - {id: 1, host: 127.0.0.1, grpc_port: 17001, http_port: 18001}\n"+
		"  - {id: 2, host: 127.0.0.1, grpc_port: 17002, http_port: 18002}\n"+
		"  - {id: 3, host: 127.0.0.1, grpc_port: 17003, http_port: 18003}\n"+
		"storage:\n  data_dir: %q\n", filepath.ToSlash(filepath.Join(dir, "data")))
	require.NoError(t, os.WriteFile(cfg, []byte(yaml), 0o644))
	s, err := New(filepath.Join(dir, "quorum-node"), cfg)
	require.NoError(t, err)
	return s
}

// TestSupervisorIgnoresAStrangersPID: operating systems reuse process ids, so a
// PID file left behind by a node that died can come to name an unrelated
// program. The supervisor must treat that node as not running -- and above
// all must not kill the stranger. The stranger here is this test process,
// which is alive and is not quorum-node.
func TestSupervisorIgnoresAStrangersPID(t *testing.T) {
	if runtime.GOOS != "windows" && runtime.GOOS != "linux" {
		t.Skip("process identity can only be checked on Windows and Linux")
	}
	s := newTestSupervisor(t)
	self := os.Getpid()
	require.True(t, ProcessAlive(self), "the liveness check must see a live process, or this proves nothing")

	require.NoError(t, os.MkdirAll(filepath.Dir(s.PIDFile(1)), 0o755))
	require.NoError(t, os.WriteFile(s.PIDFile(1), []byte(strconv.Itoa(self)), 0o644))

	_, ok := s.PID(1)
	require.False(t, ok, "a PID running a different program was taken for the node")
	err := s.Kill(1)
	require.True(t, errors.Is(err, ErrNotRunning), "Kill should refuse, got %v", err)
	require.True(t, ProcessAlive(self), "the supervisor killed a process it did not start")
}

// TestProcessAliveSeesDeadPIDs: an id with no process behind it is not alive.
// Together with the live case above, the check has been seen both ways.
func TestProcessAliveSeesDeadPIDs(t *testing.T) {
	s := newTestSupervisor(t)
	_, ok := s.PID(2)
	require.False(t, ok, "a node with no PID file is not running")
	require.True(t, errors.Is(s.Kill(2), ErrNotRunning))
	require.False(t, ProcessAlive(0x7ffffff0), "an absurd PID reported alive")
}
