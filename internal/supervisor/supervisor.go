package supervisor

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	adminv1 "github.com/Shashankkaranamr/Quorum/gen/quorum/admin/v1"
	"github.com/Shashankkaranamr/Quorum/internal/config"
	"github.com/Shashankkaranamr/Quorum/raft"
)

var (
	// ErrNotRunning means no live process is recorded for the node.
	ErrNotRunning = errors.New("supervisor: node is not running")

	// ErrAlreadyRunning means the node's recorded process is still alive.
	ErrAlreadyRunning = errors.New("supervisor: node is already running")
)

// Supervisor starts, kills and restarts the node processes of one cluster.
//
// Each node's PID is recorded in a file in its data directory, so a
// Supervisor in one quorumctl invocation can kill a node another one started.
// A PID on its own is not an identity -- the operating system reuses them --
// so before acting on a recorded PID the supervisor checks, where the platform
// allows it, that the process is still running the quorum-node binary it
// started. It will not kill a stranger that inherited the number.
type Supervisor struct {
	bin     string
	config  string
	cluster *config.Cluster

	// exited is closed when a child this supervisor started has been reaped.
	// Only children started by this process can be waited on; for the rest,
	// liveness comes from the operating system.
	exited map[raft.NodeID]chan struct{}
}

// New creates a supervisor for the cluster described by configPath, running
// nodes from the quorum-node binary at bin.
func New(bin, configPath string) (*Supervisor, error) {
	c, err := config.Load(configPath)
	if err != nil {
		return nil, err
	}
	absBin, err := filepath.Abs(bin)
	if err != nil {
		return nil, err
	}
	absCfg, err := filepath.Abs(configPath)
	if err != nil {
		return nil, err
	}
	return &Supervisor{bin: absBin, config: absCfg, cluster: c, exited: map[raft.NodeID]chan struct{}{}}, nil
}

// Cluster is the parsed configuration.
func (s *Supervisor) Cluster() *config.Cluster { return s.cluster }

// IDs are the configured node ids.
func (s *Supervisor) IDs() []raft.NodeID {
	var ids []raft.NodeID
	for _, id := range s.cluster.IDs() {
		ids = append(ids, raft.NodeID(id))
	}
	return ids
}

// Addr is a node's gRPC address.
func (s *Supervisor) Addr(id raft.NodeID) string {
	n, _ := s.cluster.Node(config.NodeID(id))
	return n.GRPCAddr()
}

func (s *Supervisor) dataDir(id raft.NodeID) string { return s.cluster.DataDir(config.NodeID(id)) }

// PIDFile is where a node's process id is recorded.
func (s *Supervisor) PIDFile(id raft.NodeID) string { return filepath.Join(s.dataDir(id), "node.pid") }

// LogFile is where a node's output goes.
func (s *Supervisor) LogFile(id raft.NodeID) string { return filepath.Join(s.dataDir(id), "node.log") }

func (s *Supervisor) readPID(id raft.NodeID) (int, bool) {
	b, err := os.ReadFile(s.PIDFile(id))
	if err != nil {
		return 0, false
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(b)))
	return pid, err == nil && pid > 0
}

// PID is the node's live process id, if it has one. A recorded PID whose
// process has died, or now belongs to a different program, counts as none.
func (s *Supervisor) PID(id raft.NodeID) (int, bool) {
	pid, ok := s.readPID(id)
	if !ok || !processAlive(pid) {
		return 0, false
	}
	if same, known := processRuns(pid, s.bin); known && !same {
		return 0, false
	}
	return pid, true
}

// Start launches a node against its data directory. The process is detached
// from the caller's terminal and process group, so it outlives the quorumctl
// that started it and does not receive that terminal's Ctrl+C.
func (s *Supervisor) Start(id raft.NodeID) (int, error) {
	if _, ok := s.cluster.Node(config.NodeID(id)); !ok {
		return 0, fmt.Errorf("supervisor: node %d is not in the config", id)
	}
	if pid, ok := s.PID(id); ok {
		return pid, fmt.Errorf("%w (node %d, pid %d)", ErrAlreadyRunning, id, pid)
	}
	if err := os.MkdirAll(s.dataDir(id), 0o755); err != nil {
		return 0, err
	}
	logf, err := os.OpenFile(s.LogFile(id), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return 0, err
	}
	cmd := exec.Command(s.bin, "-id", strconv.FormatUint(uint64(id), 10), "-config", s.config)
	cmd.Stdout, cmd.Stderr = logf, logf
	cmd.SysProcAttr = detached()
	if err := cmd.Start(); err != nil {
		_ = logf.Close()
		return 0, fmt.Errorf("supervisor: start node %d: %w", id, err)
	}
	pid := cmd.Process.Pid
	if err := os.WriteFile(s.PIDFile(id), []byte(strconv.Itoa(pid)+"\n"), 0o644); err != nil {
		_ = cmd.Process.Kill()
		_ = logf.Close()
		return 0, err
	}
	// Reap the child when it exits, so a killed child does not linger as a
	// zombie that the operating system still reports as existing.
	done := make(chan struct{})
	s.exited[id] = done
	go func() {
		_ = cmd.Wait()
		_ = logf.Close()
		close(done)
	}()
	return pid, nil
}

// Kill terminates a node abruptly -- TerminateProcess on Windows, SIGKILL
// elsewhere, with no chance for the node to run any shutdown code -- and does
// not return success until the operating system reports the process gone.
func (s *Supervisor) Kill(id raft.NodeID) error {
	pid, ok := s.PID(id)
	if !ok {
		_ = os.Remove(s.PIDFile(id))
		return fmt.Errorf("%w (node %d)", ErrNotRunning, id)
	}
	p, err := os.FindProcess(pid)
	if err != nil {
		return err
	}
	if err := p.Kill(); err != nil && processAlive(pid) {
		return fmt.Errorf("supervisor: kill node %d (pid %d): %w", id, pid, err)
	}
	if done, ok := s.exited[id]; ok {
		select {
		case <-done:
		case <-time.After(10 * time.Second):
		}
		delete(s.exited, id)
	}
	deadline := time.Now().Add(10 * time.Second)
	for processAlive(pid) {
		if time.Now().After(deadline) {
			return fmt.Errorf("supervisor: node %d (pid %d) is still alive after being killed", id, pid)
		}
		time.Sleep(10 * time.Millisecond)
	}
	return os.Remove(s.PIDFile(id))
}

// ProcessAlive reports whether the operating system has a live process with
// this id. It is how a test proves a kill really happened.
func ProcessAlive(pid int) bool { return processAlive(pid) }

// Admin returns an admin client for a node. The caller closes the connection.
func (s *Supervisor) Admin(id raft.NodeID) (adminv1.AdminClient, *grpc.ClientConn, error) {
	conn, err := grpc.NewClient(s.Addr(id), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, err
	}
	return adminv1.NewAdminClient(conn), conn, nil
}

// WaitReady waits until a node answers its admin API.
func (s *Supervisor) WaitReady(ctx context.Context, id raft.NodeID) error {
	a, conn, err := s.Admin(id)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	for {
		actx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		_, err := a.GetStatus(actx, &adminv1.GetStatusRequest{})
		cancel()
		if err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("supervisor: node %d never became ready: %w", id, errors.Join(ctx.Err(), err))
		case <-time.After(50 * time.Millisecond):
		}
	}
}
