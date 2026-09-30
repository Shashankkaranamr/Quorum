package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	adminv1 "github.com/Shashankkaranamr/Quorum/gen/quorum/admin/v1"
	"github.com/Shashankkaranamr/Quorum/internal/supervisor"
	"github.com/Shashankkaranamr/Quorum/raft"
)

// defaultNodeBin is quorum-node beside this quorumctl, which is where `make
// build` puts both.
func defaultNodeBin() string {
	name := "quorum-node"
	if runtime.GOOS == "windows" {
		name += ".exe"
	}
	if self, err := os.Executable(); err == nil {
		return filepath.Join(filepath.Dir(self), name)
	}
	return name
}

// operate runs the cluster-operation subcommands. They all go through the
// supervisor (processes) and the admin API (everything inside a process), the
// same two paths the fault-injection suite uses.
func operate(name string, args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(stderr)
	configPath := fs.String("config", "cluster.yaml", "path to the cluster topology file")
	bin := fs.String("bin", defaultNodeBin(), "the quorum-node binary to run")
	freezeFor := fs.Duration("for", 0, "freeze: thaw automatically after this long (0 = until thaw)")
	oneway := fs.Bool("oneway", false, "partition: cut only the links from the first group to the second")
	if err := fs.Parse(args); err != nil {
		return err
	}
	sup, err := supervisor.New(*bin, *configPath)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	switch name {
	case "up":
		return up(ctx, sup, stdout)
	case "down":
		for _, id := range sup.IDs() {
			switch err := sup.Kill(id); {
			case err == nil:
				fmt.Fprintf(stdout, "node %d killed\n", id)
			case errors.Is(err, supervisor.ErrNotRunning):
				fmt.Fprintf(stdout, "node %d was not running\n", id)
			default:
				return err
			}
		}
		return nil
	case "status":
		return status(ctx, sup, stdout)
	case "heal":
		for _, id := range sup.IDs() {
			if _, ok := sup.PID(id); !ok {
				continue
			}
			err := withAdmin(sup, id, func(a adminv1.AdminClient) error {
				_, err := a.Heal(ctx, &adminv1.HealRequest{})
				return err
			})
			if err != nil {
				return fmt.Errorf("heal node %d: %w", id, err)
			}
		}
		fmt.Fprintf(stdout, "healed\n")
		return nil
	case "partition":
		a, b, err := parseGroups(fs.Args(), sup.IDs())
		if err != nil {
			return err
		}
		if err := partition(ctx, sup, a, b, *oneway); err != nil {
			return err
		}
		arrow := "<->"
		if *oneway {
			arrow = "->"
		}
		fmt.Fprintf(stdout, "cut %v %s %v\n", a, arrow, b)
		return nil
	}

	// The rest take one node id.
	if fs.NArg() != 1 {
		return fmt.Errorf("%s needs a node id", name)
	}
	id, err := parseID(fs.Arg(0), sup.IDs())
	if err != nil {
		return err
	}
	switch name {
	case "kill":
		pid, _ := sup.PID(id)
		if err := sup.Kill(id); err != nil {
			return err
		}
		fmt.Fprintf(stdout, "node %d (pid %d) killed; the process is gone\n", id, pid)
		return nil
	case "start":
		pid, err := sup.Start(id)
		if err != nil {
			return err
		}
		if err := sup.WaitReady(ctx, id); err != nil {
			return err
		}
		fmt.Fprintf(stdout, "node %d started (pid %d)\n", id, pid)
		return nil
	case "freeze":
		err := withAdmin(sup, id, func(a adminv1.AdminClient) error {
			_, err := a.Freeze(ctx, &adminv1.FreezeRequest{AutoThawAfterMs: uint32(freezeFor.Milliseconds())})
			return err
		})
		if err != nil {
			return err
		}
		fmt.Fprintf(stdout, "node %d frozen (cooperatively: its loop is parked, the process is alive)\n", id)
		return nil
	default: // thaw
		return withAdmin(sup, id, func(a adminv1.AdminClient) error {
			r, err := a.Thaw(ctx, &adminv1.ThawRequest{})
			if err == nil {
				fmt.Fprintf(stdout, "node %d thawed after %dms, %d ticks missed\n", id, r.GetFrozenForMs(), r.GetTicksMissed())
			}
			return err
		})
	}
}

func withAdmin(sup *supervisor.Supervisor, id raft.NodeID, fn func(adminv1.AdminClient) error) error {
	a, conn, err := sup.Admin(id)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	return fn(a)
}

func up(ctx context.Context, sup *supervisor.Supervisor, stdout io.Writer) error {
	for _, id := range sup.IDs() {
		pid, err := sup.Start(id)
		switch {
		case errors.Is(err, supervisor.ErrAlreadyRunning):
			fmt.Fprintf(stdout, "node %d already running (pid %d)\n", id, pid)
		case err != nil:
			return err
		default:
			fmt.Fprintf(stdout, "node %d started (pid %d), log %s\n", id, pid, sup.LogFile(id))
		}
	}
	for _, id := range sup.IDs() {
		if err := sup.WaitReady(ctx, id); err != nil {
			return err
		}
	}
	fmt.Fprintln(stdout)
	return status(ctx, sup, stdout)
}

func status(ctx context.Context, sup *supervisor.Supervisor, stdout io.Writer) error {
	tw := tabwriter.NewWriter(stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintf(tw, "NODE\tPID\tROLE\tTERM\tLEADER\tCOMMIT\tAPPLIED\tLAST\tSNAP\tFROZEN\tBLOCKED\n")
	for _, id := range sup.IDs() {
		pid, alive := sup.PID(id)
		if !alive {
			fmt.Fprintf(tw, "%d\t-\tdown\n", id)
			continue
		}
		var st *adminv1.NodeStatus
		err := withAdmin(sup, id, func(a adminv1.AdminClient) error {
			actx, cancel := context.WithTimeout(ctx, time.Second)
			defer cancel()
			var err error
			st, err = a.GetStatus(actx, &adminv1.GetStatusRequest{})
			return err
		})
		if err != nil {
			fmt.Fprintf(tw, "%d\t%d\tunreachable\n", id, pid)
			continue
		}
		var blocked []string
		for _, p := range st.GetPeers() {
			if p.GetBlockedByInjection() {
				blocked = append(blocked, strconv.FormatUint(p.GetNodeId(), 10))
			}
		}
		role := strings.ToLower(strings.TrimPrefix(st.GetRole().String(), "ROLE_"))
		fmt.Fprintf(tw, "%d\t%d\t%s\t%d\t%d\t%d\t%d\t%d\t%d\t%v\t%s\n", id, pid, role, st.GetTerm(),
			st.GetLeaderId(), st.GetCommitIndex(), st.GetLastApplied(), st.GetLastLogIndex(),
			st.GetSnapshotIndex(), st.GetFrozen(), strings.Join(blocked, ","))
	}
	return tw.Flush()
}

// partition cuts links between groups a and b. Both ends of every link are
// told, so a partition holds even if one side has already opened a stream:
// the sender stops sending and the receiver refuses what arrives. With oneway,
// only a -> b is cut.
func partition(ctx context.Context, sup *supervisor.Supervisor, a, b []raft.NodeID, oneway bool) error {
	block := func(on raft.NodeID, peers []raft.NodeID, req *adminv1.BlockLinksRequest) error {
		for _, p := range peers {
			req.PeerIds = append(req.PeerIds, uint64(p))
		}
		return withAdmin(sup, on, func(adm adminv1.AdminClient) error {
			_, err := adm.BlockLinks(ctx, req)
			return err
		})
	}
	for _, id := range a {
		if err := block(id, b, &adminv1.BlockLinksRequest{OutboundOnly: oneway}); err != nil {
			return fmt.Errorf("node %d: %w", id, err)
		}
	}
	for _, id := range b {
		if err := block(id, a, &adminv1.BlockLinksRequest{InboundOnly: oneway}); err != nil {
			return fmt.Errorf("node %d: %w", id, err)
		}
	}
	return nil
}

// parseGroups reads "1,2 | 3" in whatever pieces the shell delivered it:
// "1,2", "|", "3", or "1,2|3", or just two arguments.
func parseGroups(args []string, ids []raft.NodeID) (a, b []raft.NodeID, err error) {
	joined := strings.Join(args, " ")
	var parts []string
	if strings.Contains(joined, "|") {
		parts = strings.Split(joined, "|")
	} else {
		parts = args
	}
	if len(parts) != 2 {
		return nil, nil, fmt.Errorf("partition needs two groups, like: partition 1,2 '|' 3")
	}
	parse := func(s string) ([]raft.NodeID, error) {
		var out []raft.NodeID
		for _, f := range strings.FieldsFunc(s, func(r rune) bool { return r == ',' || r == ' ' }) {
			id, err := parseID(f, ids)
			if err != nil {
				return nil, err
			}
			out = append(out, id)
		}
		if len(out) == 0 {
			return nil, fmt.Errorf("partition: empty group in %q", joined)
		}
		return out, nil
	}
	if a, err = parse(parts[0]); err != nil {
		return nil, nil, err
	}
	if b, err = parse(parts[1]); err != nil {
		return nil, nil, err
	}
	for _, x := range a {
		for _, y := range b {
			if x == y {
				return nil, nil, fmt.Errorf("partition: node %d is in both groups", x)
			}
		}
	}
	return a, b, nil
}

func parseID(s string, ids []raft.NodeID) (raft.NodeID, error) {
	n, err := strconv.ParseUint(strings.TrimSpace(s), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%q is not a node id", s)
	}
	for _, id := range ids {
		if uint64(id) == n {
			return id, nil
		}
	}
	return 0, fmt.Errorf("node %d is not in the config (nodes: %v)", n, ids)
}
