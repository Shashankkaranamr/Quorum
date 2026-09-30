// Command quorumctl starts, stops, inspects and deliberately breaks a local
// Quorum cluster.
//
// It is the single operator surface for the project. The fault-injection test
// suite and the visualizer's buttons both drive the same code underneath, so
// what the demo shows cannot drift away from what the tests exercise.
//
// `plan`, `version`, `put` and `get` work. Every other subcommand is
// recognized, documented, and exits non-zero saying which phase implements it.
// Failing loudly matters here: a stub that exits 0 would let a broken
// end-to-end script look green.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/Shashankkaranamr/Quorum/internal/client"
	"github.com/Shashankkaranamr/Quorum/internal/config"
	"github.com/Shashankkaranamr/Quorum/raft"
)

var version = "dev"

// command describes one subcommand and, when it is not yet implemented, the
// phase that will implement it. Keeping the roadmap in the help output means
// the tool never pretends to do more than it does.
type command struct {
	name    string
	args    string
	summary string
	phase   int // 0 means implemented now
}

var commands = []command{
	{"plan", "[-config F]", "show what `up` would start: ports, data dirs, quorum", 0},
	{"version", "", "print version and exit", 0},
	{"up", "[-config F] [-bin B]", "start every node in the config as a separate process", 0},
	{"down", "[-config F]", "kill every running node (there is no graceful stop on Windows)", 0},
	{"status", "[-config F]", "show each node's process, role, term, commit and applied index", 0},
	{"kill", "[-config F] <id>", "SIGKILL/TerminateProcess a node, with no graceful shutdown", 0},
	{"start", "[-config F] [-bin B] <id>", "restart a killed node against its existing data directory", 0},
	{"freeze", "[-config F] [-for D] <id>", "park a node's event loop: alive, reachable, doing nothing", 0},
	{"thaw", "[-config F] <id>", "resume a frozen node", 0},
	{"partition", "[-config F] [-oneway] <ids> '|' <ids>", "cut the links between two groups (-oneway: first cannot reach second)", 0},
	{"heal", "[-config F]", "remove every injected partition", 0},
	{"put", "[-config F] <key> <value>", "write through the leader", 0},
	{"get", "[-config F] <key>", "linearizable read via ReadIndex", 0},
}

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintf(os.Stderr, "quorumctl: %v\n", err)
		os.Exit(1)
	}
}

// errNotYet reports a recognized subcommand that a later phase implements.
type errNotYet struct{ cmd command }

func (e errNotYet) Error() string {
	return fmt.Sprintf("`%s` is not implemented yet; it arrives in phase %d. See PLAN.md.",
		e.cmd.name, e.cmd.phase)
}

func run(args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		usage(stdout)
		return nil
	}

	name := args[0]
	if name == "-h" || name == "--help" || name == "help" {
		usage(stdout)
		return nil
	}

	var cmd command
	found := false
	for _, c := range commands {
		if c.name == name {
			cmd, found = c, true
			break
		}
	}
	if !found {
		usage(stderr)
		return fmt.Errorf("unknown command %q", name)
	}

	switch cmd.name {
	case "version":
		fmt.Fprintf(stdout, "quorumctl %s\n", version)
		return nil
	case "plan":
		return plan(args[1:], stdout, stderr)
	case "put", "get":
		return kv(cmd.name, args[1:], stdout, stderr)
	case "up", "down", "status", "kill", "start", "freeze", "thaw", "partition", "heal":
		return operate(cmd.name, args[1:], stdout, stderr)
	default:
		return errNotYet{cmd}
	}
}

func usage(w io.Writer) {
	fmt.Fprintf(w, "quorumctl %s -- control a local Quorum cluster\n\n", version)
	fmt.Fprintf(w, "usage: quorumctl <command> [arguments]\n\n")

	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	byPhase := map[int][]command{}
	for _, c := range commands {
		byPhase[c.phase] = append(byPhase[c.phase], c)
	}
	phases := make([]int, 0, len(byPhase))
	for p := range byPhase {
		phases = append(phases, p)
	}
	sort.Ints(phases)

	for _, p := range phases {
		if p == 0 {
			fmt.Fprintf(tw, "available now\n")
		} else {
			fmt.Fprintf(tw, "\nphase %d\n", p)
		}
		for _, c := range byPhase[p] {
			fmt.Fprintf(tw, "  %s %s\t%s\n", c.name, c.args, c.summary)
		}
	}
	_ = tw.Flush()
	fmt.Fprintf(w, "\nThis is phase 6 of 8. PLAN.md defines what each phase delivers.\n")
}

// errNotFound is get's answer for a key that does not exist. It exits non-zero
// so a script can tell "absent" from "empty value".
var errNotFound = errors.New("key not found")

// kv runs put or get against the cluster in the config file. Each invocation is
// a new client session: registering one is a log entry of its own, which is a
// fine price for a command-line tool and not what a long-lived client does.
func kv(name string, args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(stderr)
	configPath := fs.String("config", "cluster.yaml", "path to the cluster topology file")
	timeout := fs.Duration("timeout", 10*time.Second, "give up after this long")
	if err := fs.Parse(args); err != nil {
		return err
	}
	want := 2
	if name == "get" {
		want = 1
	}
	if fs.NArg() != want {
		return fmt.Errorf("%s needs %d argument(s), got %d", name, want, fs.NArg())
	}

	c, err := config.Load(*configPath)
	if err != nil {
		return err
	}
	addrs := map[raft.NodeID]string{}
	for _, n := range c.Nodes {
		addrs[raft.NodeID(n.ID)] = n.GRPCAddr()
	}
	cl, err := client.Dial(client.Config{Addrs: addrs})
	if err != nil {
		return err
	}
	defer func() { _ = cl.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	if err := cl.Register(ctx); err != nil {
		return err
	}

	key := fs.Arg(0)
	if name == "put" {
		res, err := cl.Put(ctx, key, []byte(fs.Arg(1)))
		if err != nil {
			return err
		}
		fmt.Fprintf(stdout, "OK (applied at index %d, via node %d)\n", res.AppliedIndex, cl.Leader())
		return nil
	}
	res, err := cl.Get(ctx, key)
	if err != nil {
		return err
	}
	if !res.Found {
		return fmt.Errorf("%q: %w (read at index %d)", key, errNotFound, res.ReadIndex)
	}
	fmt.Fprintf(stdout, "%s\n", res.Value)
	return nil
}

func plan(args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("plan", flag.ContinueOnError)
	fs.SetOutput(stderr)
	configPath := fs.String("config", "cluster.yaml", "path to the cluster topology file")
	if err := fs.Parse(args); err != nil {
		return err
	}

	c, err := config.Load(*configPath)
	if err != nil {
		return err
	}
	for _, w := range c.Warnings() {
		fmt.Fprintf(stderr, "quorumctl: warning: %s\n", w)
	}

	fmt.Fprintf(stdout, "cluster %q from %s\n", c.ClusterID, *configPath)
	fmt.Fprintf(stdout, "%d nodes, quorum %d, tolerates %d simultaneous failure(s)\n\n",
		len(c.Nodes), c.Quorum(), len(c.Nodes)-c.Quorum())

	tw := tabwriter.NewWriter(stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintf(tw, "NODE\tGRPC\tHTTP\tDATA DIR\tCOMMAND\n")
	for _, n := range c.Nodes {
		fmt.Fprintf(tw, "%d\t%s\t%s\t%s\tquorum-node -id %d -config %s\n",
			n.ID, n.GRPCAddr(), n.HTTPAddr(), c.DataDir(n.ID), n.ID, *configPath)
	}
	_ = tw.Flush()

	fmt.Fprintf(stdout, "\nelection timeout %d-%dms, heartbeat %dms, tick %dms\n",
		c.Raft.ElectionTimeoutMinTicks*c.Raft.TickMS,
		c.Raft.ElectionTimeoutMaxTicks*c.Raft.TickMS,
		c.Raft.HeartbeatTimeoutTicks*c.Raft.TickMS,
		c.Raft.TickMS)
	fmt.Fprintf(stdout, "\n%s\n", strings.TrimSpace(`
this prints the plan but does not start anything; "quorumctl up" does.
`))
	return nil
}
