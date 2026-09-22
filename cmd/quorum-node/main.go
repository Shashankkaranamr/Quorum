// Command quorum-node runs a single Raft replica.
//
// Three to five of these run as independent OS processes, each with its own
// data directory and its own ports, and together they form the cluster. They
// are started, killed and restarted by quorumctl or by the visualizer.
//
// Phase 1 status: this binary loads and validates its configuration, reports
// what it would run as, and exits. The consensus loop arrives in phase 2 and
// the gRPC listeners in phase 5. It exists now so that `make run` is a real
// command rather than a placeholder, and so the config contract is exercised
// end to end from the first commit.
package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"text/tabwriter"

	"github.com/Shashankkaranamr/Quorum/internal/config"
)

// version is overridden at build time via -ldflags.
var version = "dev"

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintf(os.Stderr, "quorum-node: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("quorum-node", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var (
		configPath  = fs.String("config", "cluster.yaml", "path to the cluster topology file")
		id          = fs.Uint64("id", 0, "this node's id, as listed in the config file")
		showVersion = fs.Bool("version", false, "print version and exit")
	)
	fs.Usage = func() {
		fmt.Fprintf(stderr, "usage: quorum-node -id <n> [-config cluster.yaml]\n\n")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return err
	}

	if *showVersion {
		fmt.Fprintf(stdout, "quorum-node %s\n", version)
		return nil
	}
	if *id == 0 {
		fs.Usage()
		return fmt.Errorf("-id is required and must match a node in %s", *configPath)
	}

	cluster, err := config.Load(*configPath)
	if err != nil {
		return err
	}

	self, ok := cluster.Node(config.NodeID(*id))
	if !ok {
		return fmt.Errorf("node id %d is not listed in %s (configured ids: %v)",
			*id, *configPath, cluster.IDs())
	}

	for _, w := range cluster.Warnings() {
		fmt.Fprintf(stderr, "quorum-node: warning: %s\n", w)
	}

	describe(stdout, cluster, self, *configPath)
	return nil
}

func describe(w io.Writer, c *config.Cluster, self config.Node, configPath string) {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintf(tw, "quorum-node %s\n\n", version)
	fmt.Fprintf(tw, "cluster\t%s (%s)\n", c.ClusterID, configPath)
	fmt.Fprintf(tw, "node id\t%d\n", self.ID)
	fmt.Fprintf(tw, "grpc\t%s\t(raft peers, kv api, admin api)\n", self.GRPCAddr())
	fmt.Fprintf(tw, "http\t%s\t(status, metrics)\n", self.HTTPAddr())
	fmt.Fprintf(tw, "data dir\t%s\n", c.DataDir(self.ID))
	fmt.Fprintf(tw, "size\t%d nodes, quorum %d, tolerates %d failure(s)\n",
		len(c.Nodes), c.Quorum(), len(c.Nodes)-c.Quorum())

	fmt.Fprintf(tw, "\npeers\n")
	for _, p := range c.Peers(self.ID) {
		fmt.Fprintf(tw, "  node %d\t%s\n", p.ID, p.GRPCAddr())
	}

	r := c.Raft
	fmt.Fprintf(tw, "\ntiming\n")
	fmt.Fprintf(tw, "  tick\t%dms\n", r.TickMS)
	fmt.Fprintf(tw, "  election timeout\t%d-%d ticks\t(%d-%dms, randomized per election)\n",
		r.ElectionTimeoutMinTicks, r.ElectionTimeoutMaxTicks,
		r.ElectionTimeoutMinTicks*r.TickMS, r.ElectionTimeoutMaxTicks*r.TickMS)
	fmt.Fprintf(tw, "  heartbeat\t%d ticks\t(%dms)\n",
		r.HeartbeatTimeoutTicks, r.HeartbeatTimeoutTicks*r.TickMS)
	fmt.Fprintf(tw, "  snapshot after\t%d applied entries\n", r.SnapshotThresholdEntries)

	fmt.Fprintf(tw, "\nphase 1 of 8: configuration validated, consensus not implemented yet.\n")
	fmt.Fprintf(tw, "See PLAN.md for what each phase delivers.\n")
	_ = tw.Flush()
}
