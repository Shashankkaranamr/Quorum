// Command quorum-node runs a single Raft replica.
//
// Three to five of these run as independent OS processes, each with its own
// data directory and its own ports, and together they form the cluster. They
// are started, killed and restarted by quorumctl or by the visualizer.
//
// It recovers its state from its data directory, then serves the Raft peer
// transport and the client KV API on its gRPC port until it receives SIGINT or
// SIGTERM, when it shuts down cleanly. A SIGKILL or TerminateProcess is the
// crash the write-ahead log exists to survive. -describe prints the node's
// configuration and exits instead.
package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"syscall"
	"text/tabwriter"

	"github.com/Shashankkaranamr/Quorum/internal/config"
	"github.com/Shashankkaranamr/Quorum/internal/node"
	"github.com/Shashankkaranamr/Quorum/raft"
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
		describeCfg = fs.Bool("describe", false, "print this node's configuration and exit")
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

	if *describeCfg {
		describe(stdout, cluster, self, *configPath)
		return nil
	}
	return serve(cluster, self, stdout, stderr)
}

// serve runs the node until a shutdown signal, or until it fails on its own --
// which only a storage failure causes, and which must stop the process rather
// than let it carry on without a durable log.
func serve(c *config.Cluster, self config.Node, stdout, stderr io.Writer) error {
	logger := log.New(stderr, fmt.Sprintf("quorum-node %d: ", self.ID), log.LstdFlags|log.Lmicroseconds)
	n, err := node.Start(node.Options{Cluster: c, ID: raft.NodeID(self.ID), Logf: logger.Printf})
	if err != nil {
		return err
	}
	fmt.Fprintf(stdout, "quorum-node %d serving on %s (data %s)\n", self.ID, n.Addr(), c.DataDir(self.ID))

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sig)
	select {
	case s := <-sig:
		logger.Printf("received %v, shutting down", s)
		return n.Stop()
	case <-n.Done():
		err := n.Stop()
		return fmt.Errorf("the node stopped on its own: %w", err)
	}
}

func describe(w io.Writer, c *config.Cluster, self config.Node, configPath string) {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintf(tw, "quorum-node %s\n\n", version)
	fmt.Fprintf(tw, "cluster\t%s (%s)\n", c.ClusterID, configPath)
	fmt.Fprintf(tw, "node id\t%d\n", self.ID)
	fmt.Fprintf(tw, "grpc\t%s\t(raft peers, kv api, admin api)\n", self.GRPCAddr())
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

	fmt.Fprintf(tw, "\nrun without -describe to start serving.\n")
	_ = tw.Flush()
}
