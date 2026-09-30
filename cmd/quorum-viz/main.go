// Command quorum-viz serves the live cluster visualizer.
//
// It starts any node in the config that is not already running, watches every
// node's admin status stream, and serves a page that shows the cluster live
// and drives it: kill and restart processes, cut links (both ways or one
// way), freeze and thaw, write. The buttons go through the same supervisor
// and admin calls as quorumctl and the fault-injection suite, so the demo
// drives real failures rather than animating a script.
//
// On exit it leaves the nodes running; `quorumctl down` stops them.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"syscall"
	"time"

	"github.com/Shashankkaranamr/Quorum/internal/supervisor"
	"github.com/Shashankkaranamr/Quorum/internal/viz"
	"github.com/Shashankkaranamr/Quorum/web"
)

var version = "dev"

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintf(os.Stderr, "quorum-viz: %v\n", err)
		os.Exit(1)
	}
}

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

func run(args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("quorum-viz", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var (
		configPath  = fs.String("config", "cluster.yaml", "path to the cluster topology file")
		bin         = fs.String("bin", defaultNodeBin(), "the quorum-node binary to run")
		listen      = fs.String("listen", "127.0.0.1:8080", "address to serve the visualizer on")
		noStart     = fs.Bool("no-start", false, "do not start nodes that are not running")
		showVersion = fs.Bool("version", false, "print version and exit")
	)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *showVersion {
		fmt.Fprintf(stdout, "quorum-viz %s\n", version)
		return nil
	}

	sup, err := supervisor.New(*bin, *configPath)
	if err != nil {
		return err
	}
	logger := log.New(stderr, "quorum-viz: ", log.LstdFlags)
	if !*noStart {
		for _, id := range sup.IDs() {
			switch pid, err := sup.Start(id); {
			case errors.Is(err, supervisor.ErrAlreadyRunning):
				logger.Printf("node %d already running (pid %d)", id, pid)
			case err != nil:
				return err
			default:
				logger.Printf("node %d started (pid %d)", id, pid)
			}
		}
	}

	server := viz.New(sup, web.Assets, logger.Printf)
	defer server.Close()
	lis, err := net.Listen("tcp", *listen)
	if err != nil {
		return err
	}
	httpServer := &http.Server{Handler: server.Handler(), ReadHeaderTimeout: 5 * time.Second}
	fmt.Fprintf(stdout, "quorum-viz serving %d nodes at http://%s\n", len(sup.IDs()), lis.Addr())

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sig)
	served := make(chan error, 1)
	go func() { served <- httpServer.Serve(lis) }()
	select {
	case err := <-served:
		return err
	case <-sig:
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = httpServer.Shutdown(ctx)
	fmt.Fprintln(stdout, "quorum-viz stopped; the nodes are still running (quorumctl down stops them)")
	return nil
}
