// Command quorum-viz serves the live cluster visualizer.
//
// It supervises the node processes, polls each node's admin API for role, term,
// commit index and log tail, and streams that to a browser over SSE. The
// "kill node" and "partition network" controls in the UI go through the same
// supervisor and admin calls the fault-injection suite uses, so the demo is
// driving real failures rather than animating a script.
//
// Phase 1 status: not implemented. Phase 7 builds it. This file exists so the
// binary is present in the build graph from the start and so `go build ./...`
// covers every entry point the project will ship.
package main

import (
	"fmt"
	"os"
)

var version = "dev"

func main() {
	if len(os.Args) > 1 && (os.Args[1] == "-version" || os.Args[1] == "--version") {
		fmt.Printf("quorum-viz %s\n", version)
		return
	}
	fmt.Fprintf(os.Stderr,
		"quorum-viz %s: the visualizer is built in phase 7. See PLAN.md.\n", version)
	os.Exit(2)
}
