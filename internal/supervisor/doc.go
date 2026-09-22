// Package supervisor spawns, monitors, kills and restarts node processes.
//
// It is shared by quorumctl and quorum-viz so that a "kill node" button in the
// browser and a kill in the test suite take exactly the same path.
//
// Kills are real. os.Process.Kill maps to TerminateProcess on Windows and
// SIGKILL on Unix, so there is no graceful shutdown, no flush, and no chance
// for the node to tidy up. That is the point: recovery has to be exercised
// against a genuinely abrupt failure, not a polite one.
//
// Restart respawns against the same data directory, so recovery reads the real
// write-ahead log rather than a fixture.
//
// Phase 6 fills this package in. It is currently a documented stub.
package supervisor
