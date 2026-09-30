//go:build race

package integration

// raceEnabled is true when the tests themselves run under the race detector,
// in which case the node binaries are built with it too (see build).
const raceEnabled = true
