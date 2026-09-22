// Package impure is a fixture, not real code.
//
// It exists so that TestImportAllowlistCheckerDetectsViolations can confirm the
// import allowlist checker actually reports violations. Without a negative
// control, a checker that returns nothing would make TestRaftCoreImportAllowlist
// pass for the wrong reason, and the purity claim would rest on nothing.
//
// It lives under testdata/, which the go tool ignores, so it is never built or
// linked into anything.
package impure

import (
	"net"
	"os"
	"time"
)

var (
	_ = net.Dial
	_ = os.Open
	_ = time.Now
)
