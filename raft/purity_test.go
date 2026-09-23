package raft_test

import (
	"go/ast"
	"go/build"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// allowedImports is the complete set of packages the consensus core may import.
//
// This is an allowlist rather than a denylist on purpose. A denylist of "no os,
// no net, no time" quietly passes the day someone reaches for bufio or log; an
// allowlist forces every new dependency to be a deliberate, reviewed decision.
//
// Every entry carries the reason it is allowed, so that removing one later is a
// decision someone can evaluate rather than guess at.
//
// Conspicuously absent, and the whole point of the exercise:
//
//	time, sync, sync/atomic   the core has no clock and no concurrency; logical
//	                          time advances only via Tick, and a single driver
//	                          goroutine owns the Node exclusively
//	os, io, bufio, net        no I/O of any kind
//	context                   the core never blocks, so it has nothing to cancel
//	math/rand                 election jitter is injected through Config, so
//	                          every test run is reproducible from its seed
//	log                       the core returns information; it does not emit it
//	google.golang.org/...     protobuf and gRPC live behind internal/pbconv
var allowedImports = map[string]string{
	"errors":  "sentinel errors and errors.Is/As",
	"fmt":     "error formatting only; TestRaftCoreDoesNotWriteToStdout enforces that",
	"sort":    "ordering matchIndex to compute the commit index",
	"slices":  "ordering matchIndex to compute the commit index",
	"cmp":     "comparators for slices",
	"math":    "integer bounds such as math.MaxUint64",
	"strconv": "String methods on Role, Term and similar",
	"strings": "String methods and strings.Builder",
	"bytes":   "comparing entry payloads",
}

// disallowedImports returns the direct imports of the package in dir that are
// not on the allowlist.
//
// Direct imports, not transitive ones: fmt itself imports os, so a transitive
// check would reject fmt.Errorf and be useless. The boundary that actually
// matters is what this package reaches for by name, and the companion
// stdout test closes the one gap that leaves.
//
// The allowlist is stdlib-only, so an import of any other package in this
// module is a violation by construction. If the core is ever split into
// subpackages, add them here and teach this helper to recurse into them.
func disallowedImports(t *testing.T, dir string) []string {
	t.Helper()

	pkg, err := build.ImportDir(dir, 0)
	require.NoErrorf(t, err, "scanning %s", dir)

	var bad []string
	for _, imp := range pkg.Imports {
		if _, ok := allowedImports[imp]; !ok {
			bad = append(bad, imp)
		}
	}
	sort.Strings(bad)
	return bad
}

// TestRaftCoreImportAllowlist is the mechanical form of the claim in doc.go:
// the consensus core performs no I/O.
//
// It is cheap and it is load-bearing. The deterministic simulator and the real
// multi-process deployment are only the same code path because the core cannot
// reach the clock, the disk or the network on its own, and this is what stops
// that from quietly stopping being true.
func TestRaftCoreImportAllowlist(t *testing.T) {
	// Guard against the check passing because there is nothing to check. A
	// stub package imports nothing and would satisfy any allowlist, so the
	// test confirms it is scanning the real algorithm before trusting the
	// result.
	pkg, err := build.ImportDir(".", 0)
	require.NoError(t, err)
	for _, want := range []string{"node.go", "log.go", "election.go", "replication.go", "config.go", "types.go"} {
		require.Containsf(t, pkg.GoFiles, want,
			"purity_test is not scanning %s; the allowlist would pass vacuously", want)
	}

	bad := disallowedImports(t, ".")
	require.Emptyf(t, bad, ""+
		"package raft imports %v, which is not on the allowlist in purity_test.go.\n"+
		"The consensus core must stay free of I/O, clocks and concurrency: that is what "+
		"lets the deterministic simulator exercise the same code the demo runs.\n"+
		"If the dependency is genuinely pure, add it to allowedImports with a reason.",
		bad)
}

// TestImportAllowlistCheckerDetectsViolations is a negative control.
//
// A checker that has never been observed to fail is not evidence of anything.
// If TestRaftCoreImportAllowlist passes because disallowedImports silently
// returns nothing, this test fails and says so.
func TestImportAllowlistCheckerDetectsViolations(t *testing.T) {
	fixture := filepath.Join("testdata", "impure")
	bad := disallowedImports(t, fixture)
	require.NotEmpty(t, bad,
		"the allowlist checker reported no violations for %s, which imports net, os and time. "+
			"The checker is broken, so TestRaftCoreImportAllowlist proves nothing.", fixture)
	require.Subset(t, bad, []string{"net", "os", "time"})
}

// TestRaftCoreHasNoConcurrencyPrimitives is a source-level companion to the
// import allowlist.
//
// The allowlist already excludes sync, but a goroutine or a channel needs no
// import at all -- `go f()` and `chan T` are language constructs. The core is
// single-owner by contract, and a stray goroutine would silently break that
// without tripping any import check.
func TestRaftCoreHasNoConcurrencyPrimitives(t *testing.T) {
	pkg, err := build.ImportDir(".", 0)
	require.NoError(t, err)

	fset := token.NewFileSet()
	for _, name := range pkg.GoFiles {
		file, err := parser.ParseFile(fset, name, nil, 0)
		require.NoErrorf(t, err, "parsing %s", name)

		ast.Inspect(file, func(n ast.Node) bool {
			switch node := n.(type) {
			case *ast.GoStmt:
				t.Errorf("%s: `go` statement in the consensus core; a single owner "+
					"serializes every call and that ownership is what replaces locking",
					fset.Position(node.Pos()))
			case *ast.SelectStmt:
				t.Errorf("%s: `select` in the consensus core; blocking belongs in the driver",
					fset.Position(node.Pos()))
			case *ast.ChanType:
				t.Errorf("%s: channel type in the consensus core", fset.Position(node.Pos()))
			}
			return true
		})
	}
}

// TestRaftCoreDoesNotWriteToStdout closes the gap left by allowing fmt.
//
// fmt is on the allowlist for Errorf and Sprintf. fmt.Println is I/O, and a
// consensus core that logs to stdout is both impure and, in a cluster of five
// processes sharing a terminal, useless. The core returns information; the
// driver decides what to do with it.
func TestRaftCoreDoesNotWriteToStdout(t *testing.T) {
	pkg, err := build.ImportDir(".", 0)
	require.NoError(t, err)

	fset := token.NewFileSet()
	for _, name := range pkg.GoFiles {
		file, err := parser.ParseFile(fset, name, nil, 0)
		require.NoErrorf(t, err, "parsing %s", name)

		ast.Inspect(file, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			if fn, ok := call.Fun.(*ast.Ident); ok && (fn.Name == "print" || fn.Name == "println") {
				t.Errorf("%s: builtin %s() in the consensus core", fset.Position(fn.Pos()), fn.Name)
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			x, ok := sel.X.(*ast.Ident)
			if !ok || x.Name != "fmt" {
				return true
			}
			if strings.HasPrefix(sel.Sel.Name, "Print") || strings.HasPrefix(sel.Sel.Name, "Fprint") {
				t.Errorf("%s: fmt.%s in the consensus core; return the information instead",
					fset.Position(sel.Pos()), sel.Sel.Name)
			}
			return true
		})
	}
}

// TestRaftCoreIsTheOnlyExportedPackage guards a structural choice rather than a
// behavioural one: raft/ is the repository's only non-internal package, so that
// "the core has no I/O dependencies" is verifiable by reading one import list.
// If a second exported package appears, that shortcut stops working and this
// test asks for the decision to be made consciously.
func TestRaftCoreIsTheOnlyExportedPackage(t *testing.T) {
	var exported []string
	root := filepath.Join("..")
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			return nil
		}
		rel, relErr := filepath.Rel(root, path)
		if relErr != nil {
			return relErr
		}
		rel = filepath.ToSlash(rel)
		switch {
		case rel == ".":
			return nil
		case strings.HasPrefix(rel, "."), // .git and friends
			rel == "internal" || strings.HasPrefix(rel, "internal/"),
			rel == "cmd" || strings.HasPrefix(rel, "cmd/"),
			rel == "test" || strings.HasPrefix(rel, "test/"),
			rel == "gen" || strings.HasPrefix(rel, "gen/"),
			strings.Contains(rel, "testdata"),
			rel == "proto" || strings.HasPrefix(rel, "proto/"),
			rel == "web" || rel == "docs" || rel == "data" || rel == "bin":
			return filepath.SkipDir
		}
		if p, perr := build.ImportDir(path, 0); perr == nil && len(p.GoFiles) > 0 {
			exported = append(exported, rel)
		}
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, []string{"raft"}, exported,
		"raft must remain the only exported package; move new packages under internal/ "+
			"or update this test deliberately")
}
