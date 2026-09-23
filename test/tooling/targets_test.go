package tooling

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// repoRoot is relative to this test's working directory, which the go tool
// sets to the package directory.
const repoRoot = "../.."

var (
	// A Makefile target that documents itself: `name: deps ## description`.
	// Recipe lines are indented with a tab, so anchoring at column zero keeps
	// the help-target's own grep expression from matching itself.
	makeTargetRE = regexp.MustCompile(`(?m)^([a-z][a-z0-9-]*):[^#\n]*## +(.+?)\s*$`)

	// An entry in make.ps1's $Targets table: `'name' = 'description'`.
	psTargetRE = regexp.MustCompile(`(?m)^\s*'([a-z0-9-]+)'\s*=\s*(.+?)\s*$`)

	// A dispatch arm in make.ps1's switch statement: `'name' { ... }`.
	psSwitchRE = regexp.MustCompile(`(?m)^\s*'([a-z0-9-]+)'\s*\{`)

	inlineCodeRE = regexp.MustCompile("`([^`]+)`")

	// A build command at the start of a line in a code block. Anchoring
	// matters: run loose over prose, this would read "make sure" as an
	// invocation of a target named sure.
	buildCmdRE = regexp.MustCompile(`(?m)^\s*(?:\./|\.\\)?make(?:\.ps1)?\s+([a-z][a-z0-9-]*)`)
)

func readRepoFile(t *testing.T, name string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(repoRoot, name))
	require.NoErrorf(t, err, "reading %s", name)
	return string(b)
}

// section returns the text between the line containing start and the next line
// that is exactly end, which is how the $Targets table and the switch body are
// isolated from the rest of the script.
func section(t *testing.T, src, start, end string) string {
	t.Helper()
	i := strings.Index(src, start)
	require.GreaterOrEqualf(t, i, 0, "could not find %q; make.ps1 was restructured "+
		"and this test needs updating", start)
	rest := src[i+len(start):]
	j := strings.Index(rest, "\n"+end)
	require.GreaterOrEqualf(t, j, 0, "could not find the end of the %q block", start)
	return rest[:j]
}

func makefileTargets(t *testing.T) map[string]string {
	t.Helper()
	out := map[string]string{}
	for _, m := range makeTargetRE.FindAllStringSubmatch(readRepoFile(t, "Makefile"), -1) {
		out[m[1]] = m[2]
	}
	require.NotEmpty(t, out, "parsed no targets out of the Makefile")
	return out
}

func powershellTargets(t *testing.T) map[string]string {
	t.Helper()
	body := section(t, readRepoFile(t, "make.ps1"), "$Targets = [ordered]@{", "}")
	out := map[string]string{}
	for _, m := range psTargetRE.FindAllStringSubmatch(body, -1) {
		out[m[1]] = strings.Trim(m[2], `'"`)
	}
	require.NotEmpty(t, out, "parsed no targets out of make.ps1")
	return out
}

// TestTaskRunnersExposeTheSameTargets is the whole reason this package exists.
//
// The Makefile is what a reviewer on Linux or macOS will reach for; make.ps1 is
// what actually runs on the Windows machine this is developed on. If `make ci`
// and `.\make.ps1 ci` stop meaning the same thing, one of them is lying, and
// the one that is lying will be the one nobody ran.
func TestTaskRunnersExposeTheSameTargets(t *testing.T) {
	mk, ps := makefileTargets(t), powershellTargets(t)

	require.ElementsMatch(t, keys(mk), keys(ps),
		"Makefile and make.ps1 expose different targets; add the missing one to both")

	for name, mkDesc := range mk {
		require.Equalf(t, mkDesc, ps[name],
			"target %q is described differently in the Makefile and make.ps1", name)
	}
}

// TestPowerShellTargetsAreAllDispatched catches a target that is advertised in
// the help table but has no switch arm, which would fail at runtime with a
// silent no-op and an exit code of zero.
func TestPowerShellTargetsAreAllDispatched(t *testing.T) {
	src := readRepoFile(t, "make.ps1")
	body := section(t, src, "switch ($Target) {", "}")

	dispatched := map[string]bool{}
	for _, m := range psSwitchRE.FindAllStringSubmatch(body, -1) {
		dispatched[m[1]] = true
	}

	for _, name := range keys(powershellTargets(t)) {
		require.Truef(t, dispatched[name],
			"make.ps1 lists target %q but the switch statement has no arm for it, "+
				"so running it would silently do nothing and exit 0", name)
	}
}

// codeSnippets returns the fenced code blocks and inline code spans of a
// markdown document. Only these are scanned for build commands, so ordinary
// prose containing the word "make" is not mistaken for one.
func codeSnippets(md string) []string {
	var (
		out    []string
		fenced strings.Builder
		inside bool
	)
	for _, line := range strings.Split(md, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "```") {
			if inside {
				out = append(out, fenced.String())
				fenced.Reset()
			}
			inside = !inside
			continue
		}
		if inside {
			fenced.WriteString(line)
			fenced.WriteString("\n")
			continue
		}
		for _, m := range inlineCodeRE.FindAllStringSubmatch(line, -1) {
			out = append(out, m[1])
		}
	}
	return out
}

// TestDocumentedCommandsExist keeps the documentation honest about the entry
// points it tells a reader to run. A quickstart that references a target nobody
// kept is the first thing a reviewer tries and the first thing that fails.
func TestDocumentedCommandsExist(t *testing.T) {
	targets := makefileTargets(t)

	var found int
	for _, doc := range []string{"README.md", "DESIGN.md", "PLAN.md", "PROGRESS.md", "CLAUDE.md"} {
		for _, snippet := range codeSnippets(readRepoFile(t, doc)) {
			for _, m := range buildCmdRE.FindAllStringSubmatch(snippet, -1) {
				found++
				require.Containsf(t, targets, m[1],
					"%s documents `make %s`, which is not a target in the Makefile", doc, m[1])
			}
		}
	}
	require.NotZero(t, found, "the docs reference no build commands at all")
}

func keys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
