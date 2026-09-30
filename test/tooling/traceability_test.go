package tooling

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	// A bullet in §6's "It will guarantee" list: `- **Guarantee**, ...`.
	guaranteeRE = regexp.MustCompile(`(?m)^- \*\*(.+?)\*\*`)

	// A row of the guarantees table: `| Guarantee | Tests |`.
	tableRowRE = regexp.MustCompile(`(?m)^\| ([^|]+?) \| (.+) \|\s*$`)

	// A test or fuzz function named in backticks, perhaps with a subtest
	// suffix like /* or /name.
	testNameRE = regexp.MustCompile("`((?:Test|Fuzz)[A-Za-z0-9_]+)(?:/[^`]*)?`")

	testFuncRE = regexp.MustCompile(`(?m)^func ((?:Test|Fuzz)[A-Za-z0-9_]+)\(`)
)

// between returns the text from the line starting with start up to the next
// line starting with end (or the end of src).
func between(src, start, end string) (string, error) {
	i := strings.Index(src, "\n"+start)
	if i < 0 {
		return "", fmt.Errorf("no section %q", start)
	}
	rest := src[i+1+len(start):]
	if j := strings.Index(rest, "\n"+end); j >= 0 {
		rest = rest[:j]
	}
	return rest, nil
}

// checkTraceability holds DESIGN.md's §6 and §7 to each other: every guarantee
// in §6 has a row in §7's guarantees table naming at least one test, and every
// test named anywhere in §7 exists. It returns every problem, not just the
// first, so a failure is a to-do list.
func checkTraceability(design string, exists func(name string) bool) []error {
	var errs []error
	sec6, err := between(design, "## 6.", "## 7.")
	if err != nil {
		return []error{err}
	}
	promised, err := between(sec6, "### It will guarantee", "### ")
	if err != nil {
		return []error{err}
	}
	sec7, err := between(design, "## 7.", "## 8.")
	if err != nil {
		return []error{err}
	}
	table, err := between(sec7, "### The guarantees in §6", "### ")
	if err != nil {
		return []error{err}
	}

	rows := map[string]string{}
	for _, m := range tableRowRE.FindAllStringSubmatch(table, -1) {
		rows[strings.TrimSpace(m[1])] = m[2]
	}
	guarantees := guaranteeRE.FindAllStringSubmatch(promised, -1)
	if len(guarantees) == 0 {
		errs = append(errs, fmt.Errorf("found no guarantees in §6; the format changed and this test needs updating"))
	}
	for _, g := range guarantees {
		tests, ok := rows[g[1]]
		switch {
		case !ok:
			errs = append(errs, fmt.Errorf("§6 guarantees %q, but §7's guarantees table has no row for it", g[1]))
		case !testNameRE.MatchString(tests):
			errs = append(errs, fmt.Errorf("§7's row for %q names no test", g[1]))
		}
	}
	for _, m := range testNameRE.FindAllStringSubmatch(sec7, -1) {
		if !exists(m[1]) {
			errs = append(errs, fmt.Errorf("§7 names %s, which does not exist", m[1]))
		}
	}
	return errs
}

// repoTests is every test and fuzz function defined in the repository.
func repoTests(t *testing.T) map[string]bool {
	t.Helper()
	found := map[string]bool{}
	err := filepath.WalkDir(repoRoot, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() && (d.Name() == ".git" || d.Name() == "gen" || d.Name() == "testdata") {
			return filepath.SkipDir
		}
		if !strings.HasSuffix(path, "_test.go") {
			return nil
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for _, m := range testFuncRE.FindAllStringSubmatch(string(b), -1) {
			found[m[1]] = true
		}
		return nil
	})
	require.NoError(t, err)
	return found
}

// TestEveryGuaranteeNamesExistingTests is phase 6 acceptance criterion 6: every
// guarantee in DESIGN.md §6 names a test, and every test DESIGN.md §7 names
// exists. That they pass is what `make test` checks.
func TestEveryGuaranteeNamesExistingTests(t *testing.T) {
	tests := repoTests(t)
	require.Greater(t, len(tests), 50, "found too few tests; the scan is broken")
	errs := checkTraceability(readRepoFile(t, "DESIGN.md"), func(n string) bool { return tests[n] })
	for _, err := range errs {
		t.Error(err)
	}
}

// TestTraceabilityCheckCatchesGaps is its negative control: a design document
// with a guarantee missing from the table, a row naming no test, and a test
// that does not exist must produce exactly those three complaints.
func TestTraceabilityCheckCatchesGaps(t *testing.T) {
	design := strings.Join([]string{
		"",
		"## 6. Guarantees",
		"### It will guarantee",
		"- **Covered**, fine.",
		"- **Forgotten**, nobody wrote a row for this.",
		"- **Hollow**, the row names no test.",
		"### It will not guarantee",
		"- **Something else**",
		"## 7. Traceability",
		"### The guarantees in §6",
		"| Guarantee | Tests |",
		"|---|---|",
		"| Covered | `TestReal` |",
		"| Hollow | a sincere hope |",
		"### Every claim",
		"| Claim | Test | Phase |",
		"| x | `TestImaginary/sub` | 6 |",
		"## 8. Next",
	}, "\n")
	errs := checkTraceability(design, func(n string) bool { return n == "TestReal" })
	var msgs []string
	for _, e := range errs {
		msgs = append(msgs, e.Error())
	}
	require.Len(t, errs, 3, "%v", msgs)
	joined := strings.Join(msgs, "\n")
	require.Contains(t, joined, `"Forgotten"`)
	require.Contains(t, joined, `"Hollow" names no test`)
	require.Contains(t, joined, "TestImaginary, which does not exist")
}

// traceabilityTables is DESIGN.md §7's tables, as the README reproduces them:
// every table row, with in-page links pointed at DESIGN.md.
func traceabilityTables(design string) ([]string, error) {
	sec7, err := between(design, "## 7.", "## 8.")
	if err != nil {
		return nil, err
	}
	var rows []string
	for _, line := range strings.Split(strings.ReplaceAll(sec7, "\r\n", "\n"), "\n") {
		if strings.HasPrefix(line, "|") {
			rows = append(rows, strings.ReplaceAll(line, "](#", "](DESIGN.md#"))
		}
	}
	return rows, nil
}

// TestReadmeCarriesTheTraceabilityTable is phase 8 acceptance criterion 3's
// guard: the README must carry DESIGN.md §7's claim-to-test tables in full, so
// the evidence a visitor sees first cannot quietly fall behind the design
// document. Regenerate the README's copy rather than editing it by hand.
func TestReadmeCarriesTheTraceabilityTable(t *testing.T) {
	rows, err := traceabilityTables(readRepoFile(t, "DESIGN.md"))
	require.NoError(t, err)
	require.Greater(t, len(rows), 50, "found too few rows in DESIGN.md §7; the format changed")
	missing := missingRows(rows, readRepoFile(t, "README.md"))
	require.Empty(t, missing, "the README is missing, or has a different version of, these DESIGN.md §7 rows")
}

// missingRows lists the rows that do not appear, as whole lines, in doc.
func missingRows(rows []string, doc string) []string {
	doc = strings.ReplaceAll(doc, "\r\n", "\n")
	var out []string
	for _, row := range rows {
		if !strings.Contains(doc, row+"\n") {
			out = append(out, row)
		}
	}
	return out
}

// TestReadmeCheckCatchesAStaleCopy is its negative control: the real README
// with one row edited, as if DESIGN.md had been updated and the README not,
// must be caught.
func TestReadmeCheckCatchesAStaleCopy(t *testing.T) {
	rows, err := traceabilityTables(readRepoFile(t, "DESIGN.md"))
	require.NoError(t, err)
	readme := strings.ReplaceAll(readRepoFile(t, "README.md"), "\r\n", "\n")
	require.Empty(t, missingRows(rows, readme))
	last := rows[len(rows)-1]
	stale := strings.Replace(readme, last+"\n", "| a row nobody updated | `TestSomething` | 8 |\n", 1)
	require.Equal(t, []string{last}, missingRows(rows, stale))
}
