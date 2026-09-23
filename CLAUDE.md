# CLAUDE.md

Instructions for any agent working on Quorum. Read this before touching code.

Quorum is a from-scratch, Raft-based replicated key-value store: 3–5 local
processes holding one consistent copy of a KV store, correct under crashes,
freezes and network partitions. It is a **portfolio project**, which sets the
bar: correctness under failure must be *demonstrated by a test that would fail
if the claim were false*, not asserted in prose.

---

## 1. Read these first, in this order

| File | What it is |
|---|---|
| **[PROGRESS.md](PROGRESS.md)** | **Current state. Start here.** What is built, what is verified, what is next. |
| [PLAN.md](PLAN.md) | The 8-phase roadmap and the acceptance criteria that define "done" per phase. |
| [DESIGN.md](DESIGN.md) | Every architectural decision and its reasoning. The authority on *why*. |
| [BUGS.md](BUGS.md) | Bugs testing actually found, and the regression test guarding each. |

If this file and DESIGN.md disagree, DESIGN.md is right and this file needs
fixing.

---

## 2. Phase discipline — the single easiest way to get this wrong

The project is built in **8 phases, one per user prompt**. The user pastes one
phase at a time.

- **Do only the phase you were given.** Do not start the next one, do not
  "helpfully" stub it out, do not implement phase 3's persistence while building
  phase 2's elections.
- **Check PROGRESS.md for the current phase** before assuming anything.
- A phase is done when every acceptance criterion in PLAN.md passes *and*
  `make ci` is green. Not before.
- If a phase's acceptance criteria seem wrong or unachievable, say so and
  propose a change — do not silently redefine them.

---

## 3. Non-negotiable invariants

These are load-bearing. Breaking one silently is worse than not implementing the
feature. Each has a test; if you find yourself wanting to weaken the test, stop
and raise it instead.

### 3.1 Package `raft` performs no I/O

`raft/` must not import `time`, `sync`, `sync/atomic`, `os`, `io`, `net`,
`context`, `math/rand`, `log`, or anything outside the allowlist in
`raft/purity_test.go`.

- Logical time advances **only** via `Tick()`. There is no clock in the core.
- Election-timeout jitter comes from a `rand` source **injected through
  `Config`**. Never `math/rand` directly — determinism depends on it.
- `fmt` is allowed for `Errorf`/`Sprintf` only. `fmt.Print*`/`Fprint*` are
  rejected by a separate test. The core returns information; it does not emit
  it.

This is what makes the deterministic simulator and the real deployment run the
same code. It is the foundation of the whole testing story.

### 3.2 The Ready ordering is a correctness requirement

```
storage.Append(rd.Entries)
storage.SetHardState(rd.HardState)
storage.Sync()                      // <- exactly one fsync per Ready batch
transport.Send(rd.Messages)         // never before Sync() returns
sm.Apply(rd.CommittedEntries)       // never before Sync() returns
n.Advance()
```

**No message may leave the process before `Sync()` returns.** A granted vote and
an accepted `AppendEntries` are durable promises; sending one before it is on
disk lets a crash make the node contradict itself, and two leaders in one term
follows directly. etcd relaxes this as an optimization. **We do not.**

### 3.3 The core has no locks

One driver goroutine owns the `*raft.Node` exclusively and serializes all input
through one `select`. Ownership replaces locking.

Mutexes/atomics are permitted in exactly three places, and nowhere else without
updating DESIGN.md §1:

1. `internal/storage` — the WAL file handle.
2. The status snapshot for the visualizer — immutable struct behind
   `atomic.Pointer[Status]`.
3. The pending-proposal registry (log index → result channel).

If you need a fourth, that is a design change, not an implementation detail.

### 3.4 Every checker needs a negative control

A checker that has never been observed to fail is not evidence. Whenever you add
a test that *verifies an invariant*, also add a test that proves the checker
catches a violation.

Existing examples to copy:

- `TestImportAllowlistCheckerDetectsViolations` runs the purity checker against
  a deliberately impure fixture.
- `TestMutatedRaftTripsInvariant/*` (phase 2) runs deliberately broken Raft
  variants against the safety-invariant checkers.

### 3.5 Never fabricate a BUGS.md entry

The brief predicts this project will hit a locking bug causing multi-second
election stalls. **Do not invent one to match.** BUGS.md records what testing
actually found. Predictions that did not materialize go in the "not yet borne
out" section. A fabricated bug log destroys the credibility of everything else.

### 3.6 Commits are authored by the user only

No `Co-Authored-By: Claude` trailer. No "Generated with Claude Code" line. The
repo-local `user.name`/`user.email` are already set correctly — do not override
them. Verify before pushing:

```bash
git log -1 --format='%an <%ae>%n%B'
```

### 3.7 The log format is close to frozen

`proto/quorum/raft/v1/raft.proto` defines what goes on the wire **and on disk**.
Renumbering or repurposing a field makes every previously written WAL
unreadable, which silently destroys the durability guarantee.

- Adding a new field with a new number: fine.
- Changing or reusing an existing number: a breaking change. `buf breaking`
  guards this.
- `ENTRY_TYPE_CONFIG` is reserved for dynamic membership and intentionally
  unimplemented. Leave it reserved.

### 3.8 Generated code is checked in

`gen/` is committed so a clean clone builds with only the Go toolchain. After
editing any `.proto`:

```bash
make proto        # regenerate
make proto-check  # fails if gen/ is stale
```

Then commit `gen/` along with the `.proto` change.

---

## 4. Commands

```bash
make help        # list targets
make build       # binaries into bin/
make test        # test suite
make race        # test suite under the race detector
make lint        # go vet + golangci-lint
make ci          # fmt-check + lint + build + test + race
make run         # print the cluster plan and one node's config
make proto       # regenerate protobuf code (needs `make tools` once)
```

On Windows use `.\make.ps1 <target>` — same names, same behaviour.
`test/tooling/targets_test.go` fails if the two runners drift apart, so **add
any new target to both files**.

---

## 5. This machine (Windows 11)

The Bash tool's `PATH` does **not** include Go. Every Bash command needs:

```bash
export PATH="/c/Program Files/Go/bin:$HOME/go/bin:$PATH"
```

PowerShell needs the machine+user PATH rebuilt if the session predates an
install:

```powershell
$env:Path = [Environment]::GetEnvironmentVariable('Path','Machine') + ';' +
            [Environment]::GetEnvironmentVariable('Path','User')
```

| | |
|---|---|
| Go | 1.27.0, `C:\Program Files\Go\bin` |
| GOBIN | `C:\Users\shash\go\bin` — has `buf` 1.73.0, `golangci-lint` 2.13.2, `protoc-gen-go`, `protoc-gen-go-grpc` |
| 64-bit gcc | WinLibs UCRT, under `%LOCALAPPDATA%\Microsoft\WinGet\Packages\BrechtSanders.WinLibs.POSIX.UCRT_*\mingw64\bin` |

---

## 6. Traps

Things that will waste a cycle if you do not know them:

- **`raft/testdata/impure/` deliberately imports `net`, `os` and `time`.** It is
  the negative-control fixture for the purity checker. Do not "fix" it. It lives
  under `testdata/`, so the go tool ignores it.
- **A 32-bit `C:\MinGW\bin\gcc.exe` shadows the 64-bit toolchain on PATH.**
  `go test -race` then fails with `sorry, unimplemented: 64-bit mode not
  compiled in`, which looks like a Go bug and is not. `make.ps1 race` finds a
  working compiler itself. Do not reorder the user's system PATH.
- **Docs cannot reference a `make` target that does not exist.**
  `TestDocumentedCommandsExist` scans code blocks in README.md, DESIGN.md,
  PLAN.md, PROGRESS.md and CLAUDE.md.
- **Large multi-heredoc Bash commands have failed to parse in this
  environment.** For prose-heavy files, use the Write tool instead of `cat <<EOF`.
- **`buf lint` exceptions are documented in `buf.yaml`.** They are naming
  conventions only; do not add more without writing down why.
- **`data/` is gitignored runtime state** (WAL segments, snapshots). Never
  commit it. `make clean` removes it.

---

## 7. Conventions

- **Comments explain *why*, not *what*.** The existing `doc.go` files set the
  tone: each states the package's responsibility, the invariant it upholds, and
  the phase that fills it in. Match that density.
- **Name tests after the claim they prove**, not the function they call:
  `TestPartitionedLeaderRefusesRead`, not `TestGet`.
- **Errors say what to do about it.** Config validation is the reference: it
  explains *why* a value is rejected, because a bad election timeout otherwise
  surfaces weeks later as "the consensus code is flaky".
- **Prefer making a wrong state unrepresentable over documenting that it is
  wrong.** See BUGS.md's worked example for the reasoning.
- Run `make ci` before saying a phase is done. Report failures verbatim.

---

## 8. Scope reminders

Explicitly **out of scope** (DESIGN.md §6 has the full list with reasoning). Do
not implement these unless the user asks:

Byzantine fault tolerance · TLS or peer auth · dynamic membership ·
multi-key transactions · ranges or watches · throughput tuning ·
cross-machine or cross-datacenter operation · kernel-level partitions ·
leader-lease reads (ReadIndex only)

Two honesty constraints that must survive into any docs you write: injected
**partitions are transport-level, not kernel firewall rules**, and **freeze is
cooperative, not `SIGSTOP`**. Kills *are* real `TerminateProcess`/`SIGKILL`.
Never let the demo imply more than it does.
