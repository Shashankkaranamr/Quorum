# Progress

The current state of Quorum. **A new contributor or agent should read this
first**, then [CLAUDE.md](CLAUDE.md) for the working rules, then
[PLAN.md](PLAN.md) for what the next phase has to deliver.

This file records what actually exists and what is actually verified. It is
updated at the end of every phase.

---

## At a glance

| | |
|---|---|
| **Current phase** | **1 of 8 — complete** |
| **Next phase** | 2 — core consensus: election and log replication |
| **Last updated** | 2026-09-23 |
| **HEAD** | `2376659` — *Phase 1: design decisions and repository scaffold* |
| **Branch** | `main`, pushed to `github.com/Shashankkaranamr/Quorum` |
| **Build state** | `make ci` green: fmt-check, lint (0 issues), build, test, race |
| **Tests** | 14 test functions, 23 cases, all passing |
| **Go** | 1.27.0 |

> **No consensus logic exists yet.** Phase 1 delivered decisions, a scaffold,
> and the tests that keep the later phases honest. Every package under
> `internal/` except `config` is a documented stub.

---

## Phase status

| Phase | Title | Status |
|---|---|---|
| 1 | Foundations and design | ✅ **complete** |
| 2 | Core consensus: election and log replication | ⬜ next |
| 3 | Crash-safe persistence | ⬜ not started |
| 4 | Snapshotting and log compaction | ⬜ not started |
| 5 | gRPC KV service, linearizable reads, deduplication | ⬜ not started |
| 6 | Fault-injection suite and bug log | ⬜ not started |
| 7 | Live cluster visualizer | ⬜ not started |
| 8 | Integration, documentation and demo | ⬜ not started |

Acceptance criteria for each phase are in [PLAN.md](PLAN.md). A phase is done
when all of them pass and `make ci` is green.

---

## What exists right now

### Real, working code

| Path | State |
|---|---|
| `internal/config/` | **Complete.** Loads and strictly validates `cluster.yaml`. Rejects unknown fields, duplicate ids/ports, and timing that is well-formed but wrong (empty election-timeout range, election timeout under 3× heartbeat, cluster smaller than 3). Warns on even cluster sizes. |
| `cmd/quorum-node/` | **Minimal but real.** Parses flags and config, prints its identity, peers, data dir, quorum and timing, exits 0. No server, no consensus. |
| `cmd/quorumctl/` | **Minimal but real.** `plan` and `version` work. Every other subcommand is recognized, documented in `--help` with its phase, and **exits non-zero** — a stub exiting 0 would let a broken script look green. |
| `cmd/quorum-viz/` | Stub. Exits 2 pointing at phase 7. |
| `proto/quorum/{raft,kv,admin}/v1/` | **Schemas complete.** Log entry format, WAL record types, Raft messages, KV contract, admin/fault-injection API. `buf lint` clean. |
| `gen/` | Generated Go from the above, **checked in** so a clean clone builds with only Go. |
| Build tooling | `Makefile` + `make.ps1` mirror, `.golangci.yml`, `buf.yaml`, `buf.gen.yaml`. |

### Documented stubs (a `doc.go` each, no logic)

`raft/` · `internal/server/` · `internal/storage/` · `internal/statemachine/` ·
`internal/transport/` + `inmem/` + `grpcx/` · `internal/pbconv/` ·
`internal/kvservice/` · `internal/admin/` · `internal/client/` ·
`internal/supervisor/` · `internal/testutil/`

Each `doc.go` states the package's responsibility, the invariants it must
uphold, and the phase that fills it in. **Read the `doc.go` before implementing
a package** — it is the spec.

### Empty, reserved for later phases

`test/integration/` (phase 6) · `web/` (phase 7) · `docs/` (phase 8)

---

## What is actually verified

23 passing cases across 14 test functions. What each group proves:

**The consensus core performs no I/O** — `raft/purity_test.go`

- `TestRaftCoreImportAllowlist` — direct imports of `raft/` checked against an
  allowlist that excludes `time`, `sync`, `os`, `net`, `context`, `math/rand`.
- `TestRaftCoreDoesNotWriteToStdout` — AST walk rejecting `fmt.Print*`,
  `fmt.Fprint*` and the `print`/`println` builtins (closes the gap left by
  allowing `fmt` for `Errorf`).
- `TestRaftCoreIsTheOnlyExportedPackage` — keeps the structural shortcut that
  makes the purity claim checkable by reading one import list.
- `TestImportAllowlistCheckerDetectsViolations` — **negative control.**

**Configuration is validated strictly** — `internal/config/config_test.go`

- 9 rejection cases, each naming the mistake it guards against.
- Defaults, accessors, quorum arithmetic, even-size warnings, and a check that
  the committed `cluster.yaml` is itself valid (so the documented quickstart
  cannot silently break).

**The two task runners cannot drift** — `test/tooling/targets_test.go`

- Target names and descriptions must match between `Makefile` and `make.ps1`.
- Every advertised PowerShell target must have a `switch` arm (otherwise it
  would no-op and exit 0).
- Docs cannot reference a `make` target that does not exist.

### Checkers confirmed capable of failing

Per [CLAUDE.md §3.4](CLAUDE.md), a checker nobody has seen fail is not evidence.
Both were deliberately broken and observed to fail:

| Checker | Injected fault | Result |
|---|---|---|
| Import allowlist | added `import "time"` to package `raft` | `FAIL: package raft imports [time], which is not on the allowlist` |
| Task-runner parity | deleted the `cover` target from `make.ps1` only | `FAIL: Makefile and make.ps1 expose different targets` |

---

## Decisions already made

All five design questions are answered in [DESIGN.md](DESIGN.md). The short
form, so nothing gets re-litigated by accident:

| Area | Decision | Where |
|---|---|---|
| Language | **Go 1.27** — for `go test -race`, first-party gRPC, explicit `File.Sync()`, cheap process spawn/kill | [§1](DESIGN.md#1-language-and-core-libraries) |
| Concurrency | Pure `Step`/`Ready` core; **no shared Raft state, no locks**; one driver goroutine owns it | [§1](DESIGN.md#1-language-and-core-libraries) |
| Persistence | **Hand-rolled append-only WAL**, not an embedded KV, so the fsync boundary stays auditable. HardState inside the WAL | [§2](DESIGN.md#2-persistence) |
| Testing | One core, two transports: deterministic tick-driven simulator + real gRPC streams per directed link | [§3](DESIGN.md#3-transport-and-how-one-core-serves-two-test-modes) |
| Reads | **ReadIndex**; leader lease documented and deliberately off | [§4](DESIGN.md#4-linearizability) |
| Retries | `(client_id, seq)` sessions; `client_id` is the log index of the RegisterClient entry | [§4](DESIGN.md#4-linearizability) |
| Membership | **Static.** `ENTRY_TYPE_CONFIG` reserved so adding it later needs no format change | [§5](DESIGN.md#5-cluster-topology-configuration-and-operation) |

### Interpretations worth knowing about

- **Protobuf schemas were written in phase 1** even though phase 1 excludes
  gRPC service code. Requirement 4 of the brief required the log entry format
  and KV contract to be settled up front; the `.proto` files are that decision
  made concrete. No service *implementations* exist — only generated stubs.
  This was flagged to the user and not objected to.
- **`cluster-5.yaml` was added** beyond the plan's single `cluster.yaml`,
  because phase 7 requires the visualizer to work at 3 and 5 nodes.
- **A 64-bit mingw-w64 toolchain was installed** on the dev machine. `go test
  -race` was broken by a 32-bit `C:\MinGW\bin\gcc.exe` earlier on PATH. The race
  detector is load-bearing for this project's concurrency claims, so it was
  fixed rather than skipped. The user's system PATH was **not** reordered;
  `make.ps1 race` resolves a working compiler itself.

---

## Open items

Nothing is blocking phase 2. Carried forward for later:

- **The claim-to-test traceability table** in
  [DESIGN.md §7](DESIGN.md#7-claim-to-test-traceability) has 3 of 22 rows filled.
  Each later phase fills its own rows; phase 6 requires it complete.
- **BUGS.md has no real entries yet** — by design. It starts collecting in
  phase 6. The predicted-but-not-yet-observed locking bug is recorded in its
  "not yet borne out" section rather than invented.
- **No CI runner is configured.** `make ci` exists and passes locally; wiring it
  to GitHub Actions is unscheduled and optional for a local-only project.
- **`internal/pbconv` is not in the original plan's layout** — it was added so
  that package `raft` can avoid depending on the protobuf runtime, which is what
  keeps the purity claim mechanically checkable.

---

## Next: phase 2

**Goal.** A correct Raft state machine, in memory, with a deterministic
simulator and invariant checkers proven capable of failing.

Full acceptance criteria: [PLAN.md § Phase 2](PLAN.md#phase-2--core-consensus-election-and-log-replication).

Suggested order of attack:

1. `raft/` types and log (`Entry`, `Message`, `HardState`, `Ready`, `Config`
   with the injected rand source) — obeying the import allowlist from the first
   line.
2. `internal/pbconv/` — conversion to and from the generated protobuf types.
3. `internal/transport/` interface, then `inmem/` with its partition matrix and
   logical-tick delay queue.
4. `internal/testutil/` harness — step every node, deliver due messages, and
   assert all five safety invariants **after every step**.
5. Election, then replication, in `raft/`.
6. The mutation fixtures (`TestMutatedRaftTripsInvariant/*`) — do not skip
   these; they are what makes the invariant checkers count as evidence.

Watch for: the temptation to reach for `time.Now()` or a mutex inside `raft/`
(the purity test will stop you, but the design is what should stop you first),
and the temptation to assert invariants only at the end of a scenario rather
than after every step.

---

## Session log

### Phase 1 — 2026-09-23

Delivered design decisions and the repository scaffold. Installed Go 1.27 (not
previously present) plus buf, the protobuf plugins, golangci-lint, and a 64-bit
mingw-w64 so the race detector works.

Wrote DESIGN.md (five decisions, guarantees and non-guarantees, claim-to-test
table), PLAN.md (8 phases with acceptance criteria), README.md, BUGS.md (format
fixed, no invented entries), the package tree with documented stubs, three
protobuf schemas, and the Makefile/make.ps1 pair.

Verified: `make ci` green; both checkers observed failing when deliberately
broken; both binaries produce correct output against `cluster.yaml` and
`cluster-5.yaml`.

Committed as `2376659`, authored solely by the repository owner with no AI
attribution trailers, pushed to `main`.
