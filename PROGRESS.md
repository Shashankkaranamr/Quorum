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
| **Current phase** | **2 of 8 — complete** |
| **Next phase** | 3 — crash-safe persistence |
| **Last updated** | 2026-09-23 |
| **Branch** | `main`, pushed to `github.com/Shashankkaranamr/Quorum` |
| **Build state** | `make ci` green: fmt-check, lint (0 issues), build, test (11s), race (59s) |
| **Tests** | 45 test functions across 5 packages, all passing |
| **Go** | 1.27.0 |

> **Raft elects and replicates, in memory.** The consensus core is complete and
> its five safety properties are checked after every simulated tick across 400
> randomized trials. There is still no real disk, no real network and no gRPC:
> `MemStorage` and the in-memory bus satisfy the interfaces that phases 3 and 5
> will put real implementations behind.

---

## Phase status

| Phase | Title | Status |
|---|---|---|
| 1 | Foundations and design | ✅ **complete** |
| 2 | Core consensus: election and log replication | ✅ **complete** |
| 3 | Crash-safe persistence | ⬜ next |
| 4 | Snapshotting and log compaction | ⬜ not started |
| 5 | gRPC KV service, linearizable reads, deduplication | ⬜ not started |
| 6 | Fault-injection suite and bug log | ⬜ not started |
| 7 | Live cluster visualizer | ⬜ not started |
| 8 | Integration, documentation and demo | ⬜ not started |

Acceptance criteria for each phase are in [PLAN.md](PLAN.md). A phase is done
when all of them pass and `make ci` is green.

---

## What exists right now

### Complete

| Path | State |
|---|---|
| `raft/` | **The consensus core.** Leader election with randomized timeouts, log replication with prevLog consistency checks and conflict-term backoff, the Figure 8 commit rule, and the Step/Ready protocol. No I/O, no clock, no goroutines, no locks - enforced by `purity_test.go`. |
| `internal/transport/` | `Transport` interface (Send only - see DESIGN.md §10) and `inmem/`, a deterministic bus with drops, delays, duplication, reordering and directed (one-way capable) partitions, all seeded. |
| `internal/storage/` | `Storage` interface and `MemStorage`. Buffers on Append/SetHardState and publishes only on Sync, so a crash loses exactly what a real node would. Can be configured to cost logical ticks per fsync. |
| `internal/server/` | The driver: the one place Ready ordering is decided, plus the `tick_lag` and fsync metrics. Shared by the simulator and, from phase 3, the real goroutine loop. |
| `internal/testutil/` | The simulated cluster and the invariant checkers - the primary deliverable of phase 2. |
| `internal/pbconv/` | Core types to protobuf and back, so `raft/` never imports the protobuf runtime. |
| `internal/config/` | Loads and strictly validates `cluster.yaml`. |
| `proto/`, `gen/` | Schemas and checked-in generated code. `buf lint` clean. |
| `cmd/quorum-node`, `cmd/quorumctl` | Minimal but real: parse config, print what they would do, exit 0. Unimplemented subcommands exit non-zero naming their phase. |

### Documented stubs (a `doc.go` each, no logic)

`internal/statemachine/` (phase 5) - `internal/transport/grpcx/` (phase 5) -
`internal/kvservice/` (phase 5) - `internal/admin/` (phase 6/7) -
`internal/client/` (phase 5) - `internal/supervisor/` (phase 6) -
`cmd/quorum-viz/` (phase 7)

Each `doc.go` states the package's responsibility, the invariants it must
uphold, and the phase that fills it in. **Read the `doc.go` before implementing
a package** - it is the spec.

---

## What is actually verified

45 test functions across 5 packages. What each group proves:

**The consensus core stays pure** - `raft/purity_test.go`

- Import allowlist, now also asserting it is scanning the real implementation
  files rather than passing vacuously against a stub.
- `TestRaftCoreHasNoConcurrencyPrimitives` rejects `go`, `select` and channel
  types, which no import check can see.
- No `fmt.Print*`; `raft` is the only exported package.
- Negative control: the checker is run against a deliberately impure fixture.

**Raft's safety properties hold under adversity** - `internal/testutil/`

- `TestRandomizedTrialsUpholdSafety`: 200 trials per cluster size (3 and 5),
  each 500 ticks with randomized loss, delay, duplication, reordering,
  partitions and crashes, then healed and run to convergence. All seven
  invariants checked **after every tick**. The suite asserts it actually
  injected faults and actually committed entries, so a trial that did nothing
  cannot count as evidence. Last run: 39k committed entries, 92k dropped
  messages, 93k blocked by partitions, zero violations.
- `TestElectionConverges`: 1000 seeds per size. Worst 38 ticks (n=3), 25 (n=5).
- `TestLivenessBoundUnderTransientFaults`: 10% drops, delays to 3 ticks; worst
  51 ticks against a bound of 150.
- `TestMinorityPartitionCannotCommit`, `TestLeaderFailoverPreservesCommitted`,
  `TestProposalsCommitAndApply`, `TestTickLagIsMeasured`.

**The fiddly Figure 2 mechanics** - `raft/node_test.go`, `raft/figure8_test.go`

- `TestUpToDateComparison` - the §5.4.1 term-then-index comparison, including
  the case that fails if it is written backwards.
- `TestElectionTimerResetDiscipline` - resets on exactly two events, and
  explicitly does NOT reset on a refused vote or on any response.
- `TestStaleAppendEntriesDoesNotTruncate` - reordered and duplicated messages.
- `TestFigure8CommitRule` - the constructed Figure 8 scenario.

**The wire and disk format round-trips** - `internal/pbconv/pbconv_test.go`

### Every checker has been observed to fail for the right reason

Per [CLAUDE.md](CLAUDE.md), a checker nobody has seen fail is not evidence.
Two layers:

| Checker | How it was shown to fail |
|---|---|
| ElectionSafety | `MutationVoteTwicePerTerm` produced *two leaders in term 2: node 5 and node 3* |
| LeaderCompleteness | `MutationSkipUpToDateCheck` produced *node 5 became leader in term 4 without entry 111* |
| LeaderAppendOnly | `MutationLeaderTruncatesOwnLog` produced *leader 4 log shrank from 114 to 113 entries* |
| CommittedEntriesAreStable | `MutationCommitAnyTerm` via `TestFigure8CommitRule` |
| LogMatching, StateMachineSafety, WellFormed | hand-built violating histories (`TestCheckerDetectsHandBuiltViolations`) |
| Import allowlist | `import "time"` added to package `raft` |
| Task-runner parity | a target deleted from `make.ps1` only |

`TestCheckerAcceptsAHealthyHistory` is the other half: a checker that fires on
everything is as useless as one that fires on nothing.

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

Nothing is blocking phase 3. Carried forward:

- **goleak is deferred from phase 2 to phase 3.** Vacuous while there are no
  goroutines; meaningful once the real driver loop exists.
- **`Mutation` ships in production code** so the negative controls can reach it
  from another package. Fenced by `TestZeroConfigIsUnmutated` and
  `TestEveryMutationIsDistinct`; reasoning in DESIGN.md §10.
- **`InstallSnapshot` message types exist but are rejected.** The message space
  is fixed; phase 4 implements them.
- **The claim-to-test traceability table** (DESIGN.md §7) now has 21 rows
  filled. Phase 6 requires it complete.
- **No CI runner is configured.** `make ci` passes locally: test 11s, race 59s.

---

## Next: phase 3

**Goal.** A node that restarts never loses or contradicts what it already agreed
to, and the fsync boundary is auditable.

Full acceptance criteria: [PLAN.md](PLAN.md).

Phase 2 left the ground prepared, so this is narrower than it looks:

1. The `Storage` interface already exists and `MemStorage` already models the
   buffer/Sync split, including losing unsynced writes on a crash. The real WAL
   goes behind the same interface; `internal/testutil` should not need to
   change.
2. `Driver.ProcessReady` already enforces Append, SetHardState, Sync, Send,
   Apply in that order and counts syncs. `TestNoSendBeforeSync` and
   `TestOneFsyncPerReady` become assertions against a recording test double
   rather than new plumbing.
3. `raft.New` already takes restored `HardState`, `Entries` and `Applied`, and
   `Cluster.Restart` already exercises that path against durable-only state.
4. The record framing is already specified in `proto/quorum/raft/v1/raft.proto`
   (`WalEntryBatch`, `WalSnapshotPointer`) and `internal/pbconv` already
   round-trips the types.

Watch for: `MemStorage.SetApplied` is currently never called, so a restarted
node replays its whole log - fine now, but phase 4 needs it. And the torn-tail
test is the one that matters: truncate at **every** byte offset, not a sample.

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

### Phase 2 - 2026-09-23

Implemented the Raft core (election, replication, Figure 8 commit rule), the
deterministic simulator, the invariant checkers, the driver with its Ready
ordering and tick-lag metrics, and the protobuf conversion layer.

Two interface changes from the phase 1 design, both recorded in DESIGN.md §10:
`Transport` lost `Recv` (a channel needs a goroutine, which the simulator must
not have), and the leader counts itself in the commit quorum only up to its
durable index rather than its last index.

Found and fixed two real bugs, both in the tests rather than the consensus code,
both recorded in BUGS.md: failure-message arguments evaluated on every passing
assertion (5x slowdown, found by profiling after two rounds of guessing), and a
reconvergence assertion that could have passed without anything reconverging.

No Raft safety violation was found. Recorded in BUGS.md as a note rather than
left implied, since "our tests found no bugs" is the honest claim.
