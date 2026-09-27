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
| **Current phase** | **3 of 8 — complete** |
| **Next phase** | 4 — snapshotting and log compaction |
| **Last updated** | 2026-09-27 |
| **Branch** | `main` at `github.com/Shashankkaranamr/Quorum` |
| **Build state** | `make ci` green: fmt-check, lint (0 issues), build, test, race (~98s end to end) |
| **Tests** | 72 test and fuzz functions across 7 packages, all passing |
| **Go** | 1.27.0 |

> **Raft elects, replicates and survives restarts.** Each node can now persist
> to a real write-ahead log that recovers cleanly from a crash at any byte, and
> the simulator can run every node on one, so crash tests exercise real
> recovery from disk rather than a model of it. There is still no network and
> no gRPC: the driver is stepped by the simulator, and the goroutine loop
> arrives with phase 5's transport.

---

## Phase status

| Phase | Title | Status |
|---|---|---|
| 1 | Foundations and design | ✅ **complete** |
| 2 | Core consensus: election and log replication | ✅ **complete** |
| 3 | Crash-safe persistence | ✅ **complete** |
| 4 | Snapshotting and log compaction | ⬜ next |
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
| `internal/storage/` | **The durability boundary.** `WAL`: segmented write-ahead log, one CRC-framed record and at most one fsync per Ready, recovery that truncates a torn tail and refuses any other damage, fsync counters and latency in `WALStats`. `MemStorage`: the simulator's model of the same contract, sharing the batch logic so the two cannot disagree. |
| `internal/transport/` | `Transport` interface (Send only - see DESIGN.md §10) and `inmem/`, a deterministic bus with drops, delays, duplication, reordering and directed (one-way capable) partitions, all seeded. |
| `internal/server/` | The driver: the one place Ready ordering is decided, plus the `tick_lag` metrics. Shared by the simulator and, from phase 5, the real goroutine loop. |
| `internal/testutil/` | The simulated cluster and the invariant checkers. Storage is pluggable: `MemStorageFactory` (default) or `WALStorageFactory` to put every node on a real WAL. |
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

72 test and fuzz functions across 7 packages. What each group proves:

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
  cannot count as evidence.
- `TestRandomizedTrialsUpholdSafetyOnDisk`: the same trials, 10 per size, with
  every node on a real WAL, so each crash is followed by real recovery.
- `TestElectionConverges`: 1000 seeds per size. Worst 38 ticks (n=3), 25 (n=5).
- `TestLivenessBoundUnderTransientFaults`: 10% drops, delays to 3 ticks; worst
  51 ticks against a bound of 150.
- `TestMinorityPartitionCannotCommit`, `TestLeaderFailoverPreservesCommitted`,
  `TestProposalsCommitAndApply`, `TestTickLagIsMeasured`.

**Durability** - `internal/storage/`, `internal/server/`, `internal/testutil/persistence_test.go`

- `TestWALCodecRoundTrip`, `TestWALCodecRejectsEveryBitFlip`,
  `TestWALCodecClassifiesDamage`, and `FuzzWALDecode` (2.2M executions over two
  minutes, no failures): the decoder never panics, never returns a partial
  record, never accepts a CRC mismatch.
- `TestWALTruncationAtEveryOffset`: a real WAL truncated at every one of its
  254 byte offsets recovers to exactly the whole records before the tear, and
  a write after recovery survives a second recovery.
- `TestWALRefusesDamageThatIsNotATornTail`: damage in a closed segment, a
  missing segment, or an unreadable record with a valid CRC stops the node.
- `TestNoSendBeforeSync`: every message from every node, across randomized
  schedules with crashes and restarts, checked at the moment of sending against
  what that node had made durable. About 138,000 messages per run, on both the
  simulator's storage and the real WAL.
- `TestOneFsyncPerReady`: one Sync per Ready; one fsync per Ready with durable
  state (including a 500-entry batch), none for a message-only Ready.
- `TestRestartDoesNotDoubleVote`: grant, crash, restart from disk, refuse a
  second candidate in the same term; and the crash-inside-fsync variant.
- `TestClusterRestartRecoversCommitted`: 12 full-cluster crashes on real WALs;
  every committed entry (84 to 116 per run) re-applied identically everywhere.
- `TestWALAgreesWithMemStorage`: the simulator's crash model and the real log
  agree after reopening.

**The fiddly Figure 2 mechanics** - `raft/node_test.go`, `raft/figure8_test.go`

- `TestUpToDateComparison`, `TestElectionTimerResetDiscipline`,
  `TestStaleAppendEntriesDoesNotTruncate`, `TestFigure8CommitRule`.

**The wire and disk format round-trips** - `internal/pbconv/pbconv_test.go`

### Every checker has been observed to fail for the right reason

Per [CLAUDE.md](CLAUDE.md), a checker nobody has seen fail is not evidence.

| Checker | How it was shown to fail |
|---|---|
| ElectionSafety | `MutationVoteTwicePerTerm` produced *two leaders in term 2: node 5 and node 3* |
| LeaderCompleteness | `MutationSkipUpToDateCheck` produced *node 5 became leader in term 4 without entry 111* |
| LeaderAppendOnly | `MutationLeaderTruncatesOwnLog` produced *leader 4 log shrank from 114 to 113 entries* |
| CommittedEntriesAreStable | `MutationCommitAnyTerm` via `TestFigure8CommitRule` |
| LogMatching, StateMachineSafety, WellFormed | hand-built violating histories (`TestCheckerDetectsHandBuiltViolations`) |
| Import allowlist | `import "time"` added to package `raft` |
| Task-runner parity | a target deleted from `make.ps1` only |
| Decoder fuzz contract | three broken decoders (`TestDecodeContractCatchesBrokenDecoders`) |
| Torn-tail prefix check | neighbouring prefixes rejected at every offset (`TestTruncationCheckDistinguishesEveryPrefix`); recovery with truncation removed fails at byte 4 |
| Durability-ordering audit | send-before-persist and send-before-sync drivers (`TestDurabilityAuditCatchesMisorderedDrivers`) |
| Double-vote check | storage that forgets its vote (`TestDoubleVoteCheckCatchesAmnesia`) |
| Whole-cluster restart check | storage that loses a committed entry: *node 1 re-applied 88 entries, but 89 were committed before the crash* |

`TestCheckerAcceptsAHealthyHistory` is the other half for the Raft checkers,
and the durability audit has its own: a correct Ready spanning two terms must
not be flagged (see BUGS.md, 2026-09-27, for why that case exists).

---

## Decisions already made

All five design questions are answered in [DESIGN.md](DESIGN.md). The short
form, so nothing gets re-litigated by accident:

| Area | Decision | Where |
|---|---|---|
| Language | **Go 1.27** — for `go test -race`, first-party gRPC, explicit `File.Sync()`, cheap process spawn/kill | [§1](DESIGN.md#1-language-and-core-libraries) |
| Concurrency | Pure `Step`/`Ready` core; **no shared Raft state, no locks**; one driver goroutine owns it | [§1](DESIGN.md#1-language-and-core-libraries) |
| Persistence | **Hand-rolled append-only WAL**, not an embedded KV, so the fsync boundary stays auditable. One record per Ready, holding its entries and hard state | [§2](DESIGN.md#2-persistence) |
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
- **Phase 3 criterion 4 was refined, and flagged.** "Exactly one fsync per
  Ready" became "exactly one Sync per Ready, and exactly one fsync per Ready
  that has anything durable in it". A heartbeat-only Ready has nothing to make
  durable. Reasoning in PLAN.md and DESIGN.md §10.
- **`WalEntryBatch` gained a `hard_state` field** so a Ready is one record.
  A new field number, accepted by `buf breaking`; DESIGN.md §10.

---

## Open items

Nothing is blocking phase 4. Carried forward:

- **goleak is deferred again, to phase 5.** Phase 3 added no goroutines; the
  first one is the driver loop, which needs phase 5's transport.
- **`WAL.InitialState` returns applied = 0**, and `MemStorage.SetApplied` is
  still never called, so a restarted node re-applies its whole log. Correct,
  and phase 4's snapshots are what fix the cost.
- **Recovery refuses `WalSnapshotPointer` records** with an error naming
  phase 4. The codec already round-trips and fuzzes them.
- **The WAL keeps the recovered log in memory** alongside the core's copy until
  the process exits. Bounded by compaction in phase 4.
- **Mid-segment media corruption in the last segment** is indistinguishable
  from a torn tail and is truncated. Outside the crash-fault model; stated in
  DESIGN.md §2.
- **`Mutation` ships in production code** so the negative controls can reach it
  from another package. Fenced by `TestZeroConfigIsUnmutated` and
  `TestEveryMutationIsDistinct`; reasoning in DESIGN.md §10.
- **`InstallSnapshot` message types exist but are rejected.** Phase 4.
- **The claim-to-test traceability table** (DESIGN.md §7) now has 35 rows
  filled. Phase 6 requires it complete.
- **No CI runner is configured.** `make ci` passes locally.

---

## Next: phase 4

**Goal.** History is compacted so a lagging or rejoining node catches up quickly
instead of replaying everything.

Full acceptance criteria: [PLAN.md](PLAN.md).

What phase 3 left in place:

1. The snapshot write sequence is already specified (DESIGN.md §2: payload,
   then pointer, then delete superseded segments) and the pointer record
   already round-trips through the codec. Recovery currently refuses it; phase
   4 gives it a meaning.
2. Segments exist precisely so that compaction can delete a prefix a file at a
   time. `listSegments` requires consecutive numbering from wherever the log
   starts, not from 1, so deleting leading segments needs no format change.
3. The simulator already runs nodes on a real WAL
   (`testutil.WALStorageFactory`), so "crash at each step of snapshotting" can
   be tested with real files.

Watch for: the recovered commit index is checked against the last recovered
entry; after compaction that check must account for the snapshot's index.

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

### Phase 3 - 2026-09-27

Implemented the write-ahead log: the framing codec, segmented storage with
rolling, recovery that truncates a torn tail and refuses any other damage, and
fsync counters and latency. Made the simulator's storage pluggable so every
crash test can run on real disk, and moved the fsync-cost knob from
`MemStorage` to the simulator so it applies to both.

One format addition, recorded in DESIGN.md §10: `WalEntryBatch` gained
`hard_state`, so each Ready is exactly one CRC-protected record and a torn write
can only lose a whole batch. One acceptance criterion refined and flagged: a
Ready with nothing durable in it costs no fsync.

Found one real bug, in the new test rather than the storage or consensus code,
recorded in BUGS.md: the durability audit rejected a correct Ready that spanned
two terms. Its regression test was run against the pre-fix audit and observed
to fail before the fix went in. A second audit bug (violations from before a
restart were being dropped) was found by reading the code while fixing the
first, and is noted in the same entry.

Also corrected stale documentation found along the way: README.md still
described phase 1, `internal/server/doc.go` still called itself a stub, and a
BUGS.md note named a metric (`fsync_p99_micros`) that has never existed.

Verified: `make ci` green; every new checker observed failing on a deliberately
broken input; `buf breaking` clean against the previous commit.
