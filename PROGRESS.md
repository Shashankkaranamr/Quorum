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
| **Current phase** | **5 of 8 — complete** |
| **Next phase** | 6 — fault-injection suite and bug log |
| **Last updated** | 2026-09-30 |
| **Branch** | `main` at `github.com/Shashankkaranamr/Quorum` |
| **Build state** | `make ci` green: fmt-check, lint (0 issues), build, test, race (~180s end to end, cold cache) |
| **Tests** | 121 test and fuzz functions across 11 packages, all passing |
| **Go** | 1.27.0 |

> **A real replicated key-value store.** Three `quorum-node` processes serve
> linearizable GET and PUT over gRPC. Reads go through ReadIndex, so a leader
> cut off from the majority refuses to answer rather than answer stale. Writes
> carry `(client_id, seq)`, so a retry after an ambiguous failure is applied
> exactly once. `quorumctl put`/`get` work against a real cluster. Recorded
> histories under leader kills and partitions pass Porcupine's linearizability
> check. What is missing is the operator's side: `quorumctl up`, `kill` and
> `partition` against running processes, which need the admin API, and the
> visualizer. Those are phases 6 and 7.

---

## Phase status

| Phase | Title | Status |
|---|---|---|
| 1 | Foundations and design | ✅ **complete** |
| 2 | Core consensus: election and log replication | ✅ **complete** |
| 3 | Crash-safe persistence | ✅ **complete** |
| 4 | Snapshotting and log compaction | ✅ **complete** |
| 5 | gRPC KV service, linearizable reads, deduplication | ✅ **complete** |
| 6 | Fault-injection suite and bug log | ⬜ next |
| 7 | Live cluster visualizer | ⬜ not started |
| 8 | Integration, documentation and demo | ⬜ not started |

Acceptance criteria for each phase are in [PLAN.md](PLAN.md). A phase is done
when all of them pass and `make ci` is green.

---

## What exists right now

### Complete

| Path | State |
|---|---|
| `raft/` | **The consensus core.** Leader election with randomized timeouts, log replication with prevLog consistency checks and conflict-term backoff, the Figure 8 commit rule, the Step/Ready protocol, and log compaction: `Node.Compact`, chunked resumable `InstallSnapshot`, Figure 13's install rule, and ReadIndex. No I/O, no clock, no goroutines, no locks - enforced by `purity_test.go`. |
| `internal/storage/` | **The durability boundary.** `WAL`: segmented write-ahead log, one CRC-framed record and at most one fsync per Ready, recovery that truncates a torn tail and refuses any other damage, fsync counters and latency in `WALStats`. `SaveSnapshot`: `.snap` file, then a pointer record carrying the log tail and hard state at the head of a fresh segment, then deletion of everything older. Recovery starts from the last pointer. `MemStorage`: the simulator's model of the same contract, sharing the log logic (`logstate.go`) so the two cannot disagree. |
| `internal/statemachine/` | **The replicated key-value store** with its client session table: put, delete, `(client_id, seq)` deduplication with a cached response, and a deterministic snapshot of data and sessions together. |
| `internal/transport/` | `Transport` interface (Send only - see DESIGN.md §10) and `inmem/`, a deterministic bus with drops, delays, duplication, reordering and directed (one-way capable) partitions, all seeded. |
| `internal/transport/grpcx/` | **The real transport**: one client stream per directed link, a goroutine per peer that reconnects with backoff, and directed fault injection (`Partition`, `BlockOutbound`, `BlockInbound`, `Heal`). Its fault state is one of the two synchronized places in production code. |
| `internal/kvservice/` | **The client KV API**: writes proposed and awaited through the loop, reads through ReadIndex, and the status contract (`NOT_LEADER` with a dialable hint, `LOST_LEADERSHIP`, `TIMEOUT`, `NO_QUORUM`). |
| `internal/client/` | **The client library**: session registration, per-request seq reused across retries, redirect on hints, `ErrUnknownOutcome` when an ambiguous write runs out of time. |
| `internal/node/` | **One replica, assembled**: WAL, state machine restored from snapshot, core, loop, transport and KV service on one gRPC listener. What `quorum-node` runs and what the in-process tests start. |
| `internal/server/` | The driver: the one place Ready ordering is decided, compaction at a threshold, the `tick_lag` metrics, and `Loop`, the goroutine that drives it against the real clock. `Loop` owns the node, storage, state machine and every pending request; handlers reach it through channels only. |
| `internal/testutil/` | The simulated cluster and the invariant checkers, now compaction-aware and with a seventh invariant, SnapshotFidelity. Storage is pluggable (`MemStorageFactory`, `WALStorageFactory`), and so is the state machine: the default `Digest` is a running hash of everything applied. |
| `internal/pbconv/` | Core types to protobuf and back, so `raft/` never imports the protobuf runtime. |
| `internal/config/` | Loads and strictly validates `cluster.yaml`. |
| `proto/`, `gen/` | Schemas and checked-in generated code. `buf lint` clean. |
| `cmd/quorum-node` | Serves until SIGINT/SIGTERM. `-describe` prints its configuration and exits (what `make run` uses). |
| `cmd/quorumctl` | `plan`, `version`, `put`, `get` work. Unimplemented subcommands exit non-zero naming their phase. |

### Documented stubs (a `doc.go` each, no logic)

`internal/admin/` (phase 6/7) - `internal/supervisor/` (phase 6) -
`cmd/quorum-viz/` (phase 7)

Each `doc.go` states the package's responsibility, the invariants it must
uphold, and the phase that fills it in. **Read the `doc.go` before implementing
a package** - it is the spec.

---

## What is actually verified

121 test and fuzz functions across 11 packages. What each group proves:

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

**The client API, over real gRPC** - `internal/node/`, `test/integration/`, `raft/readindex_test.go`

- `TestProcessesServeReadsAndWrites`: three real processes, the real
  `quorumctl` put and get, then every process killed with
  TerminateProcess/SIGKILL and restarted, and the value is still there.
- `TestPartitionedLeaderRefusesRead`: a leader cut off from the majority, which
  still believes it leads, answers `NO_QUORUM` after the majority has
  overwritten the key.
- `TestAmbiguousRetryAppliesOnce`: the leader applies a write and dies before
  answering; the retry with the same seq gets `duplicate = true`, and the
  write was applied exactly once.
- `TestNotLeaderRedirect`: 2 attempts from a follower, then 1 per write.
- `TestLinearizabilityUnderFaults`: five clients, one read-only, for 8s of
  leader kills and partitions. Porcupine finds 5,000 to 10,000 operations per
  run linearizable.
- `TestNoopCommittedOnElection`: 387 elected terms in randomized schedules,
  each opened by a no-op.
- ReadIndex unit tests: quorum required, the term's first commit awaited,
  reads abandoned on step-down.
- goleak on `internal/node` and `grpcx`: no goroutine outlives a stopped node.

**Snapshots and compaction** - `internal/testutil/snapshot_test.go`, `internal/storage/snapshot_test.go`, `internal/statemachine/`, `raft/snapshot_test.go`

- `TestSnapshotAtThreshold`: crossing the threshold writes a `.snap` file and
  deletes `000001.log`; below it, neither happens.
- `TestSnapshotRestoreIsIdentical`: crash every node, restart from snapshot
  plus a 17-entry WAL tail, and each state-machine hash (data and sessions) is
  identical to before the crash.
- `TestFarBehindFollowerGetsSnapshot`: `MsgInstallSnapshot` counted at the
  transport, at least three 16-byte chunks, on a clean and a lossy network,
  and the follower ends with the leader's state hash.
- `TestSessionsSurviveSnapshot`: a write that survives only in the snapshot's
  session table is still recognized as a duplicate after a full-cluster
  restart, with `duplicate = true` and the original applied index.
- `TestCrashDuringSnapshot`: five crash points in `SaveSnapshot` on the real
  WAL, each recovering to exactly the state before or after the snapshot and
  then accepting further writes.
- `TestWipedNodeCatchesUp`: a node whose data directory is deleted comes back
  empty and is caught up by snapshot to the leader's exact state.
- `TestRandomizedTrialsUpholdSafetyWithSnapshots`: the randomized trials with
  compaction every 15 entries. About 1,600 snapshots taken and 277 installed,
  in memory and on disk, all invariants checked after every tick.
- `TestNoSendBeforeSync` snapshot variants: about 137,000 messages audited
  across about 2,200 snapshot saves.
- `TestWALAgreesWithMemStorageThroughSnapshots`, `TestRefusedRecoveryReleasesTheLog`,
  and core unit tests for chunk reassembly, Figure 13, stale snapshots and
  `Compact`'s refusals.

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
| SnapshotFidelity | a restore that flips one bit: *node 2 reached state 5984...308 at index 20 by restoring a snapshot, but node 1 reached 5984...309 there by applying* |
| Snapshot restore check | a restore that drops a key (`TestRestoreCheckCatchesALossyRestore`); its first run was **not** caught, see BUGS.md |
| InstallSnapshot count | a follower only a few entries behind must see zero (`TestSnapshotCountIsZeroForANearbyFollower`) |
| Session-survival check | a snapshot without its session table: *node 1 applied the retry of (client 2, seq 5) at index 25 as new (status STATUS_SESSION_EXPIRED)* |
| Snapshot crash check | segments deleted before the pointer was durable (`TestSnapshotCrashCheckCatchesMisorderedWrites`) |
| Durability audit, snapshots | an acknowledgement sent before its snapshot was durable: *acknowledging index 9 with only 0 entries durable* |
| Compaction-aware checkers | hand-built violations in logs that start at different indices (`TestCheckerHandlesCompactedLogs`) |
| Stale-read check | `MutationReadWithoutQuorum`: *the partitioned leader answered k="v1" after the majority wrote v2* |
| Applied-once check | a naive retry under a new seq: *client 2's write was applied 2 times (and recognized as a retry 0 times)* |
| Linearizability, end to end | the full workload against `MutationReadWithoutQuorum` must be `Illegal`; its first run was **not**, see BUGS.md |
| Linearizability model | a hand-built stale read must be `Illegal`, and the corrected history `Ok` |
| No-op-per-term check | a log where a term opens with a command, including at a snapshot boundary |
| Fault-state race check | the pre-fix transport under `-race`: six data-race reports |

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
- **Phase 4 built the key-value state machine** (`internal/statemachine`),
  which PLAN.md lists under phase 5. Criterion 4, dedup surviving a snapshot,
  cannot be tested against anything less. The gRPC service and client library
  are untouched and remain phase 5.
- **Two more format additions, both new field numbers** accepted by
  `buf breaking`: `WalSnapshotPointer` gained `hard_state` and `entries`, so
  recovery can start from the pointer alone, and `InstallSnapshotResponse`
  gained `metadata`. Also new: `quorum.kv.v1.StateMachineSnapshot` and its
  parts. DESIGN.md §10, phase 4, has the reasoning for all of it.
- **A Ready that installs a snapshot costs more than one fsync** (the file,
  the pointer, then the batch). They are counted apart, in
  `WALStats.SnapshotFsyncs`, so "one fsync per Ready with durable state" still
  holds exactly for `Fsyncs`.
- **The synchronization list changed in phase 5.** DESIGN.md §1 expected the
  WAL handle, the status snapshot and the proposal registry to need locks. In
  the event only the status snapshot does (an atomic pointer), plus the
  transport's fault state (one mutex). The loop owning every request is what
  freed the registry. CLAUDE.md §3.3 and DESIGN.md §1 are updated.
- **`internal/node` was added**, not in the original layout, so the binary and
  the in-process tests run one assembly.
- **The linearizability workload has a read-only client** on purpose, and the
  test comment says why.
- **`TestWipedNodeCatchesUp` carries a caveat**: a wiped node forgets its vote.
  The test is safe because no election is in progress when it returns. Nothing
  claims wiping is safe in general; DESIGN.md §10.

---

## Open items

Nothing is blocking phase 6. Carried forward:

- **No CheckQuorum.** A partitioned leader keeps believing it leads. It
  correctly refuses reads and cannot commit writes, but its unconfirmed reads
  queue in the core until it hears a higher term. Availability, not safety;
  DESIGN.md §10.
- **Session expiry is not implemented.** Sessions are never collected, so
  `SESSION_EXPIRED` means "never registered". Registration is not
  deduplicated either; a retried registration costs a log entry.
- **Faults can only be injected in-process.** The admin API that lets
  `quorumctl` partition or freeze a running process is phase 6.
- **`http_port` serves nothing yet.** It is configured and validated for phase
  7's status endpoint.
- **Snapshots live in memory.** The core keeps the latest image to send, and a
  follower builds an incoming one in memory. Fine for a demo-sized store.
- **The WAL keeps a copy of the log after the snapshot in memory**, which the
  pointer record needs. Bounded by compaction.
- **No compaction margin.** A follower only slightly behind gets a snapshot.
  Correct; a throughput tuning, out of scope.
- **Mid-segment media corruption in the last segment** is indistinguishable
  from a torn tail and is truncated. Outside the crash-fault model; DESIGN.md
  §2.
- **`Mutation` ships in production code** so the negative controls can reach it
  from another package. Fenced by `TestZeroConfigIsUnmutated` and
  `TestEveryMutationIsDistinct`; reasoning in DESIGN.md §10.
- **The claim-to-test traceability table** (DESIGN.md §7) now has 64 rows
  filled. Phase 6 requires every guarantee in §6 to have one.
- **golangci-lint's first run after a dependency change can exceed its 5m
  timeout** on a cold cache; the second is seconds. Not a code problem.
- **No CI runner is configured.** `make ci` passes locally.

---

## Next: phase 6

**Goal.** Every claim about behaviour under failure is backed by a test that
would fail if the claim were false.

Full acceptance criteria: [PLAN.md](PLAN.md).

What phase 5 left in place:

1. `internal/node` runs a full replica; `test/integration` already builds the
   binaries, writes a config on free ports, starts processes and kills them
   with `Process.Kill`. `internal/supervisor` can grow out of that code.
2. `grpcx.Transport` already implements directed partitions and heal. The
   admin API needs to expose them, and freeze, which parks `server.Loop`,
   still has to be built.
3. The Porcupine model, history recorder and read-only-client workload are in
   `internal/node/linearizability_test.go`. `TestChaosSeeded` needs them
   against real processes, so they will want to move somewhere shared.

Watch for:

- DESIGN.md §6's "Durability of acknowledged writes across process crashes"
  and "A minority partition cannot commit" both need real-process tests in
  this phase, not only simulator ones.
- Freeze is cooperative (CLAUDE.md §8). The admin call must park the loop, and
  the docs must not call it `SIGSTOP`.

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

### Phase 4 - 2026-09-30

Implemented log compaction end to end. In the core: `Compact`, chunked,
resumable `InstallSnapshot` and Figure 13's install rule. In storage:
`SaveSnapshot` for both the WAL and `MemStorage`, recovery from the last
snapshot pointer, and deletion of superseded segments and files. In the driver:
compaction at a threshold, and snapshot installation ahead of the Ready's other
durable writes. Built the key-value state machine and its session table,
earlier than planned, because criterion 4 needs the real thing.

Three format additions, all new field numbers, and the reasoning for each is
in DESIGN.md §10. The pointer record carries the log tail and hard state, so
segment deletion order stops mattering. The snapshot acknowledgement names its
snapshot. The state-machine snapshot has a schema.

The invariant checkers compared logs by position, which compaction breaks.
They now compare by index, and gained SnapshotFidelity, which compares every
replica's state hash at every applied index.

Found three real bugs, all recorded in BUGS.md with regression tests observed
failing first:

- recovery leaked the tail segment's file handle when it refused to start
  (a phase 3 defect, caught by Windows refusing to delete an open file);
- the restore-identity test could not detect lost data, caught by its negative
  control not failing;
- snapshot sending mistook "past the end of the log" for "compacted", caught
  by the phase 2 mutation controls.

No consensus or storage-correctness bug was found. BUGS.md records that too,
and why it counts for something.

Verified: `make ci` green, `buf lint` and `buf breaking` clean,
`make proto-check` clean with `gen/` staged, and every new check observed
failing on a deliberately broken input.

### Phase 5 - 2026-09-30

Built the client-facing system. ReadIndex went into the core, with
confirmation rounds carried in the `read_context` field reserved in phase 1.
Then `server.Loop`, the goroutine driving the core against the real clock and
owning every pending request. Then the gRPC transport with directed fault
injection, the KV service, the client library, and `internal/node` to
assemble a replica. `quorum-node` now serves, and `quorumctl put`/`get` work
against a real three-process cluster. goleak arrived with the first
goroutines.

One design change, recorded in DESIGN.md §1 and §10 and in CLAUDE.md §3.3: the
synchronized places in production code are now the status snapshot and the
transport's fault state. The proposal registry needed no lock once the loop
owned it.

Found two real bugs, both in BUGS.md with regression tests observed failing
first:

- a data race in the transport's fault state, found by `make race`: a map read
  as a function argument, before the lock that guards it;
- the linearizability workload could not produce a stale read from a
  partitioned leader, found by its end-to-end negative control not failing.
  A read-only client fixed it.

No consensus or storage bug was found; BUGS.md says so and why it counts.

Verified: `make ci` green, `buf lint` and `buf breaking` clean, the real-time
tests repeated three times under `-race` without a failure, and every new check
observed failing on a deliberately broken input.
