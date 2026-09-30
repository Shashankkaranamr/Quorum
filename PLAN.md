# Quorum — 8-Phase Roadmap

This file is the contract for what "done" means at each step. Each phase lists
its deliverables and a set of acceptance criteria that are either a named test
or an observable behaviour someone else could check.

Criteria are written to be falsifiable. "Leader election works" is not an
acceptance criterion; "3- and 5-node clusters elect exactly one leader within
`10 × election_timeout_max` ticks across 1000 seeds, and `TestElectionSafety`
fails if two leaders ever hold the same term" is.

A phase is done when every criterion passes and `make ci` is green. Nothing from
a later phase is started before that.

> **This file defines the destination. [PROGRESS.md](PROGRESS.md) records where
> we actually are** — what is built, what is verified, and what the next concrete
> steps are. Check it before starting work.

**Current status: complete — all eight phases.**

| Phase | Title | Status |
|---|---|---|
| 1 | Foundations and design | ✅ complete |
| 2 | Core consensus: election and log replication | ✅ complete |
| 3 | Crash-safe persistence | ✅ complete |
| 4 | Snapshotting and log compaction | ✅ complete |
| 5 | gRPC KV service, linearizable reads, deduplication | ✅ complete |
| 6 | Fault-injection suite and bug log | ✅ complete |
| 7 | Live cluster visualizer | ✅ complete |
| 8 | Integration, documentation and demo | ✅ complete |

---

## Phase 1 — Foundations and design ✅

**Goal.** Decide everything that constrains later phases, and scaffold a repo
that builds, tests and lints from a clean clone.

**Deliverables.** `DESIGN.md` answering the five design questions; this file;
the package tree; build tooling; `README.md`; `BUGS.md`; protobuf schemas for
the log entry format and the KV contract.

**Acceptance criteria**

1. `DESIGN.md` answers all five design questions — language and concurrency,
   persistence, dual-mode transport, linearizability, topology — and contains an
   explicit "what this will and will not guarantee" section naming Byzantine
   faults, static membership, localhost-only scope and the fsync/hardware
   caveat.
2. `make build`, `make test` and `make lint` all pass on a clean clone with only
   the Go toolchain installed. Generated protobuf code is checked in, so no
   protobuf tooling is needed to build or test.
3. `make test` runs at least two **non-vacuous** tests: the consensus-core
   import allowlist, and cluster configuration validation. The allowlist test
   has been observed to fail when `import "time"` is added to package `raft`.
4. The log entry format (`EntryType`, `Entry`, `Command`) and the KV
   request/response contract (`Status`, `LeaderHint`, `client_id`/`seq`) are
   fixed in `.proto` files, including `ENTRY_TYPE_NOOP` for ReadIndex and a
   reserved `ENTRY_TYPE_CONFIG`.
5. `BUGS.md` exists with the entry format decided: date, symptom, root cause,
   fix, and the regression test that now guards it.
6. Initial commit is authored solely by the repository owner and pushed to
   `github.com/Shashankkaranamr/Quorum`.

---

## Phase 2 — Core consensus: election and log replication ✅

**Goal.** A correct Raft state machine, in memory, with a deterministic
simulator and invariant checkers that have been proven capable of failing.

**Delivered.** `raft/` (types, config, log, node, election, replication);
`internal/transport/` interface and `inmem/`; `internal/testutil/` harness,
checkers and mutation fixtures; `internal/pbconv/`. Plus two additions to the
original scope, both consistent with DESIGN.md and recorded in §10 there:
`internal/storage` (interface + `MemStorage`) and `internal/server` (the shared
Ready-processing function and the `tick_lag` metrics), which phase 3 needs
anyway and which make the durability ordering testable from the fast suite.

**Acceptance criteria**

1. ✅ **Purity holds.** `TestRaftCoreImportAllowlist` passes with the full
   algorithm implemented, and now also asserts it is scanning the real files
   rather than a stub. `TestRaftCoreHasNoConcurrencyPrimitives` additionally
   rejects `go`, `select` and channel types, which no import check can see.
2. ✅ **Elections converge.** `TestElectionConverges`: 1000 seeds at n=3 and
   n=5. Worst observed 38 ticks (n=3) and 25 ticks (n=5) against a bound of
   200. The test reports the worst case so the margin is measured, not assumed.
3. ✅ **All five safety invariants are checked after every simulated step**, by
   `testutil.Checker`, plus `CommittedEntriesAreStable` and `WellFormed`.
   `TestRandomizedTrialsUpholdSafety` runs 200 trials per cluster size with
   randomized loss, delay, duplication, reordering, partitions and crashes.
4. ✅ **The checkers have teeth**, in two layers:
   `TestMutatedRaftTripsInvariant` breaks one Raft rule at a time and requires
   the matching checker to fire; `TestCheckerDetectsHandBuiltViolations` feeds
   each checker a synthetic violating history, covering the ones no single
   mutation reaches. Every checker has now been observed to fail for the right
   reason, and to accept a healthy history.
5. ✅ **Replication works.** `TestProposalsCommitAndApply` asserts identical
   apply ORDER, not merely identical contents.
6. ✅ **Leader failure is survivable.** `TestLeaderFailoverPreservesCommitted`.
7. ✅ **A minority cannot commit.** `TestMinorityPartitionCannotCommit` strands
   the OLD LEADER in the minority, which is the case that actually proves
   something, then heals and requires full reconvergence — including an
   assertion that the minority was genuinely behind first, so the test cannot
   pass vacuously.
8. ✅ `make race` is clean (59s). ⬜ **goleak is deferred** (to phase 3, and
   then again to phase 5). It is vacuous here: this phase has no goroutines at
   all, by design. It becomes meaningful when the real driver goroutine exists.

**Also delivered beyond the original criteria**

- `TestFigure8CommitRule` constructs the Figure 8 scenario exactly and asserts
  both that the correct rule refuses to commit an old-term entry on a majority,
  and that removing the rule loses a committed entry.
- `TestLivenessBoundUnderTransientFaults`: a leader within 150 ticks on a lossy
  but unpartitioned network; worst observed 51 ticks.
- `TestTickLagIsMeasured`: the `tick_lag` instrumentation phase 6 depends on,
  exercised against storage configured to cost logical ticks per fsync.
- `TestFailureMessagesAreLazy`: a regression guard for a real bug found this
  phase (BUGS.md, 2026-09-23).

---

## Phase 3 — Crash-safe persistence ✅

**Goal.** A node that restarts never loses or contradicts what it already
agreed to, and the fsync boundary is auditable.

**Delivered.** `internal/storage/`: the framing codec (`codec.go`), the
segmented write-ahead log with recovery (`wal.go`), and fsync counters and
latency (`WALStats`). The simulator can now run any node on a real WAL
(`testutil.WALStorageFactory`), so every crash test below exercises real
recovery from disk rather than a model of it. One format addition, recorded in
DESIGN.md §10: `WalEntryBatch` gained a `hard_state` field, so each Ready is
exactly one CRC-protected record.

**Acceptance criteria**

1. ✅ **The codec round-trips and survives garbage.** `TestWALCodecRoundTrip`
   covers every record type alone and back to back. `FuzzWALDecode` holds the
   decoder to its contract at every record boundary of its input -- never
   panic, never consume bytes on error, never return a record it did not read
   in full, never accept a CRC mismatch (recomputed independently) -- and ran
   2.2 million executions over two minutes with no failure.
   `TestWALCodecRejectsEveryBitFlip` is the deterministic complement.
   Negative control: `TestDecodeContractCatchesBrokenDecoders` points the
   contract at three deliberately broken decoders and requires each to fail.
2. ✅ **A torn tail yields a prefix.** `TestWALTruncationAtEveryOffset`
   truncates a real 8-record WAL at every one of its 254 byte offsets. Each time
   recovery must return exactly the whole records before the tear, report
   exactly the bytes it cut, and accept a new write that then survives a second
   recovery -- which is what proves the torn bytes were really removed.
   Negative control: `TestTruncationCheckDistinguishesEveryPrefix` requires
   every prefix to recover to a distinct state (otherwise a lost record would
   be invisible) and the check to reject the neighbouring prefix on both sides
   at every offset. Separately, making recovery skip the truncation was
   observed to fail the test at byte 4.
3. ✅ **Nothing is sent before it is durable.** `TestNoSendBeforeSync` wraps
   every node's storage and transport in an audit that checks each outbound
   message, at the instant it is handed over, against what that node had made
   durable: a granted vote needs the vote on disk, an acknowledged index needs
   the entries on disk, any message needs its term on disk. It runs across
   randomized schedules with crashes and restarts, 40 seeds on the simulator's
   storage and 3 on the real WAL per cluster size: about 138,000 messages.
   Negative control: `TestDurabilityAuditCatchesMisorderedDrivers` processes a
   real Ready in two wrong orders and requires the audit to flag both, and
   processes a correct Ready spanning two terms and requires it not to.
4. ✅ **One fsync per Ready -- refined, see below.**
   `TestOneFsyncPerReady` measures every Sync individually on a real WAL: the
   driver makes exactly one Sync per Ready; a Ready carrying entries or hard
   state costs exactly one fsync, including one carrying 500 entries; a Ready
   carrying neither costs none.
   *Refinement, flagged rather than silently applied:* the criterion as written
   says the fsync counter increments exactly once per Ready. A Ready that
   carries only messages or committed entries -- every leader heartbeat -- has
   nothing to make durable, and fsyncing an unchanged file for it would add a
   disk flush to every heartbeat for no durability at all. The WAL skips it.
   What the test asserts is therefore "exactly one Sync per Ready, and exactly
   one fsync per Ready that has anything durable in it, never more".
5. ✅ **A restart never double-votes.** `TestRestartDoesNotDoubleVote`, on the
   real WAL: grant a vote, crash, restart from disk, and a second candidate in
   the same term is refused. It also crashes inside the fsync, where the vote
   never became durable and so, by the ordering rule, was never sent.
   Negative control: `TestDoubleVoteCheckCatchesAmnesia` runs the same scenario
   on storage that forgets its hard state and requires the double vote to be
   caught.
6. ✅ **Whole-cluster restart loses nothing.**
   `TestClusterRestartRecoversCommitted`: 12 runs (3 and 5 nodes, 6 seeds
   each) on real WALs build a history with message loss and individual
   crashes, then crash every node at once and restart them all from disk. Every
   committed entry (84 to 116 per run) is re-applied identically on every node,
   and the cluster then commits new entries. Negative control:
   `TestRestartCheckCatchesLostCommittedEntries` runs it on storage that loses
   the newest committed entry in recovery, and requires the loss to be caught.

**Also delivered beyond the original criteria**

- `TestRandomizedTrialsUpholdSafetyOnDisk`: phase 2's randomized safety trials
  with every node on a real WAL, so every simulated crash is followed by real
  recovery. All invariants hold.
- `TestWALAgreesWithMemStorage`: a differential test that the simulator's crash
  model and the real log agree exactly after reopening, including overwrites
  of uncommitted suffixes and segment rolls.
- `TestWALRefusesDamageThatIsNotATornTail`: damage in a closed segment, a
  missing segment, or a record with a valid CRC that this binary cannot read
  all stop the node from starting, rather than being truncated away.
- A real bug in the new ordering audit, found by the randomized run and
  recorded in BUGS.md (2026-09-27).

**Not done, and why.** goleak moves again, to phase 5. This phase added no
goroutines: the WAL is owned by the driver and the driver is still stepped by
the simulator. The goroutine loop needs a transport that can deliver inbound
messages, which is phase 5's gRPC transport.

---

## Phase 4 — Snapshotting and log compaction ✅

**Goal.** History is compacted so a lagging or rejoining node catches up quickly
instead of replaying everything.

**Deliverables.** Snapshot creation and installation; `InstallSnapshot`
handling; WAL segment reclamation; state machine snapshot/restore including the
session table.

**Delivered.** In the core: `Node.Compact`, chunked `InstallSnapshot` with
resumable, stop-and-wait transfer, and Figure 13's install rule (`raft/`). In
storage: `SaveSnapshot` on both the WAL and `MemStorage`; recovery that starts
from the last snapshot pointer; reclamation of superseded segments and
snapshot files (`internal/storage/`). In the driver: compaction at a threshold,
and snapshot installation as step 0 of the Ready (`internal/server/`). The
replicated key-value store with its session table and a deterministic snapshot
encoding (`internal/statemachine/`). A new invariant, SnapshotFidelity, in the
simulator. Format additions and design decisions are in DESIGN.md §10, phase 4.

**Acceptance criteria**

1. ✅ **Snapshots fire and reclaim space.** `TestSnapshotAtThreshold`, on real
   WALs with 1 KiB segments. Below the threshold there is no snapshot file on
   any node and segment `000001.log` still exists. After crossing it, each node
   holds exactly one `.snap` file, named for its snapshot index, and
   `000001.log` has been **deleted**, as `WALStats.SegmentsDeleted` confirms.
2. ✅ **Restore is exact.** `TestSnapshotRestoreIsIdentical`. A key-value
   history with a session table is compacted at index 30 with a 17-entry log
   tail. Every node is crashed and restarted, isolated so nothing new commits,
   and given one step to restore the snapshot and replay the tail. Its
   state-machine hash, covering data and sessions, must equal the hash from
   before the crash. The test also asserts each node really restored from its
   snapshot rather than replaying from index 1. Negative control:
   `TestRestoreCheckCatchesALossyRestore` drops one key on restore and must be
   caught. Its first run was not caught, which exposed a weakness in the
   workload (BUGS.md, 2026-09-30).
3. ✅ **A lagging follower really uses InstallSnapshot.**
   `TestFarBehindFollowerGetsSnapshot` isolates a follower until the leader has
   compacted past its whole log, then heals. It counts `MsgInstallSnapshot`
   messages actually **handed to the transport** addressed to that follower.
   With 16-byte chunks there must be at least the three one snapshot needs. It
   also checks the follower's driver installed one and its state machine was
   restored, and that every node ends with the same state hash. It runs on a
   clean network and on one with 10% loss, 10% duplication and reordering.
   Negative control: `TestSnapshotCountIsZeroForANearbyFollower` requires the
   count to stay at zero for a follower that is only a few entries behind.
4. ✅ **Deduplication survives compaction.** `TestSessionsSurviveSnapshot`. A
   client writes, and another client's traffic pushes the compaction point past
   that write, so it survives only in the snapshot's session table. The whole
   cluster restarts from snapshots. The client retries with the same
   `(client_id, seq)`. Every node must return `duplicate = true` with the
   original applied index and leave the value unchanged. Negative control:
   `TestSessionCheckCatchesASessionlessSnapshot` leaves the session table out of
   the snapshot, and the retry must be caught being applied as new.
5. ✅ **A crash mid-snapshot is recoverable.** `TestCrashDuringSnapshot` crashes
   the real WAL in five places:
   - between the `.snap` file and the pointer;
   - between the pointer and segment deletion;
   - mid-way through the pointer write, leaving a torn record;
   - part-way through deletion, leaving a gap;
   - mid-way through the `.snap` file.

   Each must recover to exactly the state before the snapshot or exactly the
   state after it, depending only on whether the pointer was durable. Each must
   remove what the crash left behind, and must accept a write that then
   survives a second recovery. Negative control:
   `TestSnapshotCrashCheckCatchesMisorderedWrites` builds the disk a snapshot
   would leave if it deleted segments before its pointer was durable, and
   requires the check to reject it.
6. ✅ **A wiped node rejoins.** `TestWipedNodeCatchesUp` deletes a node's data
   directory entirely, while it is down, on real disk. After restart it comes
   back with an empty log and term 0. It must receive InstallSnapshot, persist
   the snapshot, and end with exactly the leader's applied index and state hash.
   Caveat, in the test and in DESIGN.md §10: a wiped node forgets its vote,
   which this test does not claim is safe in general.

**Also delivered beyond the original criteria**

- `TestRandomizedTrialsUpholdSafetyWithSnapshots`: the phase 2 randomized
  trials with compaction every 15 entries and 16-byte chunks, in memory and on
  disk. In the full run about 1,600 snapshots were taken and 277 installed from
  a leader, under loss, duplication, partitions and crashes. Every invariant,
  SnapshotFidelity included, was checked after every tick.
- The SnapshotFidelity invariant: every replica's state-machine hash, compared
  at every applied index, whether the replica applied or restored its way
  there. Negative control: `TestSnapshotFidelityCatchesACorruptTransfer`, a
  restore that flips one bit.
- `TestNoSendBeforeSync` now also runs with snapshots on: about 137,000
  messages audited across about 2,200 snapshot saves. Negative control:
  `TestDurabilityAuditCatchesAnEarlySnapshotAck`.
- `TestWALAgreesWithMemStorageThroughSnapshots`: the phase 3 differential test,
  extended with local snapshots, installed snapshots whose last entry the log
  may not hold, and reopening from disk at random points.
- `TestCheckerHandlesCompactedLogs`: the checkers compare logs by index now.
  Hand-built violations in offset logs must still be caught, and healthy
  compaction must not be flagged.
- One real defect in recovery's error path, found and fixed with a regression
  test (BUGS.md, 2026-09-30).

**Not done, and why.** goleak is still deferred to phase 5, for the reason
given under phase 3. The key-value **state machine** was built here because
criterion 4 cannot be met without it. The **gRPC service** that proposes into
it, and the client library, are phase 5. Snapshots are held in memory, not
streamed from disk; see DESIGN.md §10.

---

## Phase 5 — gRPC KV service, linearizable reads, deduplication ✅

**Goal.** GET and PUT over gRPC that are linearizable, and retries that cannot
double-apply.

**Deliverables.** `internal/kvservice/`, `internal/statemachine/`,
`internal/transport/grpcx/`, `internal/client/`, real listeners in
`cmd/quorum-node`, working `quorumctl put`/`get`.

**Delivered.**
- ReadIndex in the core (`raft/`), with confirmation rounds carried in
  `read_context`.
- `server.Loop`, the driver goroutine against the real clock. It owns every
  pending request, so the proposal registry needs no lock.
- `grpcx`: one stream per directed link, with directed fault injection.
- `kvservice`, the client library, and `internal/node`, which assembles a
  replica.
- A real `quorum-node` and `quorumctl put`/`get`.

The state machine itself was built in phase 4. Design changes, including the
revised synchronization list, are in DESIGN.md §10.

**Acceptance criteria**

1. ✅ **A real cluster serves reads and writes.**
   `TestProcessesServeReadsAndWrites` (`test/integration/`) builds both
   binaries and starts three `quorum-node` processes on free localhost ports.
   The real `quorumctl put k v` then `quorumctl get k` returns `v`, and a
   missing key exits non-zero. It then kills every process with
   TerminateProcess/SIGKILL, restarts them from their data directories, and
   the value is still there.
2. ✅ **A no-op is committed on every election.** `TestNoopCommittedOnElection`
   runs 60 randomized simulator schedules with crashes, partitions, loss and
   compaction, covering 387 elected terms. In every log on every node, the
   first entry of every term is a no-op, including a term that starts exactly
   at a snapshot boundary. After the cluster settles, the leader has committed
   in its own term. Negative control: `TestNoopCheckCatchesATermWithoutOne`.
   Unit tests pin the mechanics ReadIndex builds on it:
   `TestReadIndexWaitsForTheTermsFirstCommit`, `TestReadIndexWaitsForAQuorum`
   and `TestReadsAreAbandonedOnStepDown`.
3. ✅ **A partitioned leader will not serve a stale read.**
   `TestPartitionedLeaderRefusesRead`, on real gRPC:
   - k=v1 is written, then the leader is cut off in both directions.
   - The majority elects a new leader and writes k=v2.
   - The test asserts the old leader **still believes it leads**, then reads
     k from it directly. The answer must be `STATUS_NO_QUORUM`; an OK fails
     the test.

   Negative control: `TestStaleReadCheckCatchesAQuorumlessRead` runs the same
   scenario with `MutationReadWithoutQuorum`, and the old leader answers
   `v1`.
4. ✅ **Retry after an ambiguous failure applies exactly once.**
   `TestAmbiguousRetryAppliesOnce`. A fault hook makes the leader apply the Put
   and then fail the RPC, and the test kills that leader. The client library
   retries with the same seq, reaches the new leader, and gets `duplicate =
   true`. The node that answered applied the client's write exactly once.
   Negative control: `TestAppliedOnceCheckCatchesANaiveRetry` retries under a
   new seq, and the write is caught being applied twice.
5. ✅ **Redirect is cheap and correct.** `TestNotLeaderRedirect`. A write to a
   follower returns `STATUS_NOT_LEADER`, with a hint naming the leader's id and
   its dialable address, and is not applied. A client whose first guess is a
   follower needs exactly 2 attempts, then 1 for each of the next 20 writes.
6. ✅ **Histories are linearizable.** `TestLinearizabilityUnderFaults`:
   - Five concurrent clients, one of them read-only, work on three keys.
   - For 8 seconds the leader is repeatedly killed and restarted, or cut off
     and healed.
   - Each run records 5,000 to 10,000 operations. Porcupine checks the history
     against a sequential map and finds it linearizable.

   Negative controls: `TestLinearizabilityCatchesQuorumlessReads` runs the
   same workload against `MutationReadWithoutQuorum`, and Porcupine must call
   it illegal. `TestLinearizabilityCheckerRejectsAStaleRead` checks the model
   on a hand-built history. The end-to-end control first failed to fail,
   which exposed a blind spot in the workload (BUGS.md, 2026-09-30).

**Also delivered beyond the original criteria**

- goleak on the node and transport packages. Every test fails if a node's
  loop, its peer goroutines or its gRPC server outlive it. This was deferred
  from phase 2 until the first goroutine existed.
- `TestPartitionIsDirectedAndHeals`: a one-way partition cuts one direction
  only, and healing restores it.
- `TestFaultStateIsSafeToChangeUnderTraffic`, the regression test for a real
  data race found by `make race` (BUGS.md, 2026-09-30).

**Not done, and why.**
- **The admin API**, which lets `quorumctl` and the visualizer inject faults
  into a running process, is phase 6. Until then only in-process tests call
  `Partition` and `Heal`.
- **Session expiry** is not implemented. Sessions are never collected, so the
  exactly-once caveat about expiry does not arise yet (DESIGN.md §10).
- **CheckQuorum** is not implemented. A partitioned leader keeps believing it
  leads, and correctly refuses to serve, but its unconfirmed reads queue in the
  core until it hears a higher term. That affects availability, not safety.

---

## Phase 6 — Fault-injection suite and bug log ✅

**Goal.** Every claim about behaviour under failure is backed by a test that
would fail if the claim were false.

**Deliverables.** `internal/supervisor/`, `internal/admin/`,
`test/integration/`, the fault-injection subcommands of `quorumctl`, and real
entries in `BUGS.md`.

**Delivered.**
- `internal/supervisor`: spawns real `quorum-node` processes, records their
  PIDs, kills with TerminateProcess/SIGKILL, and confirms the PID is gone
  before reporting success. It refuses to act on a recorded PID that now
  belongs to another program.
- `internal/admin`: the Admin API from phase 1's proto. Status comes from the
  loop's published snapshot, so a frozen node still answers. Directed
  `BlockLinks`, `Heal`, `Freeze` with optional auto-thaw, and `Thaw`
  reporting ticks missed.
- `server.Loop` freeze and thaw, which are cooperative, not SIGSTOP.
- A real fsync p99.
- Every `quorumctl` subcommand: `up`, `down`, `status`, `kill`, `start`,
  `freeze`, `thaw`, `partition` (both ways, or `-oneway`) and `heal`.
- `test/integration`: a real-process harness and the suite below.
- `internal/testutil/lincheck`: the shared Porcupine model and workload.

**Acceptance criteria**

1. ✅ **The harness can inflict real faults on real processes.**
   `TestHarnessInflictsRealFaults`, on three real processes, checks each fault
   directly:
   - Kill: the PID is asserted alive before and gone after, and the admin API
     stops answering.
   - Restart: the node comes back with at least the log it had on disk.
   - Bidirectional partition: blocked peers show in status, and the
     partitioned node's commit index stands still while the others commit.
   - One-way partition: the leader can hear but not be heard. The followers
     elect a new leader, and the old one steps down because it **hears** the
     new term, which proves the other direction still works.
   - Freeze: status still answers and says frozen; the process is alive; the
     commit index stands still. Thaw reports the time frozen and ticks missed,
     and a bounded freeze thaws itself.

   `TestQuorumctlDrivesTheCluster` runs the same operations through the real
   `quorumctl` binary.
2. ✅ **Killing the leader mid-write loses nothing.**
   `TestAckedWritesSurviveLeaderKill`. Four writers run continuously and the
   leader process is killed. A new leader appears within the bound (about
   200 ms observed, against a 5 s bound). All of the roughly 2,000
   acknowledged writes are read back with their values. Negative control:
   `TestAckedCheckCatchesLostData` wipes every data directory, and the check
   reports 20 of 20 writes lost.
3. ✅ **A minority refuses writes; a majority continues; healing reconciles.**
   `TestMinorityPartitionRefusesWrites`, on five processes. The minority,
   holding the old leader, answers a write with an error, never a success. The
   majority commits ten writes. After healing, all five logs have the same last
   index and term, fully committed, with identical tails, and the refused
   write is nowhere. Negative control for the log comparison:
   `TestLogMatchCheckCatchesDivergence`.
4. ✅ **Rolling restart stays available.** `TestRollingRestart`. Each node is
   killed and restarted in turn while three clients write and read. Zero
   operations failed; the slowest put-and-get took about 700 ms. Every one of
   the roughly 2,300 acknowledged writes survived, checked with the same
   verifier as criterion 2.
5. ✅ **Seeded chaos stays linearizable.** `TestChaosSeeded`. Each seed runs
   8 s of randomized faults against a real cluster, with five clients, one of
   them read-only: kill and restart, isolation, one-way cuts, freezes, and
   leader isolation. The history is checked with the Porcupine model and
   workload whose negative controls run in `internal/node`. There are 3 seeds
   by default; a 12-seed soak recorded about 93,000 operations, all
   linearizable. Replay a seed with `QUORUM_CHAOS_SEED=n`; the seed fixes the
   schedule, not the scheduler (DESIGN.md §10).
6. ✅ **The traceability table is complete.** DESIGN.md §7 opens with a table
   mapping each §6 guarantee to its tests. `TestEveryGuaranteeNamesExistingTests`
   fails if a guarantee has no row or a named test does not exist, and
   `TestTraceabilityCheckCatchesGaps` is its negative control. The OS-crash
   half of the durability guarantee cannot be tested here, and its row says so.
7. ✅ **`BUGS.md` has real entries.** Ten across the project, each with a
   regression test observed failing first. Two came from this phase:
   - a restarted node forced an election, because two reconnect backoffs
     outlasted the election timeout;
   - the tick-lag metric could not see a stalled loop, and logical time ran
     slow under load.

**Also delivered beyond the original criteria**

- Under `make race`, the node processes are built with the race detector too,
  and a race report in any node's log fails the test.
  `TestNodesAreRaceCheckedUnderRace` checks the build flags, so the log scan
  cannot pass vacuously.
- `TestRestartedFollowerDoesNotDisrupt`, `TestTickLagSeesASlowDisk` and
  `TestTickLagIsZeroOnAFastDisk`: the regression tests for this phase's bugs.
- `TestSupervisorIgnoresAStrangersPID`: a PID file naming a live process that
  is not quorum-node, in this case the test itself, is ignored and not killed.

**Not done, and why.** The `http_port` still serves nothing; the visualizer's
backend is phase 7. The status stream it will read, `WatchStatus`, is built and
tested (`TestWatchStatusStreamsLiveState`, which also checks that it keeps
streaming while the node is frozen).

---

## Phase 7 — Live cluster visualizer ✅

**Goal.** Behaviour under failure is something you can watch, not just claim.

**Deliverables.** `cmd/quorum-viz/` (supervisor + SSE aggregator), `web/`
frontend served from `embed.FS`, `WatchStatus` streaming.

**Delivered.**
- `internal/viz`, the backend. It fans in every node's `WatchStatus`
  stream, rebuilds one picture of the cluster every 100 ms, and pushes it to
  browsers over server-sent events. It turns a fixed table of POST routes into
  supervisor, admin and KV calls. It takes no locks: one goroutine owns the
  picture, another runs controls one at a time.
- `web/`: vanilla JS and CSS embedded in the binary. It shows a topology with
  each directed link drawn separately, node cards with log tails aligned by
  index, a partition builder, and an event timeline.
- `cmd/quorum-viz`, and `make viz`.
- Admin status gained `blocked_outbound` and `blocked_inbound`, so the UI can
  draw a one-way cut. These are new field numbers; `buf breaking` is clean.
- Partition, heal, freeze and thaw moved onto the supervisor, so `quorumctl`,
  the visualizer and the tests share one implementation.

**Acceptance criteria**

1. ✅ **Live state per node.** `TestVizShowsLiveState`, on real processes at 3
   and 5 nodes, reads only the SSE stream a browser reads. It requires each
   node's role, term, leader, commit, applied and log tail. A write made
   through the page must appear committed in every node's log tail within
   500 ms of returning: observed 145–195 ms.
2. ✅ **"Kill node" is a real kill.** `TestVizKillIsRealAndRestartRecovers`
   presses kill over HTTP, then checks with the operating system, not the
   page, that the PID is gone. "Start" brings back a new process with at least
   the log it had on disk.
3. ✅ **"Partition network" is a real transport-level cut.**
   `TestVizShowsDirectedPartitions` makes a one-way cut from the page. It
   confirms the cut on the node's own admin API: outbound blocked, inbound
   not. The stream must show exactly that one directed link down. It then
   checks a two-way cut and a heal.
4. ✅ **An election is legible.** `TestVizMakesElectionsLegible` kills the
   leader from the page. The stream shows it down within 15 ms, then a new
   leader in a higher term about 520 ms after the kill. That time is mostly
   the election itself; the page shows the new leader within one refresh of
   the cluster having one. The old leader stays shown as down, with its last
   report marked stale, and the election is on the timeline.
5. ✅ **The UI cannot corrupt the cluster.** This holds by structure: the nodes
   are separate processes, reachable only over RPC.
   - `TestVizCannotReachRaftState` fails if `internal/viz`, `cmd/quorum-viz`
     or `web` ever depends on a package holding node state. Its negative
     control, `TestNodeStateCheckFlagsANodeAssembly`, must flag
     `internal/node`.
   - `TestRoutesAreExactlyTheDocumentedControls` pins every route and the
     call it makes.
   - `TestControlsRefuseCrossSiteRequests`: other web pages cannot press the
     buttons.
6. ✅ **Works at 3 and 5 nodes.** `TestShapeFollowsTheConfig` loads the
   repository's own `cluster.yaml` and `cluster-5.yaml` and gets 3 and 5 nodes
   with 6 and 20 directed links. Criterion 1's test runs live at both sizes.

**Also checked by eye.** The page was opened in Chrome against a real
three-node cluster from `cluster.yaml`. It showed "live", the leader and term,
and all six directed links. Node labels were then moved outward from the ring,
because the top node's label overlapped its links.

**Not done, and why.** There are no automated browser tests. Every behaviour
above is tested at the HTTP and SSE layer, which is what the page consumes.
The rendering is checked by eye only.

---

## Phase 8 — Integration, documentation and demo ✅

**Goal.** A stranger can clone the repo, run it, and evaluate the claims.

**Deliverables.** Finalized README with architecture diagram and traceability
table; recorded walkthrough; reconciled DESIGN.md; `make ci` green end to end.

**Acceptance criteria**

1. ✅ **Two commands on a clean machine.** `make viz` brings up a cluster and
   the visualizer; `make faults` (new this phase) runs the full real-process
   fault suite. Both are in the README with their `make.ps1` equivalents. Both
   were run from a fresh `git clone` of the committed tree: `make viz` built
   everything, started three nodes, elected a leader and served the page;
   `make faults` passed all 16 real-process tests in 151 s. *The limit of that
   check, stated plainly:* it ran on the development machine (Windows 11), so
   Go was installed and its module cache warm. It is a fresh clone, not a
   fresh machine, and Linux and macOS were not tried.
2. ✅ **`make ci` is green**: format check, lint, build, the whole test suite
   including the integration suite, and all of it again under the race
   detector, with the node processes themselves race-built.
3. ✅ **The README carries the evidence**: an architecture diagram, the full
   guarantees and non-guarantees with the honesty constraints, and DESIGN.md
   §7's complete claim-to-test tables. `TestReadmeCarriesTheTraceabilityTable`
   fails if the README's copy drifts from DESIGN.md's, with a negative
   control, `TestReadmeCheckCatchesAStaleCopy`.
4. ✅ **A recorded walkthrough**, [docs/walkthrough](docs/walkthrough/README.md):
   eight captioned frames of the visualizer on a real five-node cluster,
   driven only by the page's own buttons. They show a normal election, a
   partition in which the minority refuses a write while the majority
   commits, a heal that reconciles every log, and a leader kill followed by
   recovery. It is frames rather than video, because a video export would
   have been a browser download, which needs explicit permission. Recording it
   found a real bug; the frames are from the re-recording after the fix.
5. ✅ **`BUGS.md` is finalized**: eleven real entries, five of them in
   production code, each naming a regression test observed failing before its
   fix, with an index table at the top.
6. ✅ **DESIGN.md is reconciled with reality.** Every body statement the
   implementation had made false is corrected in place, marked *As built*,
   pointing to §10, which records every divergence phase by phase. §9 gained
   the deferrals that arose during implementation: CheckQuorum, session
   expiry, and browser tests.

**Also delivered**

- A client liveness fix. A stale leader's follower redirected the client back
  to the stale leader, trapping writes in a minority. The walkthrough found
  it; `TestClientEscapesAStaleMinority` guards it. `TestChaosSeeded` gained a
  five-node run that isolates pairs of nodes.
- `http_port` removed. Nothing ever listened on it.
- `Client.Pin`, and "via node N" writes in the visualizer, to show a
  minority's refusal honestly.
