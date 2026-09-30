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

**Current status: phase 4 complete.**

| Phase | Title | Status |
|---|---|---|
| 1 | Foundations and design | ✅ complete |
| 2 | Core consensus: election and log replication | ✅ complete |
| 3 | Crash-safe persistence | ✅ complete |
| 4 | Snapshotting and log compaction | ✅ complete |
| 5 | gRPC KV service, linearizable reads, deduplication | not started |
| 6 | Fault-injection suite and bug log | not started |
| 7 | Live cluster visualizer | not started |
| 8 | Integration, documentation and demo | not started |

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

## Phase 5 — gRPC KV service, linearizable reads, deduplication

**Goal.** GET and PUT over gRPC that are linearizable, and retries that cannot
double-apply.

**Deliverables.** `internal/kvservice/`, `internal/statemachine/`,
`internal/transport/grpcx/`, `internal/client/`, real listeners in
`cmd/quorum-node`, working `quorumctl put`/`get`.

**Acceptance criteria**

1. **A real cluster serves reads and writes.** Three separate OS processes over
   localhost gRPC; `quorumctl put k v` then `quorumctl get k` returns `v`.
2. **A no-op is committed on every election.** `TestNoopCommittedOnElection`
   asserts an `ENTRY_TYPE_NOOP` entry at the start of each term. ReadIndex
   depends on it.
3. **A partitioned leader will not serve a stale read.** `TestPartitionedLeaderRefusesRead`:
   partition the leader from the majority, write a new value on the majority
   side, then read from the old leader. It must return `STATUS_NO_QUORUM` — a
   successful response with the old value fails the test.
4. **Retry after an ambiguous failure applies exactly once.**
   `TestAmbiguousRetryAppliesOnce`: kill the leader after the entry commits but
   before the response is sent; the client retries with the same `seq`; assert
   the value was applied once and the response carries `duplicate = true`.
5. **Redirect is cheap and correct.** `TestNotLeaderRedirect`: a write sent to a
   follower returns `STATUS_NOT_LEADER` with a usable `leader_hint`, and the
   client library succeeds in at most 2 attempts in steady state.
6. **Histories are linearizable.** `TestLinearizabilityUnderFaults`: a
   concurrent randomized workload recorded as a history and checked with
   Porcupine, with leader kills and partitions injected during the run.

---

## Phase 6 — Fault-injection suite and bug log

**Goal.** Every claim about behaviour under failure is backed by a test that
would fail if the claim were false.

**Deliverables.** `internal/supervisor/`, `internal/admin/`,
`test/integration/`, the fault-injection subcommands of `quorumctl`, and real
entries in `BUGS.md`.

**Acceptance criteria**

1. **The harness can inflict real faults on real processes.** Kill
   (`TerminateProcess`/`SIGKILL`), restart against the same data directory,
   partition an arbitrary subset **bidirectionally and one-way**, heal, freeze
   and thaw. Each verified directly — a kill asserts the PID is gone.
2. **Killing the leader mid-write loses nothing.**
   `TestAckedWritesSurviveLeaderKill`: write continuously, kill the leader
   mid-flight, and require that **every write that received a success response**
   is still readable afterwards, and that a new leader is elected within a
   bounded time.
3. **A minority refuses writes; a majority continues; healing reconciles.**
   `TestMinorityPartitionRefusesWrites`: the minority side returns errors rather
   than false successes, the majority keeps committing, and after `heal` the
   minority's log matches the majority's exactly.
4. **Rolling restart stays available.** `TestRollingRestart`: restart every node
   one at a time; no committed data is lost and the cluster serves throughout.
5. **Seeded chaos stays linearizable.** `TestChaosSeeded`: N iterations of a
   randomized fault schedule, each reproducible from its seed, with a Porcupine
   check over the recorded history.
6. **The traceability table is complete.** Every guarantee in DESIGN.md §6 names
   a test that exists and passes. A claim with no test is a defect in this phase.
7. **`BUGS.md` has real entries**, each naming the regression test that now
   guards it. Entries record what testing actually found; nothing is invented to
   match a prediction.

---

## Phase 7 — Live cluster visualizer

**Goal.** Behaviour under failure is something you can watch, not just claim.

**Deliverables.** `cmd/quorum-viz/` (supervisor + SSE aggregator), `web/`
frontend served from `embed.FS`, `WatchStatus` streaming.

**Acceptance criteria**

1. **Live state per node.** Role, term, `commitIndex`, `lastApplied`, leader,
   and a log tail, updating within **500 ms** of a change. Entries visibly
   replicate across nodes.
2. **"Kill node" is a real kill.** The OS process terminates — verified by PID
   absence, not by a UI state change — and "restart" brings it back from its
   existing data directory.
3. **"Partition network" is a real transport-level cut**, and the UI shows which
   directed links are down. One-way partitions are expressible.
4. **An election is legible.** Killing the leader produces a visible term bump
   and a new leader within one refresh, with the old leader shown as
   unreachable.
5. **The UI cannot corrupt the cluster.** All controls go through the admin API;
   there is no path from the browser to Raft state that bypasses it.
6. **Works at 3 and 5 nodes**, driven by `cluster.yaml` and `cluster-5.yaml`
   with no code change.

---

## Phase 8 — Integration, documentation and demo

**Goal.** A stranger can clone the repo, run it, and evaluate the claims.

**Deliverables.** Finalized README with architecture diagram and traceability
table; recorded walkthrough; reconciled DESIGN.md; `make ci` green end to end.

**Acceptance criteria**

1. **Two commands on a clean machine.** One brings up a cluster, one runs the
   full fault suite. Verified from a fresh clone, documented in the README.
2. **`make ci` is green**: format check, lint, build, unit tests, race detector
   and the integration suite.
3. **The README carries the evidence**: architecture diagram, the
   guarantees/non-guarantees section, and the complete claim-to-test
   traceability table.
4. **A recorded walkthrough** of the visualizer showing, at minimum: a normal
   election, a network partition with the minority refusing writes, and a leader
   kill followed by recovery. No live link, no hosting.
5. **`BUGS.md` is finalized** with at least three real entries, each naming its
   regression test.
6. **DESIGN.md is reconciled with reality.** Every place the implementation
   diverged from the original design is noted with what changed and why. A
   design document that was never wrong about anything was not load-bearing.
