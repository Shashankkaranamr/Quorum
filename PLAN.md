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

**Current status: phase 1 complete.**

| Phase | Title | Status |
|---|---|---|
| 1 | Foundations and design | ✅ complete |
| 2 | Core consensus: election and log replication | not started |
| 3 | Crash-safe persistence | not started |
| 4 | Snapshotting and log compaction | not started |
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

## Phase 2 — Core consensus: election and log replication

**Goal.** A correct Raft state machine, in memory, with a deterministic
simulator and invariant checkers that have been proven capable of failing.

**Deliverables.** `raft/` (node, log, election, replication, Ready);
`internal/transport/` interface and `inmem/`; `internal/testutil/` harness,
invariant checkers and mutation fixtures; `internal/pbconv/`.

**Acceptance criteria**

1. **Purity holds.** `TestRaftCoreImportAllowlist` still passes with the full
   algorithm implemented: no `time`, `sync`, `os`, `net`, `context` or
   `math/rand` in package `raft`. Election jitter comes from an injected seed.
2. **Elections converge.** For cluster sizes 3 and 5, over **1000 seeds**, a
   cold-start cluster elects exactly one leader within
   `10 × election_timeout_max` ticks. `TestElectionConverges` reports the seed
   of any run that does not.
3. **All five safety invariants are checked after every simulated step**, not
   just at the end of a scenario: `TestElectionSafety`, `TestLeaderAppendOnly`,
   `TestLogMatching`, `TestLeaderCompleteness`, `TestStateMachineSafety`.
4. **The checkers have teeth.** `TestMutatedRaftTripsInvariant/*` runs
   deliberately broken Raft variants — a node that votes twice in a term, a
   leader that truncates its own log, a commit rule that counts itself twice —
   and **fails if the corresponding checker does not catch them**. A checker
   that cannot fail is not evidence.
5. **Replication works.** `TestProposalsCommitAndApply`: entries proposed on the
   leader are committed and applied in identical order on every live node.
6. **Leader failure is survivable.** `TestLeaderFailoverPreservesCommitted`:
   kill the leader mid-scenario; a new leader is elected and every entry that
   was committed before the kill is present in the new leader's log.
7. **A minority cannot commit.** `TestMinorityPartitionCannotCommit`: partition
   2 of 5 nodes; proposals on the minority side never reach `applied` on any
   node; the majority side continues committing.
8. `make race` is clean. `go.uber.org/goleak` reports no leaked goroutines in
   driver tests.

---

## Phase 3 — Crash-safe persistence

**Goal.** A node that restarts never loses or contradicts what it already
agreed to, and the fsync boundary is auditable.

**Deliverables.** `internal/storage/` WAL, framing codec, recovery; the
`Storage` implementation behind the driver; fsync metrics.

**Acceptance criteria**

1. **The codec round-trips and survives garbage.** `TestWALCodecRoundTrip` plus
   `FuzzWALDecode`: the decoder must never panic, never return a record it did
   not fully read, and never accept a record whose CRC does not match.
2. **A torn tail yields a prefix.** `TestWALTruncationAtEveryOffset`: write a
   known WAL, truncate it at **every byte offset**, and require that recovery
   returns a clean prefix of the original records every single time — never a
   partial record, never an error that loses valid earlier records.
3. **Nothing is sent before it is durable.** `TestNoSendBeforeSync`: a storage
   test double records call order and fails the test if any message in a Ready
   is handed to the transport before that Ready's `Sync()` returned.
4. **One fsync per Ready.** `TestOneFsyncPerReady`: the fsync counter increments
   exactly once per processed Ready batch, regardless of how many entries it
   contains.
5. **A restart never double-votes.** `TestRestartDoesNotDoubleVote`: crash a
   node after it grants a vote but before the term advances; on restart it must
   refuse a second vote in the same term.
6. **Whole-cluster restart loses nothing.** `TestClusterRestartRecoversCommitted`:
   crash all nodes, restart them all, and require every entry that was committed
   before the crash to still be committed and applied.

---

## Phase 4 — Snapshotting and log compaction

**Goal.** History is compacted so a lagging or rejoining node catches up quickly
instead of replaying everything.

**Deliverables.** Snapshot creation and installation; `InstallSnapshot`
handling; WAL segment reclamation; state machine snapshot/restore including the
session table.

**Acceptance criteria**

1. **Snapshots fire and reclaim space.** `TestSnapshotAtThreshold`: crossing
   `snapshot_threshold_entries` produces a snapshot file and **deletes** the WAL
   segments it supersedes.
2. **Restore is exact.** `TestSnapshotRestoreIsIdentical`: restarting from
   snapshot plus WAL tail produces a state machine whose hash is identical to
   the one that was running before the restart.
3. **A lagging follower really uses InstallSnapshot.** `TestFarBehindFollowerGetsSnapshot`:
   advance the leader past the follower's compacted next index, and assert the
   `InstallSnapshot` message type was actually sent — not merely that the
   follower eventually converged.
4. **Deduplication survives compaction.** `TestSessionsSurviveSnapshot`: take a
   snapshot, restart from it, replay a duplicate `(client_id, seq)`, and require
   it to be deduplicated with `duplicate = true`.
5. **A crash mid-snapshot is recoverable.** `TestCrashDuringSnapshot`: crash
   between writing the `.snap` file and committing the `WalSnapshotPointer`, and
   between the pointer and segment deletion. Both must recover to a consistent
   state.
6. **A wiped node rejoins.** `TestWipedNodeCatchesUp`: delete a node's data
   directory entirely, restart it, and require it to catch up via snapshot
   transfer and match the others.

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
