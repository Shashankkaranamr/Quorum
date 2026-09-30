# Bug log

Every bug that testing actually found in Quorum, and what now stops it coming
back.

This file exists because "it works" and "I could not find a way to break it" are
different claims, and only the second one is worth anything in a distributed
system. A bug log with real entries is evidence that the fault-injection suite
does something. A bug log that is empty at the end of the project would mean the
tests were not trying hard enough.

**Two rules for this file:**

1. **Nothing is invented.** Entries record what testing found. If a bug that
   seemed likely never occurred, that is recorded as a note rather than
   fabricated as an entry.
2. **Every entry names a regression test.** A bug that was fixed but is not
   guarded is not finished. The test must fail against the old code — if it
   passes before the fix, it is testing the wrong thing.

Entries are newest first.

---

## Entry format

Each entry uses exactly these fields:

````markdown
## YYYY-MM-DD — One-line summary

**Phase:** which phase was being built
**Severity:** safety violation | liveness / availability | correctness of tests | operational

**Symptom observed.**
What was actually seen, before anything was understood about the cause: the
failing test and its seed, the log excerpt, the timing. Written as an
observation, not a diagnosis.

**Root cause.**
What was actually wrong, and — the part worth writing down — why it was not
obvious. The value of this file is in the gap between the symptom and the cause.

**Fix.**
What changed, in one or two sentences, with the commit or file reference.

**Regression test.**
The named test that now guards this, and confirmation it fails against the
pre-fix code. A test that passes both before and after is not a regression test.

**What it says about the design.**
Optional. Whether this was a slip or a sign the design was wrong somewhere.
````

---

## 2026-09-30 — Restarting any node forced an election

**Phase:** 6
**Severity:** liveness / availability

**Symptom observed.**
While logging per-node metrics at the end of `TestAckedWritesSurviveLeaderKill`,
the restarted node had started 6 elections in the few seconds it had been back.
A probe then restarted one follower of a healthy three-node cluster. In both
runs the term went from 10 to 12 and the leader was re-elected. Nothing was
wrong with the leader; the restart alone cost the cluster an election.

**Root cause.**
A restarted follower that hears nothing from the leader for an election
timeout (200–400 ms in the test config) campaigns, and its higher term deposes
the leader. The leader was sending heartbeats every 60 ms. They did not arrive,
because two reconnect backoffs, stacked, were longer than the election
timeout:

- `grpcx`'s own peer goroutine, which waited up to 500 ms between reconnect
  attempts;
- gRPC's `ClientConn` underneath it. Once its server has gone away, it redials
  on its own schedule, **starting at 1 s** and growing towards two minutes.
  While it waits, every stream attempt fails at once.

The first fix capped only the first of these, at one heartbeat interval. The
regression test still failed three runs out of three. The gRPC default was the
larger delay, and it was out of sight: nothing in this code sets it.

**Fix.**
Both are capped at one heartbeat interval: `grpcx.Options.MaxBackoff`, which
the node sets from its config, and `grpc.WithConnectParams` on every peer
connection (`internal/transport/grpcx/grpcx.go`). Config validation keeps the
election timeout at least three heartbeats, so a restarted node now hears the
leader well inside it.

**Regression test.**
`TestRestartedFollowerDoesNotDisrupt` restarts a follower three times and
requires the same leader in the same term afterwards. Before any fix: failed 3
of 3 runs (`term 9 -> 11`, and `deposed leader 1`). With only the transport's
own cap: still failed 3 of 3. With both caps: passed 3 of 3, nine restarts with
no election.

**What it says about the design.**
Safety was never at risk. An election is always safe, which is why nothing in
phases 2–5 noticed: every test passed through the extra elections. It took a
counter, `elections_started`, and someone reading it. The design does name
pre-vote as the protocol-level defence against a returning node disrupting the
cluster (§9, deferred). The actual trigger here was simpler: a library default
two orders of magnitude slower than the protocol it was carrying.

---

## 2026-09-30 — The tick-lag metric could not see a stalled loop

**Phase:** 6
**Severity:** observability, and a slow clock under load

**Symptom observed.**
The same metrics log reported `worst tick lag 0 ticks` on every node of a
cluster under constant write load, across thousands of fsyncs. A metric whose
job is to show the loop falling behind reported that it never had.
`TestTickLagSeesASlowDisk` puts a real loop on storage whose every fsync takes
five ticks. It reported lag 0, and only 16 ticks processed in 0.75 s, when
about 150 were due.

**Root cause.**
The loop counted one logical tick per `time.Ticker` event. A `Ticker` does not
queue: when its receiver is busy, it holds at most one pending tick and drops
the rest. So however long an fsync blocked the loop, one tick was waiting
afterwards, never more, and the lag the driver computes (ticks waiting minus
one) was structurally zero. There was a second consequence: logical time,
which drives election and heartbeat timers, silently ran slow whenever the
loop was busy. That is the opposite of what DESIGN.md §1 describes, a slow
fsync delaying ticks, measurably.

The simulator had it right. It counts ticks per step and makes a slow sync
cost ticks explicitly, which is why `TestTickLagIsMeasured` passed from phase 2
on. The real loop simply did not share that path.

**Fix.**
The ticker now only wakes the loop. The number of ticks is computed from the
wall clock, as ticks due since the last one, and each is delivered to the
driver (`internal/server/loop.go`). Time spent frozen is deliberately not
replayed; that behaviour is unchanged.

**Regression test.**
`TestTickLagSeesASlowDisk`. Against the pre-fix loop: `worst tick lag 0 ticks,
16 ticks processed`, failed; this was run and observed. After the fix it
reports lag of 4–9 ticks and 60 ticks processed in 0.3 s, which is correct.
`TestTickLagIsZeroOnAFastDisk` is its other half: on fast storage the lag must
stay at most 1, so the metric cannot pass by always reporting lag.

**What it says about the design.**
This is the measurement DESIGN.md §1 promised for the one hazard the
architecture admits: one goroutine owns ticks and fsyncs, so a slow disk
delays elections. It existed from phase 2, and in the real loop it could never
have fired. It is the same lesson as the negative-control entries, applied to
a metric: a number nobody has seen move is not evidence that nothing happened.

---

## 2026-09-30 — The transport read its fault state outside its lock

**Phase:** 5
**Severity:** correctness of fault injection (a data race)

**Symptom observed.**
The first `make race` run with the phase 5 suite reported ten `WARNING: DATA
RACE` reports. All were in `TestLinearizabilityUnderFaults` and its negative
control. Every one paired a read in `grpcx.Transport.Send`, `Stream`,
`peer.run` or `peer.stream` with a write in `Transport.Heal`, called from the
test's fault-injection goroutine. Without `-race`, every test had passed.

**Root cause.**
The injected-fault state is two maps, of blocked outbound and blocked inbound
links, guarded by one mutex. The check was written as
`t.isBlocked(t.blockedIn, m.From)`, with `isBlocked` taking the lock and
looking the id up. Go evaluates the argument `t.blockedIn` before the call, so
the map field was read **before** the lock was taken. `Heal` replaces both maps
under the lock. A stream handler could therefore read the field while `Heal`
was writing it. The lock was taken in every function that touched the maps, and
the read still happened outside it.

This is the first lock in production code, and this project's architecture was
chosen specifically to keep locks out of the consensus core (DESIGN.md §1).
The bug is not in the core, and it is not the predicted stall. It is still a
bug in the one place a lock was needed.

**Fix.**
`isBlocked` takes a direction instead of a map, and selects the map inside the
lock (`internal/transport/grpcx/grpcx.go`).

**Regression test.**
`TestFaultStateIsSafeToChangeUnderTraffic` sends messages both ways between two
transports while repeatedly partitioning and healing. Against the pre-fix code,
under `-race`, it fails with six data-race reports; this was run and observed.
After the fix it is clean, and so are three further `-race` runs of the whole
node suite. It can only fail under the race detector, which `make ci` runs.

**What it says about the design.**
It supports keeping the list of shared state short. The mutex was in the right
place, and every access went through a helper that took it. The helper's
signature still let a read escape. Five lines of guarded state produced a real
race, which is a fair measure of what a lock around the whole Raft state would
have cost. It also shows why `make ci` includes `make race`: nothing else would
have found this.

---

## 2026-09-30 — The linearizability workload could not produce a stale read

**Phase:** 5
**Severity:** correctness of tests

**Symptom observed.**
`TestLinearizabilityCatchesQuorumlessReads` failed on its first run: Porcupine
judged the history **linearizable**. The test runs the same workload as
`TestLinearizabilityUnderFaults`, against a cluster whose ReadIndex skips the
quorum confirmation, with the leader repeatedly partitioned, and requires the
checker to find a stale read. Lengthening the partitions from under a second to
1.5s changed nothing: 10,869 operations, 5 partitions, verdict `Ok`, twice.

**Root cause.**
In the workload, every client both read and wrote. When the leader was cut off,
each client's next write stalled on it until the 500ms request timeout, then
moved on to the new leader. All of them stalled on their first write after the
cut and all left at about the same moment. After that, nobody was reading from
the old leader. A stale read needs a write to complete on the new leader while
some client still reads from the old one, and this workload never arranged
that. So the main test could not have detected stale reads from a partitioned
leader, which is the failure ReadIndex exists to prevent. It was passing, but
not for that reason.

**Fix.**
Client 0 only reads. A read-only client stays with the node it believes leads
for as long as that node answers, as real read-mostly clients do. The same
workload runs in both tests.

**Regression test.**
`TestLinearizabilityCatchesQuorumlessReads` itself. With the read-only client it
returns `Illegal` on every run (three of three observed, about 11,000
operations each), while `TestLinearizabilityUnderFaults` still returns `Ok`
against correct ReadIndex. The hand-built control,
`TestLinearizabilityCheckerRejectsAStaleRead`, passed throughout. That is why it
is not enough on its own: it proves the checker, not the workload.

**What it says about the design.**
This is the third time a negative control has caught a test that could not fail
(see phase 2's reconvergence entry and phase 4's restore entry). The pattern is
consistent: the checker was fine and the workload never produced the failure.
An end-to-end control, which injects a real bug and runs the real workload, is
worth more than a hand-built history.

---

## 2026-09-30 — A refused recovery kept the log's tail segment open

**Phase:** 4 (the defect dates from phase 3)
**Severity:** operational

**Symptom observed.**
The first run of `TestWALRefusesAMissingOrDamagedSnapshot` passed its
assertions and then failed in cleanup, on both subtests:

```
TempDir RemoveAll cleanup: unlinkat ...\wal\000005.log: The process cannot
access the file because it is being used by another process.
```

The test had already checked that `OpenWAL` refused to start, and it held no
handle of its own.

**Root cause.**
Recovery opens the last segment for appending (`openTail`) as it reaches it,
because that is where it truncates a torn tail. Some of its checks run after
that point: the commit index against the last recovered entry (phase 3), and
now the snapshot file a pointer names. When one of those refused, `OpenWAL`
returned the error and dropped the `WAL` — with the tail segment's file still
open. On Windows an open file cannot be deleted, so the leaked handle pinned the
directory. It is the directory an operator would want to move aside to
investigate a node that refused to start.

The phase 3 path had the same leak. No test reached it, because nothing
produced a recovered commit index beyond the log except deliberate damage, and
the damage tests all failed before reaching the tail.

**Fix.**
`OpenWAL` closes the tail segment if recovery fails after opening it
(`internal/storage/wal.go`).

**Regression test.**
`TestRefusedRecoveryReleasesTheLog` makes recovery refuse after the tail is
open, then deletes the log directory. Against the pre-fix code it fails with
`the refused log still holds a file open`; this was run and observed. The check
depends on the platform: it catches the leak only where an open file cannot be
deleted, which includes Windows, the development platform. Elsewhere it passes
either way, and says so in its comment.

**What it says about the design.**
Opening the tail inside the recovery loop keeps recovery to a single pass, and
that is still the right call. What was missing was treating "recovery has side
effects" as something that needs cleanup on every exit path. It was caught by
accident, and only because Windows is stricter about open files.

---

## 2026-09-30 — The snapshot-restore check could not detect lost data

**Phase:** 4
**Severity:** correctness of tests

**Symptom observed.**
`TestRestoreCheckCatchesALossyRestore` failed on its first run with `An error
is expected but got nil`. It is the negative control for
`TestSnapshotRestoreIsIdentical`. It restores every snapshot with its last key
removed, and requires the hash comparison to notice. The comparison did not
notice.

**Root cause.**
The scenario wrote 45 values across 13 keys, compacted at index 30 and left a
17-entry log tail. The tail rewrote every one of the 13 keys. So a restore that
lost a key had it put back by the replay, and by the time the hashes were
compared the damage was gone. The positive test was passing, but it could not
have failed for the class of bug it exists to catch: a snapshot that silently
drops data.

**Fix.**
The first 25 writes now go to keys that are never written again, so they exist
only in the snapshot, and the tail churns a separate set of keys. A comment at
the workload says why its shape matters.

**Regression test.**
The negative control itself. Against the old workload it passes vacuously, as
observed above. Against the new one it catches the lossy restore with differing
hashes (`57be9591... before the restart, 9b34d9be... after`), and
`TestSnapshotRestoreIsIdentical` still passes.

**What it says about the design.**
Nothing about snapshots, which were correct. It is the same lesson as the
phase 2 reconvergence entry, and the reason every check here gets a negative
control: the workload a check runs against decides what it can see. This one
could only see damage that nothing afterwards overwrote.

---

## 2026-09-30 — Snapshot sending treated "entry missing" as "entry compacted"

**Phase:** 4
**Severity:** correctness of tests

**Symptom observed.**
The first full test run after adding snapshot transfer panicked in
`TestMutatedRaftTripsInvariant`:

```
raft: node 4 must send node 2 a snapshot but holds none (log starts at 1)
```

No test involving snapshots had failed. The panic came from the phase 2
negative controls.

**Root cause.**
`sendAppend` decided to send a snapshot whenever the log could not report a term
for `prevLogIndex`. There are two reasons that can happen. One is that the
entry is below the snapshot, which is what the new code meant. The other is
that the entry is past the end of the log. In correct Raft a leader's nextIndex
never passes its own last index plus one. `MutationLeaderTruncatesOwnLog` makes
a leader drop its own entries, so nextIndex ends up past the end. The phase 2
code had quietly rewound in that case; the new code sent a snapshot the node
did not have.

This affects only a deliberately broken leader. But if the control had died on
a panic instead of reporting `LeaderAppendOnly`, the proof that that checker has
teeth would have gone with it.

**Fix.**
`sendAppend` clamps nextIndex to the end of the log before looking up the
previous term, so "missing" can only mean "compacted"
(`raft/replication.go`).

**Regression test.**
`TestMutatedRaftTripsInvariant/a_leader_truncating_its_own_log_breaks_Leader_Append-Only`,
which panicked before the fix and now reports `LeaderAppendOnly` as it did in phase 2.

**What it says about the design.**
One `ok == false` covered two conditions with different remedies. The negative
controls found it because they are the only tests that build states correct
Raft never reaches. That is a reason to keep them running with every other test
rather than behind a build tag (DESIGN.md §10, phase 2).

---

## 2026-09-27 — The durability audit flagged a correct Ready that spanned two terms

**Phase:** 3
**Severity:** correctness of tests

**Symptom observed.**
The first full run of `TestNoSendBeforeSync` failed one variant (simulator
storage, five nodes) on seed 32 of 40:

```
node 1 sent RequestVoteResp 1->2 t14 granted=true:
  granting a vote that is not durable (durable term=18 vote=3)
```

Seeds 0 to 31 had passed before it (the test stops at the first failure, so
the remaining seeds of that variant did not run), and the other three
variants passed in full, including every seed on the real write-ahead log.
Every Raft safety invariant held on seed 32 itself.

**Root cause.**
The audit's rule for a granted vote was "the durable hard state names this term
and this candidate". That is the property stated one message at a time — but the
unit of durability is not the message, it is the Ready.

One Ready can span several steps. Node 1 granted candidate 2 in term 14; then,
before its next Ready was processed, it received a term-18 message and voted for
candidate 3. Only the final hard state, term 18 and vote 3, is written. The
term-14 grant was never durable on its own, and it does not need to be: the
whole batch is durable before any of it is sent, so the disk holds exactly what
a node that persisted after every single step would hold. A node whose disk says
term 18 can never act in term 14 again, so its term-14 promise cannot be
contradicted.

The rule confused "the promise is in the durable state" with "the durable state
is at or beyond the promise". It took a randomized schedule to find because the
triggering Ready needs two vote requests from different terms to arrive at one
node between two consecutive Ready cycles.

While fixing it I also found, by reading rather than by a failing test, that the
harness replaced a node's audit when the node restarted. Any violation recorded
before a crash would have been dropped from the final check. The audits of every
life are now kept, and the test requires that at least one restart happened.

**Fix.**
A message stamped with a term older than the node's durable term is covered by
that durable term. The structural checks — nothing sent during a Sync, nothing
sent with writes still buffered — still apply to every message. See
`durabilityAudit.checkSend` in `internal/server/driver_test.go`.

**Regression test.**
`TestDurabilityAuditCatchesMisorderedDrivers`, subtest *correct order, one Ready
spanning two terms*, builds this exact Ready deterministically: a vote request
in term 1, then one in term 2, then a single Ready. Against the pre-fix audit it
fails with `granting a vote that is not durable (durable term=2 vote=3)`; this
was run and observed before the fix went in. The same test's other subtests
require the fixed audit to still catch two genuinely misordered drivers.

**What it says about the design.**
Nothing is wrong with Raft or the driver; this was a checker that was too
strict. It is still worth recording, for two reasons. It is the argument for why
a batched Ready is safe at all, written down where the next person will find it.
And the tempting "fix" in the other direction — make the driver persist after
every step so that the per-message rule holds — would have been a real
performance regression, driven by a wrong test. A false positive from a safety
checker deserves the same scrutiny as a true one.

---

## 2026-09-23 — Failure-message arguments evaluated on every passing assertion

**Phase:** 2
**Severity:** correctness of tests

**Symptom observed.**
`TestRandomizedTrialsUpholdSafety` took 42s for 400 trials, which would have
made `make race` unusable in CI. Nothing was failing; it was simply far slower
than 500 logical ticks across five in-memory nodes should ever be.

**Root cause.**
Failure messages were written as
`require.Truef(t, ok, "...%s", c.Describe())`. Go evaluates a function-call
argument eagerly, so `Describe()` rendered every node's role, term, indices and
log tail on **every passing assertion**, not just on failure. One of those calls
sat inside the per-index loop comparing logs after each trial, so it ran
hundreds of thousands of times.

A CPU profile settled it immediately: `testutil.summarize` was 48.6% of total
samples, with `fmt.(*pp).doPrintf` beneath it at 38.9%. Guessing had already
sent me to optimize the invariant checkers twice, which bought 42s → 33s. The
profile found the other 26s in one step.

**Fix.**
`Cluster` now implements `fmt.Stringer`, and tests pass `c` rather than
`c.Describe()`. The value is cheap to pass; `fmt` only calls `String()` when the
message is actually formatted, which is only on failure. Runtime went 33.5s →
6.8s, a 5x improvement with no loss of diagnostic detail.

**Regression test.**
`TestFailureMessagesAreLazy` (internal/testutil/mutation_test.go). `Cluster`
counts how many times it has been rendered; the test runs a passing scenario and
requires the count to be zero. It fails against the pre-fix code.

**What it says about the design.**
Nothing about Raft, and everything about how easy it is to make a test suite
expensive enough that people stop running it. Also a reminder that profiling
beats guessing: two rounds of plausible-looking optimization moved 20% of the
problem, and the profiler found the remaining 80% in one command.

---

## 2026-09-23 — A reconvergence assertion that could pass without reconverging

**Phase:** 2
**Severity:** correctness of tests

**Symptom observed.**
`TestMinorityPartitionCannotCommit` logged `reconverged 1 ticks after heal`.
One tick to re-elect, catch up two lagging nodes and agree on a commit index is
not impossible, but it is fast enough to be suspicious.

**Root cause.**
The test healed the partition and then waited for every node to agree with the
leader on term and commit index. It never established that they *disagreed*
first. Had the minority happened to already match the majority — or had a later
change to the setup made the partition ineffective — the condition would have
been true on entry and the test would have passed having verified nothing. This
is the same failure mode as an assertion that is accidentally always true: the
test stays green precisely when it stops testing.

**Fix.**
The test now asserts, immediately before healing, that at least one minority
node differs from the majority leader in term or commit index, and logs what it
found. It also asserts `Net.Stats.Partitioned` is non-zero, so a partition that
silently stopped dropping messages fails rather than passes.

With the guard in place the divergence is real and logged:
`minority node 2 is behind at heal time: term 1 commit 6 vs majority term 2
commit 8`. The one-tick reconvergence was genuine — the new leader broadcasts a
single AppendEntries carrying everything the followers are missing.

**Regression test.**
The guard is inside `TestMinorityPartitionCannotCommit` itself
(internal/testutil/consensus_test.go): removing the partition setup now fails
the test at the divergence check instead of passing at the convergence check.

**What it says about the design.**
A test that cannot fail is worth nothing, and "it passed suspiciously fast" is
a signal worth chasing. This is the same principle the invariant checkers are
built on, applied to the scenario tests: every negative control in this project
exists because a check nobody has watched fail is not evidence.

---

## Notes on predictions not yet borne out

*Kept so that the difference between "expected and found" and "expected and not
found" stays visible.*

- **Any Raft safety violation during phase 2.** None. The five safety
  invariants are checked after every simulated tick across 400 randomized
  trials with message loss, delay, duplication, reordering, partitions and
  crashes, and the first complete run passed. That is a weaker statement than
  it sounds: it says the checkers found nothing, and the checkers are only
  worth anything because each has been separately shown to catch a deliberately
  broken implementation (`TestMutatedRaftTripsInvariant`,
  `TestCheckerDetectsHandBuiltViolations`). Both bugs found this phase were in
  the tests, not in the consensus code. Recorded here rather than left implied,
  because "we found no bugs" and "our tests found no bugs" are different claims
  and only the second one is true.

- **A locking bug causing multi-second election stalls.** Anticipated at the
  outset as likely. The architecture chosen in
  [DESIGN.md §1](DESIGN.md#1-language-and-core-libraries) makes that specific
  bug structurally hard to write — the consensus core holds no locks and
  performs no I/O — while relocating the same *symptom* to a different cause:
  head-of-line blocking in the single driver goroutine, where a slow fsync
  delays ticks and can stall an election. The driver's tick-lag metrics
  (`MaxTickLagTicks`, `TotalTickLagTicks`) exist from the first version of that
  loop, and phase 3 added fsync latency (`WALStats.FsyncMax`, `FsyncTotal`),
  specifically so this is measurable rather than mysterious. Whether it actually
  happens will be recorded here either way. *Phase 5:* the real driver loop now
  exists, and no election stall has been observed. That covers roughly 25,000
  client operations under leader kills and partitions at a 10ms tick, and a
  real three-process cluster. The project's first production lock **did** have
  a bug: a data race in the transport's fault-injection state (2026-09-30,
  above). It was a race, not a stall, and it was not in the consensus code.
  *Phase 6:* the measurement itself turned out to be blind in the real loop
  (2026-09-30, above). Now that it works, `TestTickLagSeesASlowDisk` shows the
  predicted symptom on demand: a five-tick fsync produces four or more ticks of
  lag. What has still not been observed is a stall from anything short of an
  injected slow disk. Phase 6 did find two election-related defects, both above:
  restarts forcing elections, and a slow clock under load. Neither was a lock.

- **A write-ahead log recovery bug during phase 3.** None found. The decoder
  was fuzzed for 2.2 million executions, a real WAL was recovered after
  truncation at every byte offset, and every node in the randomized safety
  trials ran on a real WAL through repeated crashes. As with phase 2, that is a
  claim about what the tests found, and it is only worth anything because each
  check was separately shown to fail on a deliberately broken input: three
  broken decoders, a recovery with its truncation removed, storage that forgets
  its vote, and storage that loses a committed entry. *Addendum, phase 4:*
  recovery did turn out to have one defect, but an operational one, not a
  correctness one. It leaked a file handle when it refused to start
  (2026-09-30, above). It never recovered a wrong state.

- **A linearizability or durability violation under real-process chaos in
  phase 6.** None. Twelve seeds of `TestChaosSeeded`, about 93,000 operations
  under kills, isolations, one-way cuts, freezes and leader isolations, were
  all linearizable. Every acknowledged write survived leader kills and a
  rolling restart. Under `make race` the node processes themselves run with
  the race detector, and none reported a race. The bugs this phase found were
  in availability and observability, not safety.

- **A snapshot or compaction bug in the consensus code during phase 4.** None
  found. With compaction every 15 entries and snapshots sent in 16-byte chunks,
  the randomized trials took about 1,600 snapshots and installed 277 from a
  leader, through loss, duplication, partitions and crashes, in memory and on
  disk. Every invariant was checked after every tick, including the new
  SnapshotFidelity check, which compares every replica's state-machine hash at
  every index. The durability audit checked about 137,000 messages across
  about 2,200 snapshot saves. All three bugs found this phase were in tests or
  error handling. As in earlier phases, this is only evidence because each new
  check was shown to fail on a deliberately broken input: a corrupting restore,
  a lossy restore, a snapshot without sessions, an acknowledgement sent before
  its snapshot was durable, and segments deleted before their pointer.
