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
  happens will be recorded here either way.

- **A write-ahead log recovery bug during phase 3.** None found. The decoder
  was fuzzed for 2.2 million executions, a real WAL was recovered after
  truncation at every byte offset, and every node in the randomized safety
  trials ran on a real WAL through repeated crashes. As with phase 2, that is a
  claim about what the tests found, and it is only worth anything because each
  check was separately shown to fail on a deliberately broken input: three
  broken decoders, a recovery with its truncation removed, storage that forgets
  its vote, and storage that loses a committed entry.
