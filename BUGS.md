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

Entries are newest first. Real ones start appearing in phase 6, when the
fault-injection suite begins running; see [PLAN.md](PLAN.md).

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

### Worked example

> **This is a template showing the format, not a real bug.** It will be deleted
> when the first real entry is added.

## 0000-00-00 — EXAMPLE: follower accepted an append it had not persisted

**Phase:** 3
**Severity:** safety violation

**Symptom observed.**
`TestClusterRestartRecoversCommitted` failed at seed `0x5f3a91`, roughly 1 run
in 200. A key that had been acknowledged to the client read back as absent after
a whole-cluster restart. The leader's log contained the entry; two of three
followers did not.

**Root cause.**
The driver sent the `AppendEntriesResponse` from the same `Ready` batch that
contained the entries, but the response was constructed before `Sync()` was
called rather than after. The acknowledgement was therefore a promise the node
had not yet kept, and a crash in that window lost the entry while the leader
counted it toward the commit quorum. It was hard to see because the ordering
looked correct in the source: the `Send` call came after the `Sync` call
textually, but the message slice had already been captured.

**Fix.**
Moved message construction after `Sync()` in `internal/server/driver.go`, and
made `Ready.Messages` a function rather than a field so it cannot be read early.

**Regression test.**
`TestNoSendBeforeSync` — a storage test double that records call ordering and
fails if any message reaches the transport before its batch is durable.
Confirmed to fail against the pre-fix commit.

**What it says about the design.**
The fsync-before-send rule was documented but not enforced by a type. Making the
wrong order unrepresentable was the real fix; moving the line was only the patch.

---

## Notes on predictions not yet borne out

*Kept so that the difference between "expected and found" and "expected and not
found" stays visible.*

- **A locking bug causing multi-second election stalls.** Anticipated at the
  outset as likely. The architecture chosen in
  [DESIGN.md §1](DESIGN.md#1-language-and-core-libraries) makes that specific
  bug structurally hard to write — the consensus core holds no locks and
  performs no I/O — while relocating the same *symptom* to a different cause:
  head-of-line blocking in the single driver goroutine, where a slow fsync
  delays ticks and can stall an election. `Metrics.tick_lag_ticks` and
  `Metrics.fsync_p99_micros` exist from the first version of that loop
  specifically so this is measurable rather than mysterious. Whether it actually
  happens will be recorded here either way.
