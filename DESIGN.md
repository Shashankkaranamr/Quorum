# Quorum — Design

This document records the decisions made before any consensus code was written,
and the reasoning behind them, so that a reviewer can predict what the
implementation looks like before reading it.

It is written to be falsifiable. Where it claims a property, it names the test
that would fail if the claim were untrue, or says which phase adds that test.
Where it claims less than the reader might assume, it says so explicitly —
see [§6, What this will and will not guarantee](#6-what-this-will-and-will-not-guarantee).

**Status:** phase 2 of 8 complete. Sections 1–6 are decided. Where the
implementation has since diverged from them, §10 records what changed and why;
see [PLAN.md](PLAN.md) for the roadmap and [PROGRESS.md](PROGRESS.md) for where
the work actually is.

---

## Contents

1. [Language and core libraries](#1-language-and-core-libraries)
2. [Persistence](#2-persistence)
3. [Transport, and how one core serves two test modes](#3-transport-and-how-one-core-serves-two-test-modes)
4. [Linearizability](#4-linearizability)
5. [Cluster topology, configuration and operation](#5-cluster-topology-configuration-and-operation)
6. [What this will and will not guarantee](#6-what-this-will-and-will-not-guarantee)
7. [Claim-to-test traceability](#7-claim-to-test-traceability)
8. [Repository layout](#8-repository-layout)
9. [Deliberate deferrals](#9-deliberate-deferrals)
10. [Implementation deltas](#10-implementation-deltas)

---

## 0. The shape of the system

```
                    ┌──────────────────────────────────────────┐
                    │  cmd/quorum-node  (one OS process each)   │
                    │                                           │
   client  ────────▶│  kvservice ──┐                            │
   (gRPC)           │              │  propose / read-index      │
                    │              ▼                            │
                    │        ┌───────────────┐                  │
                    │        │ internal/     │   Tick / Step     │
                    │        │   server      │──────────┐        │
                    │        │ (driver loop) │          ▼        │
                    │        │               │   ┌────────────┐  │
                    │        │  ONE goroutine│◀──│   raft     │  │
                    │        │  owns the core│   │ pure state │  │
                    │        └──┬────┬────┬──┘   │  machine   │  │
                    │           │    │    │      │ no I/O     │  │
                    │     Append│    │Send│Apply └────────────┘  │
                    │           ▼    ▼    ▼                      │
                    │   storage  transport  statemachine         │
                    │    (WAL)   (grpcx)     (kv + sessions)     │
                    └──────────────┬───────────────────────────┬─┘
                                   │                           │
                            real gRPC streams            admin API
                            to peer processes       (status, fault injection)
                                                             │
                                                    cmd/quorum-viz, quorumctl
```

The one structural idea everything else follows from: **the consensus algorithm
is a pure state machine, and all I/O lives outside it.** Section 1 explains
why, section 3 explains what it buys.

---

## 1. Language and core libraries

### Decision: Go

Judged against what this project actually has to do:

| Requirement | Why Go |
|---|---|
| Model a concurrent state machine without data races | `go test -race` is a mature dynamic race detector. The brief flags a locking bug in Raft's shared state as a known risk; Go is the candidate where "there is no race here" is a flag you run, not an argument you make. |
| Mature gRPC | `google.golang.org/grpc` is the reference implementation, maintained by the same project as the protocol. |
| Straightforward binary persistence | `os.File.Sync()` is a one-line, explicit fsync. Nothing sits between the code and the durability boundary. |
| Run 3–5 independent OS processes and kill them from tests | One static binary, no runtime to install. `os/exec` spawns; `os.Process.Kill()` is `TerminateProcess` on Windows and `SIGKILL` on Unix — an abrupt kill with no cleanup handlers, which is the only kind worth testing recovery against. |
| Be legible to a reviewer | etcd, Consul and the canonical Raft teaching labs are Go. The idioms are recognizable, which matters for something whose purpose is to be read. |

**Alternatives, and why not.** *Rust* (tokio + tonic) is a strong contender:
the borrow checker eliminates data races by construction, which is the single
biggest risk here. It loses on the other axis — async Rust adds real ceremony
around shared mutable state and timer cancellation, and the deterministic
simulation in §3 needs machinery that Go gets from an ordinary function call.
*Java 21+* has mature gRPC and virtual threads, but weaker deterministic-sim
ergonomics and clumsier process and fsync control. *Python and Node* were
available with zero setup and rejected anyway: neither makes "a concurrent
state machine without data races" an interesting claim, which is most of what
this project is trying to demonstrate.

### Libraries

| Need | Choice |
|---|---|
| gRPC | `google.golang.org/grpc` |
| Protobuf runtime | `google.golang.org/protobuf` |
| Proto codegen | `buf` — a pure-Go compiler, so there is **no native protoc dependency** and the project builds on a clean Windows machine with only the Go toolchain |
| Config | `go.yaml.in/yaml/v3` (the maintained successor to the archived `gopkg.in/yaml.v3`) |
| Tests | stdlib `testing` + `github.com/stretchr/testify/require` |
| Linearizability checking | `github.com/anishathalye/porcupine` (phase 5) |
| Goroutine-leak detection | `go.uber.org/goleak` (phase 3; vacuous earlier, see §10) |
| Lint | `golangci-lint` + `go vet` |

Generated protobuf code is **checked in**, so cloning and running `make test`
requires no protobuf tooling at all. `make proto-check` fails if the checked-in
output has drifted from the `.proto` sources.

### Concurrency model — the architectural crux

The Raft core is a pure state machine: **no I/O, no clock, no goroutines, no
locks.**

```go
// package raft
func (n *Node) Tick()                                   // logical time, +1
func (n *Node) Step(m Message)                           // inbound message
func (n *Node) Propose(cmd []byte) (index, term uint64, err error)
func (n *Node) Ready() Ready                             // persist / send / apply
func (n *Node) Advance()                                 // that Ready is done
```

Logical time advances only when `Tick` is called. Election-timeout jitter comes
from a `rand` source injected through `Config`, never from `math/rand`, so every
run reproduces from its seed.

**Which primitives protect Raft's shared state? None — there is no shared Raft
state.** A single driver goroutine owns the `*raft.Node` exclusively and
serializes every input through one `select`. Ownership replaces locking.

Mutexes and atomics exist in exactly three places, each isolated and each
documented where it lives:

1. `internal/storage` — guards the WAL file handle.
2. The status snapshot the visualizer reads — an immutable struct published
   through `atomic.Pointer[Status]`, written by the driver, read lock-free.
   Readers cannot block the consensus loop and cannot corrupt it.
3. The pending-proposal registry mapping log index → result channel, written by
   the driver and read by gRPC handler goroutines.

This was chosen over the more common "goroutine per concern plus one big mutex"
design specifically because the classic failure — *holding the Raft lock across
an fsync or an RPC, stalling elections for seconds* — is **structurally
impossible when the core performs no I/O at all.**

#### The claim is checked, not asserted

`raft/purity_test.go` walks package `raft`'s direct imports against an
allowlist. `time`, `sync`, `os`, `net`, `context`, `math/rand` and `log` are
absent from it, so reaching for any of them fails the build.

The allowlist permits `fmt` (for `Errorf`), so a second test parses the package
AST and rejects `fmt.Print*`, `fmt.Fprint*` and the `print`/`println` builtins.

A third test is a **negative control**: it runs the same checker against a
fixture that imports `net`, `os` and `time`, and fails if the checker reports
nothing. A checker that has never been observed to fail is not evidence, and
this one has been observed to fail — introducing `import "time"` into the core
produces:

```
--- FAIL: TestRaftCoreImportAllowlist
    package raft imports [time], which is not on the allowlist in purity_test.go.
```

#### The Ready loop, and the ordering that is a correctness requirement

```go
select {
case <-ticker.C:       n.Tick()
case m := <-recvCh:    n.Step(m)
case p := <-proposeCh: n.Propose(p)
}

rd := n.Ready()
storage.Append(rd.Entries)            // 1. buffered
storage.SetHardState(rd.HardState)    // 2. buffered
storage.Sync()                        // 3. THE fsync — exactly one per Ready
transport.Send(rd.Messages)           // 4. never before (3) returns
sm.Apply(rd.CommittedEntries)         // 5. never before (3) returns
n.Advance()                           // 6.
```

**Invariant: no message may leave the process before `Sync()` returns.** A
granted vote and an accepted `AppendEntries` are both durable promises; sending
either before it is on disk means a crash can make the node contradict itself,
and two leaders in one term follows directly. etcd relaxes this for follower
appends as a throughput optimization. Quorum does not, and pays the latency
instead. Phase 3 adds a storage test double that fails the test if a `Send` is
observed before its corresponding `Sync`.

Steps 1–6 are extracted into a function shared by the real driver and the
deterministic simulator, so the ordering itself is covered by the fast tests
rather than only end-to-end.

#### The hazard this does not remove

One goroutine handles ticks, messages and durability. A slow fsync therefore
delays ticks, and enough delay stalls an election. That is the *same symptom* as
the classic "held a lock across I/O" bug, relocated from many goroutines into
one auditable loop where it can be measured.

So it is measured. `Metrics.tick_lag_ticks` and `Metrics.fsync_p99_micros` are
in the admin API from the first version of the loop, not added after something
goes wrong. If the loop needs to be split — a durability goroutine with a
barrier, so ticks keep flowing during an fsync — that will be a documented
change with a measurement behind it.

> **A note on [BUGS.md](BUGS.md).** The brief predicts this project will hit a
> subtle locking bug causing multi-second election stalls. The architecture
> above makes that specific bug unlikely and this specific *class* of bug
> likely. BUGS.md will record what testing actually finds. No entry will be
> invented to match the prediction; a fabricated bug log would undermine the
> exact thing the project is trying to demonstrate.

---

## 2. Persistence

### Decision: a custom append-only write-ahead log, not an embedded KV store

**The tradeoff.** An embedded store (bbolt, pebble) gives B-tree indexing,
page-level crash safety and years of hardening for free. It costs the thing this
project is for: the durability boundary disappears into a dependency whose
correctness would be *asserted* rather than demonstrated. The brief requires
fsync to be "an explicit, auditable step in the design."

Raft's storage needs are narrow enough that the trade is worth making: an
append-only entry log, a small mutable `HardState`, and snapshot blobs. That is
a few hundred lines, and every one of them is on the page.

**What is given up, stated plainly:** no indexing beyond an in-memory offset
table, recovery cost linear in the log since the last snapshot, and none of the
hardening a mature storage engine has. Snapshots bound the first two. The third
is bounded by fuzzing the decoder and by the torn-tail tests in phase 3.

### Layout

```
data/node-<id>/
  wal/000001.log            append-only segments, rolled at 16 MiB
  snap/<index>-<term>.snap  state machine snapshots
```

`HardState` is written **as a record type inside the WAL**, not to a separate
file. That avoids atomic-rename and directory-fsync entirely — neither is
portable to Windows, and getting them subtly wrong is a classic source of
"durable" data that is not.

### Record framing

```
[u32 length][u32 crc32c][u8 record type][payload]
```

Protobuf is neither self-delimiting nor corruption-detecting, so the framing
supplies both. Record types are `HardState`, `WalEntryBatch` and
`WalSnapshotPointer`, all defined in
[`proto/quorum/raft/v1/raft.proto`](proto/quorum/raft/v1/raft.proto) — the same
types that go on the wire. **One serialization format to get right, one to
fuzz.**

### Durability invariants

- **One `Sync()` per Ready batch.** The batch is the unit of atomicity. A
  phase-3 test asserts the fsync count matches the Ready count exactly.
- **Recovery yields a prefix.** On startup, scan forward and stop at the first
  record with a bad CRC or an impossible length, then truncate there. A torn
  tail from a crash mid-write is never a partial record, and never a corrupt
  one. Phase 3 tests this by truncating a real WAL at *every* byte offset and
  requiring that recovery produces a clean prefix each time.
- **Payload before pointer, for snapshots.** Write and fsync the `.snap` file;
  *then* append and fsync a `WalSnapshotPointer`; *then* delete superseded WAL
  segments. A pointer therefore always names a complete file, and a complete
  file with no pointer is simply ignored. A crash at any point in the sequence
  is recoverable, which phase 4 tests by crashing at each step.

### Scope of the durability guarantee

`Sync()` calls `File.Sync()`, which is `FlushFileBuffers` on Windows and `fsync`
on Unix. **This protects against process crash and OS crash. It does not protect
against a drive with a volatile write cache that ignores flush barriers on power
loss.** No amount of application code can fix that, and claiming otherwise would
be dishonest.

---

## 3. Transport, and how one core serves two test modes

Raft correctness testing needs two things that usually pull in opposite
directions: fast deterministic tests where partitions and delays cost no real
time, and a demo where killing a node and cutting a network are genuinely real.

Because the core performs no I/O and holds no clock, **both modes run identical
consensus code.** Only the clock and the implementations behind two interfaces
change:

```go
type Transport interface {
	Send(to NodeID, m Message)
	Recv() <-chan Message
}

type Storage interface {
	Append([]Entry)
	SetHardState(HardState)
	Sync() error
	// ...
}
```

Ready-processing — steps 1–6 above — is itself a shared function, so the
durability ordering is exercised by the fast tests too, not only end-to-end.

### (a) Deterministic simulation — `internal/transport/inmem`, `internal/testutil`

A single-threaded harness owns N cores, in-memory storage and a message bus.
Messages queue against **logical tick deadlines**, so a scenario that would take
sixty seconds of real elections runs in microseconds. No goroutines, no
`time.Sleep`, no wall clock anywhere.

Every scheduling decision derives from a test-supplied seed, so a failing run
reproduces exactly from that seed alone. Faults available: bidirectional and
one-way partitions, uniform and targeted drops, bounded and unbounded delays,
duplication and reordering.

After **every step**, the harness asserts the five Raft safety properties:

| Property | Statement |
|---|---|
| Election Safety | At most one leader is elected in a given term |
| Leader Append-Only | A leader never overwrites or deletes entries in its own log |
| Log Matching | If two logs contain an entry with the same index and term, the logs are identical in all preceding entries |
| Leader Completeness | If an entry is committed in a term, it is present in the log of every leader of every later term |
| State Machine Safety | If a node has applied an entry at a given index, no other node applies a different entry at that index |

And because a checker nobody has seen fail proves nothing, phase 2 also ships
**negative controls**: deliberately mutated Raft implementations, each of which
must trip a specific checker. If a mutation passes, the checker is broken and
the suite says so.

### (b) Real processes — `internal/transport/grpcx`

Each node dials every peer and holds **one long-lived unidirectional gRPC stream
per directed link**. A directed link maps 1:1 to a stream, which is what makes
one-way partitions expressible — and one-way partitions are where the subtler
Raft bugs live. A leader that can still send heartbeats but cannot hear
acknowledgements behaves very differently from one that is fully isolated.

### Fault injection in real-process mode

| Fault | How real it is |
|---|---|
| **Kill** | Genuinely real. `TerminateProcess` / `SIGKILL` on the child PID. No graceful shutdown, no flush, no cleanup handlers. |
| **Restart** | Real. Respawned against the same data directory, so recovery reads the actual WAL. |
| **Partition** | Enforced in each node's transport interceptor: streams to blocked peers are closed and new connections from them are rejected. Real sockets really close. The consensus core cannot distinguish this from a severed cable. **It is not a kernel firewall rule** — see below. |
| **Freeze** | An admin call parks the driver's event loop. The process stays alive and the socket keeps accepting while nothing is processed. From the cluster's point of view this is a faithful freeze; it is *cooperative*, not `SIGSTOP`. |

**On partitions, precisely.** The block is applied below Raft and above TCP.
What is real: connections close, packets stop being delivered, the affected
nodes genuinely cannot communicate, and no consensus code is aware that anything
was injected. What is not real: the operating system's routing table is
untouched, so this does not exercise kernel-level or NIC-level behaviour, and a
bug that only manifests under real packet loss would not be caught. A
firewall-based mode (`netsh advfirewall` / `iptables`) is a possible extension
and would need elevation; it is deliberately not a dependency of the test suite.

### The honest gap

The deterministic suite covers the consensus core, Ready-processing, the state
machine and the codec. The three things it does **not** cover are:

1. the gRPC transport,
2. WAL file I/O,
3. the goroutine and ticker wiring in the driver.

Each has dedicated tests, and all three are exercised end-to-end by the
real-process fault-injection suite in phase 6. That list is exhaustive and is
the honest answer to "is the fast simulation testing the real thing?"

---

## 4. Linearizability

Linearizable means a client that has been told a write succeeded will see that
write, or something later, in every subsequent read — even if the leader that
accepted it has since died. Two mechanisms are needed: one for reads, one to
stop retries from applying a write twice.

### Reads: ReadIndex

Serving a read from the leader's own memory is wrong. A leader that has been
partitioned away does not know it yet, and will happily answer with stale data
while a new leader elsewhere accepts writes.

```
1. On election, the leader commits a no-op entry.
2. readIndex = commitIndex.
3. Confirm leadership: a heartbeat round carrying a read context,
   acknowledged by a quorum.
4. Wait until lastApplied >= readIndex, then serve from the state machine.
```

**Step 1 is not optional.** Raft forbids committing an entry from a previous
term by counting replicas (§5.4.2), so a new leader does not know its true
commit index until it commits something from its own term. The no-op is how it
finds out. `ENTRY_TYPE_NOOP` exists in the log format for exactly this.

**Step 3 is what makes the read safe.** A silently-partitioned leader cannot
collect a quorum of echoes, so its read returns `STATUS_NO_QUORUM` rather than a
stale value. Phase 5 proves this with a test that partitions a leader, reads
from it, and fails if it answers.

`GetResponse.read_index` exposes the index the read linearized at, which makes
the mechanism observable in the visualizer and checkable in a test.

**Leader lease is documented and deliberately off.** It would skip step 3 and be
faster, at the cost of depending on bounded clock drift between machines. This
project would rather be slower and unconditionally correct.

### Writes: client sessions and deduplication

The ambiguous failure is the hard case: a client sends a `Put`, the leader
commits it, the leader dies before responding. The client does not know whether
it happened. Retrying blindly can apply the write twice; not retrying can lose
it.

```
1. A client calls RegisterClient, which is itself a Raft log entry.
2. Its client_id is the LOG INDEX that entry landed at.
3. Every request carries (client_id, seq), seq monotonic per client.
4. The state machine keeps sessions[client_id] = {last_seq, last_response}.
5. On apply, seq <= last_seq returns the cached response instead of
   applying anything.
```

Deriving the client id from a log index makes it unique across the cluster with
no coordination, no randomness and no collision risk, and it survives leader
change because it was agreed by consensus like everything else.

Two properties this depends on:

- **The session table is updated in the deterministic apply path on every
  replica**, not just the leader — otherwise a new leader would not recognize
  the duplicate.
- **The session table is part of the snapshot.** A node restarting from a
  snapshot must still deduplicate. Phase 4 tests exactly this.

`PutResponse.duplicate` reports whether a response came from the session cache.
That field is how phase 5 *proves* exactly-once instead of claiming it: kill the
leader after commit but before the response, retry with the same seq, and assert
the value was written once and `duplicate` is true.

### The response contract

| Status | Meaning | What the client does |
|---|---|---|
| `OK` | Applied | Done |
| `NOT_LEADER` | **Definitely not applied** | Retry at `leader_hint` |
| `LOST_LEADERSHIP` | **Unknown whether applied** | Retry with the *same* seq; dedup resolves it |
| `NO_QUORUM` | Read could not confirm leadership | Retry, possibly elsewhere |
| `SESSION_EXPIRED` | Session was garbage collected | Re-register; see the caveat below |
| `TIMEOUT` | Unknown | Retry with the same seq |

Keeping `NOT_LEADER` and `LOST_LEADERSHIP` distinct is the whole point.
Collapsing them into one generic error is how systems end up applying writes
twice.

### Limits, stated now rather than discovered later

- **One in-flight request per session.** The response cache is a single slot.
  Pipelining would need a windowed cache.
- **Session expiry can break exactly-once** for a client idle past the
  garbage-collection window. This is inherent to bounded session state — the
  Raft dissertation has the same caveat. It is reported as `SESSION_EXPIRED`
  rather than hidden.
- **Reads are not deduplicated.** They are idempotent and are not logged.

---

## 5. Cluster topology, configuration and operation

### Static membership

Every node reads the same [`cluster.yaml`](cluster.yaml) and learns the full peer
set from it. Membership cannot change while the cluster runs.

Dynamic membership was considered and rejected as **not low-cost**: it needs
catch-up rounds for a joining server, the single-server-change safety argument
or joint consensus, and careful handling of a removed leader. It is a project in
itself.

The cost of deferring it is made near-zero by reserving `ENTRY_TYPE_CONFIG` in
the log format now. Adding membership later needs no format change — which
matters, because a format change would make every WAL written before it
unreadable.

### Configuration

Decoding is **strict**: an unknown field is an error, not a silently ignored
typo. A misspelled `election_timeout_min_ticks` that quietly falls back to a
default shows up weeks later as erratic elections and gets blamed on the
consensus code.

Validation rejects more than schema errors. It rejects configurations that are
*well-formed but wrong*:

- Cluster size outside 3–5. A two-node cluster has a quorum of two and therefore
  tolerates zero failures.
- Duplicate node ids, or two nodes sharing a host:port.
- `election_timeout_min >= election_timeout_max`. An empty randomization range
  means every node times out together, and the cluster split-votes indefinitely.
- `election_timeout_min < 3 × heartbeat_timeout`. This encodes Raft's
  `broadcastTime << electionTimeout` requirement. If a leader can only miss one
  or two heartbeats before a follower gives up, ordinary scheduling jitter
  triggers spurious elections.

An even cluster size is legal but produces a warning: it tolerates the same
number of failures as the next size down.

Timing is expressed in **logical ticks**, not milliseconds, everywhere except
`tick_ms`. The core counts ticks; the driver decides how long a tick lasts. That
is what lets the simulator compress real time.

### Ports and data directories

Node *i* gets:

| | |
|---|---|
| `grpc_port` | `7000+i` — Raft peer transport, KV API and admin API, three services on one listener |
| `http_port` | `8000+i` — status and metrics; what the visualizer scrapes |
| data dir | `data/node-<id>` |

The visualizer serves on `8080`. [`cluster-5.yaml`](cluster-5.yaml) is the
five-node variant.

### Operating it from a terminal

`quorumctl` is the single operator surface. The fault-injection suite and the
visualizer's buttons drive the same code underneath, so what the demo shows
cannot drift from what the tests exercise.

```
quorumctl plan                  show what `up` would start          (works now)
quorumctl up                    start every node as a process       (phase 6)
quorumctl status                role, term, commit index per node   (phase 6)
quorumctl kill <id>             TerminateProcess / SIGKILL          (phase 6)
quorumctl start <id>            restart against the same data dir   (phase 6)
quorumctl freeze|thaw <id>      park / resume the event loop        (phase 6)
quorumctl partition 1,2 | 3     cut links between two groups        (phase 6)
quorumctl heal                  remove every injected partition     (phase 6)
quorumctl put <k> <v>           write through the leader            (phase 5)
quorumctl get <k>               linearizable read via ReadIndex     (phase 5)
```

Unimplemented subcommands are recognized, documented, and **exit non-zero**
naming the phase that implements them. A stub that exits 0 would let a broken
end-to-end script look green.

---

## 6. What this will and will not guarantee

### It will guarantee

- **Linearizable single-key reads and writes**, as long as a majority of nodes
  are alive and can reach each other.
- **At-most-once application of client commands** within a live session, across
  retries after ambiguous failures.
- **Durability of acknowledged writes** across process crashes and OS crashes.
- **No committed entry is lost, reordered, or overwritten**, and no two nodes
  apply different commands at the same log index.
- **A minority partition cannot commit anything.** It returns errors rather than
  false successes.

### It will not guarantee

- **No Byzantine fault tolerance.** Nodes are assumed to fail by crashing, not
  by lying. There is no message authentication and no TLS between peers; a
  node that sends malformed or malicious messages is out of scope.
- **No dynamic membership.** Cluster size is fixed at startup.
- **Localhost, single machine only.** No cross-datacenter operation, no real
  network hardware, no clock skew between physical machines.
- **No multi-key transactions**, no atomic multi-key operations, no range
  queries, no watches.
- **No throughput tuning.** Correctness is prioritized over performance
  everywhere the two conflict — see the strict fsync-before-send ordering in §1.
- **No protection against hardware that lies about flushes.** See §2.
- **No liveness guarantee under pathological message loss.** This is FLP, not a
  shortcut: safety always holds; liveness holds only under partial synchrony.
- **Partitions in the demo are transport-level, not kernel-level.** See §3.
- **Freeze is cooperative, not `SIGSTOP`.** See §3.
- **Session expiry can break exactly-once** for a sufficiently idle client. See
  §4.
- **No authentication or authorization** on the client API.

---

## 7. Claim-to-test traceability

Every guarantee in §6 must name a test that would fail if the guarantee were
false. The table is filled in as the tests are written; phase 6 requires it to
be complete, and phase 8 requires it to appear in the README.

| Claim | Test | Phase |
|---|---|---|
| The consensus core performs no I/O | `TestRaftCoreImportAllowlist`, `TestRaftCoreDoesNotWriteToStdout` | ✅ 1 |
| That purity check can actually fail | `TestImportAllowlistCheckerDetectsViolations` | ✅ 1 |
| A misconfigured cluster is rejected at startup | `TestLoadRejectsBadConfigs` | ✅ 1 |
| The core performs no concurrency either | `TestRaftCoreHasNoConcurrencyPrimitives` | ✅ 2 |
| Exactly one leader per term | `ElectionSafety` checker, asserted every tick by `TestRandomizedTrialsUpholdSafety` | ✅ 2 |
| Leader Append-Only holds | `LeaderAppendOnly` checker, every tick | ✅ 2 |
| Log Matching holds | `LogMatching` checker, every tick | ✅ 2 |
| Leader Completeness holds | `LeaderCompleteness` checker, on every election | ✅ 2 |
| State Machine Safety holds | `StateMachineSafety` checker, on every apply | ✅ 2 |
| A committed entry is never altered | `CommittedEntriesAreStable` checker, every tick | ✅ 2 |
| Every checker can actually fail | `TestMutatedRaftTripsInvariant/*` and `TestCheckerDetectsHandBuiltViolations/*` | ✅ 2 |
| The checkers accept a healthy history | `TestCheckerAcceptsAHealthyHistory` | ✅ 2 |
| An old-term entry is not committed by replica count (Figure 8) | `TestFigure8CommitRule` | ✅ 2 |
| The §5.4.1 up-to-date comparison is the right way round | `TestUpToDateComparison` | ✅ 2 |
| The election timer resets only on a granted vote or a leader's AppendEntries | `TestElectionTimerResetDiscipline` | ✅ 2 |
| A stale or duplicated AppendEntries never truncates the log | `TestStaleAppendEntriesDoesNotTruncate` | ✅ 2 |
| Elections converge within the bound | `TestElectionConverges` (1000 seeds), `TestLivenessBoundUnderTransientFaults` | ✅ 2 |
| A minority partition cannot commit | `TestMinorityPartitionCannotCommit` | ✅ 2 |
| Committed entries survive a leader kill | `TestLeaderFailoverPreservesCommitted` | ✅ 2 |
| Tick lag is measurable | `TestTickLagIsMeasured` | ✅ 2 |
| Wire and disk encoding round-trip | `TestMessageRoundTrip`, `TestEntryTypeValuesMatch` | ✅ 2 |
| Nothing is sent before it is durable | `TestNoSendBeforeSync` | 3 |
| Exactly one fsync per Ready batch | `TestOneFsyncPerReady` | 3 |
| Recovery from a torn WAL yields a prefix | `TestWALTruncationAtEveryOffset` | 3 |
| A restarted node never votes twice in a term | `TestRestartDoesNotDoubleVote` | 3 |
| Snapshot + tail reproduces exact state | `TestSnapshotRestoreIsIdentical` | 4 |
| Dedup survives snapshot restore | `TestSessionsSurviveSnapshot` | 4 |
| A partitioned leader will not serve a stale read | `TestPartitionedLeaderRefusesRead` | 5 |
| A retried write applies exactly once | `TestAmbiguousRetryAppliesOnce` | 5 |
| Client histories are linearizable | `TestLinearizabilityUnderFaults` (Porcupine) | 5 |
| Acknowledged writes survive a leader kill | `TestAckedWritesSurviveLeaderKill` | 6 |
| A killed node recovers from disk | `TestKilledNodeRecovers` | 6 |
| Random fault schedules stay linearizable | `TestChaosSeeded` | 6 |

---

## 8. Repository layout

```
raft/                     PURE consensus core — the only exported package
internal/
  server/                 the driver loop; the one place I/O ordering is decided
  storage/                WAL, snapshots, framing codec
  statemachine/           KV map + client session table
  transport/              interface
    inmem/                deterministic simulator
    grpcx/                real gRPC + fault injection
  pbconv/                 core types <-> protobuf, so raft/ stays protobuf-free
  kvservice/              client gRPC API, ReadIndex, leader redirect
  admin/                  status + fault injection API
  client/                 client library: client_id, seq, retry, redirect
  supervisor/             spawn / kill / restart node processes
  config/                 cluster.yaml loading and validation
  testutil/               cluster harness, invariant checkers, Porcupine model
cmd/
  quorum-node/            one replica
  quorumctl/              operator CLI and fault injection
  quorum-viz/             visualizer backend
proto/quorum/{raft,kv,admin}/v1/
gen/                      generated protobuf code (checked in)
test/
  tooling/                tests about the build tooling itself
  integration/            real-process end-to-end suite
web/                      visualizer frontend, served from embed.FS
```

`raft/` is top-level and exported while everything else is `internal/`. That is
deliberate: it makes "the core has no I/O dependencies" verifiable by reading
one package's import list. `TestRaftCoreIsTheOnlyExportedPackage` keeps it that
way.

The visualizer frontend is vanilla JS plus server-sent events, served from
`embed.FS` inside the Go binary — no npm toolchain, no build step, one binary.

---

## 9. Deliberate deferrals

Things considered and consciously left out, so that "we didn't think of it" and
"we decided against it" stay distinguishable:

| Deferred | Why | Cost of adding later |
|---|---|---|
| Dynamic membership | Needs catch-up rounds and the joint-consensus safety argument; a project in itself | Low — `ENTRY_TYPE_CONFIG` is reserved, so no log format change |
| Pre-vote | Reduces disruption from a rejoining node, but is an optimization, not a safety fix | Low — one message field |
| Leader lease reads | Faster, but depends on bounded clock drift | Low — ReadIndex stays as the fallback |
| Follower reads | Followers forwarding a read index to the leader | Low — same mechanism, one extra hop |
| Batching and pipelining | Throughput work; correctness comes first | Medium |
| TLS / peer authentication | Localhost only, no Byzantine model | Medium |
| Kernel-level partitions | Needs elevation, not portable | Medium — a second fault-injection backend |
| Multi-key transactions | Out of scope for a KV store demonstrating consensus | High |

---

## 10. Implementation deltas

Where the code diverged from this document, and why. Recorded as it happens
rather than reconciled at the end, so that "we changed our mind" and "we forgot"
stay distinguishable. Phase 8 consolidates this into the body of the document.

### Phase 2

**`Transport` lost its `Recv` method.** §3 originally sketched
`Recv() <-chan Message` alongside `Send`. A channel needs a goroutine to feed
it, and the deterministic simulator is deliberately single-threaded so that a
trial replays exactly from its seed. Rather than give the simulator a goroutine,
the interface narrowed to `Send` alone and inbound delivery became the driver's
concern: the simulator's scheduler pushes straight into `Node.Step`, and phase
5's gRPC transport will expose its own channel for the real driver to select on.
Both still run identical consensus code, which was the point. The reasoning is
repeated at the interface itself in `internal/transport/transport.go`.

**The leader counts itself only up to its durable index.** A leader is part of
its own commit quorum. Counting entries it had merely appended in memory would
leave a window where a crash after commit but before fsync loses an entry the
cluster believed committed — the quorum would have been one short all along. So
`matchIndex[self]` tracks the stable (fsynced) index, not the last index, and
advances in `Advance()` rather than at append time. This costs one Ready cycle
of commit latency and is a deliberate divergence from etcd, which historically
counted at append time. See `Node.updateSelfProgress`.

**`internal/storage` and `internal/server` arrived in phase 2, not phase 3.**
PLAN.md scheduled storage for phase 3. Phase 2 needs a `Storage` implementation
for the simulator anyway, and putting the shared Ready-processing function in
place now is what makes the fsync-before-send ordering testable from the fast
suite rather than only end-to-end. Phase 3 replaces `MemStorage` with the real
write-ahead log behind the same interface; nothing else moves.

**`raft.Mutation` ships in production code.** The invariant checkers are the
primary deliverable of phase 2, and the only way to show a checker works is to
run it against a Raft that is genuinely broken. The checkers live in
`internal/testutil`, a different package, so the knob has to be reachable from
`raft.Config`. It is fenced: the zero value is correct Raft
(`TestZeroConfigIsUnmutated`), every value names the exact rule it removes, and
each is surgical enough that a reported violation names the rule that was
broken. The alternative — a build tag — would have kept it out of normal builds
at the cost of the negative controls not running in `make test`, which defeats
the purpose.

**goleak is deferred from phase 2 to phase 3.** PLAN.md listed it under phase 2.
This phase has no goroutines at all by design, so the check is vacuous. It
becomes meaningful when the real driver goroutine exists.
