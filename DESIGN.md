# Quorum — Design

This document records the decisions made before any consensus code was written,
and the reasoning behind them, so that a reviewer can predict what the
implementation looks like before reading it.

It is written to be falsifiable. Where it claims a property, it names the test
that would fail if the claim were untrue, or says which phase adds that test.
Where it claims less than the reader might assume, it says so explicitly —
see [§6, What this will and will not guarantee](#6-what-this-will-and-will-not-guarantee).

**Status:** phase 6 of 8 complete. Sections 1–6 are decided. Where the
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
| Goroutine-leak detection | `go.uber.org/goleak` (phase 5, with the first goroutine; vacuous earlier, see §10) |
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

Mutexes and atomics exist in exactly two places in production code, each
isolated and each documented where it lives:

1. The status snapshot the visualizer reads — an immutable struct published
   through `atomic.Pointer` by the driver loop (`server.Loop`), read lock-free.
   Readers cannot block the consensus loop and cannot corrupt it.
2. The injected-fault state in `internal/transport/grpcx` — which links are
   blocked — behind one mutex. It is read by the driver loop as it sends, by
   every peer goroutine and by every inbound stream handler, and changed by
   whoever injects the fault.

This list originally had three entries, and different ones; §10, phase 5,
records the change. The write-ahead log needs no lock because only the driver
loop touches it. The pending-proposal registry needs none because it lives
inside the loop too: an RPC handler sends its request on a channel and waits
for the reply on another, and never touches the registry.

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
instead. `TestNoSendBeforeSync` audits every message every node sends, at the
instant it is handed to the transport, against what that node's storage had
made durable by then.

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

`HardState` is written **inside the WAL**, not to a separate file. That avoids
atomic-rename entirely — it is not portable to Windows, and getting it subtly
wrong is a classic source of "durable" data that is not.

Directory fsync is avoided on the hot path but not entirely: creating a segment
(once per 16 MiB) makes the new file's directory entry durable before anything
is written into it. On Unix that is an fsync of the directory. Windows has no
directory fsync; there the WAL relies on NTFS journalling the file's creation
and on `FlushFileBuffers` of the file itself committing its metadata. That is
the platform's documented behaviour, not something Quorum can verify.

### Record framing

```
[u32 length][u32 crc32c][u8 record type][payload]
```

Protobuf is neither self-delimiting nor corruption-detecting, so the framing
supplies both. The CRC covers the length, the type and the payload, so a
corrupted length is caught by the checksum rather than only by whatever lies at
the offset it points to. Integers are little-endian; a record is capped at 64
MiB so a flipped high bit cannot make recovery allocate gigabytes.

There are two record types, both defined in
[`proto/quorum/raft/v1/raft.proto`](proto/quorum/raft/v1/raft.proto):
`WalEntryBatch` — one Ready's entries **and** its hard state — and
`WalSnapshotPointer` (phase 4). **Each `Sync()` writes exactly one record**, so
a torn write can only ever lose a whole batch; there is no state in which a
batch's entries are durable and its hard state is not. The entry and hard-state
messages are the same types that go on the wire. **One serialization format to
get right, one to fuzz.**

### Durability invariants

- **One `Sync()` per Ready batch, and at most one fsync.** The batch is the unit
  of atomicity. A Ready with entries or hard state costs exactly one record and
  one fsync however large it is; a Ready with neither (a leader's heartbeat)
  costs none, because there is nothing to make durable. `TestOneFsyncPerReady`
  measures every Sync individually.
- **Recovery yields a prefix.** On startup, scan forward and stop at the first
  record with a bad CRC or an impossible length, then truncate there and fsync
  the truncation before writing anything new. A torn tail from a crash
  mid-write is never a partial record, and never a corrupt one.
  `TestWALTruncationAtEveryOffset` truncates a real WAL at *every* byte offset.
- **Damage that is not a torn tail stops the node.** A segment is closed only
  after it is fully synced, so a bad record in any segment but the last cannot
  be a torn write; neither can a missing segment, nor a record whose CRC is
  valid but whose type or payload this binary cannot read. Truncating any of
  those would discard records that had been made durable and promised to other
  nodes. Recovery refuses to start instead, and says why.
- **A failed write or fsync is fatal to the log.** After either, the WAL
  refuses every further call. What a failed fsync left on disk is unknowable —
  on Linux the dirty pages may already have been dropped, so a retry can report
  success for data that is gone. The only safe recovery is a restart that
  re-reads the disk.
- **Payload before pointer, for snapshots.** Write and fsync the `.snap` file;
  *then* roll to a fresh segment and append and fsync a `WalSnapshotPointer`;
  *then* delete every older segment and snapshot file. A pointer therefore
  always names a complete file, and a complete file with no pointer is ignored
  and removed at the next start. The pointer carries the log entries after the
  snapshot and the current hard state, so recovery starts at the last segment
  that begins with one and never needs anything older. That makes deletion
  order irrelevant: a crash part-way through deleting leaves stale segments
  that recovery skips and removes. `TestCrashDuringSnapshot` crashes between
  every pair of steps, and inside the file write, the pointer write and the
  deletions. Each case must recover to exactly the state before the snapshot or
  exactly the state after it, and keep working.

### Scope of the durability guarantee

`Sync()` calls `File.Sync()`, which is `FlushFileBuffers` on Windows and `fsync`
on Unix. **This protects against process crash and OS crash. It does not protect
against a drive with a volatile write cache that ignores flush barriers on power
loss.** No amount of application code can fix that, and claiming otherwise would
be dishonest.

**Nor does it detect every kind of media corruption.** Damage in the middle of
the *last* segment, with valid records after it, is indistinguishable from a
torn tail, and recovery truncates there — losing the valid records behind it.
A torn write can only ever be at the end, so this cannot happen from a crash;
it needs the disk to corrupt data at rest, which is outside the fault model
(§6). Distinguishing the two would need a second copy or per-record sequence
numbers; neither is worth its cost for a crash-fault system.

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
quorumctl up                    start every node as a process       (works now)
quorumctl status                role, term, commit index per node   (works now)
quorumctl kill <id>             TerminateProcess / SIGKILL          (works now)
quorumctl start <id>            restart against the same data dir   (works now)
quorumctl freeze|thaw <id>      park / resume the event loop        (works now)
quorumctl partition 1,2 '|' 3   cut links between two groups        (works now)
quorumctl heal                  remove every injected partition     (works now)
quorumctl put <k> <v>           write through the leader            (works now)
quorumctl get <k>               linearizable read via ReadIndex     (works now)
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
false. `TestEveryGuaranteeNamesExistingTests` (test/tooling) holds this section
to that: every guarantee in §6 must have a row in the first table below, and
every test named anywhere in this section must exist in the repository.

### The guarantees in §6

| Guarantee | Tests |
|---|---|
| Linearizable single-key reads and writes | `TestLinearizabilityUnderFaults`, `TestChaosSeeded` (Porcupine over histories recorded under kills, partitions, one-way cuts and freezes), `TestPartitionedLeaderRefusesRead` |
| At-most-once application of client commands | `TestAmbiguousRetryAppliesOnce`, `TestSessionsSurviveSnapshot` |
| Durability of acknowledged writes | Process crashes: `TestAckedWritesSurviveLeaderKill`, `TestRollingRestart`, `TestProcessesServeReadsAndWrites` and `TestKilledNodeRecovers`, all killing real processes; `TestClusterRestartRecoversCommitted` and `TestWALTruncationAtEveryOffset` on the recovery path. **OS crashes are not tested**: the suite cannot crash the operating system. That half of the claim rests on nothing being acknowledged before it is fsynced (`TestNoSendBeforeSync`, `TestOneFsyncPerReady`) and on the OS honouring `fsync`/`FlushFileBuffers`, whose limits §2 states. |
| No committed entry is lost, reordered, or overwritten | `CommittedEntriesAreStable`, `StateMachineSafety`, `LogMatching` and `SnapshotFidelity` checked after every tick by `TestRandomizedTrialsUpholdSafety`, `TestRandomizedTrialsUpholdSafetyOnDisk` and `TestRandomizedTrialsUpholdSafetyWithSnapshots`; `TestMinorityPartitionRefusesWrites` on real processes |
| A minority partition cannot commit anything. | `TestMinorityPartitionCannotCommit` (simulator), `TestMinorityPartitionRefusesWrites` (five real processes) |

### Every claim, and the test behind it

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
| Nothing is sent before it is durable | `TestNoSendBeforeSync` | ✅ 3 |
| That audit can actually fail | `TestDurabilityAuditCatchesMisorderedDrivers` | ✅ 3 |
| One Sync per Ready; one fsync per Ready with durable state, never more | `TestOneFsyncPerReady` | ✅ 3 |
| The WAL decoder never accepts a torn, partial or CRC-mismatched record | `FuzzWALDecode`, `TestWALCodecRejectsEveryBitFlip` | ✅ 3 |
| That fuzz contract can actually fail | `TestDecodeContractCatchesBrokenDecoders` | ✅ 3 |
| Recovery from a torn WAL yields a prefix | `TestWALTruncationAtEveryOffset` | ✅ 3 |
| That prefix check can actually fail | `TestTruncationCheckDistinguishesEveryPrefix` | ✅ 3 |
| Damage that is not a torn tail is refused, not truncated | `TestWALRefusesDamageThatIsNotATornTail` | ✅ 3 |
| A restarted node never votes twice in a term | `TestRestartDoesNotDoubleVote` | ✅ 3 |
| That double-vote check can actually fail | `TestDoubleVoteCheckCatchesAmnesia` | ✅ 3 |
| A whole-cluster crash loses no committed entry | `TestClusterRestartRecoversCommitted` | ✅ 3 |
| That restart check can actually fail | `TestRestartCheckCatchesLostCommittedEntries` | ✅ 3 |
| The safety invariants hold with every node on real disk | `TestRandomizedTrialsUpholdSafetyOnDisk` | ✅ 3 |
| The simulator's crash model matches the real log | `TestWALAgreesWithMemStorage` | ✅ 3 |
| Crossing the threshold compacts, and deletes the superseded segments | `TestSnapshotAtThreshold` | ✅ 4 |
| Snapshot + tail reproduces exact state | `TestSnapshotRestoreIsIdentical` | ✅ 4 |
| That restore check can actually fail | `TestRestoreCheckCatchesALossyRestore` | ✅ 4 |
| A far-behind follower is caught up by InstallSnapshot, observed being sent | `TestFarBehindFollowerGetsSnapshot` | ✅ 4 |
| That count is not counting something else | `TestSnapshotCountIsZeroForANearbyFollower` | ✅ 4 |
| Dedup survives snapshot restore | `TestSessionsSurviveSnapshot` | ✅ 4 |
| That session check can actually fail | `TestSessionCheckCatchesASessionlessSnapshot` | ✅ 4 |
| A crash at any point while snapshotting is recoverable | `TestCrashDuringSnapshot` | ✅ 4 |
| That crash check can actually fail | `TestSnapshotCrashCheckCatchesMisorderedWrites` | ✅ 4 |
| A node whose data directory is deleted rejoins by snapshot | `TestWipedNodeCatchesUp` | ✅ 4 |
| Every replica's state is identical at every index, applied or restored | `SnapshotFidelity` checker, every tick of `TestRandomizedTrialsUpholdSafetyWithSnapshots` | ✅ 4 |
| That checker can actually fail | `TestSnapshotFidelityCatchesACorruptTransfer`, `TestCheckerHandlesCompactedLogs` | ✅ 4 |
| A snapshot is never acknowledged before it is durable | `TestNoSendBeforeSync` (snapshot variants) | ✅ 4 |
| That audit can actually fail for snapshots | `TestDurabilityAuditCatchesAnEarlySnapshotAck` | ✅ 4 |
| The simulator's storage matches the real log through snapshots | `TestWALAgreesWithMemStorageThroughSnapshots` | ✅ 4 |
| A real multi-process cluster serves reads and writes, and survives every process being killed | `TestProcessesServeReadsAndWrites` | ✅ 5 |
| A no-op opens every term, and the leader's commits in its own term | `TestNoopCommittedOnElection` | ✅ 5 |
| That no-op check can actually fail | `TestNoopCheckCatchesATermWithoutOne` | ✅ 5 |
| ReadIndex confirms only with a quorum, only after the term's first commit, and abandons reads on step-down | `TestReadIndexWaitsForAQuorum`, `TestReadIndexWaitsForTheTermsFirstCommit`, `TestReadsAreAbandonedOnStepDown` | ✅ 5 |
| A partitioned leader will not serve a stale read | `TestPartitionedLeaderRefusesRead` | ✅ 5 |
| That stale-read check can actually fail | `TestStaleReadCheckCatchesAQuorumlessRead` | ✅ 5 |
| A retried write applies exactly once | `TestAmbiguousRetryAppliesOnce` | ✅ 5 |
| That applied-once check can actually fail | `TestAppliedOnceCheckCatchesANaiveRetry` | ✅ 5 |
| A write to a follower is refused, redirected, and costs at most one extra attempt | `TestNotLeaderRedirect` | ✅ 5 |
| Client histories are linearizable under leader kills and partitions | `TestLinearizabilityUnderFaults` (Porcupine) | ✅ 5 |
| That linearizability check can actually fail, end to end and on a hand-built history | `TestLinearizabilityCatchesQuorumlessReads`, `TestLinearizabilityCheckerRejectsAStaleRead` | ✅ 5 |
| Partitions are directed, and heal | `TestPartitionIsDirectedAndHeals` | ✅ 5 |
| Injecting faults under traffic is race-free | `TestFaultStateIsSafeToChangeUnderTraffic`, under `make race` | ✅ 5 |
| Every goroutine exits when a node stops | goleak in `internal/node` and `internal/transport/grpcx` | ✅ 5 |
| The harness really kills (PID gone), restarts on the same data, partitions both ways and one way, heals, freezes and thaws | `TestHarnessInflictsRealFaults` | ✅ 6 |
| quorumctl drives every operation end to end | `TestQuorumctlDrivesTheCluster` | ✅ 6 |
| Acknowledged writes survive a leader kill, and a new leader appears within a bound | `TestAckedWritesSurviveLeaderKill` | ✅ 6 |
| That acknowledged-writes check can actually fail | `TestAckedCheckCatchesLostData` | ✅ 6 |
| A killed node recovers from disk | `TestKilledNodeRecovers` | ✅ 6 |
| A minority refuses writes, the majority continues, and healing reconciles the logs exactly | `TestMinorityPartitionRefusesWrites` | ✅ 6 |
| That log-convergence check can actually fail | `TestLogMatchCheckCatchesDivergence` | ✅ 6 |
| A rolling restart loses nothing and serves throughout | `TestRollingRestart` | ✅ 6 |
| Random fault schedules stay linearizable | `TestChaosSeeded` | ✅ 6 |
| A recorded PID is never acted on once it belongs to another program | `TestSupervisorIgnoresAStrangersPID` | ✅ 6 |
| This traceability section names only tests that exist, and covers every guarantee | `TestEveryGuaranteeNamesExistingTests`, `TestTraceabilityCheckCatchesGaps` | ✅ 6 |

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
  node/                   assembles one replica; what quorum-node runs and what in-process tests start
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

### Phase 3

**`WalEntryBatch` gained a `hard_state` field; `HardState` is no longer a
record type of its own.** §2 originally listed three record types, with the
hard state written as a separate record. Two records per Ready means a crash
between them recovers the batch's entries without its hard state. That turns
out to be harmless — nothing from the batch had been sent — but only by an
argument about what the core can put in one Ready, and that argument would
need re-checking every time the core changes. One record per Ready makes the
batch atomic unconditionally, under one CRC. The change is a new field with a
new number, which `buf breaking` accepts, and no WAL had been written before it.

**A Ready with nothing durable costs no fsync.** PLAN.md's criterion said the
fsync count increments exactly once per Ready. The driver still calls `Sync()`
exactly once per Ready, but the WAL skips the fsync when the batch holds no
entries and no hard state — which is every leader heartbeat. Fsyncing an
unchanged file would add a disk flush to every heartbeat for no durability.
Flagged in PLAN.md rather than silently applied.

**The fsync cost knob moved from `MemStorage` to the simulator.**
`SyncCostTicks` was a field on `MemStorage`; it is now only
`testutil.Options.SyncCostTicks`, keyed off the driver's Sync count. That lets
it apply equally when a simulated cluster runs on the real WAL.

**The simulator's storage is pluggable.** `testutil.Options.Storage` takes a
factory, called at start and again on every restart. `WALStorageFactory` puts
each node on a real write-ahead log, so a simulated crash is followed by real
recovery from disk. Storage the simulator runs on must be able to `Crash()` —
drop buffered writes and release its files without flushing — or the
simulator refuses it, because a crash that cost nothing would prove nothing.

**goleak moves to phase 5.** This phase added no goroutines. The driver loop
that will be the first one needs a transport that can deliver inbound
messages, and that is phase 5's gRPC transport.

### Phase 4

**`WalSnapshotPointer` gained `hard_state` and `entries`, and a pointer is
always the first record of a fresh segment.** §2 described the pointer as
naming a file and nothing more, with deletion of superseded segments to follow.
Deletion order is not durable: without a directory fsync per delete, a crash
can leave any subset of the deleted segments behind, including one with a gap
in the middle, which phase 3's recovery rightly refuses. So the pointer now
carries everything the node still needs from before it, meaning the log entries
after the snapshot and the current hard state, and SaveSnapshot rolls to a new
segment before writing it. Recovery starts at the last segment that begins with
a pointer, ignores anything older, and removes it. The deleted segments can
then disappear in any order, and a crash part-way through leaves nothing
recovery has to reason about. It stays one record under one CRC, like
`WalEntryBatch`. These are new field numbers, which `buf breaking` accepts.

**`InstallSnapshotResponse` gained `metadata`, and the last chunk is
acknowledged with an `AppendEntriesResponse`.** A `bytes_received` on its own
does not say which snapshot it counts. An acknowledgement delayed past the
leader's next compaction would then be read as progress on the newer
snapshot. Once the last chunk is installed, the follower's log agrees with the
leader's up to the snapshot index. Answering with an AppendEntries success at
that index lets ordinary replication resume with no second protocol for "done".
etcd does the same.

**`Storage.SaveSnapshot` does not buffer.** Append and SetHardState buffer
until Sync. A snapshot is durable when SaveSnapshot returns. It is called
either between Readies, for a snapshot the node took itself, or as step 0 of a
Ready that installs one from the leader, before that Ready's entries are
buffered. A Ready carrying a snapshot therefore costs more than one fsync: the
snapshot file, the pointer, and then the batch. `WALStats.SnapshotFsyncs`
counts the first two separately, so the invariant "one fsync per Ready with
durable state" still describes `Fsyncs` exactly. `InitialState` now returns a
`Recovered` struct including the snapshot. Its separate applied index is gone,
because the snapshot's index is the applied index a restarted node starts from.

**The core holds the latest snapshot's bytes in memory.** The core cannot read
files, and as leader it must send its snapshot to any follower behind the
compacted prefix. So `Node.Compact` keeps the image the driver hands it, and a
restarted node receives it back through `Config.Snapshot`. A follower builds an
incoming snapshot in memory. A follower that restarts mid-transfer reports zero
bytes received and the leader starts again, rather than persisting partial
state. For a key-value store sized for a demo this is the right trade. A store
whose snapshot did not fit in memory would need the transfer to stream from
the file, which the chunked wire format already allows.

**The state machine arrived in phase 4, not phase 5.** Criterion 4 requires a
retried `(client_id, seq)` to be deduplicated after a restart from a snapshot.
That needs the real apply path and session table, not a stand-in.
`internal/statemachine` is now the key-value store with sessions, the
deterministic snapshot encoding (`quorum.kv.v1.StateMachineSnapshot`, sorted,
so equal states give equal bytes) and restore. The client-facing gRPC service
that proposes into it is still phase 5.

**A follower answers an AppendEntries below its snapshot with success at its
commit index.** A follower that has installed a snapshot, or compacted past
where the leader thinks it is, can no longer check an AppendEntries whose
`prevLogIndex` is below the snapshot. Rejecting it would send a conflict hint
above the leader's nextIndex. The leader rightly refuses to move forward on a
rejection, so the two would stall. Everything below the follower's commit index
is in the leader's log by Leader Completeness, so reporting the commit index is
safe. A stale InstallSnapshot, one that does not reach past the follower's
commit index, is answered the same way.

**Compaction is to the applied index, with no retained margin.** The leader
folds everything the state machine has applied into the snapshot. A follower
only slightly behind at that moment is then caught up by snapshot instead of by
a few AppendEntries. That is correct, and costs extra transfer on a busy
cluster. etcd keeps a margin of entries for this. It is a throughput tuning,
out of scope by §6, and noted here so it is not mistaken for an oversight.

**The snapshot file is written under its final name, not renamed into place.**
A rename buys nothing here. The file is not trusted until a durable pointer
names it, and no pointer is written until the file is fsynced. SaveSnapshot
refuses any snapshot not newer than the current one, so it can never truncate
a file an existing pointer names.

**A wiped node forgets its vote.** `TestWipedNodeCatchesUp` deletes a node's
data directory and requires it to rejoin by snapshot. A wiped node has
forgotten which candidate it voted for, so in general it could vote twice in
one term. The test is safe only because no election is in progress when the
node returns. A production system treats a wiped node as a new member, which
needs the membership changes deferred in §9. The test's comment says so, and
nothing here claims that wiping a node is safe in general.

**The invariant checkers compare logs by index, and gained SnapshotFidelity.**
Once nodes compact on their own schedules, their logs start at different
indices. Log Matching, Leader Append-Only and Leader Completeness all compared
positions and had to learn indices. A leader dropping a prefix into a snapshot
is not an Append-Only violation, and a committed entry inside a new leader's
snapshot is not missing. SnapshotFidelity is new. Every replica's
state-machine hash is recorded at every applied index, and any two that differ
at the same index are a violation, whether a replica got there by applying or
by restoring. State Machine Safety cannot see entries a replica never applied
because a snapshot stood in for them, so this is the only check that a snapshot
carried exactly what the log produced. The simulator's default state machine
(`testutil.Digest`) is a running hash of everything applied, so the check runs
in every randomized trial at negligible cost.

### Phase 5

**The synchronization list in §1 changed.** Phase 1 expected three places to
need a mutex or atomic: the WAL file handle, the status snapshot and the
pending-proposal registry. Two of those turned out to need none, and a place
not on the list did. The WAL is touched only by the driver loop. The registry
lives inside the loop as well: `server.Loop` owns the node, the storage, the
state machine and every pending request, and a gRPC handler sends its request
on a channel and waits for the reply on another. The status snapshot is still
an `atomic.Pointer`. The new one is `grpcx`'s injected-fault state, which
links are blocked. It is read by the loop as it sends, by every peer goroutine
and by every inbound stream handler, and changed by whoever injects the fault.
A channel-only design would need a goroutine owning the fault state and a
round trip per message sent, to protect two small maps. One mutex, documented
where it lives, is the honest choice. Its first version had a race, caught by
the race detector (BUGS.md, 2026-09-30).

**A ReadIndex round is a sequence number in `read_context`.** The field was
reserved in phase 1 as bytes. The core numbers rounds with a `uint64`, and
`pbconv` carries it as 8 little-endian bytes. The leader stamps every
AppendEntries with its latest round. The follower echoes it on its response,
success **or rejection**: either proves the follower still recognizes this
leader in this term. A response echoing round s confirms every read registered
at or before s, so one heartbeat round confirms any number of reads. Reads made
before the leader's first commit in its term wait for that commit before their
round starts (§4, step 1). `raft.MutationReadWithoutQuorum` removes the
confirmation, and it is the negative control for both the stale-read test and
the linearizability test.

**There is no CheckQuorum, so a partitioned leader keeps believing it leads.**
It is refused by ReadIndex, which is correct and exactly what
`TestPartitionedLeaderRefusesRead` requires: its reads time out as `NO_QUORUM`,
and its writes time out as `TIMEOUT`. The cost is that its unconfirmed reads
stay queued in the core until it hears a higher term. The loop forgets its own
waiters once their caller gives up, but the core's queue is bounded only by the
client request rate over the partition's duration. CheckQuorum, where a leader
steps down when it has not heard from a majority, would bound it. Like
pre-vote, it is an availability optimization and not a safety one (§9).

**`ErrLostLeadership` covers two cases.** A pending proposal is answered
`LOST_LEADERSHIP` when its leader steps down, and also when an entry of a
different term is applied at its index, meaning ours was overwritten. The
client cannot tell those apart and does not need to: in both, it retries with
the same seq, and deduplication settles it.

**A write whose outcome the client never learned can never overtake a later
one.** The client library returns `ErrUnknownOutcome` when its context expires
after an ambiguous failure. If that write commits later, after the client has
moved on to seq+1, the state machine sees a seq at or below `last_seq` and
treats it as a retry. It is not applied. So an unanswered write takes effect
before the client's next write or never. The linearizability test models it
more loosely, as "may take effect at any time after it was invoked". That is
conservative: it can only make the checker accept more, never reject a correct
history.

**`internal/node` is new.** It assembles a replica from config: WAL, state
machine restored from the snapshot, core, loop, transport and KV service on one
gRPC listener. `cmd/quorum-node` is a thin wrapper around it. The in-process
tests start the same assembly, so they exercise the wiring the binary ships.
`Node.Kill` in-process drops the WAL without flushing, which simulates a crash.
The real-process test, `TestProcessesServeReadsAndWrites`, kills actual
processes with TerminateProcess/SIGKILL.

**`quorum-node` serves until signalled.** Its phase 1 behaviour, printing its
configuration and exiting, is now `-describe`, and `make run` uses it.

**Registration is not deduplicated,** and session expiry is not implemented. A
retried `RegisterClient` may create a second session, which costs a log entry.
Sessions are never garbage-collected, so `SESSION_EXPIRED` is returned only for
a client id that was never registered. The exactly-once caveat in §4 about
expiry therefore does not arise yet.

**The linearizability workload includes a read-only client.** Without one, the
workload could not produce a stale read from a partitioned leader, even with
ReadIndex's quorum check removed (BUGS.md, 2026-09-30). Every writer is dragged
off a cut-off leader within one request timeout, and they all leave at about
the same moment. A reader stays with the node it believes leads for as long as
that node answers, as real read-mostly clients do.

### Phase 6

**Logical time in the real loop comes from the wall clock.** §1 says a slow
fsync delays ticks and that the delay is measured. The loop as first written
counted one tick per `time.Ticker` event. A ticker drops the ticks its
receiver is too busy to take, so the measured lag was structurally zero, and
logical time ran slow whenever the loop was busy. The ticker now only wakes
the loop. The ticks due are counted from the clock and delivered, so a stall
shows up both as lag and as timers catching up, as the simulator always
modelled it. Time spent frozen is deliberately not replayed. BUGS.md,
2026-09-30.

**Reconnect backoff is bounded by the heartbeat interval, in both layers.** A
restarted node that hears nothing for an election timeout campaigns and
deposes a healthy leader. `grpcx`'s own reconnect backoff and gRPC's internal
redial schedule (1 s rising to two minutes by default) both exceeded the
election timeout, so every restart cost an election. Both are now capped at
one heartbeat interval. §5's validation keeps that under a third of the
election timeout. Pre-vote (§9) remains the protocol-level defence; this
removes the trigger. BUGS.md, 2026-09-30.

**The supervisor records PIDs and checks who owns them.** Each node's PID is
written to `node.pid` in its data directory, so one `quorumctl` can kill what
another started. Operating systems reuse PIDs. Before acting on a recorded
PID, the supervisor checks the process is still running the `quorum-node`
binary it started, through `QueryFullProcessImageName` on Windows and
`/proc/<pid>/exe` on Linux. On other platforms that check is unavailable, and
a stale PID file could in principle name a stranger;
`TestSupervisorIgnoresAStrangersPID` is skipped there. A kill is not reported
until the operating system says the process is gone.

**`quorumctl down` kills.** Windows has no portable way to ask another process
to shut down cleanly, and a kill is exactly what the write-ahead log exists to
survive. So `down` is a real kill on every platform, and its help text says so.

**Nodes started by `up` are detached** into their own process group (Windows)
or session (Unix). They outlive the `quorumctl` that started them and do not
receive its terminal's Ctrl+C.

**Admin status never goes through the loop.** It is read from the snapshot the
loop publishes, so a frozen node still answers `GetStatus`, which is when it
matters most. `tick_lag_ticks` reports the worst lag seen: at the instant the
snapshot is published, the loop has just caught up, so the current backlog is
zero by construction. `ms_since_last_contact` is known only on a leader, which
hears from every peer; elsewhere it is -1, and "reachable" means only "not
blocked by injection". `fsync_p99_micros` now exists: the WAL keeps the last
512 per-Ready fsync latencies.

**Under `make race`, the nodes run under the race detector too.** The
integration tests build `quorum-node` with `-race` when they themselves run
with it, and fail any test whose node logs contain a race report.
`TestNodesAreRaceCheckedUnderRace` checks the binary's build flags, so the log
scan cannot pass merely because nothing was instrumented.

**"Reproducible from its seed" means the schedule, not the interleaving.**
`TestChaosSeeded`'s seed fixes every fault, target, duration and client
operation sequence. It cannot fix how real processes interleave on a real
clock. A failing seed replays the same attack, not the same history, so the
test logs the schedule. `QUORUM_CHAOS_SEED` reruns one seed and
`QUORUM_CHAOS_ITERATIONS` runs a longer soak.

**The guarantees table is enforced.** §7 now opens with a table mapping each
§6 guarantee to its tests, and `TestEveryGuaranteeNamesExistingTests` fails if
a guarantee lacks a row or a named test does not exist. Durability across
**OS** crashes is the one half-claim no test here can exercise, and its row
says so rather than implying otherwise.
