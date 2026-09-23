# Quorum

**A fault-tolerant replicated key-value store built on a from-scratch Raft
implementation.**

Raft is the consensus algorithm that lets several machines hold one trustworthy
shared answer even when individual machines crash, freeze or lose contact with
each other. Quorum runs three to five nodes as separate local processes, each
holding an identical copy of the store, and guarantees that as long as a
majority of them are alive and can talk to each other, reads and writes stay
correct and consistent — even mid-failure. It implements leader election with
randomized timeouts so exactly one node accepts writes at a time, log
replication that only commits an entry once a majority have durably recorded it,
and crash-safe persistence so a node that restarts never loses or contradicts
what it had already agreed to. Snapshots compact the history so a rejoining node
catches up quickly instead of replaying everything. GET and PUT are exposed over
gRPC and are linearizable: a client is guaranteed to see the effects of every
write it has already been told succeeded, even if the leader that accepted it
later dies. This is the same underlying problem etcd solves for Kubernetes'
cluster state.

> **Status: phase 1 of 8 — design complete, implementation starting.**
>
> The design decisions are made and written down in [DESIGN.md](DESIGN.md); the
> roadmap and the acceptance criteria for each phase are in [PLAN.md](PLAN.md).
> What exists today is the scaffold, the configuration layer, the protobuf
> schemas that fix the log format and client contract, and the tests that keep
> the consensus core honest. **No consensus logic is implemented yet.**

---

## Correctness is proven, not assumed

That is the actual point of this project, so it is worth being concrete about
what that means here:

- A **fault-injection suite** (phase 6) kills the leader mid-write, partitions
  the network into a minority and a majority, and crashes and restarts
  individual nodes — then checks that a new leader was elected, that the
  minority side *refused* writes rather than accepting them, and that every node
  caught back up correctly.
- Every claim in [DESIGN.md §6](DESIGN.md#6-what-this-will-and-will-not-guarantee)
  is mapped to a named test in the
  [traceability table](DESIGN.md#7-claim-to-test-traceability). A guarantee with
  no test behind it is treated as a defect.
- **The checkers are themselves checked.** The Raft safety-invariant checkers
  are run against deliberately broken Raft implementations, and the suite fails
  if a checker does *not* catch its mutation. A checker nobody has seen fail
  proves nothing.
- [BUGS.md](BUGS.md) documents each bug that testing actually found: symptom,
  root cause, fix, and the regression test that now guards it.

A live cluster visualizer (phase 7) shows the current leader, terms and log
entries replicating in real time, with "kill node" and "partition network"
controls, so the behaviour under failure is something you can watch. The demo is
a recorded walkthrough — the whole thing runs on one machine, with no server, no
hosting and no public exposure.

---

## Quickstart

Requires **Go 1.25+** and nothing else. Generated protobuf code is checked in,
so no protobuf tooling is needed to build or test.

```bash
git clone https://github.com/Shashankkaranamr/Quorum
cd Quorum

make build     # build every binary into bin/
make test      # run the test suite
make lint      # go vet + golangci-lint
make run       # show the cluster plan and one node's configuration
```

On Windows, where `make` is usually absent, `make.ps1` mirrors every target:

```powershell
.\make.ps1 build
.\make.ps1 test
.\make.ps1 run
```

The two are kept in step by a test that fails if either grows a target the other
does not have.

`make help` lists everything. `make ci` runs what CI runs: format check, lint,
build, tests and the race detector.

> **`make race` needs a 64-bit C compiler**, because Go's race detector needs
> cgo. That is the default on Linux and macOS. On Windows it often is not — an
> old 32-bit MinGW earlier on PATH fails with `sorry, unimplemented: 64-bit mode
> not compiled in`, which reads like a Go problem and is not. `make.ps1` looks
> for a usable compiler itself; if it finds none, install one with
> `winget install BrechtSanders.WinLibs.POSIX.UCRT`.
>
> Regenerating the protobuf code (`make proto`) additionally needs `make tools`
> once, which installs `buf` and the codegen plugins with `go install`. Neither
> is needed just to build or test.

### Configuring a cluster

[`cluster.yaml`](cluster.yaml) defines a three-node cluster;
[`cluster-5.yaml`](cluster-5.yaml) defines a five-node one. Node *i* gets gRPC
port `7000+i`, HTTP port `8000+i`, and `data/node-<i>` for its write-ahead log
and snapshots.

Configuration is validated strictly. Unknown fields are errors rather than
silently ignored typos, and configurations that are well-formed but wrong —
an election timeout too close to the heartbeat interval, an empty randomization
range, a cluster of two — are rejected at startup rather than showing up later
as erratic elections that look like a consensus bug.

---

## What it does and does not guarantee

**It guarantees** linearizable single-key reads and writes while a majority is
alive and connected; at-most-once application of client commands within a live
session; durability of acknowledged writes across process and OS crashes; and
that no committed entry is ever lost, reordered, or applied differently on two
nodes.

**It does not guarantee** Byzantine fault tolerance (crash-stop only, no message
authentication), dynamic membership, anything beyond a single machine on
localhost, multi-key transactions, throughput, or protection against hardware
that lies about flush barriers. Liveness holds only under partial synchrony —
safety always.

Two things the demo deliberately does not overstate: injected **partitions are
enforced in the transport layer**, not by kernel firewall rules, and **freeze is
cooperative**, not `SIGSTOP`. Kills, however, are real `TerminateProcess` /
`SIGKILL`. The full list, with reasoning, is in
[DESIGN.md §6](DESIGN.md#6-what-this-will-and-will-not-guarantee).

---

## How it is put together

The one structural idea everything follows from: **the consensus algorithm is a
pure state machine, and all I/O lives outside it.**

Package `raft` has no clock, no network, no disk, no goroutines and no locks.
Logical time advances only when `Tick()` is called; randomness is injected. A
single driver goroutine owns the node exclusively, so there is no shared Raft
state and therefore nothing to lock — which makes the classic failure of holding
a lock across an fsync structurally impossible.

That claim is checked rather than asserted: `raft/purity_test.go` walks the
package's imports against an allowlist, and a negative control confirms the
checker actually fails when the core is made impure.

It also buys the thing that makes the testing credible. The fast deterministic
simulator and the real multi-process deployment run **identical consensus code**
— only the clock and the two I/O interfaces differ. The three things the fast
suite does not cover (gRPC transport, WAL file I/O, driver goroutine wiring) are
named explicitly in [DESIGN.md §3](DESIGN.md#3-transport-and-how-one-core-serves-two-test-modes)
and covered end-to-end in phase 6.

```
raft/            pure consensus core — the only exported package
internal/
  server/        the driver loop; the one place I/O ordering is decided
  storage/       write-ahead log, snapshots, framing codec
  statemachine/  KV map + client session table
  transport/     interface, with inmem/ (simulator) and grpcx/ (real gRPC)
  kvservice/     client API, ReadIndex reads, leader redirect
  admin/         status and fault injection
  supervisor/    spawn, kill and restart node processes
  config/        cluster.yaml loading and validation
  testutil/      cluster harness, invariant checkers, linearizability model
cmd/             quorum-node, quorumctl, quorum-viz
proto/           schemas for the log format, KV contract and admin API
```

---

## Documentation

| | |
|---|---|
| [PROGRESS.md](PROGRESS.md) | Current state: what is built, what is verified, what is next |
| [DESIGN.md](DESIGN.md) | The five design decisions and why, the guarantees, and the claim-to-test table |
| [PLAN.md](PLAN.md) | The 8-phase roadmap with acceptance criteria per phase |
| [BUGS.md](BUGS.md) | Bugs testing actually found, with the regression test guarding each |
| [CLAUDE.md](CLAUDE.md) | Working rules and invariants for anyone (or any agent) contributing |

## License

MIT — see [LICENSE](LICENSE).
