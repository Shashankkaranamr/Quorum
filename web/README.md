# Visualizer frontend

Reserved for **phase 7**. Currently empty.

The live cluster visualizer: current leader, terms, and log entries replicating
in real time, with "kill node" and "partition network" controls.

**Vanilla JS plus server-sent events, served from `embed.FS` inside
`cmd/quorum-viz`.** No npm, no bundler, no build step — one binary. This was a
deliberate choice over React: the page renders a handful of node cards and a log
list, and a toolchain would cost more than it returns. Assets in this directory
are embedded directly.

Every control goes through the admin API (`proto/quorum/admin/v1`), which is the
same API the fault-injection suite drives. There is no path from the browser to
Raft state that bypasses it, so the UI cannot corrupt the cluster — and what the
demo shows cannot drift from what the tests exercise.

Acceptance criteria: [PLAN.md § Phase 7](../PLAN.md#phase-7--live-cluster-visualizer).
