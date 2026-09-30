# Visualizer frontend

The live cluster visualizer's page: `index.html`, `app.js`, `style.css`,
embedded into the `quorum-viz` binary by `embed.go`. Run it with `make viz`
(or `.\make.ps1 viz`) and open http://127.0.0.1:8080.

**Vanilla JS plus server-sent events.** No npm, no bundler, no build step —
one binary. This was a deliberate choice over React: the page renders a
topology diagram, a handful of node cards and a timeline, and a toolchain would
cost more than it returns.

What the page shows, all from one stream (`/api/events`):

- **Topology.** Every directed link is its own arrow, so a one-way partition is
  one red dashed arrow beside a green one. A node's colour is its role; grey
  means its process is gone or it is not answering; a dashed blue ring means
  frozen.
- **Node cards.** Role, term, leader, commit, applied, last index, snapshot
  index, tick lag, fsync p99 and elections started, plus the log tail —
  aligned by index across cards, coloured by term, hollow until committed — so
  an entry can be watched appearing on the leader and then on each follower.
- **Timeline.** Elections, processes going and coming, faults injected and
  healed.

Every button is a POST to one of the controls in
[`internal/viz/http.go`](../internal/viz/http.go), which reach the cluster only
through the supervisor (processes) and the admin and KV APIs — the same calls
`quorumctl` and the fault-injection suite make. There is no path from the
browser to Raft state that bypasses them; `TestVizCannotReachRaftState` and
`TestRoutesAreExactlyTheDocumentedControls` enforce it. Controls also require
a custom request header, so another web page cannot press them through your
browser.

The labels keep the project's honesty constraints: a partition is cut in each
node's transport, not by a kernel firewall, and a freeze parks the node's loop
cooperatively — it is not `SIGSTOP`. A kill is a real `TerminateProcess` /
`SIGKILL`.
