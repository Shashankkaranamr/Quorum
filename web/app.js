// Quorum visualizer. Everything on this page comes from /api/events, a stream
// of the aggregated cluster state, and every button is a POST to one of the
// controls listed in internal/viz/http.go -- which reach the cluster only
// through the supervisor and the admin and KV APIs.
"use strict";

const CONTROL_HEADER = "X-Quorum-Viz";
const LOG_WINDOW = 16;
let state = null;
const cards = new Map();

const $ = (id) => document.getElementById(id);

function el(tag, attrs = {}, ...children) {
  const n = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs)) {
    if (k === "class") n.className = v;
    else if (k.startsWith("on")) n.addEventListener(k.slice(2), v);
    else n.setAttribute(k, v);
  }
  for (const c of children) n.append(c);
  return n;
}

function svg(tag, attrs = {}) {
  const n = document.createElementNS("http://www.w3.org/2000/svg", tag);
  for (const [k, v] of Object.entries(attrs)) n.setAttribute(k, v);
  return n;
}

// ---------------------------------------------------------------------------
// Controls
// ---------------------------------------------------------------------------

function toast(msg, isErr) {
  const t = $("toast");
  t.textContent = msg;
  t.classList.toggle("err", !!isErr);
}

async function control(path, body) {
  try {
    const res = await fetch(path, {
      method: "POST",
      headers: { "Content-Type": "application/json", [CONTROL_HEADER]: "1" },
      body: JSON.stringify(body || {}),
    });
    if (!res.ok) throw new Error((await res.text()).trim() || res.statusText);
    toast(path.replace("/api/", "") + ": ok");
  } catch (e) {
    toast(path.replace("/api/", "") + ": " + e.message, true);
  }
}

// ---------------------------------------------------------------------------
// Rendering
// ---------------------------------------------------------------------------

// A node's display status: what the visualizer saw, then what it said.
function status(n) {
  if (n.process !== "running") return "down";
  if (!n.reachable) return "unreachable";
  return n.role;
}

function termColor(term) {
  return `hsl(${(term * 67) % 360} 60% 52%)`;
}

function renderHeader(s) {
  $("cluster").textContent = s.clusterId;
  $("leader").textContent = s.leader ? "node " + s.leader : "none";
  $("term").textContent = s.term || "—";
  $("load").checked = s.load;
}

function renderTopology(s) {
  const root = $("topo");
  root.replaceChildren();
  const defs = svg("defs");
  for (const kind of ["up", "cut", "down"]) {
    const m = svg("marker", { id: "arrow-" + kind, viewBox: "0 0 10 10", refX: "9", refY: "5",
      markerWidth: "7", markerHeight: "7", orient: "auto-start-reverse" });
    m.append(svg("path", { d: "M0,0 L10,5 L0,10 z", fill: `var(--${kind})` }));
    defs.append(m);
  }
  root.append(defs);

  const cx = 200, cy = 170, r = 110, nodeR = 28;
  const pos = new Map();
  s.nodes.forEach((n, i) => {
    const a = -Math.PI / 2 + (2 * Math.PI * i) / s.nodes.length;
    pos.set(n.id, { x: cx + r * Math.cos(a), y: cy + r * Math.sin(a) });
  });

  // Each directed link is its own line, offset to one side, so a one-way cut
  // shows as one red arrow beside one green one.
  for (const l of s.links) {
    const a = pos.get(l.from), b = pos.get(l.to);
    const dx = b.x - a.x, dy = b.y - a.y, len = Math.hypot(dx, dy);
    const ux = dx / len, uy = dy / len, off = 5;
    const ox = -uy * off, oy = ux * off;
    const kind = l.injected ? "cut" : l.up ? "up" : "down";
    root.append(svg("line", {
      x1: a.x + ux * nodeR + ox, y1: a.y + uy * nodeR + oy,
      x2: b.x - ux * (nodeR + 3) + ox, y2: b.y - uy * (nodeR + 3) + oy,
      stroke: `var(--${kind})`, "stroke-width": l.injected ? 2.2 : 1.6,
      "stroke-dasharray": l.injected ? "5 4" : l.up ? "" : "2 4",
      "marker-end": `url(#arrow-${kind})`, opacity: l.up || l.injected ? 1 : 0.5,
    }));
  }

  for (const n of s.nodes) {
    const p = pos.get(n.id), st = status(n);
    const fill = st === "leader" ? "var(--leader)" : st === "candidate" ? "var(--candidate)"
      : st === "follower" ? "var(--follower)" : "var(--down)";
    if (n.frozen && n.reachable) {
      root.append(svg("circle", { cx: p.x, cy: p.y, r: nodeR + 5, fill: "none", stroke: "var(--frozen)",
        "stroke-width": 3, "stroke-dasharray": "3 3" }));
    }
    root.append(svg("circle", { cx: p.x, cy: p.y, r: nodeR, fill, stroke: "var(--panel)", "stroke-width": 2 }));
    const t = svg("text", { x: p.x, y: p.y + 5, "text-anchor": "middle", class: "nodelabel" });
    t.textContent = n.id;
    root.append(t);
    // The label goes outward from the centre, away from the links, which all
    // run inside the ring.
    const ox = (p.x - cx) / r, oy = (p.y - cy) / r;
    const lx = p.x + ox * (nodeR + 12), ly = p.y + oy * (nodeR + 12) + (oy > 0.3 ? 10 : oy < -0.3 ? -2 : 4);
    const anchor = ox > 0.3 ? "start" : ox < -0.3 ? "end" : "middle";
    const sub = svg("text", { x: lx, y: ly, "text-anchor": anchor, class: "sub" });
    sub.textContent = st === "down" || st === "unreachable" ? st
      : `${st} · t${n.term}${n.frozen ? " · frozen" : ""}`;
    root.append(sub);
  }
}

function makeCard(n) {
  const c = {};
  c.root = el("article", { class: "card" });
  c.title = el("span");
  c.badges = el("span");
  c.root.append(el("h3", {}, c.title, c.badges));
  c.stale = el("p", { class: "stale" });
  c.kv = el("dl", { class: "kv" });
  c.log = el("div", { class: "log", title: "log tail: colour is term; hollow is not yet committed" });
  c.axis = el("div", { class: "logaxis" });
  c.kill = el("button", { class: "danger", title: "TerminateProcess / SIGKILL: no graceful shutdown",
    onclick: () => control(`/api/nodes/${n.id}/kill`) }, "Kill");
  c.start = el("button", { title: "restart on the same data directory",
    onclick: () => control(`/api/nodes/${n.id}/start`) }, "Start");
  c.freeze = el("button", { title: "park the node's event loop (cooperative, not SIGSTOP)",
    onclick: () => control(`/api/nodes/${n.id}/${c.frozen ? "thaw" : "freeze"}`) }, "Freeze");
  c.root.append(c.stale, c.kv, c.log, c.axis, el("div", { class: "row" }, c.kill, c.start, c.freeze));
  $("nodes").append(c.root);
  cards.set(n.id, c);
  return c;
}

function renderCards(s) {
  const hi = Math.max(0, ...s.nodes.map((n) => n.lastIndex || 0));
  const lo = Math.max(1, hi - LOG_WINDOW + 1);
  for (const n of s.nodes) {
    const c = cards.get(n.id) || makeCard(n);
    const st = status(n);
    c.root.className = "card " + st + (n.frozen && n.reachable ? " frozen" : "");
    c.title.textContent = `node ${n.id}`;
    c.badges.replaceChildren(el("span", { class: "badge " + st }, st));
    if (n.frozen && n.reachable) c.badges.append(el("span", { class: "badge frozen" }, "frozen"));
    c.frozen = n.frozen;

    c.stale.textContent = n.process === "down" ? `no process${n.stale ? " — last seen as " + n.role + ", term " + n.term : ""}`
      : !n.reachable ? `process ${n.pid} is not answering${n.stale ? "; last seen as " + n.role : ""}`
      : `pid ${n.pid} · ${n.addr}`;

    const kv = [
      ["term", n.term], ["leader", n.leader || "—"],
      ["commit", n.commit], ["applied", n.applied],
      ["last", `${n.lastIndex}@${n.lastTerm}`], ["snapshot", n.snapIndex],
      ["tick lag", n.metrics.tickLag], ["fsync p99", `${(n.metrics.fsyncP99Us / 1000).toFixed(1)}ms`],
      ["elections", n.metrics.elections], ["fsyncs", n.metrics.fsyncs],
    ];
    c.kv.replaceChildren(...kv.flatMap(([k, v]) => [el("dt", {}, k), el("dd", {}, String(v))]));

    // The log tail, aligned by index across every card, so an entry can be
    // seen appearing on the leader and then on each follower.
    const byIndex = new Map((n.log || []).map((e) => [e.index, e]));
    const cells = [];
    for (let i = lo; i <= hi; i++) {
      const e = byIndex.get(i);
      if (!e) {
        cells.push(el("span", { class: "e gap", title: `#${i}: ${i <= n.snapIndex ? "in snapshot" : "not held"}` }));
        continue;
      }
      const col = termColor(e.term);
      cells.push(el("span", {
        class: "e" + (e.committed ? "" : " uncommitted"),
        style: `background:${col};border-color:${col}`,
        title: `#${e.index} term ${e.term}${e.committed ? "" : " (uncommitted)"}: ${e.summary}`,
      }));
    }
    c.log.replaceChildren(...cells);
    c.axis.replaceChildren(el("span", {}, `#${lo}`), el("span", {}, `#${hi}`));

    c.kill.disabled = n.process !== "running";
    c.start.disabled = n.process === "running";
    c.freeze.disabled = !n.reachable;
    c.freeze.textContent = n.frozen ? "Thaw" : "Freeze";
  }
}

function renderGroups(s) {
  const g = $("groups");
  if (g.childElementCount) return; // membership is static; build once
  g.append(el("span", { class: "hd" }, "node"), el("span", { class: "hd" }, "A"), el("span", { class: "hd" }, "B"));
  for (const n of s.nodes) {
    g.append(el("span", {}, `node ${n.id}`),
      el("input", { type: "checkbox", "data-group": "a", value: n.id, "aria-label": `node ${n.id} in group A` }),
      el("input", { type: "checkbox", "data-group": "b", value: n.id, "aria-label": `node ${n.id} in group B` }));
  }
}

function renderEvents(s) {
  const items = [...(s.events || [])].reverse().map((e) =>
    el("li", {}, el("time", {}, new Date(e.at).toLocaleTimeString()), el("span", { class: e.kind }, e.text)));
  $("events").replaceChildren(...items);
}

function render(s) {
  state = s;
  renderHeader(s);
  renderTopology(s);
  renderGroups(s);
  renderCards(s);
  renderEvents(s);
}

// ---------------------------------------------------------------------------
// Wiring
// ---------------------------------------------------------------------------

function picked(group) {
  return [...document.querySelectorAll(`#groups input[data-group="${group}"]:checked`)].map((i) => Number(i.value));
}

$("cut").addEventListener("click", () => {
  const a = picked("a"), b = picked("b");
  if (!a.length || !b.length) return toast("pick at least one node for A and one for B", true);
  control("/api/partition", { a, b, oneway: $("oneway").checked });
});
$("oneway").addEventListener("change", (e) => {
  $("cut").textContent = e.target.checked ? "Cut A → B" : "Cut A ⟷ B";
});
$("heal").addEventListener("click", () => control("/api/heal"));
$("putform").addEventListener("submit", (e) => {
  e.preventDefault();
  control("/api/put", { key: $("key").value, value: $("value").value });
});
$("load").addEventListener("change", (e) => control("/api/load", { on: e.target.checked }));

function connect() {
  const src = new EventSource("/api/events");
  src.addEventListener("state", (e) => {
    $("conn").textContent = "live";
    $("conn").className = "conn live";
    render(JSON.parse(e.data));
  });
  src.onerror = () => {
    $("conn").textContent = "reconnecting…";
    $("conn").className = "conn lost";
  };
}
connect();
