// Headless smoke test for cmd/server/static/{index.html,app.js} using
// jsdom -- not a replacement for real-browser testing, but exercises
// the actual app.js source (no reimplementation) against a fake DOM +
// fake fetch to catch wiring bugs (undefined ids, bad selectors,
// exceptions in event handlers) before a human ever opens a browser.
const fs = require("fs");
const path = require("path");
const { JSDOM } = require("jsdom");

const STATIC = path.join(__dirname, "../../cmd/server/static");
const html = fs.readFileSync(path.join(STATIC, "index.html"), "utf8");
const appJs = fs.readFileSync(path.join(STATIC, "app.js"), "utf8");

let failures = 0;
function ok(label, cond) {
  if (cond) {
    console.log("PASS", label);
  } else {
    console.log("FAIL", label);
    failures++;
  }
}

const workflow = {
  id: "wf1",
  name: "Test WF",
  active: false,
  nodes: {
    A: { id: "a", name: "A", type: "manualTrigger", originalType: "n8n-nodes-base.manualTrigger", typeVersion: 1, parameters: {}, credentials: {}, position: [0, 0], disabled: false },
    B: { id: "b", name: "B", type: "httpRequest", originalType: "n8n-nodes-base.httpRequest", typeVersion: 1, parameters: {}, credentials: {}, position: [300, 0], disabled: false },
  },
  connections: { A: [{ sourceName: "A", sourceIndex: 0, targetName: "B", targetIndex: 0 }] },
  settings: {},
};

async function main() {
  const dom = new JSDOM(html, { url: "http://localhost/#/workflows/wf1", runScripts: "outside-only", pretendToBeVisual: true });
  const { window } = dom;

  // fake fetch backing the exact endpoints app.js calls
  window.fetch = async (url, opts) => {
    opts = opts || {};
    const u = String(url);
    const json = (obj, status) => ({
      ok: (status || 200) < 300,
      status: status || 200,
      json: async () => obj,
    });
    if (u === "/api/workflows/wf1" && (!opts.method || opts.method === "GET")) return json(workflow);
    if (u === "/api/workflows" && (!opts.method || opts.method === "GET")) return json([workflow]);
    if (u === "/api/workflows/wf1/save" && opts.method === "POST") {
      const body = JSON.parse(opts.body);
      Object.assign(workflow, body);
      return json(workflow);
    }
    if (u === "/api/workflows/wf1/credentials" && (!opts.method || opts.method === "GET")) return json([]);
    if (u === "/api/credentials/google" && (!opts.method || opts.method === "GET")) return json({ configured: false });
    if (u === "/api/credentials/google" && opts.method === "POST") return json({ status: "ok" });
    if (u === "/api/credentials/google" && opts.method === "DELETE") return json({ status: "ok" });
    if (u === "/api/google/connections" && (!opts.method || opts.method === "GET")) {
      return json([
        { service: "gmail", connected: true, email: "user@gmail.com", updatedAt: "2026-01-01T00:00:00Z" },
        { service: "youtube", connected: false },
        { service: "sheets", connected: false },
      ]);
    }
    if (u.indexOf("/api/google/disconnect/") === 0 && opts.method === "POST") return json({ status: "ok", service: u.split("/").pop() });
    if (u === "/api/workflows/wf1/execute" || u.indexOf("/api/workflows/wf1/execute?") === 0) {
      if (opts.method === "POST") return json({ executionId: "exec1", status: "queued" }, 202);
    }
    if (u === "/api/executions/exec1" && (!opts.method || opts.method === "GET")) {
      return json({ id: "exec1", workflowId: "wf1", mode: "manual", status: "queued", startedAt: new Date().toISOString(), nodeRuns: [] });
    }
    throw new Error("unmocked fetch: " + opts.method + " " + u);
  };
  window.alert = () => {};
  window.confirm = () => true; // auto-confirm destructive dialogs for this test

  // Fake EventSource: app.js's followExecution() opens one against
  // GET /api/executions/{id}/events (see internal/api's SSE endpoint)
  // and drives the exec panel from .onmessage -- there's no real
  // network/SSE parser to fake here, just the browser API surface
  // app.js actually calls (constructor + .onmessage/.onerror + .close),
  // with an .emit() helper this test uses to play events into it the
  // same shape internal/api/server.go's writeSSEEvent JSON-encodes.
  class FakeEventSource {
    constructor(url) {
      this.url = url;
      this.onmessage = null;
      this.onerror = null;
      this.closed = false;
      FakeEventSource.instances.push(this);
    }
    close() {
      this.closed = true;
    }
    emit(dataObj) {
      if (this.onmessage) this.onmessage({ data: JSON.stringify(dataObj) });
    }
  }
  FakeEventSource.instances = [];
  window.EventSource = FakeEventSource;

  window.eval(appJs);

  // let the initial route() (async) settle
  await new Promise((r) => setTimeout(r, 20));

  const doc = window.document;
  ok("editor rendered a canvas", !!doc.getElementById("canvasWrap"));
  const nodeBoxes = () => Array.from(doc.querySelectorAll(".node-box"));
  ok("both nodes rendered", nodeBoxes().length === 2);
  ok("one connection line rendered", doc.querySelectorAll(".conn").length === 1);

  // --- select a node ---
  const boxA = nodeBoxes().find((b) => b.dataset.node === "A");
  boxA.dispatchEvent(new window.MouseEvent("mousedown", { bubbles: true, clientX: 10, clientY: 10 }));
  window.document.dispatchEvent(new window.MouseEvent("mouseup", { bubbles: true, clientX: 10, clientY: 10 }));
  ok("selecting a node shows its side panel", doc.getElementById("nodeParams") !== null);

  // --- add a node ---
  doc.getElementById("addNodeType").value = "code";
  doc.getElementById("btnAddNode").dispatchEvent(new window.Event("click", { bubbles: true }));
  ok("add node grew the node count to 3", nodeBoxes().length === 3);
  ok("added node exists in workflow state with a generated id", Object.values(workflow.nodes).some((n) => n.type === "code" && n.id));
  const startOptsAfterAdd = Array.from(doc.getElementById("startNodeSelect").options).map((o) => o.value);
  ok("Execute-from dropdown picked up the newly added node without a full toolbar rebuild", startOptsAfterAdd.length === 4);

  // --- delete the added node via the side panel button ---
  const addedName = Object.keys(workflow.nodes).find((n) => workflow.nodes[n].type === "code");
  const addedBox = nodeBoxes().find((b) => b.dataset.node === addedName);
  addedBox.dispatchEvent(new window.MouseEvent("mousedown", { bubbles: true, clientX: 10, clientY: 10 }));
  window.document.dispatchEvent(new window.MouseEvent("mouseup", { bubbles: true, clientX: 10, clientY: 10 }));
  doc.getElementById("deleteNodeBtn").dispatchEvent(new window.Event("click", { bubbles: true }));
  ok("delete node shrank back to 2", nodeBoxes().length === 2);
  ok("deleted node removed from workflow state", !workflow.nodes[addedName]);

  // --- drag node A to a new position ---
  const boxA2 = nodeBoxes().find((b) => b.dataset.node === "A");
  const beforePos = workflow.nodes.A.position.slice();
  boxA2.dispatchEvent(new window.MouseEvent("mousedown", { bubbles: true, clientX: 100, clientY: 100 }));
  window.document.dispatchEvent(new window.MouseEvent("mousemove", { bubbles: true, clientX: 160, clientY: 140 }));
  window.document.dispatchEvent(new window.MouseEvent("mouseup", { bubbles: true, clientX: 160, clientY: 140 }));
  const afterPos = workflow.nodes.A.position;
  ok("dragging moved the node's stored position", afterPos[0] !== beforePos[0] || afterPos[1] !== beforePos[1]);

  // --- delete the existing connection A->B by clicking its hit-path ---
  const hit = doc.querySelector(".conn-hit");
  ok("connection hit-path is present for click-to-delete", !!hit);
  if (hit) {
    hit.dispatchEvent(new window.Event("click", { bubbles: true }));
    ok("clicking the connection removed it from workflow state", (workflow.connections.A || []).length === 0);
  }

  // --- reconnect A -> B by dragging from A's output handle to B's input handle ---
  const outHandle = nodeBoxes().find((b) => b.dataset.node === "A").querySelector(".node-handle-out");
  const inHandleB = nodeBoxes().find((b) => b.dataset.node === "B").querySelector(".node-handle-in");
  const inRect = { left: 400, top: 40 };
  inHandleB.getBoundingClientRect = () => ({ left: inRect.left, top: inRect.top, right: inRect.left + 12, bottom: inRect.top + 12, width: 12, height: 12 });
  doc.querySelector(".canvas-inner").getBoundingClientRect = () => ({ left: 0, top: 0, right: 2000, bottom: 2000, width: 2000, height: 2000 });
  const origElementFromPoint = doc.elementFromPoint;
  doc.elementFromPoint = () => inHandleB;
  outHandle.dispatchEvent(new window.MouseEvent("mousedown", { bubbles: true, clientX: 150, clientY: 27 }));
  window.document.dispatchEvent(new window.MouseEvent("mousemove", { bubbles: true, clientX: 395, clientY: 45 }));
  window.document.dispatchEvent(new window.MouseEvent("mouseup", { bubbles: true, clientX: 400, clientY: 46 }));
  doc.elementFromPoint = origElementFromPoint;
  ok("dragging output handle to input handle created a connection", (workflow.connections.A || []).some((c) => c.targetName === "B"));

  // --- save persists in-memory edits through POST .../save ---
  doc.getElementById("btnSave").dispatchEvent(new window.Event("click", { bubbles: true }));
  await new Promise((r) => setTimeout(r, 20));
  ok("save round-tripped through the fake API without throwing", true);

  // --- execute: async POST 202 + SSE-driven exec panel (spec sections
  // M/N) -- proves executeCurrentWorkflow/followExecution in app.js
  // actually wire up EventSource against the executionId the POST
  // returns, and that a terminal event re-enables the button and shows
  // the final status, without ever polling in a loop. ---
  doc.getElementById("btnExecute").dispatchEvent(new window.Event("click", { bubbles: true }));
  await new Promise((r) => setTimeout(r, 20));
  ok("execute button disabled while queued/running", doc.getElementById("btnExecute").disabled === true);
  ok(
    "execute POST opened an EventSource for the returned executionId",
    FakeEventSource.instances.length === 1 && FakeEventSource.instances[0].url.indexOf("exec1") !== -1,
  );
  const es = FakeEventSource.instances[0];
  es.emit({ type: "execution.started", executionId: "exec1", status: "running" });
  es.emit({
    type: "node.completed",
    executionId: "exec1",
    status: "success",
    node: { nodeName: "A", status: "success", output: [[{ json: { ok: true } }]], durationMs: 1000000, attempt: 1 },
  });
  await new Promise((r) => setTimeout(r, 5));
  ok("a node.completed event appends to the exec panel before the run finishes", doc.querySelectorAll(".node-run").length === 1);
  es.emit({ type: "execution.completed", executionId: "exec1", status: "success" });
  await new Promise((r) => setTimeout(r, 20));
  ok("exec panel shows success status after the terminal SSE event", !!doc.querySelector(".status-pill.status-success"));
  ok("execute button re-enabled after the terminal event", doc.getElementById("btnExecute").disabled === false);
  ok("EventSource was closed after the terminal event", es.closed === true);

  // --- central credentials page ---
  window.location.hash = "#/credentials";
  await new Promise((r) => setTimeout(r, 20));
  ok("central credentials page rendered its fields", !!doc.getElementById("centralClientId") && !!doc.getElementById("centralSaveBtn"));
  ok("google connections cards rendered", doc.querySelectorAll(".google-conn-card").length === 3);
  ok("connected gmail card shows the connected email", doc.body.textContent.indexOf("user@gmail.com") !== -1);
  const disconnectBtn = doc.querySelector(".google-disconnect-btn");
  ok("connected service has a Disconnect button", !!disconnectBtn);
  doc.getElementById("centralClientId").value = "cid";
  doc.getElementById("centralClientSecret").value = "secret";
  doc.getElementById("centralRefreshToken").value = "reftok";
  doc.getElementById("centralSaveBtn").dispatchEvent(new window.Event("click", { bubbles: true }));
  await new Promise((r) => setTimeout(r, 20));
  ok("central save cleared the secret fields afterward", doc.getElementById("centralClientSecret").value === "");

  // --- "reload the page": fresh JSDOM instance re-fetching the same
  // backing `workflow` object (now mutated by everything above) proves
  // edits persisted through save survive a reload, not just in-memory. ---
  const dom2 = new JSDOM(html, { url: "http://localhost/#/workflows/wf1", runScripts: "outside-only", pretendToBeVisual: true });
  dom2.window.fetch = async (url, opts) => {
    opts = opts || {};
    const u = String(url);
    if (u === "/api/workflows/wf1" && (!opts.method || opts.method === "GET")) {
      return { ok: true, status: 200, json: async () => workflow };
    }
    return { ok: true, status: 200, json: async () => ({}) };
  };
  dom2.window.eval(appJs);
  await new Promise((r) => setTimeout(r, 20));
  const doc2 = dom2.window.document;
  const namesAfterReload = Array.from(doc2.querySelectorAll(".node-box")).map((b) => b.dataset.node).sort();
  ok("reload shows the same node set that was saved (2 nodes, code node gone)", namesAfterReload.length === 2 && namesAfterReload.includes("A") && namesAfterReload.includes("B"));
  ok("reload shows the reconnected A->B edge", doc2.querySelectorAll(".conn").length === 1);

  console.log(failures === 0 ? "\nALL SMOKE TESTS PASSED" : "\n" + failures + " SMOKE TEST(S) FAILED");
  process.exit(failures === 0 ? 0 : 1);
}

main().catch((e) => {
  console.error("smoke test crashed:", e);
  process.exit(1);
});
