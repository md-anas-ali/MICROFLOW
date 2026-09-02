// MicroFlow UI — plain JS, no build step, no external dependencies.
// Talks only to the existing API surface (internal/api/server.go):
//   GET    /api/workflows
//   POST   /api/workflows/import
//   POST   /api/workflows/{id}/save
//   GET    /api/workflows/{id}
//   GET    /api/workflows/{id}/export
//   POST   /api/workflows/{id}/execute?startNode=...  (async: 202 {executionId,status:"queued"})
//   GET    /api/executions/{id}                        (current snapshot; durable-store fallback)
//   POST   /api/executions/{id}/cancel
//   GET    /api/executions/{id}/events                 (SSE; see followExecution below)
//   GET    /api/workflows/{id}/credentials
//   POST   /api/workflows/{id}/credentials
//   GET    /api/credentials/google              (legacy manual-paste, kept for back-compat)
//   POST   /api/credentials/google               (legacy manual-paste, kept for back-compat)
//   DELETE /api/credentials/google                (legacy manual-paste, kept for back-compat)
//   GET    /api/google/connections               (n8n-style Connect: per-service status)
//   GET    /api/google/connect/{service}          (n8n-style Connect: starts OAuth, browser nav)
//   POST   /api/google/disconnect/{service}       (n8n-style Connect: disconnect one service)
// Execution monitoring/history: GET /api/executions returns the newest
// runs plus queue stats; GET /api/executions/events is the global live SSE
// stream; per-run GET .../{id}/events remains the detailed node stream.
//
// Credentials, three scopes (see internal/vault/central.go and
// internal/api/google_connect.go):
//  - Per-service connected account (/api/google/*): Gmail, YouTube, and
//    Sheets each connect independently to their own Google account via
//    a real "Connect Google" OAuth flow (no pasted tokens) -- this is
//    the primary path, rendered on the "Google Connections" page.
//  - Legacy central (/api/credentials/google): the older single
//    shared-account manual-paste flow. Kept working for installs that
//    already used it; falls back automatically for any service that
//    hasn't been individually (re)connected via the new flow.
//  - Per-node override (/api/workflows/{id}/credentials): the original
//    mechanism, kept for backward compatibility and for the rare case a
//    specific node needs a *different* Google account than the rest.
//    Scoped to a single (workflowID, nodeName) pair, exactly what
//    vault.Put/cmd/setcred already write.
// No response here ever returns clientSecret/refreshToken -- only
// nodeName/nodeType/updatedAt/email/configured -- so the UI never has
// secret bytes to accidentally render.

(function () {
  "use strict";

  const viewEl = document.getElementById("view");
  const connEl = document.getElementById("connStatus");
  const sidebar = document.getElementById("sidebar");
  const sidebarBackdrop = document.getElementById("sidebarBackdrop");
  const navToggle = document.getElementById("navToggle");
  let executionsMonitorES = null;
  let executionsMonitorTimer = null;
  let executionDetailES = null;

  // ---------------- small helpers ----------------

  function escapeHtml(s) {
    return String(s == null ? "" : s).replace(/[&<>"']/g, (c) => ({
      "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
    }[c]));
  }

  function fmtDate(iso) {
    if (!iso) return "\u2014";
    const d = new Date(iso);
    if (isNaN(d.getTime())) return "\u2014";
    return d.toLocaleString();
  }

  function fmtDurationMs(ns) {
    // Execution.Duration is encoded as a Go time.Duration -> JSON number
    // of *nanoseconds*.
    if (ns == null) return "\u2014";
    const ms = ns / 1e6;
    if (ms < 1000) return Math.round(ms) + "ms";
    return (ms / 1000).toFixed(2) + "s";
  }

  let toastHost = document.getElementById("toastHost");
  if (!toastHost) {
    toastHost = document.createElement("div");
    toastHost.id = "toastHost";
    document.body.appendChild(toastHost);
  }
  function toast(msg, type) {
    const el = document.createElement("div");
    el.className = "toast" + (type ? " toast-" + type : "");
    el.textContent = msg;
    toastHost.appendChild(el);
    setTimeout(() => el.remove(), 4500);
  }

  async function api(path, opts) {
    opts = opts || {};
    let res;
    try {
      res = await fetch(path, opts);
    } catch (e) {
      // A caller-triggered AbortController (see executeCurrentWorkflow's
      // timeout) is not a connectivity problem -- rethrow as-is so
      // callers can tell an intentional abort apart from a real network
      // failure, and don't flip the header status to "unreachable" for it.
      if (e.name === "AbortError") throw e;
      setConn(false);
      throw new Error("Network error \u2014 could not reach the MicroFlow API");
    }
    setConn(true);
    if (!res.ok) {
      let msg = "Request failed (" + res.status + ")";
      try {
        const body = await res.json();
        if (body && body.error) msg = body.error;
      } catch (_) { /* not JSON */ }
      throw new Error(msg);
    }
    return res;
  }

  async function apiJSON(path, opts) {
    const res = await api(path, opts);
    return res.status === 204 ? null : res.json();
  }

  function setConn(ok) {
    connEl.classList.remove("conn-unknown", "conn-ok", "conn-err");
    connEl.classList.add(ok ? "conn-ok" : "conn-err");
    connEl.textContent = ok ? "connected" : "unreachable";
  }

  // ---------------- sidebar (mobile) ----------------

  navToggle.addEventListener("click", () => {
    sidebar.classList.toggle("open");
    sidebarBackdrop.classList.toggle("open");
  });
  sidebarBackdrop.addEventListener("click", closeSidebar);
  function closeSidebar() {
    sidebar.classList.remove("open");
    sidebarBackdrop.classList.remove("open");
  }

  function setActiveNav(route) {
    document.querySelectorAll(".nav-link").forEach((a) => {
      a.classList.toggle("active", a.dataset.route === route);
    });
  }

  // ---------------- router ----------------

  function parseHash() {
    const h = location.hash.replace(/^#\/?/, "");
    const parts = h.split("/").filter(Boolean);
    return parts;
  }

  function stopExecutionMonitoring() {
    if (executionsMonitorES) { executionsMonitorES.close(); executionsMonitorES = null; }
    if (executionDetailES) { executionDetailES.close(); executionDetailES = null; }
    if (executionsMonitorTimer) { clearInterval(executionsMonitorTimer); executionsMonitorTimer = null; }
  }

  async function route() {
    closeSidebar();
    stopExecutionMonitoring();
    const parts = parseHash();
    try {
      if (parts.length === 0 || parts[0] === "dashboard") {
        setActiveNav("dashboard");
        await renderDashboard();
      } else if (parts[0] === "workflows" && parts.length === 1) {
        setActiveNav("workflows");
        await renderWorkflowsList();
      } else if (parts[0] === "workflows" && parts.length >= 2) {
        setActiveNav("workflows");
        const executionId = new URLSearchParams(location.hash.split("?")[1] || "").get("execution") || "";
        await renderEditor(decodeURIComponent(parts[1]), executionId);
      } else if (parts[0] === "executions") {
        setActiveNav("executions");
        await renderExecutions(parts[1] ? decodeURIComponent(parts[1]) : "");
      } else if (parts[0] === "import") {
        setActiveNav("import");
        renderImport();
      } else if (parts[0] === "credentials") {
        setActiveNav("credentials");
        await renderCentralCredentialsPage();
      } else {
        setActiveNav("");
        viewEl.innerHTML = emptyState("Not found", "That page doesn't exist.");
      }
    } catch (e) {
      viewEl.innerHTML = emptyState("Something went wrong", escapeHtml(e.message || String(e)));
    }
  }
  window.addEventListener("hashchange", route);

  function loadingRow(label) {
    return '<div class="loading-row"><span class="spinner"></span> ' + escapeHtml(label || "Loading\u2026") + "</div>";
  }

  function emptyState(title, body, icon) {
    return (
      '<div class="empty-state"><div class="big">' + (icon || "\u25A2") + "</div>" +
      "<h3>" + escapeHtml(title) + "</h3><p>" + (body || "") + "</p></div>"
    );
  }

  // ---------------- dashboard ----------------

  async function renderDashboard() {
    viewEl.innerHTML =
      '<div class="page-head"><div><h1>Dashboard</h1><div class="sub">MicroFlow at a glance</div></div></div>' +
      loadingRow("Loading workflows\u2026");

    let wfs;
    try {
      wfs = await apiJSON("/api/workflows");
    } catch (e) {
      viewEl.innerHTML =
        '<div class="page-head"><div><h1>Dashboard</h1></div></div>' +
        emptyState("Can't reach the API", escapeHtml(e.message));
      return;
    }
    wfs = wfs || [];

    const active = wfs.filter((w) => w.active).length;
    let totalNodes = 0;
    wfs.forEach((w) => { totalNodes += Object.keys(w.nodes || {}).length; });

    const recent = wfs
      .slice()
      .sort((a, b) => new Date(b.updatedAt || 0) - new Date(a.updatedAt || 0))
      .slice(0, 6);

    viewEl.innerHTML =
      '<div class="page-head"><div><h1>Dashboard</h1><div class="sub">MicroFlow at a glance</div></div>' +
      '<div><a class="btn btn-primary" href="#/import">+ Import workflow</a></div></div>' +
      '<div class="grid-stats">' +
      statCard(wfs.length, "Workflows") +
      statCard(active, "Active") +
      statCard(totalNodes, "Total nodes") +
      "</div>" +
      "<h3>Recently updated</h3>" +
      (recent.length
        ? renderWorkflowTable(recent, false)
        : emptyState("No workflows yet", 'Import an n8n export to get started, or <a href="#/import">go to Import</a>.'));
  }

  function statCard(num, label) {
    return (
      '<div class="card stat-card"><div class="stat-num">' + escapeHtml(num) +
      '</div><div class="stat-label">' + escapeHtml(label) + "</div></div>"
    );
  }

  // ---------------- executions monitor/history ----------------

  async function renderExecutions(selectedID) {
    viewEl.innerHTML =
      '<div class="page-head"><div><h1>Executions</h1><div class="sub">Live queue, running workflows, and the last 12 hours of history</div></div></div>' +
      '<div id="execMonitorStats" class="grid-stats"></div>' +
      '<div class="execution-layout"><div>' +
      '<div class="section-head"><h3>Live & recent executions</h3><span id="execLiveState" class="live-indicator">● live</span></div>' +
      '<div id="execHistoryTable">' + loadingRow("Loading executions…") + '</div>' +
      '</div><div id="execDetailHost" class="card exec-detail-host">' +
      emptyState("Select an execution", "Running executions update live here.", "↗") +
      '</div></div>';

    const workflowNames = {};
    try {
      const wfs = await apiJSON("/api/workflows");
      (wfs || []).forEach((wf) => { workflowNames[wf.id] = wf.name || wf.id; });
    } catch (_) {}

    const tableHost = document.getElementById("execHistoryTable");
    const statsHost = document.getElementById("execMonitorStats");
    const detailHost = document.getElementById("execDetailHost");
    let selected = selectedID || "";
    let detailFollowID = "";

    function renderStats(queue) {
      queue = queue || {};
      statsHost.innerHTML =
        statCard(queue.accepted || 0, "Accepted / active") +
        statCard(queue.running || 0, "Running") +
        statCard(queue.waiting || 0, "Waiting in queue") +
        statCard((queue.maxConcurrent || 0) + " / " + (queue.maxQueued || 0), "Workers / queue limit");
    }

    function renderRows(executions) {
      if (!executions || !executions.length) {
        tableHost.innerHTML = emptyState("No executions", "Run a workflow and its execution will appear here.");
        return;
      }
      const rows = executions.map((ex) => {
        const terminal = ["success", "error", "cancelled"].indexOf(ex.status) >= 0;
        const workflow = workflowNames[ex.workflowId] || ex.workflowId;
        const action = terminal
          ? '<button class="btn btn-sm" data-exec-open="' + escapeHtml(ex.id) + '">View</button>'
          : '<button class="btn btn-sm" data-exec-open="' + escapeHtml(ex.id) + '">Live</button>' +
            '<button class="btn btn-sm btn-danger" data-exec-cancel="' + escapeHtml(ex.id) + '">Cancel</button>';
        return '<tr class="clickable" data-exec-row="' + escapeHtml(ex.id) + '">' +
          '<td><div class="wf-name">' + escapeHtml(workflow) + '</div><div class="exec-id">' + escapeHtml(ex.id) + '</div></td>' +
          '<td><span class="status-pill status-' + escapeHtml(ex.status) + '">' + escapeHtml(ex.status) + '</span></td>' +
          '<td>' + escapeHtml(ex.mode || "") + '</td>' +
          '<td>' + escapeHtml(fmtDate(ex.startedAt)) + '</td>' +
          '<td>' + (ex.finishedAt ? escapeHtml(fmtDate(ex.finishedAt)) : '<span class="live-dot">● running</span>') + '</td>' +
          '<td class="wf-actions">' + action + '</td></tr>';
      }).join("");
      tableHost.innerHTML =
        '<div class="history-note">History is automatically deleted after 12 hours. Running/queued executions are never removed by cleanup.</div>' +
        '<table class="wf-table"><thead><tr><th>Workflow</th><th>Status</th><th>Mode</th><th>Started</th><th>Finished</th><th>Actions</th></tr></thead><tbody>' + rows + '</tbody></table>';

      tableHost.querySelectorAll("tr[data-exec-row]").forEach((row) => row.addEventListener("click", (ev) => {
        if (ev.target.closest("button")) return;
        openExecution(row.dataset.execRow);
      }));
      tableHost.querySelectorAll("button[data-exec-open]").forEach((btn) => btn.addEventListener("click", (ev) => {
        ev.stopPropagation(); openExecution(btn.dataset.execOpen);
      }));
      tableHost.querySelectorAll("button[data-exec-cancel]").forEach((btn) => btn.addEventListener("click", async (ev) => {
        ev.stopPropagation();
        btn.disabled = true;
        try {
          await apiJSON("/api/executions/" + encodeURIComponent(btn.dataset.execCancel) + "/cancel", { method: "POST" });
          toast("Cancellation requested", "success");
          await load();
        } catch (e) { toast("Cancel failed: " + e.message, "error"); btn.disabled = false; }
      }));
    }

    async function load() {
      try {
        const data = await apiJSON("/api/executions?limit=100");
        renderStats(data.queue || {});
        renderRows(data.executions || []);
        if (selected) {
          const found = (data.executions || []).find((x) => x.id === selected);
          if (found) renderExecutionDetail(found, true);
        }
      } catch (e) {
        tableHost.innerHTML = emptyState("Can't load executions", escapeHtml(e.message));
      }
    }

    function renderExecutionDetail(ex, live) {
      if (!ex) return;
      const runs = ex.nodeRuns || [];
      const workflowID = ex.workflowId || "";
      detailHost.innerHTML =
        '<div class="exec-detail-head"><div><h3>' + escapeHtml(workflowNames[workflowID] || workflowID) + '</h3><div class="exec-id">' + escapeHtml(ex.id) + '</div></div>' +
        '<span class="status-pill status-' + escapeHtml(ex.status) + '">' + escapeHtml(ex.status) + '</span></div>' +
        '<div class="exec-detail-meta"><span>Mode: ' + escapeHtml(ex.mode || "") + '</span><span>Started: ' + escapeHtml(fmtDate(ex.startedAt)) + '</span>' +
        (ex.finishedAt ? '<span>Finished: ' + escapeHtml(fmtDate(ex.finishedAt)) + '</span>' : '<span class="live-dot">● LIVE</span>') + '</div>' +
        (ex.error ? '<div class="err-text exec-detail-error">' + escapeHtml(ex.error) + '</div>' : '') +
        '<div class="exec-detail-actions">' +
        (!(["success", "error", "cancelled"].indexOf(ex.status) >= 0) ? '<button id="detailCancel" class="btn btn-danger btn-sm">Cancel</button>' : '') +
        (workflowID && workflowNames[workflowID] ? '<a class="btn btn-sm" href="#/workflows/' + encodeURIComponent(workflowID) + '?execution=' + encodeURIComponent(ex.id) + '">Open live canvas</a>' : '') +
        '</div>' +
        '<div class="exec-node-list">' + (runs.length ? runs.map(renderNodeRun).join("") : '<div class="empty-state">Waiting for the first node event…</div>') + '</div>';
      detailHost.querySelectorAll(".node-run-head").forEach((h) => h.addEventListener("click", () => h.parentElement.classList.toggle("open")));
      const cancel = document.getElementById("detailCancel");
      if (cancel) cancel.addEventListener("click", async () => {
        cancel.disabled = true;
        try { await apiJSON("/api/executions/" + encodeURIComponent(ex.id) + "/cancel", { method: "POST" }); toast("Cancellation requested", "success"); }
        catch (e) { toast("Cancel failed: " + e.message, "error"); cancel.disabled = false; }
      });
      if (live && ["success", "error", "cancelled"].indexOf(ex.status) < 0) followExecutionDetail(ex.id);
      else if (executionDetailES && detailFollowID === ex.id) { executionDetailES.close(); executionDetailES = null; detailFollowID = ""; }
    }

    function followExecutionDetail(execID) {
      if (detailFollowID === execID && executionDetailES) return;
      if (executionDetailES) executionDetailES.close();
      detailFollowID = execID;
      try { executionDetailES = new EventSource("/api/executions/" + encodeURIComponent(execID) + "/events"); }
      catch (_) { return; }
      executionDetailES.onmessage = function (msg) {
        try {
          const ev = JSON.parse(msg.data);
          if (selected !== execID) return;
          load();
          apiJSON("/api/executions/" + encodeURIComponent(execID)).then((latest) => renderExecutionDetail(latest, true)).catch(() => {});
        } catch (_) {}
      };
      executionDetailES.onerror = function () {
        if (executionDetailES) executionDetailES.close();
        executionDetailES = null;
        detailFollowID = "";
      };
    }

    function openExecution(id) {
      selected = id;
      apiJSON("/api/executions/" + encodeURIComponent(id)).then((ex) => renderExecutionDetail(ex, true)).catch((e) => {
        detailHost.innerHTML = emptyState("Execution unavailable", escapeHtml(e.message));
      });
    }

    await load();
    executionsMonitorTimer = setInterval(load, 2000);
    try {
      executionsMonitorES = new EventSource("/api/executions/events");
      executionsMonitorES.onmessage = () => load();
      executionsMonitorES.onerror = () => { /* 2s refresh remains as a resilient fallback */ };
    } catch (_) {}
    if (selected) openExecution(selected);
  }

  // ---------------- workflows list ----------------

  async function renderWorkflowsList() {
    viewEl.innerHTML =
      '<div class="page-head"><div><h1>Workflows</h1><div class="sub">All saved workflows</div></div>' +
      '<div><a class="btn btn-primary" href="#/import">+ Import workflow</a></div></div>' +
      loadingRow();

    let wfs;
    try {
      wfs = await apiJSON("/api/workflows");
    } catch (e) {
      viewEl.innerHTML =
        '<div class="page-head"><div><h1>Workflows</h1></div></div>' +
        emptyState("Can't reach the API", escapeHtml(e.message));
      return;
    }
    wfs = wfs || [];

    viewEl.innerHTML =
      '<div class="page-head"><div><h1>Workflows</h1><div class="sub">' + wfs.length + " total</div></div>" +
      '<div><a class="btn btn-primary" href="#/import">+ Import workflow</a></div></div>' +
      (wfs.length
        ? renderWorkflowTable(wfs, true)
        : emptyState("No workflows yet", 'Import an n8n export to get started, or <a href="#/import">go to Import</a>.'));

    wireWorkflowTable();
  }

  function renderWorkflowTable(wfs, withActions) {
    const rows = wfs.map((w) => {
      const nodeCount = Object.keys(w.nodes || {}).length;
      return (
        '<tr class="clickable" data-id="' + escapeHtml(w.id) + '">' +
        '<td class="wf-name">' + escapeHtml(w.name || "(untitled)") + "</td>" +
        "<td>" + (w.active
          ? '<span class="badge badge-active">active</span>'
          : '<span class="badge badge-inactive">inactive</span>') + "</td>" +
        "<td>" + nodeCount + "</td>" +
        "<td>" + escapeHtml(fmtDate(w.updatedAt)) + "</td>" +
        (withActions
          ? '<td class="wf-actions">' +
            '<button class="btn btn-sm" data-act="execute" data-id="' + escapeHtml(w.id) + '">Execute</button>' +
            '<button class="btn btn-sm" data-act="export" data-id="' + escapeHtml(w.id) + '">Export</button>' +
            "</td>"
          : "") +
        "</tr>"
      );
    }).join("");

    return (
      '<table class="wf-table"><thead><tr><th>Name</th><th>Status</th><th>Nodes</th><th>Updated</th>' +
      (withActions ? "<th>Actions</th>" : "") + "</tr></thead><tbody>" + rows + "</tbody></table>"
    );
  }

  function wireWorkflowTable() {
    viewEl.querySelectorAll("tr.clickable").forEach((tr) => {
      tr.addEventListener("click", (ev) => {
        if (ev.target.closest("button")) return;
        location.hash = "#/workflows/" + encodeURIComponent(tr.dataset.id);
      });
    });
    viewEl.querySelectorAll('button[data-act="export"]').forEach((btn) => {
      btn.addEventListener("click", (ev) => {
        ev.stopPropagation();
        exportWorkflow(btn.dataset.id);
      });
    });
    viewEl.querySelectorAll('button[data-act="execute"]').forEach((btn) => {
      btn.addEventListener("click", async (ev) => {
        ev.stopPropagation();
        btn.disabled = true;
        btn.textContent = "Running\u2026";
        try {
          const ex = await apiJSON("/api/workflows/" + encodeURIComponent(btn.dataset.id) + "/execute", { method: "POST" });
          toast("Execution queued", "success");
          location.hash = "#/executions/" + encodeURIComponent(ex.executionId);
        } catch (e) {
          toast(e.message, "error");
        } finally {
          btn.disabled = false;
          btn.textContent = "Execute";
        }
      });
    });
  }

  function exportWorkflow(id) {
    // GET .../export sets Content-Disposition: attachment, so a plain
    // navigation (new tab) lets the browser handle the download itself
    // — no need to fetch+blob it manually.
    window.open("/api/workflows/" + encodeURIComponent(id) + "/export", "_blank");
  }

  // ---------------- import ----------------

  function renderImport() {
    viewEl.innerHTML =
      '<div class="page-head"><div><h1>Import workflow</h1>' +
      '<div class="sub">Paste or drop an n8n workflow export (JSON)</div></div></div>' +
      '<div id="importDrop" class="import-drop">' +
      "Drop a <code>.json</code> file here, or click to choose one" +
      '<input id="importFile" type="file" accept="application/json,.json" style="display:none">' +
      "</div>" +
      '<div style="margin:14px 0;color:var(--text-dim);font-size:12px;">\u2014 or paste JSON below \u2014</div>' +
      '<textarea id="importText" class="json-paste" placeholder="{ &quot;nodes&quot;: [...], &quot;connections&quot;: {...} }"></textarea>' +
      '<div style="margin-top:12px;"><button id="importBtn" class="btn btn-primary">Import</button></div>' +
      '<div id="importResult"></div>';

    const drop = document.getElementById("importDrop");
    const fileInput = document.getElementById("importFile");
    const textArea = document.getElementById("importText");
    const resultEl = document.getElementById("importResult");

    drop.addEventListener("click", () => fileInput.click());
    fileInput.addEventListener("change", () => {
      const f = fileInput.files[0];
      if (f) readFileInto(f, textArea);
    });
    ["dragover", "dragenter"].forEach((evt) =>
      drop.addEventListener(evt, (e) => { e.preventDefault(); drop.classList.add("dragover"); })
    );
    ["dragleave", "drop"].forEach((evt) =>
      drop.addEventListener(evt, (e) => { e.preventDefault(); drop.classList.remove("dragover"); })
    );
    drop.addEventListener("drop", (e) => {
      const f = e.dataTransfer.files && e.dataTransfer.files[0];
      if (f) readFileInto(f, textArea);
    });

    document.getElementById("importBtn").addEventListener("click", async () => {
      const raw = textArea.value.trim();
      if (!raw) { toast("Paste or choose a JSON file first", "error"); return; }
      const btn = document.getElementById("importBtn");
      btn.disabled = true;
      btn.textContent = "Importing\u2026";
      resultEl.innerHTML = "";
      try {
        const result = await apiJSON("/api/workflows/import", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: raw,
        });
        resultEl.innerHTML = renderImportResult(result);
        toast("Imported \u201c" + (result.workflow && result.workflow.name || "workflow") + "\u201d", "success");
      } catch (e) {
        toast(e.message, "error");
        resultEl.innerHTML = '<div class="unsupported-box">' + escapeHtml(e.message) + "</div>";
      } finally {
        btn.disabled = false;
        btn.textContent = "Import";
      }
    });
  }

  function readFileInto(file, textArea) {
    const reader = new FileReader();
    reader.onload = () => { textArea.value = reader.result; };
    reader.onerror = () => toast("Could not read file", "error");
    reader.readAsText(file);
  }

  function renderImportResult(result) {
    const wf = result.workflow || {};
    const checklist = result.checklist || [];
    const unsupported = result.unsupported || [];
    let html = '<div class="checklist"><h3>Imported: ' + escapeHtml(wf.name || wf.id) + "</h3>";
    if (checklist.length) {
      html += "<ul>" + checklist.map((c) => "<li>" + escapeHtml(c) + "</li>").join("") + "</ul>";
    }
    if (unsupported.length) {
      html += '<div class="unsupported-box"><strong>Not fully supported (' + unsupported.length + "):</strong><ul>" +
        unsupported.map((u) => "<li>" + escapeHtml(u) + "</li>").join("") + "</ul></div>";
    }
    html += '<div style="margin-top:12px;"><a class="btn btn-primary" href="#/workflows/' +
      encodeURIComponent(wf.id) + '">Open workflow \u2192</a></div></div>';
    return html;
  }

  // ---------------- Google Connections page ----------------
  //
  // n8n-style "Connect with Google": one Connect/Reconnect/Disconnect
  // button per service (Gmail, YouTube, Sheets), each independently
  // wired to its own Google account via the Authorization Code flow
  // (GET /api/google/connections, GET /api/google/connect/{service},
  // POST /api/google/disconnect/{service} -- see
  // internal/api/google_connect.go). No client ID/secret/refresh token
  // ever touches this page or this browser tab.
  //
  // The legacy manual-paste central credential (GET/POST/DELETE
  // /api/credentials/google) is kept below as a collapsed "Advanced"
  // section for backward compatibility -- most people never need it.

  const GOOGLE_SERVICE_LABELS = { gmail: "Gmail", youtube: "YouTube", sheets: "Google Sheets" };
  const GOOGLE_SERVICE_ORDER = ["gmail", "youtube", "sheets"];

  async function renderCentralCredentialsPage() {
    viewEl.innerHTML =
      '<div class="page-head"><div><h1>Google Connections</h1>' +
      '<div class="sub">Connect each service to its own Google account \u2014 no copying refresh tokens, ' +
      "no manual setup. Once connected, every workflow's Gmail/YouTube/Sheets nodes use it automatically, " +
      "including scheduled runs with no browser open.</div></div></div>" +
      '<div id="googleConnCards" class="google-conn-cards">' + loadingRow("Checking connections\u2026") + "</div>" +
      '<div id="googleConnNotConfigured" class="field-error" style="display:none;"></div>' +
      '<details class="cred-advanced"><summary>Advanced: manual credential (override)</summary>' +
      '<div class="card cred-page-card">' +
      '<div class="cred-section-note">Only needed if you already have a Google OAuth client ID/secret/refresh ' +
      "token from elsewhere and want to paste it in directly, instead of using Connect above. Most people never " +
      "need this.</div>" +
      '<div id="centralCredStatus" class="cred-status">' + loadingRow("Checking saved account\u2026") + "</div>" +
      '<div class="field"><label>Client ID</label><input type="text" id="centralClientId" autocomplete="off"></div>' +
      '<div class="field"><label>Client Secret</label><input type="password" id="centralClientSecret" autocomplete="off"></div>' +
      '<div class="field"><label>Refresh Token</label><input type="password" id="centralRefreshToken" autocomplete="off"></div>' +
      '<div class="cred-page-actions">' +
      '<button id="centralSaveBtn" class="btn btn-primary">Save / Update</button>' +
      '<button id="centralClearBtn" class="btn btn-danger">Clear</button>' +
      "</div>" +
      '<div id="centralCredError" class="field-error"></div>' +
      "</div></details>";

    handleGoogleCallbackQueryParams();
    wireCentralCredentialsPage();
    refreshCentralCredentialStatus();
    refreshGoogleConnections();
  }

  // Google's OAuth redirect lands back here as
  // /#/credentials?googleConnected=gmail or ?googleError=<message> --
  // show a toast once, then strip the query string so a page refresh
  // doesn't replay it.
  function handleGoogleCallbackQueryParams() {
    const hash = location.hash || "";
    const qIdx = hash.indexOf("?");
    if (qIdx === -1) return;
    const params = new URLSearchParams(hash.slice(qIdx + 1));
    const connected = params.get("googleConnected");
    const err = params.get("googleError");
    if (connected) {
      toast((GOOGLE_SERVICE_LABELS[connected] || connected) + " connected", "success");
    } else if (err) {
      toast(err, "error");
    }
    if (connected || err !== null) {
      history.replaceState(null, "", hash.slice(0, qIdx));
    }
  }

  function googleServiceCardHTML(view) {
    const label = GOOGLE_SERVICE_LABELS[view.service] || view.service;
    let statusHTML;
    if (view.connected && view.needsReconnect) {
      statusHTML = '<span class="badge badge-inactive">connection expired</span>' +
        '<div class="google-conn-msg">Google connection expired. Please reconnect.</div>';
    } else if (view.connected) {
      statusHTML = '<span class="badge badge-active">connected</span>' +
        (view.email ? '<div class="google-conn-email">Connected as: ' + escapeHtml(view.email) + "</div>" : "") +
        (view.updatedAt ? '<div class="cred-status-time">since ' + escapeHtml(fmtDate(view.updatedAt)) + "</div>" : "");
    } else {
      statusHTML = '<span class="badge badge-inactive">not connected</span>';
    }

    let actionsHTML;
    if (view.connected) {
      actionsHTML =
        '<a class="btn btn-secondary btn-sm" href="/api/google/connect/' + encodeURIComponent(view.service) + '">' +
        (view.needsReconnect ? "Reconnect" : "Reconnect") + "</a> " +
        '<button class="btn btn-danger btn-sm google-disconnect-btn" data-service="' + escapeHtml(view.service) + '">Disconnect</button>';
    } else {
      actionsHTML = '<a class="btn btn-primary btn-sm" href="/api/google/connect/' + encodeURIComponent(view.service) + '">Connect Google</a>';
    }

    return (
      '<div class="google-conn-card">' +
      "<h3>" + escapeHtml(label) + "</h3>" +
      '<div class="google-conn-status">' + statusHTML + "</div>" +
      '<div class="google-conn-actions">' + actionsHTML + "</div>" +
      "</div>"
    );
  }

  async function refreshGoogleConnections() {
    const cardsEl = document.getElementById("googleConnCards");
    const notConfiguredEl = document.getElementById("googleConnNotConfigured");
    if (!cardsEl) return; // navigated away
    try {
      const services = await apiJSON("/api/google/connections");
      const byService = {};
      (services || []).forEach((v) => { byService[v.service] = v; });
      cardsEl.innerHTML = GOOGLE_SERVICE_ORDER
        .map((svc) => byService[svc] || { service: svc, connected: false })
        .map(googleServiceCardHTML)
        .join("");
      wireGoogleDisconnectButtons();
      notConfiguredEl.style.display = "none";
    } catch (e) {
      // Server has no GOOGLE_OAUTH_CLIENT_ID/SECRET/REDIRECT_URL set --
      // Connect buttons aren't available; the Advanced section below
      // still works.
      cardsEl.innerHTML = "";
      notConfiguredEl.style.display = "";
      notConfiguredEl.textContent =
        "\"Connect with Google\" isn't set up on this server yet (missing GOOGLE_OAUTH_CLIENT_ID/SECRET/REDIRECT_URL). " +
        "Use the Advanced section below, or ask an operator to configure it.";
    }
  }

  function wireGoogleDisconnectButtons() {
    document.querySelectorAll(".google-disconnect-btn").forEach((btn) => {
      btn.addEventListener("click", async () => {
        const service = btn.dataset.service;
        const label = GOOGLE_SERVICE_LABELS[service] || service;
        if (!confirm("Disconnect " + label + "? Workflows using it will stop working until it's reconnected.")) return;
        btn.disabled = true;
        try {
          await apiJSON("/api/google/disconnect/" + encodeURIComponent(service), { method: "POST" });
          toast(label + " disconnected", "success");
          refreshGoogleConnections();
        } catch (e) {
          toast("Disconnect failed: " + e.message, "error");
          btn.disabled = false;
        }
      });
    });
  }

  // ---------------- legacy manual-paste central credential (advanced/override) ----------------

  // Re-fetches central status and updates just the status line -- never
  // touches the input fields, so it's safe to call again right after a
  // save/clear without disturbing anything else the person is typing.
  async function refreshCentralCredentialStatus() {
    const statusEl = document.getElementById("centralCredStatus");
    if (!statusEl) return; // navigated away
    try {
      const status = await apiJSON("/api/credentials/google");
      statusEl.innerHTML = status && status.configured
        ? '<span class="badge badge-active">configured</span> <span class="cred-status-time">last updated ' +
          escapeHtml(fmtDate(status.updatedAt)) + "</span>"
        : '<span class="badge badge-inactive">not configured</span>';
    } catch (e) {
      statusEl.innerHTML = '<span class="cred-status-err">Could not check saved status: ' + escapeHtml(e.message) + "</span>";
    }
  }

  function wireCentralCredentialsPage() {
    const errEl = document.getElementById("centralCredError");

    document.getElementById("centralSaveBtn").addEventListener("click", async () => {
      const btn = document.getElementById("centralSaveBtn");
      errEl.textContent = "";

      const clientId = document.getElementById("centralClientId").value.trim();
      const clientSecret = document.getElementById("centralClientSecret").value.trim();
      const refreshToken = document.getElementById("centralRefreshToken").value.trim();
      if (!clientId || !clientSecret || !refreshToken) {
        errEl.textContent = "Client ID, Client Secret, and Refresh Token are all required.";
        return;
      }

      btn.disabled = true;
      btn.textContent = "Saving\u2026";
      try {
        await apiJSON("/api/credentials/google", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ clientId: clientId, clientSecret: clientSecret, refreshToken: refreshToken }),
        });
        toast("Google account saved \u2014 every Google node will use it automatically", "success");
        // Clear secret fields after a successful save -- nothing secret
        // should linger in the DOM/inputs longer than it has to.
        document.getElementById("centralClientSecret").value = "";
        document.getElementById("centralRefreshToken").value = "";
        refreshCentralCredentialStatus();
      } catch (e) {
        errEl.textContent = e.message;
        toast("Save failed: " + e.message, "error");
      } finally {
        btn.disabled = false;
        btn.textContent = "Save / Update";
      }
    });

    document.getElementById("centralClearBtn").addEventListener("click", async () => {
      const btn = document.getElementById("centralClearBtn");
      errEl.textContent = "";
      btn.disabled = true;
      try {
        await apiJSON("/api/credentials/google", { method: "DELETE" });
        document.getElementById("centralClientId").value = "";
        document.getElementById("centralClientSecret").value = "";
        document.getElementById("centralRefreshToken").value = "";
        toast("Google account cleared", "success");
        refreshCentralCredentialStatus();
      } catch (e) {
        errEl.textContent = e.message;
        toast("Clear failed: " + e.message, "error");
      } finally {
        btn.disabled = false;
      }
    });
  }

  // ---------------- google credentials (per node, editor side panel) ----------------
  //
  // Optional override: only needed if one specific node must use a
  // *different* Google account than the central one above. Most
  // workflows never need this section at all.

  // Exactly the node types internal/nodes/google.go calls
  // Creds.Resolve(workflowID, node.Name) for -- the same set
  // cmd/setcred and internal/api/server.go's isGoogleCredentialNodeType
  // accept. Keep these three lists in sync if a Google node type is
  // ever added.
  const GOOGLE_CREDENTIAL_NODE_TYPES = ["googleSheets", "youTube", "gmail"];
  function isGoogleCredentialNode(type) {
    return GOOGLE_CREDENTIAL_NODE_TYPES.indexOf(type) !== -1;
  }

  function renderCredentialSection() {
    return (
      '<div class="cred-section">' +
      "<h3>Google Credentials (override)</h3>" +
      '<div class="cred-section-note">Usually not needed \u2014 every Google node uses the ' +
      '<a href="#/credentials">central Google account</a> automatically. Only fill this in if ' +
      "this specific node should use a different account.</div>" +
      '<div id="credStatus" class="cred-status">' + loadingRow("Checking saved credential\u2026") + "</div>" +
      '<div class="field"><label>Client ID</label><input type="text" id="credClientId" autocomplete="off"></div>' +
      '<div class="field"><label>Client Secret</label><input type="password" id="credClientSecret" autocomplete="off"></div>' +
      '<div class="field"><label>Refresh Token</label><input type="password" id="credRefreshToken" autocomplete="off"></div>' +
      '<button id="saveCredBtn" class="btn btn-primary btn-sm">Save Override</button>' +
      '<div id="credError" class="field-error"></div>' +
      "</div>"
    );
  }

  // Re-fetches this workflow's saved-credential list and updates just
  // the status line -- never touches the input fields, so it's safe to
  // call again right after a save without disturbing anything else.
  async function refreshCredentialStatus(workflowId, nodeName) {
    const statusEl = document.getElementById("credStatus");
    if (!statusEl) return; // side panel moved on to a different node
    try {
      const creds = await apiJSON("/api/workflows/" + encodeURIComponent(workflowId) + "/credentials");
      const existing = (creds || []).find((c) => c.nodeName === nodeName);
      statusEl.innerHTML = existing
        ? '<span class="badge badge-active">override saved</span> <span class="cred-status-time">last updated ' +
          escapeHtml(fmtDate(existing.updatedAt)) + "</span>"
        : '<span class="badge badge-inactive">no override \u2014 using central account</span>';
    } catch (e) {
      statusEl.innerHTML = '<span class="cred-status-err">Could not check saved status: ' + escapeHtml(e.message) + "</span>";
    }
  }

  function wireCredentialSection(nodeName) {
    const workflowId = editorState.workflow.id;
    refreshCredentialStatus(workflowId, nodeName);

    const errEl = document.getElementById("credError");
    document.getElementById("saveCredBtn").addEventListener("click", async () => {
      const btn = document.getElementById("saveCredBtn");
      errEl.textContent = "";

      const clientId = document.getElementById("credClientId").value.trim();
      const clientSecret = document.getElementById("credClientSecret").value.trim();
      const refreshToken = document.getElementById("credRefreshToken").value.trim();
      if (!clientId || !clientSecret || !refreshToken) {
        errEl.textContent = "Client ID, Client Secret, and Refresh Token are all required.";
        return;
      }

      btn.disabled = true;
      btn.textContent = "Saving\u2026";
      try {
        await apiJSON("/api/workflows/" + encodeURIComponent(workflowId) + "/credentials", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            nodeName: nodeName,
            clientId: clientId,
            clientSecret: clientSecret,
            refreshToken: refreshToken,
          }),
        });
        toast("Override saved for this node", "success");
        // Clear the secret fields after a successful save -- nothing
        // secret should linger in the DOM/inputs longer than it has to.
        document.getElementById("credClientSecret").value = "";
        document.getElementById("credRefreshToken").value = "";
        refreshCredentialStatus(workflowId, nodeName);
      } catch (e) {
        errEl.textContent = e.message;
        toast("Save failed: " + e.message, "error");
      } finally {
        btn.disabled = false;
        btn.textContent = "Save Override";
      }
    });
  }

  // ---------------- editor ----------------

  let editorState = null; // { workflow, selectedNodeName, lastExecution, _layout }

  // Every model.NodeType the Go backend knows how to execute (see
  // internal/model/workflow.go's NodeType consts) -- what the "+ Add
  // node" control offers. New nodes leave originalType empty; the n8n
  // export path (internal/parser/export.go) already falls back to
  // reverseTypeMap[node.Type] for that case, so exporting a
  // freshly-added node still produces a valid n8n type string.
  const NODE_TYPE_OPTIONS = [
    ["manualTrigger", "Manual Trigger"],
    ["scheduleTrigger", "Schedule Trigger"],
    ["webhookTrigger", "Webhook Trigger"],
    ["errorTrigger", "Error Trigger"],
    ["code", "Code"],
    ["if", "IF"],
    ["wait", "Wait"],
    ["noOp", "No Op"],
    ["splitOut", "Split Out"],
    ["splitInBatches", "Split In Batches"],
    ["httpRequest", "HTTP Request"],
    ["executeCommand", "Execute Command"],
    ["readWriteFile", "Read/Write File"],
    ["googleSheets", "Google Sheets"],
    ["youTube", "YouTube"],
    ["gmail", "Gmail"],
    ["stickyNote", "Sticky Note"],
  ];

  async function renderEditor(id, executionId) {
    viewEl.innerHTML = loadingRow("Loading workflow\u2026");
    let wf;
    try {
      wf = await apiJSON("/api/workflows/" + encodeURIComponent(id));
    } catch (e) {
      viewEl.innerHTML = emptyState("Couldn't load that workflow", escapeHtml(e.message));
      return;
    }
    wf.nodes = wf.nodes || {};
    wf.connections = wf.connections || {};
    editorState = { workflow: wf, selectedNodeName: null, lastExecution: null, _layout: null, executionId: executionId || "" };
    paintEditor();
  }

  function paintEditor() {
    const wf = editorState.workflow;
    const nodes = wf.nodes || {};
    const nodeNames = Object.keys(nodes);

    viewEl.innerHTML =
      '<div class="editor-toolbar">' +
      '<input class="wf-title-input" id="wfName" type="text" value="' + escapeHtml(wf.name || "") + '">' +
      '<label class="row-checkbox" style="margin-left:4px;"><input type="checkbox" id="wfActive" ' +
      (wf.active ? "checked" : "") + "> Active</label>" +
      '<div class="spacer"></div>' +
      '<select id="addNodeType" class="btn btn-sm" title="Node type to add">' +
      NODE_TYPE_OPTIONS.map((o) => '<option value="' + o[0] + '">' + escapeHtml(o[1]) + "</option>").join("") +
      "</select>" +
      '<button id="btnAddNode" class="btn btn-sm">+ Add node</button>' +
      '<select id="startNodeSelect" class="btn btn-sm" style="max-width:200px;">' +
      '<option value="">Auto (first trigger)</option>' +
      nodeNames.map((n) => '<option value="' + escapeHtml(n) + '">' + escapeHtml(n) + "</option>").join("") +
      "</select>" +
      '<button id="btnExecute" class="btn">\u25B6 Execute</button>' +
      '<button id="btnExport" class="btn">Export</button>' +
      '<button id="btnSave" class="btn btn-primary">Save</button>' +
      "</div>" +
      '<div class="editor-hint">Drag a node to move it \u00b7 drag from the right dot to the left dot on another node to connect \u00b7 click a connection line to delete it \u00b7 Delete/Backspace removes the selected node.</div>' +
      '<div class="editor-body">' +
      '<div class="canvas-wrap" id="canvasWrap"></div>' +
      '<div class="side-panel" id="sidePanel">' + renderSidePanelEmpty() + "</div>" +
      "</div>" +
      '<div id="execPanelHost"></div>';

    drawCanvas();
    wireEditorToolbar();
    if (editorState.lastExecution) renderExecPanel(editorState.lastExecution);
    if (editorState.executionId) {
      const execId = editorState.executionId;
      apiJSON("/api/executions/" + encodeURIComponent(execId))
        .then((ex) => {
          editorState.lastExecution = ex;
          drawCanvas();
          renderExecPanel(ex);
          if (["success", "error", "cancelled"].indexOf(ex.status) < 0) {
            followExecution(execId, document.getElementById("btnExecute"));
          }
        })
        .catch((e) => toast("Could not load execution: " + e.message, "error"));
    }
  }

  function renderSidePanelEmpty() {
    return '<div class="side-panel-empty">Select a node to view or edit its settings.</div>';
  }

  // ---- id/name helpers for newly-added nodes ----

  function generateNodeId() {
    return "n_" + Date.now().toString(36) + "_" + Math.random().toString(36).slice(2, 8);
  }

  function uniqueNodeName(base) {
    const nodes = editorState.workflow.nodes;
    if (!nodes[base]) return base;
    let i = 2;
    while (nodes[base + " " + i]) i++;
    return base + " " + i;
  }

  // ---- add / delete node ----

  function addNode(type) {
    const wf = editorState.workflow;
    const label = (NODE_TYPE_OPTIONS.find((o) => o[0] === type) || [type, type])[1];
    const name = uniqueNodeName(label);

    // Drop the new node near the bottom-left of the current layout so
    // it's visible without having to scroll, rather than stacking every
    // new node on top of [0,0].
    const names = Object.keys(wf.nodes);
    let x = 40, y = 40;
    if (names.length) {
      let maxY = -Infinity, minX = Infinity;
      names.forEach((n) => {
        const p = wf.nodes[n].position || [0, 0];
        maxY = Math.max(maxY, p[1]);
        minX = Math.min(minX, p[0]);
      });
      x = minX; y = maxY + 110;
    }

    wf.nodes[name] = {
      id: generateNodeId(),
      name: name,
      type: type,
      originalType: "",
      typeVersion: 1,
      parameters: {},
      credentials: {},
      position: [x, y],
      disabled: false,
      retryOnFail: false,
      maxTries: 0,
      waitBetweenTriesMs: 0,
      continueOnFail: false,
    };
    editorState.selectedNodeName = name;
    drawCanvas();
    refreshStartNodeOptions();
    toast("Added \u201c" + name + "\u201d \u2014 remember to Save", "success");
  }

  // addNode/deleteNode only re-run drawCanvas() (not the whole toolbar,
  // which would blow away whatever the person is mid-typing in the
  // workflow-name field) -- so the "Execute from" dropdown needs its
  // own explicit refresh whenever the node set changes.
  function refreshStartNodeOptions() {
    const sel = document.getElementById("startNodeSelect");
    if (!sel) return;
    const prev = sel.value;
    const names = Object.keys(editorState.workflow.nodes);
    sel.innerHTML =
      '<option value="">Auto (first trigger)</option>' +
      names.map((n) => '<option value="' + escapeHtml(n) + '">' + escapeHtml(n) + "</option>").join("");
    if (names.includes(prev)) sel.value = prev;
  }

  function deleteNode(name) {
    const wf = editorState.workflow;
    if (!wf.nodes[name]) return;
    delete wf.nodes[name];
    delete wf.connections[name]; // this node's outgoing connections
    Object.keys(wf.connections).forEach((src) => {
      wf.connections[src] = (wf.connections[src] || []).filter((c) => c.targetName !== name);
    });
    if (editorState.selectedNodeName === name) editorState.selectedNodeName = null;
    document.getElementById("sidePanel").innerHTML = renderSidePanelEmpty();
    drawCanvas();
    refreshStartNodeOptions();
    toast("Deleted \u201c" + name + "\u201d \u2014 remember to Save", "success");
  }

  function deleteConnection(srcName, conn) {
    const wf = editorState.workflow;
    const list = wf.connections[srcName] || [];
    wf.connections[srcName] = list.filter(
      (c) => !(c.targetName === conn.targetName && c.sourceIndex === conn.sourceIndex && c.targetIndex === conn.targetIndex)
    );
    drawCanvas();
    toast("Connection removed \u2014 remember to Save", "success");
  }

  // ---- selection ----

  function selectNode(name) {
    editorState.selectedNodeName = name;
    const wrap = document.getElementById("canvasWrap");
    if (wrap) {
      wrap.querySelectorAll(".node-box").forEach((b) => b.classList.toggle("selected", b.dataset.node === name));
    }
    document.getElementById("sidePanel").innerHTML = renderSidePanel(name);
    wireSidePanel(name);
  }

  function deselectAll() {
    editorState.selectedNodeName = null;
    const wrap = document.getElementById("canvasWrap");
    if (wrap) wrap.querySelectorAll(".node-box").forEach((b) => b.classList.remove("selected"));
    document.getElementById("sidePanel").innerHTML = renderSidePanelEmpty();
  }

  // ---- canvas geometry ----

  function connLineGeometry(srcPos, tgtPos, layout) {
    const x1 = srcPos.x + layout.BOX_W, y1 = srcPos.y + layout.BOX_H / 2;
    const x2 = tgtPos.x, y2 = tgtPos.y + layout.BOX_H / 2;
    const midX = (x1 + x2) / 2;
    return {
      d: "M" + x1 + "," + y1 + " C" + midX + "," + y1 + " " + midX + "," + y2 + " " + x2 + "," + y2,
      arrow: x2 + "," + y2 + " " + (x2 - 7) + "," + (y2 - 4) + " " + (x2 - 7) + "," + (y2 + 4),
    };
  }

  // Re-positions only the connection lines touching one node, using the
  // layout offsets recorded by the last full drawCanvas() -- called on
  // every drag mousemove so dragging stays smooth without re-rendering
  // all ~100+ node boxes each frame. A full drawCanvas() still runs once
  // the drag ends, to correctly resize the canvas if the node moved
  // outside the previous bounding box.
  function updateConnectionsForNode(name) {
    const wrap = document.getElementById("canvasWrap");
    const layout = editorState._layout;
    if (!wrap || !layout) return;
    const nodes = editorState.workflow.nodes;
    wrap.querySelectorAll(".conn").forEach((g) => {
      if (g.dataset.src !== name && g.dataset.tgt !== name) return;
      const srcNode = nodes[g.dataset.src], tgtNode = nodes[g.dataset.tgt];
      if (!srcNode || !tgtNode) return;
      const srcPos = { x: (srcNode.position || [0, 0])[0] + layout.offX, y: (srcNode.position || [0, 0])[1] + layout.offY };
      const tgtPos = { x: (tgtNode.position || [0, 0])[0] + layout.offX, y: (tgtNode.position || [0, 0])[1] + layout.offY };
      const line = connLineGeometry(srcPos, tgtPos, layout);
      g.querySelectorAll("path").forEach((p) => p.setAttribute("d", line.d));
      const poly = g.querySelector("polygon");
      if (poly) poly.setAttribute("points", line.arrow);
    });
  }

  function drawCanvas() {
    const wrap = document.getElementById("canvasWrap");
    const wf = editorState.workflow;
    const nodes = wf.nodes || {};
    const names = Object.keys(nodes);

    if (!names.length) {
      wrap.innerHTML = emptyState("This workflow has no nodes", 'Use "+ Add node" above to start building.');
      editorState._layout = null;
      return;
    }

    const BOX_W = 150, BOX_H = 54, PAD = 60;
    let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;
    names.forEach((n) => {
      const p = nodes[n].position || [0, 0];
      minX = Math.min(minX, p[0]); minY = Math.min(minY, p[1]);
      maxX = Math.max(maxX, p[0]); maxY = Math.max(maxY, p[1]);
    });
    const offX = PAD - minX, offY = PAD - minY;
    const canvasW = (maxX - minX) + BOX_W + PAD * 2;
    const canvasH = (maxY - minY) + BOX_H + PAD * 2;
    editorState._layout = { offX, offY, BOX_W, BOX_H };

    const posOf = (n) => {
      const p = nodes[n].position || [0, 0];
      return { x: p[0] + offX, y: p[1] + offY };
    };

    // connection lines (SVG, drawn under the node boxes). Each is a <g>
    // tagged with data-src/data-tgt so drag handlers can find and
    // reposition just the lines touching a moved node, and a wide
    // invisible "hit" path so a thin line is still easy to click to
    // delete.
    let svgLines = "";
    const conns = wf.connections || {};
    Object.keys(conns).forEach((src) => {
      (conns[src] || []).forEach((c) => {
        if (!nodes[src] || !nodes[c.targetName]) return; // stale/dangling ref
        const line = connLineGeometry(posOf(src), posOf(c.targetName), { BOX_W, BOX_H });
        svgLines +=
          '<g class="conn" data-src="' + escapeHtml(src) + '" data-tgt="' + escapeHtml(c.targetName) +
          '" data-src-idx="' + c.sourceIndex + '" data-tgt-idx="' + c.targetIndex + '">' +
          '<path class="conn-line" d="' + line.d + '"/>' +
          '<path class="conn-hit" d="' + line.d + '"/>' +
          '<polygon class="conn-arrow" points="' + line.arrow + '"/>' +
          "</g>";
      });
    });

    const lastRuns = {};
    if (editorState.lastExecution && editorState.lastExecution.nodeRuns) {
      editorState.lastExecution.nodeRuns.forEach((r) => { lastRuns[r.nodeName] = r; });
    }

    let boxes = "";
    names.forEach((n) => {
      const pos = posOf(n);
      const node = nodes[n];
      const run = lastRuns[n];
      const statusClass = run ? " status-" + run.status : "";
      const selClass = editorState.selectedNodeName === n ? " selected" : "";
      const disClass = node.disabled ? " disabled" : "";
      boxes +=
        '<div class="node-box' + statusClass + selClass + disClass + '" data-node="' + escapeHtml(n) +
        '" style="left:' + pos.x + "px;top:" + pos.y + 'px;">' +
        '<div class="node-status-dot"></div>' +
        '<div class="node-handle node-handle-in" title="Drag a connection here"></div>' +
        '<div class="node-handle node-handle-out" title="Drag to another node to connect"></div>' +
        '<div class="node-type">' + escapeHtml(node.type || "node") + "</div>" +
        '<div class="node-name">' + escapeHtml(n) + "</div>" +
        "</div>";
    });

    wrap.innerHTML =
      '<div class="canvas-inner" style="width:' + canvasW + "px;height:" + canvasH + 'px;">' +
      '<svg width="' + canvasW + '" height="' + canvasH + '" style="position:absolute;top:0;left:0;pointer-events:none;">' +
      svgLines + "</svg>" + boxes + "</div>";

    wireCanvasInteractions(wrap);
  }

  // Wires node select/drag and output-handle-to-input-handle connecting.
  // Re-run after every full drawCanvas() since wrap.innerHTML wipes out
  // any previously-bound listeners along with the old DOM.
  function wireCanvasInteractions(wrap) {
    wrap.querySelectorAll(".node-box").forEach((box) => {
      box.addEventListener("mousedown", (ev) => {
        if (ev.target.closest(".node-handle")) return; // handled separately below
        ev.preventDefault();
        const name = box.dataset.node;
        selectNode(name);
        startNodeDrag(box, name, ev);
      });
    });

    wrap.querySelectorAll(".node-handle-out").forEach((handle) => {
      handle.addEventListener("mousedown", (ev) => {
        ev.preventDefault();
        ev.stopPropagation();
        const box = handle.closest(".node-box");
        startConnectionDrag(wrap, box.dataset.node, ev);
      });
    });

    wrap.querySelectorAll(".conn-hit").forEach((hit) => {
      hit.addEventListener("click", (ev) => {
        ev.stopPropagation();
        const g = hit.closest(".conn");
        const srcName = g.dataset.src;
        const conn = { targetName: g.dataset.tgt, sourceIndex: Number(g.dataset.srcIdx), targetIndex: Number(g.dataset.tgtIdx) };
        if (confirm('Delete the connection from "' + srcName + '" to "' + conn.targetName + '"?')) {
          deleteConnection(srcName, conn);
        }
      });
    });

    // Clicking empty canvas space deselects.
    wrap.addEventListener("mousedown", (ev) => {
      if (ev.target === wrap || ev.target.classList.contains("canvas-inner")) {
        deselectAll();
      }
    });
  }

  function startNodeDrag(box, name, downEv) {
    const node = editorState.workflow.nodes[name];
    const startX = downEv.clientX, startY = downEv.clientY;
    const origPos = (node.position || [0, 0]).slice();
    const layout = editorState._layout;
    let moved = false;

    function onMove(ev) {
      const dx = ev.clientX - startX, dy = ev.clientY - startY;
      if (Math.abs(dx) > 3 || Math.abs(dy) > 3) moved = true;
      node.position = [origPos[0] + dx, origPos[1] + dy];
      if (layout) {
        box.style.left = (node.position[0] + layout.offX) + "px";
        box.style.top = (node.position[1] + layout.offY) + "px";
      }
      updateConnectionsForNode(name);
    }
    function onUp() {
      document.removeEventListener("mousemove", onMove);
      document.removeEventListener("mouseup", onUp);
      if (moved) drawCanvas(); // resync bounding box/canvas size + reselect
    }
    document.addEventListener("mousemove", onMove);
    document.addEventListener("mouseup", onUp);
  }

  function startConnectionDrag(wrap, fromName, downEv) {
    const svg = wrap.querySelector("svg");
    const layout = editorState._layout;
    if (!svg || !layout) return;

    const fromNode = editorState.workflow.nodes[fromName];
    const fromPos = { x: (fromNode.position || [0, 0])[0] + layout.offX, y: (fromNode.position || [0, 0])[1] + layout.offY };
    const x1 = fromPos.x + layout.BOX_W, y1 = fromPos.y + layout.BOX_H / 2;

    const temp = document.createElementNS("http://www.w3.org/2000/svg", "path");
    temp.setAttribute("class", "conn-line conn-line-temp");
    temp.setAttribute("d", "M" + x1 + "," + y1 + " L" + x1 + "," + y1);
    svg.appendChild(temp);

    function canvasPoint(clientX, clientY) {
      const inner = wrap.querySelector(".canvas-inner");
      const rect = inner.getBoundingClientRect();
      return { x: clientX - rect.left, y: clientY - rect.top };
    }

    function onMove(ev) {
      const p = canvasPoint(ev.clientX, ev.clientY);
      const midX = (x1 + p.x) / 2;
      temp.setAttribute("d", "M" + x1 + "," + y1 + " C" + midX + "," + y1 + " " + midX + "," + p.y + " " + p.x + "," + p.y);
    }
    function onUp(ev) {
      document.removeEventListener("mousemove", onMove);
      document.removeEventListener("mouseup", onUp);
      temp.remove();

      const under = document.elementFromPoint(ev.clientX, ev.clientY);
      const inHandle = under && under.closest && under.closest(".node-handle-in");
      if (!inHandle) return;
      const toBox = inHandle.closest(".node-box");
      const toName = toBox && toBox.dataset.node;
      if (!toName || toName === fromName) return;

      const wf = editorState.workflow;
      const existing = wf.connections[fromName] || [];
      const dup = existing.some((c) => c.targetName === toName && c.sourceIndex === 0 && c.targetIndex === 0);
      if (dup) {
        toast("Already connected", "error");
        return;
      }
      wf.connections[fromName] = existing.concat([{ sourceName: fromName, sourceIndex: 0, targetName: toName, targetIndex: 0 }]);
      drawCanvas();
      toast("Connected \u201c" + fromName + "\u201d \u2192 \u201c" + toName + "\u201d \u2014 remember to Save", "success");
    }

    document.addEventListener("mousemove", onMove);
    document.addEventListener("mouseup", onUp);
  }

  // Delete/Backspace removes the selected node, but never while the
  // person is typing in a text field (params textarea, name inputs,
  // credential fields, etc.) -- and only while the editor is actually
  // on screen (checked via canvasWrap's presence rather than a route
  // flag, so this can't misfire after navigating away without also
  // tearing the listener down).
  document.addEventListener("keydown", (ev) => {
    if (ev.key !== "Delete" && ev.key !== "Backspace") return;
    if (!editorState || !editorState.selectedNodeName) return;
    if (!document.getElementById("canvasWrap")) return;
    const tag = (ev.target && ev.target.tagName) || "";
    if (tag === "INPUT" || tag === "TEXTAREA" || tag === "SELECT") return;
    ev.preventDefault();
    const name = editorState.selectedNodeName;
    if (confirm('Delete node "' + name + '"? This also removes its connections.')) {
      deleteNode(name);
    }
  });

  function renderSidePanel(nodeName) {
    const node = editorState.workflow.nodes[nodeName];
    if (!node) return renderSidePanelEmpty();
    const paramsStr = JSON.stringify(node.parameters || {}, null, 2);
    return (
      "<h3>" + escapeHtml(nodeName) + "</h3>" +
      '<div class="field"><label>Type</label><input type="text" value="' + escapeHtml(node.type || "") + '" disabled></div>' +
      '<div class="field"><label class="row-checkbox"><input type="checkbox" id="nodeDisabled" ' +
      (node.disabled ? "checked" : "") + "> Disabled</label></div>" +
      '<div class="field"><label class="row-checkbox"><input type="checkbox" id="nodeRetry" ' +
      (node.retryOnFail ? "checked" : "") + "> Retry on fail</label></div>" +
      '<div class="field"><label>Parameters (JSON)</label>' +
      '<textarea id="nodeParams" class="params-json">' + escapeHtml(paramsStr) + "</textarea>" +
      '<div id="paramsError" class="field-error"></div></div>' +
      '<button id="applyNodeBtn" class="btn btn-primary btn-sm">Apply to workflow</button> ' +
      '<button id="deleteNodeBtn" class="btn btn-danger btn-sm">Delete node</button>' +
      '<div style="color:var(--text-dim);font-size:11px;margin-top:8px;">Applies in-memory \u2014 click Save (top toolbar) to persist.</div>' +
      (isGoogleCredentialNode(node.type) ? renderCredentialSection() : "")
    );
  }

  function wireSidePanel(nodeName) {
    document.getElementById("applyNodeBtn").addEventListener("click", () => {
      const node = editorState.workflow.nodes[nodeName];
      const paramsErr = document.getElementById("paramsError");
      paramsErr.textContent = "";
      try {
        node.parameters = JSON.parse(document.getElementById("nodeParams").value || "{}");
      } catch (e) {
        paramsErr.textContent = "Invalid JSON: " + e.message;
        return;
      }
      node.disabled = document.getElementById("nodeDisabled").checked;
      node.retryOnFail = document.getElementById("nodeRetry").checked;
      toast("Applied \u2014 remember to Save", "success");
      drawCanvas();
    });

    document.getElementById("deleteNodeBtn").addEventListener("click", () => {
      if (confirm('Delete node "' + nodeName + '"? This also removes its connections.')) {
        deleteNode(nodeName);
      }
    });

    const node = editorState.workflow.nodes[nodeName];
    if (node && isGoogleCredentialNode(node.type)) {
      wireCredentialSection(nodeName);
    }
  }

  function wireEditorToolbar() {
    document.getElementById("wfName").addEventListener("input", (e) => {
      editorState.workflow.name = e.target.value;
    });
    document.getElementById("wfActive").addEventListener("change", (e) => {
      editorState.workflow.active = e.target.checked;
    });
    document.getElementById("btnAddNode").addEventListener("click", () => {
      addNode(document.getElementById("addNodeType").value);
    });
    document.getElementById("btnExport").addEventListener("click", () => exportWorkflow(editorState.workflow.id));
    document.getElementById("btnSave").addEventListener("click", saveCurrentWorkflow);
    document.getElementById("btnExecute").addEventListener("click", executeCurrentWorkflow);
  }

  async function saveCurrentWorkflow() {
    const btn = document.getElementById("btnSave");
    btn.disabled = true;
    btn.textContent = "Saving\u2026";
    try {
      const wf = await apiJSON("/api/workflows/" + encodeURIComponent(editorState.workflow.id) + "/save", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(editorState.workflow),
      });
      editorState.workflow = wf;
      toast("Saved", "success");
    } catch (e) {
      toast("Save failed: " + e.message, "error");
    } finally {
      btn.disabled = false;
      btn.textContent = "Save";
    }
  }

  // Root-cause fix: the fetch below previously had no timeout/abort
  // signal, so if the connection ever hung (Render free-tier cold
  // start/spin-down, a dropped connection with no FIN/RST, etc.) the
  // await never settled -- the "finally" block that re-enables
  // btnExecute never ran, and the button stayed disabled forever with
  // no console error and no new network request on subsequent clicks
  // (browsers don't dispatch click events on disabled buttons). The
  // AbortController below guarantees the promise always settles,
  // bounded by the same 30-minute ceiling the backend already enforces
  // (internal/runner.Runner.Timeout, "rule 22: nothing unbounded") plus
  // a small buffer, so the button can never get stuck again.
  // executeCurrentWorkflow: POST .../execute now returns immediately
  // (202 {executionId, status:"queued"} -- see api.handleExecuteAsync)
  // instead of blocking for the whole run, so there is no long-lived
  // fetch to time out here any more. followExecution below drives the
  // panel from there via SSE (spec section N: SSE is primary progress,
  // not polling).
  async function executeCurrentWorkflow() {
    const btn = document.getElementById("btnExecute");
    const startNode = document.getElementById("startNodeSelect").value;
    btn.disabled = true;
    btn.textContent = "Running\u2026";
    try {
      const q = startNode ? "?startNode=" + encodeURIComponent(startNode) : "";
      const accepted = await apiJSON("/api/workflows/" + encodeURIComponent(editorState.workflow.id) + "/execute" + q, {
        method: "POST",
      });
      if (!accepted || typeof accepted.executionId !== "string") {
        throw new Error("Server returned an unexpected response for this execution.");
      }
      await followExecution(accepted.executionId, btn);
    } catch (e) {
      toast("Execute failed: " + e.message, "error");
      btn.disabled = false;
      btn.textContent = "\u25B6 Execute";
    }
  }

  // followExecution subscribes to GET /api/executions/{id}/events (SSE)
  // and updates the exec panel live as node.completed/node.failed
  // events arrive -- see internal/api's handleExecutionEvents. Does one
  // GET .../executions/{id} up front so the panel starts from a real
  // snapshot even if the run raced ahead of this call, and one more if
  // the stream drops before a terminal event (the run itself is
  // unaffected by that -- section M/W: "Client disconnect MUST NOT
  // cancel the workflow automatically"). Resolves once the execution
  // reaches a terminal state or the stream can't be followed at all.
  function followExecution(execID, btn) {
    return new Promise((resolve) => {
      let ex = null;
      let settled = false;

      function finish() {
        if (settled) return;
        settled = true;
        btn.disabled = false;
        btn.textContent = "\u25B6 Execute";
        resolve();
      }

      function refreshSnapshot() {
        return apiJSON("/api/executions/" + encodeURIComponent(execID))
          .then((snapshot) => {
            ex = snapshot;
            editorState.lastExecution = ex;
            drawCanvas();
            renderExecPanel(ex);
          })
          .catch(() => {});
      }

      refreshSnapshot();

      var es;
      try {
        es = new EventSource("/api/executions/" + encodeURIComponent(execID) + "/events");
      } catch (e) {
        finish();
        return;
      }

      var TERMINAL_TYPES = {
        "execution.completed": true,
        "execution.failed": true,
        "execution.cancelled": true,
      };

      es.onmessage = function (msg) {
        var ev;
        try {
          ev = JSON.parse(msg.data);
        } catch (e) {
          return;
        }
        if (!ex) {
          ex = { status: "queued", mode: "manual", nodeRuns: [], startedAt: new Date().toISOString() };
        }
        if (ev.status) ex.status = ev.status;
        if (ev.error) ex.error = ev.error;
        if (ev.node) ex.nodeRuns = (ex.nodeRuns || []).concat([ev.node]);
        editorState.lastExecution = ex;
        drawCanvas();
        renderExecPanel(ex);

        if (TERMINAL_TYPES[ev.type]) {
          toast("Execution " + ex.status, ex.status === "error" ? "error" : "success");
          es.close();
          finish();
        }
      };

      es.onerror = function () {
        // The stream dropped (network blip, idle proxy, browser tab
        // backgrounded, etc.). Don't retry indefinitely -- one final
        // snapshot read is enough to un-stick the panel; the person can
        // re-open the workflow to pick the live stream back up, and the
        // run itself keeps going server-side either way.
        es.close();
        refreshSnapshot().then(finish);
      };
    });
  }

  function renderExecPanel(ex) {
    const host = document.getElementById("execPanelHost");
    if (!host) return;
    const runs = ex.nodeRuns || [];
    host.innerHTML =
      '<div class="exec-panel"><div class="exec-summary">' +
      '<span class="status-pill status-' + escapeHtml(ex.status) + '">' + escapeHtml(ex.status) + "</span>" +
      "<span>mode: " + escapeHtml(ex.mode) + "</span>" +
      "<span>started: " + escapeHtml(fmtDate(ex.startedAt)) + "</span>" +
      (ex.error ? '<span class="err-text">' + escapeHtml(ex.error) + "</span>" : "") +
      "</div>" +
      runs.map(renderNodeRun).join("") +
      "</div>";

    host.querySelectorAll(".node-run-head").forEach((h) => {
      h.addEventListener("click", () => h.parentElement.classList.toggle("open"));
    });
  }

  function renderNodeRun(r) {
    return (
      '<div class="node-run">' +
      '<div class="node-run-head"><span class="status-pill status-' + escapeHtml(r.status) + '">' + escapeHtml(r.status) + "</span>" +
      '<span class="name">' + escapeHtml(r.nodeName) + "</span>" +
      '<span class="dur">' + fmtDurationMs(r.durationMs) + (r.attempt > 1 ? " \u00b7 attempt " + r.attempt : "") + "</span></div>" +
      '<div class="node-run-body">' +
      (r.error ? '<div class="err-text">' + escapeHtml(r.error) + "</div>" : "") +
      (r.logs && r.logs.length ? "<pre>" + escapeHtml(r.logs.join("\n")) + "</pre>" : "") +
      "<div>Output:</div><pre>" + escapeHtml(JSON.stringify(r.output, null, 2)) + "</pre>" +
      "</div></div>"
    );
  }

  // ---------------- boot ----------------

  route();
})();
