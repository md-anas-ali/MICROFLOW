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
  const settingsBtn = document.getElementById("settingsBtn");
  const settingsMenu = document.getElementById("settingsMenu");
  const pageNavEl = document.getElementById("pageNav");
  let executionsMonitorES = null;
  let executionsMonitorTimer = null;
  let executionDetailES = null;

  // Currently selected Service (isolated workspace). Only the *selection*
  // lives in the browser; every request carries it in X-Microflow-Service
  // and the backend re-checks ownership, so tampering with this value can
  // never expose another Service's workflows/credentials.
  let currentService = "default";
  try { currentService = localStorage.getItem("mf.service") || "default"; } catch (_) {}
  let servicesCache = [];
  function setCurrentService(id) {
    currentService = id || "default";
    try { localStorage.setItem("mf.service", currentService); } catch (_) {}
  }
  function currentServiceName() {
    const sv = servicesCache.find((x) => x.id === currentService);
    return sv ? sv.name : currentService;
  }
  const svcQS = () => "serviceId=" + encodeURIComponent(currentService);
  const svcPath = (suffix) => "/api/services/" + encodeURIComponent(currentService) + (suffix || "");

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

  // Execution.mode is stored as "manual" | "schedule" | ...; show the two
  // trigger sources people care about as "Manual" / "Scheduled" (other
  // modes are shown as-is).
  function modeLabel(mode) {
    if (mode === "manual") return "Manual";
    if (mode === "schedule") return "Scheduled";
    return mode || "";
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
    if (path.indexOf("/api/") === 0) {
      opts.headers = Object.assign({}, opts.headers || {}, { "X-Microflow-Service": currentService });
    }
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

  // ---------------- header settings menu + page navigation ----------------

  function closeSettings() {
    settingsMenu.hidden = true;
    settingsBtn.setAttribute("aria-expanded", "false");
  }
  settingsBtn.addEventListener("click", (ev) => {
    ev.stopPropagation();
    const open = settingsMenu.hidden;
    settingsMenu.hidden = !open;
    settingsBtn.setAttribute("aria-expanded", String(open));
  });
  document.addEventListener("click", (ev) => {
    if (!ev.target.closest(".settings-wrap")) closeSettings();
  });

  // Back link (+ optional header HTML) shown above every non-home page.
  function setPageNav(backHref, backLabel, headHTML) {
    pageNavEl.innerHTML =
      (backHref ? '<a class="back-link" href="' + backHref + '">\u2190 ' + escapeHtml(backLabel) + "</a>" : "") +
      (headHTML || "");
  }
  const serviceHref = (id, tab) => "#/services/" + encodeURIComponent(id) + (tab ? "/" + tab : "");

  // Service Active/Inactive is a display + Run-guard flag kept in this
  // browser (the backend Service record has no such field). Every Service
  // is Active unless listed here; nothing is ever deleted by toggling it.
  const INACTIVE_KEY = "mf.inactiveServices";
  function inactiveSet() {
    try { return new Set(JSON.parse(localStorage.getItem(INACTIVE_KEY) || "[]")); } catch (_) { return new Set(); }
  }
  function isServiceActive(id) { return !inactiveSet().has(id); }
  function setServiceActive(id, on) {
    const s = inactiveSet();
    if (on) s.delete(id); else s.add(id);
    try { localStorage.setItem(INACTIVE_KEY, JSON.stringify(Array.from(s))); } catch (_) {}
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
    closeSettings();
    stopExecutionMonitoring();
    const parts = parseHash();
    const seg = (i) => (parts[i] ? decodeURIComponent(parts[i].split("?")[0]) : "");
    const p0 = seg(0), p1 = seg(1), p2 = seg(2), p3 = seg(3);
    try {
      if (!p0 || p0 === "dashboard" || (p0 === "services" && parts.length === 1)) {
        await renderDashboard();
      } else if (p0 === "services") {
        // Includes Google's OAuth callback: #/services/{id}/credentials?...
        await renderServiceTab(p1, p2, p3);
      } else if (p0 === "workflows" && parts.length === 1) {
        await renderServiceTab(currentService, "workflow", "");
      } else if (p0 === "workflows") {
        setPageNav(serviceHref(currentService, "workflow"), "Workflow");
        const executionId = new URLSearchParams(location.hash.split("?")[1] || "").get("execution") || "";
        await renderEditor(p1, executionId);
      } else if (p0 === "executions") {
        await renderServiceTab(currentService, "executions", p1);
      } else if (p0 === "credentials") {
        await renderServiceTab(currentService, "credentials", "");
      } else if (p0 === "service-env") {
        await renderServiceTab(currentService, "env", "");
      } else if (p0 === "import") {
        setPageNav(serviceHref(currentService, "workflow"), "Workflow");
        renderImport();
      } else if (p0 === "global-env") {
        setPageNav("#/dashboard", "Home");
        await renderEnvPage("global");
      } else if (p0 === "database") {
        setPageNav("#/dashboard", "Home");
        renderDatabasePage();
      } else {
        setPageNav("#/dashboard", "Home");
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

  // ---------------- home dashboard ----------------

  function lastRunText(ex) {
    if (!ex) return "No runs yet";
    const map = { success: "\u2705 Success", error: "\u274C Failed", cancelled: "\u26A0\uFE0F Cancelled", running: "\u23F3 Running", queued: "\u23F3 Queued", waiting: "\u23F3 Waiting" };
    return (map[ex.status] || ex.status) + " \u00B7 " + fmtDate(ex.startedAt);
  }

  function tileHTML(icon, num, label) {
    return '<div class="tile"><div class="tile-ico">' + icon + '</div><div class="tile-num">' + escapeHtml(num) +
      '</div><div class="tile-label">' + escapeHtml(label) + "</div></div>";
  }

  async function createServiceFlow() {
    const name = (prompt("Name for the new Service:") || "").trim();
    if (!name) return;
    try {
      const created = await apiJSON("/api/services", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ name }),
      });
      await loadServices();
      setCurrentService(created.id);
      toast("Service created", "success");
      location.hash = serviceHref(created.id);
    } catch (e) { toast(e.message, "error"); }
  }

  async function renderDashboard() {
    setPageNav("");
    viewEl.innerHTML = loadingRow();
    let list;
    try {
      list = await loadServices();
    } catch (e) {
      viewEl.innerHTML = emptyState("Can't reach the API", escapeHtml(e.message));
      return;
    }
    const lastRuns = await Promise.all(list.map((x) =>
      apiJSON("/api/services/" + encodeURIComponent(x.id) + "/executions?limit=1")
        .then((a) => (a && a[0]) || null).catch(() => null)));

    const activeCount = list.filter((x) => isServiceActive(x.id)).length;
    const attention = list.filter((x, i) => isServiceActive(x.id) && lastRuns[i] && lastRuns[i].status === "error").length;

    const cards = list.map((x, i) => {
      const on = isServiceActive(x.id);
      return (
        '<a class="svc-card' + (on ? "" : " off") + '" href="' + serviceHref(x.id) + '">' +
        '<div class="svc-card-ico">\uD83E\uDDE9</div>' +
        '<div class="svc-card-body"><div class="svc-card-name">' + escapeHtml(x.name) + "</div>" +
        '<div class="svc-card-sub">' + escapeHtml(lastRunText(lastRuns[i])) + "</div></div>" +
        '<span class="pill ' + (on ? "pill-on" : "pill-off") + '">' + (on ? "\uD83D\uDFE2 Active" : "\u26AA Inactive") + "</span>" +
        '<span class="menu-arrow">\u203A</span></a>'
      );
    }).join("");

    viewEl.innerHTML =
      '<div class="narrow">' +
      '<div class="tiles">' +
      tileHTML("\uD83E\uDDE9", list.length, "Services") +
      tileHTML("\uD83D\uDFE2", activeCount, "Active") +
      tileHTML("\u26AA", list.length - activeCount, "Inactive") +
      tileHTML("\uD83D\uDD34", attention, "Need attention") +
      "</div>" +
      '<h2 class="section-title">\uD83E\uDDE9 Services</h2>' +
      '<div class="svc-list">' + cards +
      '<button id="svcCreateBtn" class="svc-card svc-new">\u2795 New Service</button></div>' +
      '<details class="runall-details"><summary>\u25B6\uFE0F Run all Services</summary>' + runAllPanelHTML() + "</details>" +
      "</div>";
    document.getElementById("svcCreateBtn").addEventListener("click", createServiceFlow);
    wireRunAllPanel();
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
      const wfs = await apiJSON("/api/workflows?" + svcQS());
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
          '<td>' + escapeHtml(modeLabel(ex.mode)) + '</td>' +
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
        // Only this Service's executions (workflowNames was built from the
        // Service-scoped workflow list above).
        data.executions = (data.executions || []).filter((x) => workflowNames[x.workflowId]);
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

      // Preserve which node output panels the user opened. The live execution
      // refreshes every ~2 seconds/SSE event, so replacing detailHost used to
      // collapse an opened node automatically. Keep the open state by index.
      const openNodeIndexes = [];
      detailHost.querySelectorAll(".node-run").forEach((el, index) => {
        if (el.classList.contains("open")) openNodeIndexes.push(index);
      });

      detailHost.innerHTML =
        '<div class="exec-detail-head"><div><h3>' + escapeHtml(workflowNames[workflowID] || workflowID) + '</h3><div class="exec-id">' + escapeHtml(ex.id) + '</div></div>' +
        '<span class="status-pill status-' + escapeHtml(ex.status) + '">' + escapeHtml(ex.status) + '</span></div>' +
        '<div class="exec-detail-meta"><span>Mode: ' + escapeHtml(modeLabel(ex.mode)) + '</span><span>Started: ' + escapeHtml(fmtDate(ex.startedAt)) + '</span>' +
        (ex.finishedAt ? '<span>Finished: ' + escapeHtml(fmtDate(ex.finishedAt)) + '</span>' : '<span class="live-dot">● LIVE</span>') + '</div>' +
        (ex.error ? '<div class="err-text exec-detail-error">' + escapeHtml(ex.error) + '</div>' : '') +
        '<div class="exec-detail-actions">' +
        (!(["success", "error", "cancelled"].indexOf(ex.status) >= 0) ? '<button id="detailCancel" class="btn btn-danger btn-sm">Cancel</button>' : '') +
        (workflowID && workflowNames[workflowID] ? '<a class="btn btn-sm" href="#/workflows/' + encodeURIComponent(workflowID) + '?execution=' + encodeURIComponent(ex.id) + '">Open live canvas</a>' : '') +
        '<button id="detailCopyFull" class="btn btn-sm" title="Copy a complete human-readable diagnostic report for this execution">\uD83D\uDCCB Copy Full Execution</button>' +
        '<button id="detailCopyError" class="btn btn-sm" title="Copy only the failed/error nodes plus debugging info">\uD83D\uDD34 Copy Error Report</button>' +
        '<button id="detailDownloadJson" class="btn btn-sm" title="Download the complete raw execution data as JSON">\uD83D\uDCE6 Download Execution JSON</button>' +
        '</div>' +
        '<div class="exec-node-list">' + (runs.length ? runs.map((run, index) => renderNodeRun(run, index)).join("") : '<div class="empty-state">Waiting for the first node event…</div>') + '</div>';
      detailHost.querySelectorAll(".node-run").forEach((el, index) => {
        if (openNodeIndexes.indexOf(index) >= 0) el.classList.add("open");
      });
      detailHost.querySelectorAll(".node-run-head").forEach((h) => h.addEventListener("click", () => h.parentElement.classList.toggle("open")));
      wireNodeCopyButtons(detailHost, runs);
      const cancel = document.getElementById("detailCancel");
      if (cancel) cancel.addEventListener("click", async () => {
        cancel.disabled = true;
        try { await apiJSON("/api/executions/" + encodeURIComponent(ex.id) + "/cancel", { method: "POST" }); toast("Cancellation requested", "success"); }
        catch (e) { toast("Cancel failed: " + e.message, "error"); cancel.disabled = false; }
      });
      wireDebugReportButtons(ex.id);
      if (live && ["success", "error", "cancelled"].indexOf(ex.status) < 0) followExecutionDetail(ex.id);
      else if (executionDetailES && detailFollowID === ex.id) { executionDetailES.close(); executionDetailES = null; detailFollowID = ""; }
    }

    // wireDebugReportButtons wires the "1-Click Full Execution Copy /
    // Debug Report" buttons to GET /api/executions/{id}/debug-report
    // (see internal/api/server.go's handleDebugReport / internal/report
    // for the endpoint and internal/report/report.go for the report
    // content itself -- secret masking, "None" for missing fields,
    // truncation of huge inline blobs). Every click re-fetches from
    // that single read-only endpoint; nothing is cached or duplicated
    // in the frontend.
    function wireDebugReportButtons(execID) {
      const copyFullBtn = document.getElementById("detailCopyFull");
      const copyErrorBtn = document.getElementById("detailCopyError");
      const downloadBtn = document.getElementById("detailDownloadJson");
      if (copyFullBtn) copyFullBtn.addEventListener("click", () => copyExecutionReport(execID, "full", copyFullBtn));
      if (copyErrorBtn) copyErrorBtn.addEventListener("click", () => copyExecutionReport(execID, "error", copyErrorBtn));
      if (downloadBtn) downloadBtn.addEventListener("click", () => downloadExecutionJSON(execID, downloadBtn));
    }

    // copyTextToClipboard uses the standard async Clipboard API where
    // available (secure context required), falling back to the legacy
    // execCommand("copy") path (works on http:// / older browsers) so
    // "Copy" doesn't silently do nothing on a non-HTTPS deployment.
    // Never throws -- resolves false on any failure so callers can
    // show the standard "could not copy" toast instead of an
    // unhandled rejection.
    async function copyTextToClipboard(text) {
      if (navigator.clipboard && window.isSecureContext) {
        try { await navigator.clipboard.writeText(text); return true; }
        catch (_) { /* fall through to the legacy path below */ }
      }
      try {
        const ta = document.createElement("textarea");
        ta.value = text;
        ta.setAttribute("readonly", "");
        ta.style.position = "fixed";
        ta.style.left = "-9999px";
        document.body.appendChild(ta);
        ta.focus();
        ta.select();
        const ok = document.execCommand("copy");
        document.body.removeChild(ta);
        return ok;
      } catch (_) {
        return false;
      }
    }

    // copyExecutionReport backs "Copy Full Execution" (scope="full")
    // and "Copy Error Report" (scope="error"): GET the human-readable
    // text report and put it on the clipboard.
    async function copyExecutionReport(execID, scope, btn) {
      if (btn) btn.disabled = true;
      try {
        const res = await api("/api/executions/" + encodeURIComponent(execID) + "/debug-report?format=text&scope=" + encodeURIComponent(scope));
        const text = await res.text();
        const ok = await copyTextToClipboard(text);
        if (!ok) throw new Error("clipboard write failed");
        toast(scope === "error" ? "\u2713 Error report copied to clipboard" : "\u2713 Full execution copied to clipboard", "success");
      } catch (e) {
        toast("\u2715 Could not copy execution report", "error");
      } finally {
        if (btn) btn.disabled = false;
      }
    }

    // downloadExecutionJSON backs "Download Execution JSON": GET the
    // full raw JSON report (untruncated -- see report.ToJSON) and save
    // it as a file. Fetched as a blob (rather than navigating the
    // window to the URL) so a 404/network failure surfaces as the
    // normal failure toast instead of a broken top-level navigation.
    async function downloadExecutionJSON(execID, btn) {
      if (btn) btn.disabled = true;
      try {
        const res = await api("/api/executions/" + encodeURIComponent(execID) + "/debug-report");
        const blob = await res.blob();
        let filename = "microflow-execution-" + execID + ".json";
        const disp = res.headers.get("Content-Disposition") || "";
        const m = /filename="?([^";]+)"?/.exec(disp);
        if (m && m[1]) filename = m[1];
        const url = URL.createObjectURL(blob);
        const a = document.createElement("a");
        a.href = url;
        a.download = filename;
        document.body.appendChild(a);
        a.click();
        a.remove();
        setTimeout(() => URL.revokeObjectURL(url), 0);
        toast("\u2713 Execution JSON downloaded", "success");
      } catch (e) {
        toast("\u2715 Could not download execution JSON", "error");
      } finally {
        if (btn) btn.disabled = false;
      }
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

  // deleteWorkflow is the single place that calls DELETE
  // /api/workflows/{id}, shared by the list's row action and the
  // editor's toolbar button. Handles confirmation, double-click
  // protection (disabling the button — browsers don't dispatch click
  // events on a disabled button, same guarantee this file already
  // relies on for Execute/Save), and the 409 "still running" /
  // 404 "already gone" cases with a clear message either way.
  function deleteWorkflow(id, name, btn, opts) {
    opts = opts || {};
    if (btn.disabled) return; // extra guard against a stray re-entrant call
    if (!confirm('Delete workflow "' + (name || "(untitled)") + '"? This cannot be undone.')) return;

    btn.disabled = true;
    const originalText = btn.textContent;
    btn.textContent = "Deleting\u2026";

    apiJSON("/api/workflows/" + encodeURIComponent(id), { method: "DELETE" })
      .then(() => {
        toast("Workflow deleted", "success");
        if (opts.onSuccess) opts.onSuccess();
      })
      .catch((e) => {
        toast("Delete failed: " + e.message, "error");
        btn.disabled = false;
        btn.textContent = originalText;
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

  // ---------------- Database: Export / Import ----------------
  // Export downloads one portable backup of all persistent MicroFlow state
  // (GET /api/database/export). Import is a FULL REPLACE
  // (POST /api/database/import): the server validates the file, replaces the
  // database in a single transaction (all-or-nothing) and reloads its
  // schedules/webhooks; this page then reloads so every view reads fresh state.
  function renderDatabasePage() {
    const CONFIRM = "IMPORT DATABASE";
    const EXPORT_CONFIRM = "EXPORT DATABASE";
    viewEl.innerHTML =
      '<div class="narrow"><div class="page-head"><div><h1>\uD83D\uDDC4\uFE0F Database</h1>' +
      '<div class="sub">Move all of MicroFlow\u2019s saved state to another installation.</div></div></div>' +
      '<div class="card"><div class="env-section-title">\u2B07\uFE0F Export Database</div>' +
      '<div class="cred-section-note">Downloads one backup file with your Services, Environments, workflows, schedules and connected accounts. ' +
      'Secrets are never stored in plain text and the backup restores on any MicroFlow installation without its <code>MICROFLOW_MASTER_KEY</code>. ' +
      '<b>The file still contains your secrets in a form anyone holding it can open \u2014 store it privately.</b> ' +
      'Run history, logs and generated files are not included.</div>' +
      '<div class="field" style="margin-top:12px;"><label>Type <b>' + EXPORT_CONFIRM + '</b> to confirm</label><input type="text" id="dbExpConfirm" autocomplete="off"></div>' +
      '<button id="dbExportBtn" class="btn btn-primary btn-big" style="margin-top:12px;" disabled>Export Database</button></div>' +
      '<div class="card"><div class="env-section-title">\u2B06\uFE0F Import Database</div>' +
      '<div class="unsupported-box">Import <b>replaces everything</b> on this installation with the backup. ' +
      'It is all-or-nothing: if anything fails, nothing changes. It is refused while a workflow is running.</div>' +
      '<div id="dbDrop" class="import-drop" style="margin-top:12px;">Drop a backup <code>.json</code> here, or click to choose one' +
      '<input id="dbFile" type="file" accept="application/json,.json" style="display:none"></div>' +
      '<div id="dbFileName" class="sub" style="margin:8px 0;"></div>' +
      '<div class="field"><label>Type <b>' + CONFIRM + '</b> to confirm</label><input type="text" id="dbConfirm" autocomplete="off"></div>' +
      '<div class="field"><label>Your login password</label><input type="password" id="dbPw" autocomplete="current-password"></div>' +
      '<button id="dbImportBtn" class="btn btn-danger btn-big" disabled>Import Database</button>' +
      '<div id="dbResult"></div></div></div>';

    const exportBtn = document.getElementById("dbExportBtn");
    const drop = document.getElementById("dbDrop");
    const fileInput = document.getElementById("dbFile");
    const nameEl = document.getElementById("dbFileName");
    const confirmIn = document.getElementById("dbConfirm");
    const pwIn = document.getElementById("dbPw");
    const expConfirmIn = document.getElementById("dbExpConfirm");
    const importBtn = document.getElementById("dbImportBtn");
    const resultEl = document.getElementById("dbResult");
    let picked = null;

    const refreshBtn = () => {
      importBtn.disabled = !(picked && confirmIn.value === CONFIRM && pwIn.value);
    };
    const pick = (f) => {
      picked = f || null;
      nameEl.textContent = picked ? picked.name + " \u00B7 " + (picked.size / 1024).toFixed(1) + " KB" : "";
      refreshBtn();
    };

    exportBtn.addEventListener("click", async () => {
      if (expConfirmIn.value !== EXPORT_CONFIRM) return;
      exportBtn.disabled = true;
      exportBtn.textContent = "Exporting\u2026";
      try {
        const res = await api("/api/database/export", { headers: { "X-Microflow-Confirm": EXPORT_CONFIRM } });
        const blob = await res.blob();
        const cd = res.headers.get("Content-Disposition") || "";
        const m = /filename="([^"]+)"/.exec(cd);
        const a = document.createElement("a");
        a.href = URL.createObjectURL(blob);
        a.download = m ? m[1] : "microflow-backup.json";
        document.body.appendChild(a);
        a.click();
        a.remove();
        setTimeout(() => URL.revokeObjectURL(a.href), 10000);
        expConfirmIn.value = "";
        toast("Backup downloaded \u2014 store it privately", "success");
      } catch (e) { toast(e.message, "error"); }
      finally { exportBtn.textContent = "Export Database"; exportBtn.disabled = expConfirmIn.value !== EXPORT_CONFIRM; }
    });

    drop.addEventListener("click", () => fileInput.click());
    fileInput.addEventListener("change", () => pick(fileInput.files[0]));
    ["dragover", "dragenter"].forEach((evt) =>
      drop.addEventListener(evt, (e) => { e.preventDefault(); drop.classList.add("dragover"); }));
    ["dragleave", "drop"].forEach((evt) =>
      drop.addEventListener(evt, (e) => { e.preventDefault(); drop.classList.remove("dragover"); }));
    drop.addEventListener("drop", (e) => pick(e.dataTransfer.files && e.dataTransfer.files[0]));
    confirmIn.addEventListener("input", refreshBtn);
    pwIn.addEventListener("input", refreshBtn);
    expConfirmIn.addEventListener("input", () => { exportBtn.disabled = expConfirmIn.value !== EXPORT_CONFIRM; });

    importBtn.addEventListener("click", async () => {
      if (!picked) return;
      importBtn.disabled = true;
      importBtn.textContent = "Importing\u2026";
      resultEl.innerHTML = "";
      try {
        const r = await apiJSON("/api/database/import", {
          method: "POST",
          headers: { "Content-Type": "application/json", "X-Microflow-Confirm": CONFIRM, "X-Microflow-Password": pwIn.value },
          body: picked,
        });
        pwIn.value = "";
        const c = (r && r.counts) || {};
        resultEl.innerHTML = '<div class="card" style="margin-top:12px;">\u2705 Database imported: ' +
          escapeHtml((c.services || 0) + " service(s), " + (c.workflows || 0) + " workflow(s), " +
            ((c.global_env || 0) + (c.service_env || 0)) + " environment variable(s), " +
            (c.run_all_schedules || 0) + " Run All schedule(s)") + "." +
          (r && r.warning ? '<div class="unsupported-box" style="margin-top:8px;">' + escapeHtml(r.warning) + "</div>" : "") +
          (r && r.notice ? '<div class="unsupported-box" style="margin-top:8px;">' + escapeHtml(r.notice) + "</div>" : "") +
          "<div class=\"sub\" style=\"margin-top:8px;\">Reloading\u2026</div></div>";
        toast("Database imported", "success");
        // The previously selected Service may not exist in the restored data.
        try { localStorage.removeItem("mf.service"); } catch (_) {}
        setTimeout(() => { location.hash = "#/dashboard"; location.reload(); }, r && (r.warning || r.notice) ? 8000 : 1200);
      } catch (e) {
        toast(e.message, "error");
        resultEl.innerHTML = '<div class="unsupported-box" style="margin-top:12px;">' + escapeHtml(e.message) +
          " Nothing was changed.</div>";
        importBtn.textContent = "Import Database";
        refreshBtn();
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
      '<div class="page-head"><div><h1>\uD83D\uDD10 Credentials</h1>' +
      '<div class="sub">Connect Gmail, YouTube and Google Sheets for <b>' + escapeHtml(currentServiceName()) + "</b>.</div></div></div>" +
      '<div id="googleConnCards" class="google-conn-cards">' + loadingRow("Checking connections\u2026") + "</div>" +
      '<div id="googleConnNotConfigured" class="field-error" style="display:none;"></div>' +
      (currentService === "default"
        ? '<details class="cred-advanced"><summary>Advanced: manual credential (Default Service only)</summary>'
        : '<details class="cred-advanced" style="display:none"><summary></summary>') +
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
        '<a class="btn btn-sm" href="' + svcPath("/google/connect/" + encodeURIComponent(view.service)) + '">' +
        (view.needsReconnect ? "Reconnect" : "Reconnect") + "</a> " +
        '<button class="btn btn-danger btn-sm google-disconnect-btn" data-service="' + escapeHtml(view.service) + '">Disconnect</button>';
    } else {
      actionsHTML = '<a class="btn btn-primary btn-sm" href="' + svcPath("/google/connect/" + encodeURIComponent(view.service)) + '">Connect Google</a>';
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
      const services = await apiJSON(svcPath("/google/connections"));
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
          await apiJSON(svcPath("/google/disconnect/" + encodeURIComponent(service)), { method: "POST" });
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
      NODE_TYPE_OPTIONS.map((o) => '<option value="' + o[0] + '"' + (o[0] === "manualTrigger" ? " selected" : "") + '>' + escapeHtml(o[1]) + "</option>").join("") +
      "</select>" +
      '<button id="btnAddNode" class="btn btn-sm">+ Add node</button>' +
      '<select id="startNodeSelect" class="btn btn-sm" style="max-width:200px;">' +
      '<option value="">Auto (first trigger)</option>' +
      nodeNames.map((n) => '<option value="' + escapeHtml(n) + '">' + escapeHtml(n) + "</option>").join("") +
      "</select>" +
      '<button id="btnExecute" class="btn">\u25B6 Execute</button>' +
      '<button id="btnExport" class="btn">Export</button>' +
      '<button id="btnSave" class="btn btn-primary">Save</button>' +
      '<button id="btnDeleteWorkflow" class="btn btn-danger">Delete</button>' +
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
      // A new Schedule Trigger never fires on its own: it starts Disabled
      // and is switched on from the node panel (untick Disabled).
      disabled: type === "scheduleTrigger",
      retryOnFail: false,
      maxTries: 0,
      waitBetweenTriesMs: 0,
      continueOnFail: false,
    };
    editorState.selectedNodeName = name;
    drawCanvas();
    refreshStartNodeOptions();
    toast("Added \u201c" + name + "\u201d \u2014 remember to Save" +
      (type === "scheduleTrigger" ? " (Schedule Trigger starts Disabled \u2014 untick Disabled to enable)" : ""), "success");
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
      (node.disabled ? "checked" : "") + "> Disabled</label>" +
      (node.type === "scheduleTrigger"
        ? '<div class="sub" style="font-size:11px;margin-top:4px;">Schedule Trigger starts Disabled. Untick <b>Disabled</b> and click Apply to enable it (the workflow must also be Active).</div>' +
          '<div id="nodeNextRun" class="sub" style="font-size:12px;margin-top:6px;">Next run: \u2026</div>'
        : "") +
      "</div>" +
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
    loadNodeNextRun(nodeName);
    document.getElementById("applyNodeBtn").addEventListener("click", async () => {
      const node = editorState.workflow.nodes[nodeName];
      const paramsErr = document.getElementById("paramsError");
      paramsErr.textContent = "";
      let newParams;
      try {
        newParams = JSON.parse(document.getElementById("nodeParams").value || "{}");
      } catch (e) {
        paramsErr.textContent = "Invalid JSON: " + e.message;
        return;
      }
      const wasDisabled = !!node.disabled;
      const oldParams = node.parameters;
      node.parameters = newParams;
      node.disabled = document.getElementById("nodeDisabled").checked;
      node.retryOnFail = document.getElementById("nodeRetry").checked;
      // A Schedule Trigger's Enabled state (node.disabled) and its
      // interval/cron config (node.parameters.rule) live in the workflow
      // definition, and the server's scheduler only picks them up when the
      // workflow is saved -- so persist immediately through the normal save
      // endpoint instead of leaving them as in-memory state that is lost on
      // refresh/navigation.
      if (node.type === "scheduleTrigger" &&
          (!!node.disabled !== wasDisabled || JSON.stringify(newParams) !== JSON.stringify(oldParams))) {
        const ok = await saveCurrentWorkflow();
        if (!ok) {
          // not persisted -- don't show a state the server doesn't have
          node.disabled = wasDisabled;
          node.parameters = oldParams;
        }
        drawCanvas();
        return;
      }
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
    document.getElementById("btnDeleteWorkflow").addEventListener("click", (ev) => {
      const btn = ev.currentTarget;
      deleteWorkflow(editorState.workflow.id, editorState.workflow.name, btn, {
        onSuccess: () => { location.hash = "#/workflows"; },
      });
    });
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
      return true;
    } catch (e) {
      toast("Save failed: " + e.message, "error");
      return false;
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

    // Live SSE updates re-render this panel every time a node reports progress.
    // Preserve the user's expanded/collapsed state by node name so an opened
    // output never closes just because the execution received another event.
    const openNodeNames = new Set();
    host.querySelectorAll(".node-run.open").forEach((el) => {
      const name = el.querySelector(".node-run-head .name");
      if (name) openNodeNames.add(name.textContent);
    });

    host.innerHTML =
      '<div class="exec-panel"><div class="exec-summary">' +
      '<span class="status-pill status-' + escapeHtml(ex.status) + '">' + escapeHtml(ex.status) + "</span>" +
      "<span>mode: " + escapeHtml(modeLabel(ex.mode)) + "</span>" +
      "<span>started: " + escapeHtml(fmtDate(ex.startedAt)) + "</span>" +
      (ex.error ? '<span class="err-text">' + escapeHtml(ex.error) + "</span>" : "") +
      "</div>" +
      runs.map((run, index) => renderNodeRun(run, index)).join("") +
      "</div>";

    host.querySelectorAll(".node-run").forEach((el) => {
      const name = el.querySelector(".node-run-head .name");
      if (name && openNodeNames.has(name.textContent)) el.classList.add("open");
    });
    host.querySelectorAll(".node-run-head").forEach((h) => {
      h.addEventListener("click", () => h.parentElement.classList.toggle("open"));
    });
    wireNodeCopyButtons(host, runs);
  }

  function buildNodeCopyReport(r) {
    if (!r) return "";
    return [
      "MICROFLOW NODE EXECUTION",
      "=========================",
      "Node Name: " + (r.nodeName || "(unnamed)"),
      "Status: " + (r.status || "unknown"),
      "Attempt: " + (r.attempt || 1),
      "Started At: " + (r.startedAt || "None"),
      "Duration: " + (r.durationMs == null ? "None" : r.durationMs + " ms"),
      "",
      "INPUT:",
      JSON.stringify(r.input == null ? null : r.input, null, 2),
      "",
      "OUTPUT:",
      JSON.stringify(r.output == null ? null : r.output, null, 2),
      "",
      "ERROR:",
      r.error || "None",
      "",
      "LOGS:",
      (r.logs && r.logs.length) ? r.logs.join("\\n") : "None",
      "",
      "METADATA:",
      JSON.stringify(r.metadata == null ? {} : r.metadata, null, 2),
      "",
      "OPTIONS:",
      JSON.stringify(r.options == null ? {} : r.options, null, 2)
    ].join("\\n");
  }

  function renderNodeRun(r, index) {
    const output = r && r.output != null ? r.output : null;
    const outputText = JSON.stringify(output, null, 2);
    return (
      '<div class="node-run">' +
      '<div class="node-run-head"><span class="status-pill status-' + escapeHtml(r.status) + '">' + escapeHtml(r.status) + "</span>" +
      '<span class="name">' + escapeHtml(r.nodeName) + "</span>" +
      '<span class="dur">' + fmtDurationMs(r.durationMs) + (r.attempt > 1 ? " · attempt " + r.attempt : "") + "</span></div>" +
      '<div class="node-run-body">' +
      '<div class="node-run-output-head"><strong>Output:</strong><sl-copy-button class="node-copy-btn" data-node-copy="' + index + '" size="small" title="Copy node name, output, error, logs, metadata and options"></sl-copy-button></div>' +
      '<pre>' + escapeHtml(outputText) + "</pre>" +
      "</div></div>"
    );
  }

  function wireNodeCopyButtons(root, runs) {
    if (!root) return;
    root.querySelectorAll("sl-copy-button[data-node-copy]").forEach((btn) => {
      const index = Number(btn.dataset.nodeCopy);
      const run = runs[index];
      btn.value = buildNodeCopyReport(run);
      btn.addEventListener("click", (ev) => {
        ev.stopPropagation();
        // Shoelace performs the copy itself. The value contains the complete
        // diagnostic report while the UI only displays the node output.
        setTimeout(() => toast("✓ Node output copied", "success"), 0);
      });
    });
  }

  async function copyNodeRun(r, btn) {
    if (!r) return;
    const report = buildNodeCopyReport(r);
    const ok = await copyTextToClipboard(report);
    if (!ok) toast("✕ Could not copy node output", "error");
  }


  // ---------------- Services, Environment, Run All ----------------
  //
  // Everything below talks to internal/api/services.go. Environment lists
  // carry key names/flags only; a value is fetched one key at a time, and
  // only when the person explicitly clicks Show / Edit.

  async function loadServices() {
    servicesCache = (await apiJSON("/api/services")) || [];
    if (!servicesCache.some((x) => x.id === currentService)) {
      setCurrentService("default");
    }
    return servicesCache;
  }

  let runAllTimer = null;
  function stopRunAllPolling() {
    if (runAllTimer) { clearInterval(runAllTimer); runAllTimer = null; }
  }

  function runAllPanelHTML() {
    return (
      '<div class="card" id="runAllPanel" style="margin-bottom:16px;">' +
      "<h3>Run All Services</h3>" +
      '<div class="sub" style="margin-bottom:10px;">Runs every Service one at a time (never in parallel). ' +
      "Each Service finishes and is cleaned up before the next starts. Progress survives a server restart.</div>" +
      '<div style="display:flex;gap:10px;align-items:center;flex-wrap:wrap;">' +
      '<button id="runAllBtn" class="btn btn-primary">Run All Services</button>' +
      '<button id="runAllCancelBtn" class="btn btn-danger" style="display:none;">Cancel</button>' +
      '<label style="font-size:12px;"><input type="checkbox" id="runAllStop"> Stop if a Service fails</label>' +
      "</div>" +
      '<div id="runAllStatus" style="margin-top:12px;"></div>' +
      rasSectionHTML() +
      "</div>"
    );
  }

  // ---------------- Next Run (every schedule) ----------------
  //
  // GET /api/schedules/next-runs reports the next fire time of every
  // registered schedule (workflow Schedule Triggers + Run All Services
  // schedules), all in the scheduler's timezone (MICROFLOW_SCHEDULER_TIMEZONE,
  // UTC by default) -- the same zone cron hours are read in.

  async function fetchNextRuns() {
    try {
      const d = await apiJSON("/api/schedules/next-runs");
      const byId = {};
      (d.schedules || []).forEach((x) => { byId[x.id] = x; });
      return { timezone: d.timezone || "UTC", byId: byId, list: d.schedules || [] };
    } catch (e) {
      return { timezone: "UTC", byId: {}, list: [] };
    }
  }

  function relativeIn(iso) {
    const ms = new Date(iso).getTime() - Date.now();
    if (!(ms > 0)) return "";
    const m = Math.round(ms / 60000);
    if (m < 60) return "in " + m + " min";
    const h = Math.floor(m / 60), mm = m % 60;
    if (h < 48) return "in " + h + "h" + (mm ? " " + mm + "m" : "");
    return "in " + Math.round(h / 24) + " days";
  }

  function nextRunText(info, tz) {
    if (!info) return "\u2014";
    if (info.state === "scheduled" && info.nextRun) {
      return info.nextRunText + " " + tz + " (" + relativeIn(info.nextRun) + ")";
    }
    return { disabled: "Disabled", ended: "Ended", completed: "Completed (repeat limit reached)", invalid: "Not scheduled" }[info.state] || "\u2014";
  }

  async function loadNodeNextRun(nodeName) {
    const el = document.getElementById("nodeNextRun");
    if (!el || !editorState) return;
    const prefix = editorState.workflow.id + "/" + nodeName;
    const nr = await fetchNextRuns();
    const mine = nr.list.filter((x) => x.id === prefix || x.id.indexOf(prefix + "#") === 0);
    if (!document.getElementById("nodeNextRun")) return;
    if (!mine.length) {
      el.textContent = "Next run: \u2014 (not scheduled; save the workflow after enabling)";
      return;
    }
    el.innerHTML = "Next run: " + mine.map((x) => escapeHtml(nextRunText(x, nr.timezone))).join("<br>");
  }

  // ---------------- Run All Services: Schedule ----------------
  //
  // Talks to internal/api/run_all_schedules.go. A schedule fires Run All
  // Services automatically (cron or a plain interval) through the exact
  // same path the manual button above uses -- no separate runner/queue.
  // A newly added schedule always starts Disabled; Enable it once you're
  // happy with the timing.

  let rasEditingId = null; // null while adding, a schedule id while editing

  function rasSectionHTML() {
    return (
      '<div id="rasSection" style="margin-top:18px;border-top:1px solid var(--border,#333);padding-top:14px;">' +
      '<div style="display:flex;align-items:center;justify-content:space-between;gap:10px;flex-wrap:wrap;">' +
      "<h4 style=\"margin:0;\">\u23F0 Run All Services \u2014 Schedule</h4>" +
      '<button id="rasAddBtn" class="btn">+ Add Schedule</button>' +
      "</div>" +
      '<div class="sub" style="margin:6px 0 10px;">Set a time/day and MicroFlow will trigger Run All Services on its own ' +
      "\u2014 every Service still runs one at a time, exactly like clicking the button above. Add, edit, delete, enable or disable as many as you like.</div>" +
      rasFormHTML() +
      '<div id="rasList" class="sub">Loading\u2026</div>' +
      "</div>"
    );
  }

  const RAS_DAYS = [["1", "Mon"], ["2", "Tue"], ["3", "Wed"], ["4", "Thu"], ["5", "Fri"], ["6", "Sat"], ["0", "Sun"]];

  function rasFormHTML() {
    const dayBoxes = RAS_DAYS.map((d) =>
      '<label style="font-size:12px;"><input type="checkbox" class="ras-day" value="' + d[0] + '" checked> ' + d[1] + "</label>"
    ).join(" ");
    const hourOpts = Array.from({ length: 24 }, (_, h) => '<option value="' + h + '">' + String(h).padStart(2, "0") + "</option>").join("");
    return (
      '<div id="rasForm" class="card" style="display:none;margin-bottom:12px;padding:12px;">' +
      '<div style="display:grid;gap:8px;max-width:520px;">' +
      '<label style="font-size:12px;">Label (optional)<br>' +
      '<input id="rasLabel" type="text" placeholder="e.g. Nightly run" style="width:100%;"></label>' +
      '<div style="display:flex;gap:16px;font-size:12px;flex-wrap:wrap;">' +
      '<label><input type="radio" name="rasMode" id="rasModeDays" value="days" checked> Days &amp; time</label>' +
      '<label><input type="radio" name="rasMode" id="rasModeInterval" value="interval"> Simple interval</label>' +
      '<label><input type="radio" name="rasMode" id="rasModeCron" value="cron"> Cron (advanced)</label>' +
      "</div>" +
      '<div id="rasDaysRow"><div style="font-size:12px;margin-bottom:4px;">Days</div>' +
      '<div style="display:flex;gap:10px;flex-wrap:wrap;">' + dayBoxes + "</div>" +
      '<div style="display:flex;gap:10px;align-items:center;margin-top:8px;font-size:12px;flex-wrap:wrap;">' +
      '<label>Hour <select id="rasHour">' + hourOpts + "</select></label>" +
      '<label>Minute <input id="rasMinute" type="number" min="0" max="59" value="0" style="width:64px;"></label>' +
      "</div></div>" +
      '<div id="rasCronRow" style="display:none;"><label style="font-size:12px;">Cron ("minute hour day month weekday")<br>' +
      '<input id="rasCron" type="text" placeholder="0 19 * * *" style="width:100%;"></label>' +
      '<div class="sub">Example: <code>0 19 * * *</code> = every day at 19:00. <code>0 9 * * 1-5</code> = weekdays at 09:00.</div></div>' +
      '<div id="rasIntervalRow" style="display:none;">' +
      '<label style="font-size:12px;">Every<br>' +
      '<span style="display:flex;gap:6px;align-items:center;">' +
      '<input id="rasIntervalValue" type="number" min="1" value="1" style="width:80px;">' +
      '<select id="rasIntervalUnit"><option value="60">minute(s)</option><option value="3600">hour(s)</option><option value="86400">day(s)</option></select>' +
      "</span></label></div>" +
      '<div style="font-size:12px;display:flex;gap:14px;align-items:center;flex-wrap:wrap;">' +
      '<label><input type="radio" name="rasRepeat" id="rasRepeatUnlimited" checked> Unlimited</label>' +
      '<label><input type="radio" name="rasRepeat" id="rasRepeatN"> Repeat</label>' +
      '<input id="rasRepeatCount" type="number" min="1" value="1" style="width:70px;"> <span>time(s)</span></div>' +
      '<div style="display:flex;gap:12px;flex-wrap:wrap;font-size:12px;">' +
      '<label>Start (optional)<br><input id="rasStartAt" type="datetime-local"></label>' +
      '<label>End (optional)<br><input id="rasEndAt" type="datetime-local"></label></div>' +
      '<div class="sub" id="rasTzHint">All times use the scheduler timezone.</div>' +
      '<label style="font-size:12px;"><input type="checkbox" id="rasStopOnFailure"> Stop if a Service fails</label>' +
      '<div style="display:flex;gap:8px;">' +
      '<button id="rasSaveBtn" class="btn btn-primary">Add Schedule</button>' +
      '<button id="rasCancelBtn" class="btn">Cancel</button>' +
      "</div></div></div>"
    );
  }

  // Days + Hour + Minute are stored as an ordinary cron expression
  // ("M H * * d,d,d" / "M H * * *"), so the existing scheduler runs it
  // unchanged; rasParseDaysCron turns such a cron back into the form.
  function rasBuildDaysCron() {
    const days = Array.from(document.querySelectorAll(".ras-day:checked")).map((c) => c.value);
    if (!days.length) return null;
    const h = document.getElementById("rasHour").value;
    const m = parseInt(document.getElementById("rasMinute").value, 10);
    if (isNaN(m) || m < 0 || m > 59) return null;
    return m + " " + h + " * * " + (days.length === 7 ? "*" : days.join(","));
  }

  function rasParseDaysCron(expr) {
    const mt = /^(\d{1,2}) (\d{1,2}) \* \* (\*|[0-6](?:,[0-6])*)$/.exec((expr || "").trim());
    if (!mt || +mt[1] > 59 || +mt[2] > 23) return null;
    return { minute: +mt[1], hour: +mt[2], days: mt[3] === "*" ? RAS_DAYS.map((d) => d[0]) : mt[3].split(",") };
  }

  function rasTimingText(row) {
    if (row.cronExpr) {
      const d = rasParseDaysCron(row.cronExpr);
      if (d) {
        const names = d.days.length === 7 ? "Every day" : RAS_DAYS.filter((x) => d.days.indexOf(x[0]) >= 0).map((x) => x[1]).join(", ");
        return names + " at " + String(d.hour).padStart(2, "0") + ":" + String(d.minute).padStart(2, "0");
      }
      return "Cron " + row.cronExpr;
    }
    const s = row.intervalSeconds || 0;
    if (s > 0 && s % 86400 === 0) return "Every " + (s / 86400) + " day(s)";
    if (s > 0 && s % 3600 === 0) return "Every " + (s / 3600) + " hour(s)";
    if (s > 0 && s % 60 === 0) return "Every " + (s / 60) + " minute(s)";
    return "Every " + s + " second(s)";
  }

  function rasRepeatText(row) {
    const runs = (row.runCount || 0) + " / " + (row.maxRuns > 0 ? row.maxRuns : "\u221E");
    const win = (row.startAt ? "from " + row.startAt.replace("T", " ") : "") + (row.endAt ? " until " + row.endAt.replace("T", " ") : "");
    return "Runs " + runs + (win ? "<br>" + escapeHtml(win) : "");
  }

  function rasListHTML(rows, nr) {
    if (!rows || !rows.length) return '<div class="sub">No schedules yet.</div>';
    const tz = (nr && nr.timezone) || "UTC";
    const rowsHtml = rows.map((row) =>
      "<tr>" +
      "<td>" + escapeHtml(row.label || "\u2014") + "</td>" +
      "<td>" + escapeHtml(rasTimingText(row)) + "</td>" +
      "<td>" + rasRepeatText(row) + "</td>" +
      "<td>" + escapeHtml(nextRunText(nr && nr.byId[row.id], tz)) + "</td>" +
      "<td>" + (row.stopOnFailure ? "Yes" : "No") + "</td>" +
      '<td><span class="badge ' + (row.enabled ? "badge-active" : "badge-inactive") + '">' +
      (row.enabled ? "Enabled" : "Disabled") + "</span></td>" +
      '<td style="display:flex;gap:6px;flex-wrap:wrap;">' +
      '<button class="btn btn-sm ras-toggle" data-id="' + escapeHtml(row.id) + '" data-enabled="' + (row.enabled ? "1" : "0") + '">' +
      (row.enabled ? "Disable" : "Enable") + "</button>" +
      '<button class="btn btn-sm ras-edit" data-id="' + escapeHtml(row.id) + '">Edit</button>' +
      '<button class="btn btn-sm btn-danger ras-delete" data-id="' + escapeHtml(row.id) + '">Delete</button>' +
      "</td></tr>"
    ).join("");
    return (
      '<div class="sub" style="margin-bottom:6px;">Times shown in scheduler timezone: <b>' + escapeHtml(tz) + "</b></div>" +
      '<table class="wf-table"><thead><tr><th>Label</th><th>Schedule</th><th>Repeat / window</th><th>Next run</th><th>Stop on failure</th><th>Status</th><th></th></tr></thead>' +
      "<tbody>" + rowsHtml + "</tbody></table>"
    );
  }

  function rasSetMode(mode) {
    document.getElementById("rasModeDays").checked = mode === "days";
    document.getElementById("rasModeCron").checked = mode === "cron";
    document.getElementById("rasModeInterval").checked = mode === "interval";
    document.getElementById("rasDaysRow").style.display = mode === "days" ? "" : "none";
    document.getElementById("rasCronRow").style.display = mode === "cron" ? "" : "none";
    document.getElementById("rasIntervalRow").style.display = mode === "interval" ? "" : "none";
  }

  function rasSetDays(days) {
    document.querySelectorAll(".ras-day").forEach((c) => { c.checked = days.indexOf(c.value) >= 0; });
  }

  function rasSetRepeat(maxRuns) {
    document.getElementById("rasRepeatUnlimited").checked = !(maxRuns > 0);
    document.getElementById("rasRepeatN").checked = maxRuns > 0;
    document.getElementById("rasRepeatCount").value = String(maxRuns > 0 ? maxRuns : 1);
  }

  function rasResetForm() {
    rasEditingId = null;
    document.getElementById("rasLabel").value = "";
    document.getElementById("rasCron").value = "";
    document.getElementById("rasIntervalValue").value = "1";
    document.getElementById("rasIntervalUnit").value = "3600";
    document.getElementById("rasStopOnFailure").checked = false;
    document.getElementById("rasHour").value = "9";
    document.getElementById("rasMinute").value = "0";
    document.getElementById("rasStartAt").value = "";
    document.getElementById("rasEndAt").value = "";
    rasSetDays(RAS_DAYS.map((d) => d[0]));
    rasSetRepeat(0);
    rasSetMode("days");
    document.getElementById("rasSaveBtn").textContent = "Add Schedule";
  }

  function rasFillFormForEdit(row) {
    rasEditingId = row.id;
    document.getElementById("rasLabel").value = row.label || "";
    document.getElementById("rasStopOnFailure").checked = !!row.stopOnFailure;
    document.getElementById("rasStartAt").value = row.startAt || "";
    document.getElementById("rasEndAt").value = row.endAt || "";
    rasSetRepeat(row.maxRuns || 0);
    const d = row.cronExpr ? rasParseDaysCron(row.cronExpr) : null;
    if (d) {
      rasSetMode("days");
      rasSetDays(d.days);
      document.getElementById("rasHour").value = String(d.hour);
      document.getElementById("rasMinute").value = String(d.minute);
    } else if (row.cronExpr) {
      rasSetMode("cron");
      document.getElementById("rasCron").value = row.cronExpr;
    } else {
      rasSetMode("interval");
      const s = row.intervalSeconds || 60;
      let unit = 60, val = s;
      if (s > 0 && s % 86400 === 0) { unit = 86400; val = s / 86400; }
      else if (s > 0 && s % 3600 === 0) { unit = 3600; val = s / 3600; }
      else if (s > 0 && s % 60 === 0) { unit = 60; val = s / 60; }
      else { unit = 1; val = s; }
      document.getElementById("rasIntervalUnit").value = String(unit);
      document.getElementById("rasIntervalValue").value = String(val);
    }
    document.getElementById("rasSaveBtn").textContent = "Save Changes";
    document.getElementById("rasForm").style.display = "";
  }

  async function reloadRasList() {
    const listEl = document.getElementById("rasList");
    if (!listEl) return;
    try {
      const [rows, nr] = await Promise.all([apiJSON("/api/run-all-schedules"), fetchNextRuns()]);
      listEl.innerHTML = rasListHTML(rows, nr);
      const hint = document.getElementById("rasTzHint");
      if (hint) hint.textContent = "Hour/Minute, Start and End use the scheduler timezone: " + nr.timezone;
      wireRasListButtons();
    } catch (e) {
      listEl.innerHTML = '<div class="field-error">' + escapeHtml(e.message) + "</div>";
    }
  }

  function wireRasListButtons() {
    document.querySelectorAll(".ras-toggle").forEach((btn) => {
      btn.addEventListener("click", async () => {
        const id = btn.getAttribute("data-id");
        const enabled = btn.getAttribute("data-enabled") === "1";
        btn.disabled = true;
        try {
          await apiJSON("/api/run-all-schedules/" + encodeURIComponent(id) + "/" + (enabled ? "disable" : "enable"), { method: "POST" });
          toast(enabled ? "Schedule disabled" : "Schedule enabled", "success");
          await reloadRasList();
        } catch (e) {
          toast(e.message, "error");
          btn.disabled = false;
        }
      });
    });
    document.querySelectorAll(".ras-edit").forEach((btn) => {
      btn.addEventListener("click", async () => {
        const id = btn.getAttribute("data-id");
        try {
          const row = await apiJSON("/api/run-all-schedules/" + encodeURIComponent(id));
          rasFillFormForEdit(row);
        } catch (e) {
          toast(e.message, "error");
        }
      });
    });
    document.querySelectorAll(".ras-delete").forEach((btn) => {
      btn.addEventListener("click", async () => {
        const id = btn.getAttribute("data-id");
        if (!confirm("Delete this schedule? This cannot be undone.")) return;
        btn.disabled = true;
        try {
          await apiJSON("/api/run-all-schedules/" + encodeURIComponent(id), { method: "DELETE" });
          toast("Schedule deleted", "success");
          if (rasEditingId === id) {
            rasResetForm();
            document.getElementById("rasForm").style.display = "none";
          }
          await reloadRasList();
        } catch (e) {
          toast(e.message, "error");
          btn.disabled = false;
        }
      });
    });
  }

  function wireRunAllSchedules() {
    const addBtn = document.getElementById("rasAddBtn");
    const form = document.getElementById("rasForm");
    const saveBtn = document.getElementById("rasSaveBtn");
    const cancelBtn = document.getElementById("rasCancelBtn");
    if (!addBtn) return;
    if (addBtn.dataset.wired) {
      // wireRunAllPanel() can be called again on the same, still-mounted
      // DOM (e.g. after running a single Service) without a re-render --
      // only refresh the list then, so click handlers are never attached
      // twice (which would otherwise submit the Add/Edit form twice).
      reloadRasList();
      return;
    }
    addBtn.dataset.wired = "1";
    addBtn.addEventListener("click", () => {
      const showing = form.style.display !== "none";
      if (showing) {
        form.style.display = "none";
        return;
      }
      rasResetForm();
      form.style.display = "";
    });
    document.getElementById("rasModeDays").addEventListener("change", () => rasSetMode("days"));
    document.getElementById("rasModeCron").addEventListener("change", () => rasSetMode("cron"));
    document.getElementById("rasModeInterval").addEventListener("change", () => rasSetMode("interval"));
    cancelBtn.addEventListener("click", () => {
      form.style.display = "none";
      rasResetForm();
    });
    saveBtn.addEventListener("click", async () => {
      const payload = {
        label: document.getElementById("rasLabel").value.trim(),
        stopOnFailure: document.getElementById("rasStopOnFailure").checked,
        startAt: document.getElementById("rasStartAt").value,
        endAt: document.getElementById("rasEndAt").value,
        maxRuns: 0,
      };
      if (document.getElementById("rasRepeatN").checked) {
        const n = parseInt(document.getElementById("rasRepeatCount").value, 10);
        if (!n || n < 1) { toast("Enter a valid repeat count (or choose Unlimited)", "error"); return; }
        payload.maxRuns = n;
      }
      if (payload.startAt && payload.endAt && payload.endAt <= payload.startAt) {
        toast("End must be after Start", "error");
        return;
      }
      if (document.getElementById("rasModeCron").checked) {
        payload.cronExpr = document.getElementById("rasCron").value.trim();
        payload.intervalSeconds = 0;
        if (!payload.cronExpr) { toast("Enter a cron expression", "error"); return; }
      } else if (document.getElementById("rasModeInterval").checked) {
        const val = parseInt(document.getElementById("rasIntervalValue").value, 10);
        const unit = parseInt(document.getElementById("rasIntervalUnit").value, 10);
        if (!val || val < 1) { toast("Enter a valid interval", "error"); return; }
        payload.intervalSeconds = val * unit;
        payload.cronExpr = "";
      } else {
        const cron = rasBuildDaysCron();
        if (!cron) { toast("Pick at least one day and a valid hour/minute", "error"); return; }
        payload.cronExpr = cron;
        payload.intervalSeconds = 0;
      }
      saveBtn.disabled = true;
      try {
        if (rasEditingId) {
          await apiJSON("/api/run-all-schedules/" + encodeURIComponent(rasEditingId), {
            method: "PATCH",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payload),
          });
          toast("Schedule updated", "success");
        } else {
          await apiJSON("/api/run-all-schedules", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payload),
          });
          toast("Schedule added (disabled \u2014 enable it when ready)", "success");
        }
        form.style.display = "none";
        rasResetForm();
        await reloadRasList();
      } catch (e) {
        toast(e.message, "error");
      } finally {
        saveBtn.disabled = false;
      }
    });
    reloadRasList();
  }

  function runAllStatusHTML(st) {
    if (!st || st.status === "idle") return '<div class="sub">No run yet.</div>';
    const names = {};
    servicesCache.forEach((x) => { names[x.id] = x.name; });
    const rows = (st.results || []).map((r) =>
      "<tr><td>" + escapeHtml(r.serviceName || names[r.serviceId] || r.serviceId) + "</td><td>" +
      escapeHtml(r.workflowName || "\u2014") + '</td><td><span class="badge ' +
      (r.status === "success" ? "badge-active" : "badge-inactive") + '">' + escapeHtml(r.status) + "</span></td><td>" +
      escapeHtml(r.error || "") + "</td></tr>"
    ).join("");
    const done = new Set((st.results || []).map((r) => r.serviceId)).size;
    return (
      "<div><b>Status:</b> " + escapeHtml(st.status) +
      " &middot; " + done + " / " + (st.serviceIds || []).length + " service(s) reached" +
      (st.error ? ' &middot; <span class="field-error">' + escapeHtml(st.error) + "</span>" : "") + "</div>" +
      (rows
        ? '<table class="wf-table" style="margin-top:8px;"><thead><tr><th>Service</th><th>Workflow</th><th>Result</th><th>Error</th></tr></thead><tbody>' + rows + "</tbody></table>"
        : "")
    );
  }

  function wireRunAllPanel() {
    const btn = document.getElementById("runAllBtn");
    const cancelBtn = document.getElementById("runAllCancelBtn");
    const statusEl = document.getElementById("runAllStatus");
    if (!btn) return;
    async function refresh() {
      if (!document.getElementById("runAllStatus")) { stopRunAllPolling(); return; }
      try {
        const st = await apiJSON("/api/run-all");
        statusEl.innerHTML = runAllStatusHTML(st);
        const active = st.status === "queued" || st.status === "running";
        btn.disabled = active;
        cancelBtn.style.display = active ? "" : "none";
        if (!active) stopRunAllPolling();
      } catch (e) {
        statusEl.innerHTML = '<div class="field-error">' + escapeHtml(e.message) + "</div>";
        stopRunAllPolling();
      }
    }
    function startPolling() {
      stopRunAllPolling();
      runAllTimer = setInterval(refresh, 3000);
    }
    btn.addEventListener("click", async () => {
      if (!confirm("Run every Service one after another now?")) return;
      btn.disabled = true;
      try {
        await apiJSON("/api/run-all", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ stopOnFailure: document.getElementById("runAllStop").checked }),
        });
        toast("Run All started", "success");
      } catch (e) {
        toast(e.message, "error");
      }
      await refresh();
      startPolling();
    });
    cancelBtn.addEventListener("click", async () => {
      try { await apiJSON("/api/run-all/cancel", { method: "POST" }); toast("Cancelling after the current workflow\u2026"); }
      catch (e) { toast(e.message, "error"); }
    });
    refresh().then(() => {
      if (btn.disabled) startPolling();
    });
    wireRunAllSchedules();
  }

  async function runCurrentService(btn) {
    btn.disabled = true;
    let ok = false;
    try {
      await apiJSON(svcPath("/run"), { method: "POST" });
      toast("Started " + currentServiceName(), "success");
      ok = true;
      const st = document.getElementById("runAllStatus");
      if (st) wireRunAllPanel();
    } catch (e) {
      toast(e.message, "error");
    } finally {
      btn.disabled = false;
    }
    return ok;
  }

  // ---------------- Service pages (inside a Service) ----------------

  const SERVICE_TABS = [
    ["overview", "Overview", "\uD83D\uDCCA"],
    ["env", "Environment", "\u2699\uFE0F"],
    ["credentials", "Credentials", "\uD83D\uDD10"],
    ["workflow", "Workflow", "\uD83D\uDD04"],
    ["executions", "Executions", "\u25B6\uFE0F"],
    ["logs", "Logs", "\uD83D\uDCCB"],
    ["settings", "Settings", "\u2699\uFE0F"],
  ];

  function serviceHeadHTML(svc) {
    const on = isServiceActive(svc.id);
    return (
      '<div class="svc-head' + (on ? "" : " off") + '"><span class="svc-head-dot">' + (on ? "\uD83D\uDFE2" : "\u26AA") + "</span>" +
      '<div><div class="svc-head-name">' + escapeHtml(svc.name) + "</div>" +
      '<div class="svc-head-status">' + (on ? "Active" : "Inactive") + "</div></div></div>"
    );
  }

  async function renderServiceTab(id, tab, extra) {
    if (!servicesCache.length) { try { await loadServices(); } catch (_) {} }
    const svc = servicesCache.find((x) => x.id === id);
    if (!svc) {
      setPageNav("#/dashboard", "Services");
      viewEl.innerHTML = emptyState("Service not found", "This Service doesn't exist.", "\uD83E\uDDE9");
      return;
    }
    setCurrentService(id);
    if (!tab) {
      setPageNav("#/dashboard", "Services", serviceHeadHTML(svc));
      viewEl.innerHTML =
        '<div class="menu-list">' +
        SERVICE_TABS.map((t) =>
          '<a class="menu-item" href="' + serviceHref(id, t[0]) + '"><span class="menu-ico">' + t[2] +
          '</span><span class="menu-label">' + t[1] + '</span><span class="menu-arrow">\u203A</span></a>').join("") +
        "</div>";
      return;
    }
    if (!SERVICE_TABS.some((t) => t[0] === tab)) {
      setPageNav(serviceHref(id), svc.name);
      viewEl.innerHTML = emptyState("Not found", "That page doesn't exist.");
      return;
    }
    setPageNav(serviceHref(id), svc.name);
    if (tab === "overview") return renderServiceOverview(svc);
    if (tab === "env") return renderEnvPage("service");
    if (tab === "credentials") return renderCentralCredentialsPage();
    if (tab === "workflow") return renderServiceWorkflow(svc);
    if (tab === "executions") return renderExecutions(extra || "");
    if (tab === "logs") return renderServiceLogs(svc);
    return renderServiceSettings(svc);
  }

  async function renderServiceOverview(svc) {
    viewEl.innerHTML = loadingRow();
    const on = isServiceActive(svc.id);
    let wfs = [], runs = [];
    try { wfs = (await apiJSON("/api/workflows?" + svcQS())) || []; } catch (_) {}
    try { runs = (await apiJSON(svcPath("/executions?limit=20"))) || []; } catch (_) {}
    const okCount = runs.filter((r) => r.status === "success").length;
    const failCount = runs.filter((r) => r.status === "error").length;
    viewEl.innerHTML =
      '<div class="narrow"><div class="tiles">' +
      tileHTML(on ? "\uD83D\uDFE2" : "\u26AA", on ? "Active" : "Inactive", "Status") +
      tileHTML("\uD83D\uDD04", wfs.length ? "Ready" : "None", "Workflow") +
      tileHTML("\u2705", okCount, "Recent successes") +
      tileHTML("\u274C", failCount, "Recent failures") +
      "</div>" +
      '<div class="card"><div class="svc-card-sub">Last run</div><div class="last-run">' + escapeHtml(lastRunText(runs[0])) + "</div></div>" +
      '<button id="runNowBtn" class="btn btn-primary btn-big btn-block"' + (on ? "" : " disabled") + ">\u25B6\uFE0F Run now</button>" +
      (on ? "" : '<div class="hint">Turn this Service on in Settings to run it.</div>') +
      "</div>";
    const rb = document.getElementById("runNowBtn");
    rb.addEventListener("click", async () => {
      if (await runCurrentService(rb)) location.hash = serviceHref(svc.id, "executions");
    });
  }

  async function renderServiceWorkflow(svc) {
    viewEl.innerHTML = loadingRow();
    let wfs;
    try {
      wfs = (await apiJSON("/api/workflows?" + svcQS())) || [];
    } catch (e) {
      viewEl.innerHTML = emptyState("Can't load", escapeHtml(e.message));
      return;
    }
    // Every Service has exactly one Workflow; show the most recently updated one.
    const wf = wfs.slice().sort((a, b) => new Date(b.updatedAt || 0) - new Date(a.updatedAt || 0))[0];
    if (!wf) {
      viewEl.innerHTML = '<div class="narrow">' +
        emptyState("No workflow yet", 'Bring one in with <a href="#/import">Import</a>.', "\uD83D\uDD04") + "</div>";
      return;
    }
    const on = isServiceActive(svc.id);
    viewEl.innerHTML =
      '<div class="narrow"><div class="card wf-card"><div class="wf-card-ico">\uD83D\uDD04</div>' +
      '<div class="wf-card-title">Workflow</div>' +
      '<div class="wf-card-name">' + escapeHtml(wf.name || "(untitled)") + "</div>" +
      '<div class="wf-card-status">' + (on ? "\uD83D\uDFE2 Ready" : "\u26AA Service inactive") + "</div>" +
      '<a class="btn btn-primary btn-big" href="#/workflows/' + encodeURIComponent(wf.id) + '">Open Workflow</a>' +
      "</div></div>";
  }

  async function renderServiceLogs(svc) {
    viewEl.innerHTML = loadingRow();
    let runs;
    try {
      runs = (await apiJSON(svcPath("/executions?limit=30"))) || [];
    } catch (e) {
      viewEl.innerHTML = emptyState("Can't load", escapeHtml(e.message));
      return;
    }
    if (!runs.length) {
      viewEl.innerHTML = '<div class="narrow">' + emptyState("No logs yet", "Run the Service and its log will show up here.", "\uD83D\uDCCB") + "</div>";
      return;
    }
    viewEl.innerHTML = '<div class="narrow"><div class="svc-list">' + runs.map((r) =>
      '<a class="menu-item log-row" href="' + serviceHref(svc.id, "executions") + "/" + encodeURIComponent(r.id) + '">' +
      '<div class="svc-card-body"><div class="svc-card-name">' + escapeHtml(lastRunText(r)) + "</div>" +
      '<div class="svc-card-sub">' + escapeHtml(modeLabel(r.mode)) + "</div>" +
      (r.error ? '<div class="log-err">' + escapeHtml(String(r.error).slice(0, 160)) + "</div>" : "") +
      '</div><span class="menu-arrow">\u203A</span></a>').join("") + "</div></div>";
  }

  function renderServiceSettings(svc) {
    const on = isServiceActive(svc.id);
    viewEl.innerHTML =
      '<div class="narrow">' +
      '<div class="card toggle-card"><div><div class="toggle-title">' + (on ? "\uD83D\uDFE2 Active" : "\u26AA Inactive") + "</div>" +
      '<div class="svc-card-sub">Switching off never deletes Environment, Credentials, Workflow or data.</div></div>' +
      '<button id="svcToggle" class="switch' + (on ? " on" : "") + '" role="switch" aria-checked="' + on + '" aria-label="Active"><span></span></button></div>' +
      '<button id="svcRename" class="menu-item"><span class="menu-ico">\u270F\uFE0F</span><span class="menu-label">Rename Service</span></button>' +
      (svc.id === "default" ? "" : '<button id="svcDelete" class="menu-item danger"><span class="menu-ico">\uD83D\uDDD1\uFE0F</span><span class="menu-label">Delete Service</span></button>') +
      "</div>";

    document.getElementById("svcToggle").addEventListener("click", () => {
      setServiceActive(svc.id, !on);
      toast(!on ? "Service is Active" : "Service is Inactive", "success");
      setPageNav(serviceHref(svc.id), svc.name);
      renderServiceSettings(svc);
    });
    document.getElementById("svcRename").addEventListener("click", async () => {
      const name = (prompt("New name:", svc.name) || "").trim();
      if (!name || name === svc.name) return;
      try {
        await apiJSON("/api/services/" + encodeURIComponent(svc.id), {
          method: "PATCH", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ name }),
        });
        await loadServices();
        toast("Renamed", "success");
        renderServiceTab(svc.id, "settings", "");
      } catch (e) { toast(e.message, "error"); }
    });
    const del = document.getElementById("svcDelete");
    if (del) del.addEventListener("click", async () => {
      const typed = prompt(
        'Deleting "' + svc.name + '" permanently removes its workflows, Google connections and environment.\n' +
        "Type the Service name exactly to confirm:");
      if (typed === null) return;
      if (typed !== svc.name) { toast("Name did not match \u2014 nothing was deleted", "error"); return; }
      try {
        await apiJSON("/api/services/" + encodeURIComponent(svc.id) + "?confirmName=" + encodeURIComponent(svc.name), { method: "DELETE" });
        setServiceActive(svc.id, true);
        setCurrentService("default");
        toast("Service deleted", "success");
        await loadServices();
        location.hash = "#/dashboard";
      } catch (e) { toast(e.message, "error"); }
    });
  }

  // scope: "global" (Global Environment) or "service" (current Service's own values)
  async function renderEnvPage(scope) {
    const isGlobal = scope === "global";
    const base = isGlobal ? "/api/global-env" : svcPath("/env");
    const title = isGlobal ? "\uD83C\uDF10 Global Environment" : "\u2699\uFE0F Environment";
    const sub = isGlobal
      ? "Shared by every Service. A Service's own value wins over it."
      : "Values for <b>" + escapeHtml(currentServiceName()) + "</b> only. Anything missing here comes from Global.";
    const ENV_MENU = [
      ["view", "\uD83D\uDC41\uFE0F", "View"], ["edit", "\u270F\uFE0F", "Edit"], ["add", "\u2795", "Add"],
      ["import", "\uD83D\uDCE5", "Import"], ["remove", "\uD83D\uDDD1\uFE0F", "Remove"], ["delete", "\uD83D\uDD34", "Delete"],
    ];
    const sectionTitle = isGlobal ? "\uD83C\uDF10 Global Environment" : "\u2699\uFE0F Service Environment";
    // Both scopes (Global and Service) carry the same \u22EE menu in the section header.
    const envPath = (suffix) => (isGlobal ? "/api/global-env" : svcPath("/env")) + (suffix || "");
    const scopeName = isGlobal ? "Global" : currentServiceName();
    const sectionHead = '<div class="env-head"><div class="env-section-title">' + sectionTitle + "</div>" +
      '<div class="env-menu-wrap"><button id="envMenuBtn" class="btn btn-sm env-menu-btn" aria-haspopup="menu" aria-expanded="false" aria-label="Environment actions">\u22EE</button>' +
      '<div id="envMenu" class="env-menu" role="menu" hidden>' +
      ENV_MENU.map((m) => '<button class="env-menu-item' + (m[0] === "delete" ? " danger" : "") + '" role="menuitem" data-act="' + m[0] + '"><span class="menu-ico">' + m[1] + "</span>" + m[2] + "</button>").join("") +
      '</div></div></div><div id="envPanel"></div>';
    viewEl.innerHTML =
      '<div class="narrow"><div class="page-head"><div><h1>' + title + '</h1><div class="sub">' + sub + "</div></div></div>" +
      '<div class="env-section">' + sectionHead +
      '<div id="envTable">' + loadingRow() + "</div></div>" +
      (isGlobal
        ? '<div class="cred-section-note" style="margin-top:8px;">Values are encrypted and never shown unless you use View / Edit. ' +
          "GOOGLE_OAUTH_CLIENT_ID / GOOGLE_OAUTH_CLIENT_SECRET / GOOGLE_OAUTH_REDIRECT_URL take effect after a server restart.</div>"
        : '<div class="env-section env-inherited"><div class="env-section-title">\uD83C\uDF10 Inherited from Global</div><div id="envInherited">' + loadingRow() + "</div></div>") +
      "</div>";

    let envList = [];

    async function loadInherited(own) {
      const host = document.getElementById("envInherited");
      if (!host) return;
      try {
        const g = (await apiJSON("/api/global-env")) || [];
        const ownKeys = new Set(own.map((e) => e.key));
        host.innerHTML = g.length
          ? '<div class="env-list">' + g.map((e) =>
              '<div class="env-row"><div class="env-row-main"><code>' + escapeHtml(e.key) + "</code>" +
              (ownKeys.has(e.key) ? '<div class="env-row-sub">overridden by this Service</div>' : "") +
              "</div></div>").join("") + "</div>"
          : '<div class="sub">Nothing set in Global.</div>';
      } catch (e) { host.innerHTML = '<div class="sub">' + escapeHtml(e.message) + "</div>"; }
    }

    async function refresh() {
      const host = document.getElementById("envTable");
      if (!host) return;
      let list = [];
      try {
        list = (await apiJSON(base)) || [];
        envList = list;
        host.innerHTML = list.length
          ? '<div class="env-list">' + list.map((e) =>
              '<div class="env-row"><div class="env-row-main"><code>' + escapeHtml(e.key) + '</code><div class="env-row-sub">' +
              (e.isSecret ? "\uD83D\uDD12 secret" : "plain") + " \u00B7 " + escapeHtml(fmtDate(e.updatedAt)) +
              "</div></div>" +
              "</div>").join("") + "</div>"
          : emptyState("Nothing set yet", "Use the \u22EE menu to add one.", "\u2699\uFE0F");
      } catch (e) {
        host.innerHTML = emptyState("Can't load", escapeHtml(e.message));
      }
      loadInherited(list);
    }

    // ---------- \u22EE menu + panels (Global and Service scope) ----------
    // Values are only ever fetched per key, on an explicit Show/Edit, and
    // live only in DOM nodes / input fields (never in variables that
    // outlive the panel, never logged).
    const MASK = "\u2022\u2022\u2022\u2022\u2022\u2022\u2022\u2022";
    const jsonPost = (path, body, method) => apiJSON(path, {
      method: method || "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body),
    });
    const fetchValue = async (key) => {
      const r = await apiJSON(envPath("/" + encodeURIComponent(key)));
      return r && r.value != null ? r.value : "";
    };
    const panelHost = () => document.getElementById("envPanel");
    const closePanel = () => { const h = panelHost(); if (h) h.innerHTML = ""; };
    const panelShell = (heading, body) =>
      '<div class="card env-panel"><div class="env-panel-head"><div class="env-section-title">' + heading + "</div>" +
      '<button class="btn btn-sm" id="envPanelClose">Close</button></div>' + body + "</div>";
    const openPanel = (heading, body) => {
      panelHost().innerHTML = panelShell(heading, body);
      document.getElementById("envPanelClose").addEventListener("click", closePanel);
      panelHost().scrollIntoView({ block: "nearest" });
    };
    const keyOptions = () => envList.map((e) => '<option value="' + escapeHtml(e.key) + '">' + escapeHtml(e.key) + "</option>").join("");
    const needKeys = (heading, msg) => {
      if (envList.length) return false;
      openPanel(heading, '<div class="sub">' + msg + "</div>");
      return true;
    };
    const showToggle = (inputId, toggleId) => {
      document.getElementById(toggleId).addEventListener("change", (ev) => {
        document.getElementById(inputId).type = ev.target.checked ? "text" : "password";
      });
    };

    function openView() {
      if (needKeys("\uD83D\uDC41\uFE0F View", isGlobal ? "No variables in Global yet." : "No variables in this Service yet.")) return;
      openPanel("\uD83D\uDC41\uFE0F View",
        '<div class="env-list">' + envList.map((e) =>
          '<div class="env-row" data-key="' + escapeHtml(e.key) + '" data-secret="' + (e.isSecret ? "1" : "0") + '">' +
          '<div class="env-row-main"><code>' + escapeHtml(e.key) + '</code><div class="env-row-sub env-val">' + MASK + "</div></div>" +
          '<button class="btn btn-sm env-eye">Show</button></div>').join("") + "</div>" +
        '<div class="cred-section-note" style="margin-top:8px;">Secret values are hidden by default and hide again after 30 seconds.</div>');
      panelHost().querySelectorAll(".env-row").forEach((row) => {
        const key = row.dataset.key, isSecret = row.dataset.secret === "1";
        const out = row.querySelector(".env-val"), btn = row.querySelector(".env-eye");
        let shown = false, timer = null;
        const hide = () => { clearTimeout(timer); out.textContent = MASK; out.classList.remove("shown"); btn.textContent = "Show"; shown = false; };
        const show = async () => {
          try {
            const v = await fetchValue(key);
            out.textContent = v === "" ? "(empty)" : v;
            out.classList.add("shown"); btn.textContent = "Hide"; shown = true;
            if (isSecret) timer = setTimeout(hide, 30000);
          } catch (e) { toast(e.message, "error"); }
        };
        btn.addEventListener("click", () => (shown ? hide() : show()));
        if (!isSecret) show();
      });
    }

    function openEdit() {
      if (needKeys("\u270F\uFE0F Edit", "No variables to edit yet.")) return;
      openPanel("\u270F\uFE0F Edit",
        '<div class="field"><label>Variable</label><select id="envEditKey">' + keyOptions() + "</select></div>" +
        '<div class="field"><label>Value</label><input type="password" id="envEditVal" autocomplete="off"></div>' +
        '<div class="field"><label><input type="checkbox" id="envEditShow"> Show value</label></div>' +
        '<div class="field"><label><input type="checkbox" id="envEditSecret"> Secret</label></div>' +
        '<button id="envEditSave" class="btn btn-primary btn-big">Save changes</button>');
      showToggle("envEditVal", "envEditShow");
      const sel = document.getElementById("envEditKey"), val = document.getElementById("envEditVal");
      const load = async () => {
        const meta = envList.find((e) => e.key === sel.value);
        document.getElementById("envEditSecret").checked = !meta || meta.isSecret;
        val.value = "";
        try { val.value = await fetchValue(sel.value); } catch (e) { toast(e.message, "error"); }
      };
      sel.addEventListener("change", load);
      load();
      document.getElementById("envEditSave").addEventListener("click", async () => {
        try {
          await jsonPost(base, { key: sel.value, value: val.value, isSecret: document.getElementById("envEditSecret").checked, mode: "update" });
          toast("Updated", "success");
          closePanel(); refresh();
        } catch (e) { toast(e.message, "error"); }
      });
    }

    function openAdd() {
      openPanel("\u2795 Add",
        '<div class="field"><label>Name</label><input type="text" id="envAddKey" placeholder="e.g. API_KEY" autocomplete="off"></div>' +
        '<div class="field"><label>Value</label><input type="password" id="envAddVal" autocomplete="off"></div>' +
        '<div class="field"><label><input type="checkbox" id="envAddShow"> Show value</label></div>' +
        '<div class="field"><label><input type="checkbox" id="envAddSecret" checked> Secret</label></div>' +
        '<button id="envAddSave" class="btn btn-primary btn-big">Add variable</button>');
      showToggle("envAddVal", "envAddShow");
      document.getElementById("envAddSave").addEventListener("click", async () => {
        const key = document.getElementById("envAddKey").value.trim();
        if (!key) { toast("Enter a name", "error"); return; }
        try {
          await jsonPost(base, { key, value: document.getElementById("envAddVal").value, isSecret: document.getElementById("envAddSecret").checked, mode: "create" });
          toast("Added", "success");
          closePanel(); refresh();
        } catch (e) { toast(e.message, "error"); }
      });
    }

    function openImport() {
      openPanel("\uD83D\uDCE5 Import .env",
        '<div class="field"><label>.env file</label><input type="file" id="envImpFile" accept=".env,text/plain"></div>' +
        '<div class="field"><label>or paste KEY=VALUE lines</label><textarea id="envImpText" class="json-paste" rows="7" spellcheck="false" autocomplete="off" placeholder="API_KEY=abc123&#10;# comments and blank lines are ignored"></textarea></div>' +
        '<div class="field"><label><input type="checkbox" id="envImpOver"> Overwrite variables that already exist (otherwise they are kept)</label></div>' +
        '<button id="envImpGo" class="btn btn-primary btn-big">Import into ' + escapeHtml(scopeName) + "</button>" +
        '<div id="envImpResult" class="cred-section-note" style="margin-top:8px;">Imported values are stored as secrets. Multi-line values are not supported.</div>');
      const text = document.getElementById("envImpText");
      document.getElementById("envImpFile").addEventListener("change", async (ev) => {
        const f = ev.target.files && ev.target.files[0];
        if (!f) return;
        if (f.size > 256 * 1024) { toast("File is too large (max 256 KB)", "error"); ev.target.value = ""; return; }
        text.value = await f.text();
      });
      document.getElementById("envImpGo").addEventListener("click", async () => {
        if (!text.value.trim()) { toast("Choose a .env file or paste some lines", "error"); return; }
        try {
          const r = await jsonPost(envPath("/import"), { content: text.value, overwrite: document.getElementById("envImpOver").checked });
          text.value = ""; // don't leave secrets sitting in the page
          const bad = (r.invalid || []).map((i) => "line " + i.line + " (" + i.reason + ")");
          document.getElementById("envImpResult").textContent =
            "Added " + r.added + " \u00B7 Updated " + r.updated + " \u00B7 Kept existing " + r.skipped +
            (r.duplicatesInFile ? " \u00B7 Repeated in file " + r.duplicatesInFile + " (last one used)" : "") +
            (bad.length ? " \u00B7 Ignored: " + bad.join(", ") : "");
          toast("Import finished", "success");
          refresh();
        } catch (e) { toast(e.message, "error"); }
      });
    }

    function openRemove() {
      if (needKeys("\uD83D\uDDD1\uFE0F Remove", "No variables to remove.")) return;
      openPanel("\uD83D\uDDD1\uFE0F Remove",
        '<div class="field"><label>Variable</label><select id="envRmKey">' + keyOptions() + "</select></div>" +
        '<button id="envRmGo" class="btn btn-danger btn-big">Remove variable</button>' +
        '<div class="cred-section-note" style="margin-top:8px;">Removes only the selected variable from ' + (isGlobal ? "Global" : "this Service") + ".</div>");
      document.getElementById("envRmGo").addEventListener("click", async () => {
        const key = document.getElementById("envRmKey").value;
        if (!confirm('Remove "' + key + '" from ' + scopeName + "?\nOnly this variable is removed.")) return;
        try {
          await apiJSON(base + "/" + encodeURIComponent(key), { method: "DELETE" });
          toast("Removed", "success");
          closePanel(); refresh();
        } catch (e) { toast(e.message, "error"); }
      });
    }

    async function openDelete() {
      const name = scopeName;
      let needPw = true;
      try { const st = await apiJSON("/api/auth-status"); needPw = !st || st.loginRequired !== false; } catch (_) { needPw = true; }
      openPanel("\uD83D\uDD34 Delete Environment set",
        '<div class="sub" style="margin-bottom:10px;">Deletes <b>all ' + envList.length + " variable(s)</b> of <b>" + escapeHtml(name) +
        "</b>. " + (isGlobal
          ? "Service Environments, workflows and credentials are not touched, but every Service loses these shared values."
          : "The Service, its workflows, credentials, Global Environment and other Services are not touched.") + " This cannot be undone.</div>" +
        '<div class="field"><label>' + (needPw ? "Step 1 \u2014 " : "") + "type " + (isGlobal ? "Global" : "the Service name") + ' to confirm</label><input type="text" id="envDelName" autocomplete="off" placeholder="' + escapeHtml(name) + '"></div>' +
        (needPw ? '<div class="field"><label>Step 2 \u2014 verify with your login password</label><input type="password" id="envDelPw" autocomplete="current-password" disabled></div>' : "") +
        '<button id="envDelGo" class="btn btn-danger btn-big" disabled>Delete Environment set</button>');
      const nameIn = document.getElementById("envDelName"), pwIn = document.getElementById("envDelPw"), go = document.getElementById("envDelGo");
      const sync = () => {
        const ok = nameIn.value === name;
        if (pwIn) pwIn.disabled = !ok;
        go.disabled = !(ok && (!pwIn || pwIn.value.length > 0));
      };
      nameIn.addEventListener("input", sync);
      if (pwIn) pwIn.addEventListener("input", sync);
      go.addEventListener("click", async () => {
        go.disabled = true;
        try {
          const r = await jsonPost(envPath("/delete-all"), { confirmName: nameIn.value, password: pwIn ? pwIn.value : "" });
          if (pwIn) pwIn.value = "";
          toast("Deleted " + (r && r.deleted != null ? r.deleted : 0) + " variable(s)", "success");
          closePanel(); refresh();
        } catch (e) {
          if (pwIn) pwIn.value = "";
          sync();
          toast(e.message, "error");
        }
      });
    }

    const ACTIONS = { view: openView, edit: openEdit, add: openAdd, import: openImport, remove: openRemove, delete: openDelete };
    const menuBtn = document.getElementById("envMenuBtn"), menu = document.getElementById("envMenu");
    function outsideClick(ev) { if (!menu.contains(ev.target) && !menuBtn.contains(ev.target)) closeMenu(); }
    function closeMenu() {
      menu.hidden = true;
      menuBtn.setAttribute("aria-expanded", "false");
      document.removeEventListener("click", outsideClick);
    }
    menuBtn.addEventListener("click", () => {
      if (!menu.hidden) { closeMenu(); return; }
      menu.hidden = false;
      menuBtn.setAttribute("aria-expanded", "true");
      document.addEventListener("click", outsideClick);
    });
    menu.querySelectorAll(".env-menu-item").forEach((b) => b.addEventListener("click", () => {
      closeMenu();
      ACTIONS[b.dataset.act]();
    }));
    refresh();
  }

  // Transient UI state (toasts, live-monitor connections, run-status
  // pollers, cached service list) is dropped every ~2.5 hours so a
  // long-lived tab never accumulates state. Only in-browser state is
  // cleared -- Services, workflows, credentials and both Environments
  // live in the database and are untouched. Skipped while the workflow
  // editor is open so unsaved edits are never lost.
  const TRANSIENT_RESET_MS = 2.5 * 60 * 60 * 1000;
  setInterval(async () => {
    const parts = parseHash();
    if (parts[0] === "workflows" && parts.length >= 2) return;
    stopRunAllPolling();
    stopExecutionMonitoring();
    toastHost.innerHTML = "";
    servicesCache = [];
    try { await loadServices(); } catch (_) {}
    route();
  }, TRANSIENT_RESET_MS);

  // ---------------- boot ----------------

  (async function boot() {
    try { await loadServices(); } catch (_) { /* server without tenancy: stay on "default" */ }
    route();
  })();
})();
