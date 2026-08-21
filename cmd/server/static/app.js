// MicroFlow UI — plain JS, no build step, no external dependencies.
// Talks only to the existing API surface (internal/api/server.go):
//   GET  /api/workflows
//   POST /api/workflows/import
//   POST /api/workflows/{id}/save
//   GET  /api/workflows/{id}
//   GET  /api/workflows/{id}/export
//   POST /api/workflows/{id}/execute?startNode=...
// No other endpoints exist server-side, so execution results are only
// ever what the most recent POST .../execute call returned — there is
// no execution-history endpoint to page through.

(function () {
  "use strict";

  const viewEl = document.getElementById("view");
  const connEl = document.getElementById("connStatus");
  const sidebar = document.getElementById("sidebar");
  const sidebarBackdrop = document.getElementById("sidebarBackdrop");
  const navToggle = document.getElementById("navToggle");

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

  async function route() {
    closeSidebar();
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
        await renderEditor(decodeURIComponent(parts[1]));
      } else if (parts[0] === "import") {
        setActiveNav("import");
        renderImport();
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
          toast("Execution finished: " + ex.status, ex.status === "error" ? "error" : "success");
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

  // ---------------- editor ----------------

  let editorState = null; // { workflow, selectedNodeName, lastExecution }

  async function renderEditor(id) {
    viewEl.innerHTML = loadingRow("Loading workflow\u2026");
    let wf;
    try {
      wf = await apiJSON("/api/workflows/" + encodeURIComponent(id));
    } catch (e) {
      viewEl.innerHTML = emptyState("Couldn't load that workflow", escapeHtml(e.message));
      return;
    }
    editorState = { workflow: wf, selectedNodeName: null, lastExecution: null };
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
      '<select id="startNodeSelect" class="btn btn-sm" style="max-width:200px;">' +
      '<option value="">Auto (first trigger)</option>' +
      nodeNames.map((n) => '<option value="' + escapeHtml(n) + '">' + escapeHtml(n) + "</option>").join("") +
      "</select>" +
      '<button id="btnExecute" class="btn">\u25B6 Execute</button>' +
      '<button id="btnExport" class="btn">Export</button>' +
      '<button id="btnSave" class="btn btn-primary">Save</button>' +
      "</div>" +
      '<div class="editor-body">' +
      '<div class="canvas-wrap" id="canvasWrap"></div>' +
      '<div class="side-panel" id="sidePanel">' + renderSidePanelEmpty() + "</div>" +
      "</div>" +
      '<div id="execPanelHost"></div>';

    drawCanvas();
    wireEditorToolbar();
    if (editorState.lastExecution) renderExecPanel(editorState.lastExecution);
  }

  function renderSidePanelEmpty() {
    return '<div class="side-panel-empty">Select a node to view or edit its settings.</div>';
  }

  function drawCanvas() {
    const wrap = document.getElementById("canvasWrap");
    const wf = editorState.workflow;
    const nodes = wf.nodes || {};
    const names = Object.keys(nodes);

    if (!names.length) {
      wrap.innerHTML = emptyState("This workflow has no nodes", "");
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

    const posOf = (n) => {
      const p = nodes[n].position || [0, 0];
      return { x: p[0] + offX, y: p[1] + offY };
    };

    // connection lines (SVG, drawn under the node boxes)
    let svgLines = "";
    const conns = wf.connections || {};
    Object.keys(conns).forEach((src) => {
      (conns[src] || []).forEach((c) => {
        if (!nodes[src] || !nodes[c.targetName]) return; // stale/dangling ref
        const a = posOf(src), b = posOf(c.targetName);
        const x1 = a.x + BOX_W, y1 = a.y + BOX_H / 2;
        const x2 = b.x, y2 = b.y + BOX_H / 2;
        const midX = (x1 + x2) / 2;
        svgLines +=
          '<path class="conn-line" d="M' + x1 + "," + y1 + " C" + midX + "," + y1 + " " + midX + "," + y2 + " " + x2 + "," + y2 + '"/>' +
          '<polygon class="conn-arrow" points="' + x2 + "," + y2 + " " + (x2 - 7) + "," + (y2 - 4) + " " + (x2 - 7) + "," + (y2 + 4) + '"/>';
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
        '<div class="node-type">' + escapeHtml(node.type || "node") + "</div>" +
        '<div class="node-name">' + escapeHtml(n) + "</div>" +
        "</div>";
    });

    wrap.innerHTML =
      '<div class="canvas-inner" style="width:' + canvasW + "px;height:" + canvasH + 'px;">' +
      '<svg width="' + canvasW + '" height="' + canvasH + '" style="position:absolute;top:0;left:0;pointer-events:none;">' +
      svgLines + "</svg>" + boxes + "</div>";

    wrap.querySelectorAll(".node-box").forEach((box) => {
      box.addEventListener("click", () => {
        editorState.selectedNodeName = box.dataset.node;
        wrap.querySelectorAll(".node-box").forEach((b) => b.classList.remove("selected"));
        box.classList.add("selected");
        document.getElementById("sidePanel").innerHTML = renderSidePanel(box.dataset.node);
        wireSidePanel(box.dataset.node);
      });
    });
  }

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
      '<button id="applyNodeBtn" class="btn btn-primary btn-sm">Apply to workflow</button>' +
      '<div style="color:var(--text-dim);font-size:11px;margin-top:8px;">Applies in-memory \u2014 click Save (top toolbar) to persist.</div>'
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
  }

  function wireEditorToolbar() {
    document.getElementById("wfName").addEventListener("input", (e) => {
      editorState.workflow.name = e.target.value;
    });
    document.getElementById("wfActive").addEventListener("change", (e) => {
      editorState.workflow.active = e.target.checked;
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

  async function executeCurrentWorkflow() {
    const btn = document.getElementById("btnExecute");
    const startNode = document.getElementById("startNodeSelect").value;
    btn.disabled = true;
    btn.textContent = "Running\u2026";
    try {
      const q = startNode ? "?startNode=" + encodeURIComponent(startNode) : "";
      const ex = await apiJSON("/api/workflows/" + encodeURIComponent(editorState.workflow.id) + "/execute" + q, { method: "POST" });
      editorState.lastExecution = ex;
      drawCanvas();
      renderExecPanel(ex);
      toast("Execution " + ex.status, ex.status === "error" ? "error" : "success");
    } catch (e) {
      toast("Execute failed: " + e.message, "error");
    } finally {
      btn.disabled = false;
      btn.textContent = "\u25B6 Execute";
    }
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
