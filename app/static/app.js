/* Parquet 行组裁剪实验台前端（原生 JS，无构建步骤） */
"use strict";

const state = {
  files: [],            // 已注册文件
  samples: [],
  current: null,        // 当前文件元数据
  currentId: null,
  filter: { kind: "group", logic: "and", children: [] },
  lastPlan: null,
  lastRuns: [],         // 最近两次运行（裁剪 / 完整扫描）
};

const $ = (sel) => document.querySelector(sel);
const OPS = [
  ["eq", "="], ["ne", "≠"], ["gt", ">"], ["ge", "≥"], ["lt", "<"], ["le", "≤"],
];

function toast(msg, isError = false) {
  const el = $("#toast");
  el.textContent = msg;
  el.className = "toast" + (isError ? " error" : "");
  el.hidden = false;
  clearTimeout(toast._t);
  toast._t = setTimeout(() => { el.hidden = true; }, 4200);
}

async function api(path, options = {}) {
  const resp = await fetch(path, {
    headers: options.body ? { "Content-Type": "application/json" } : undefined,
    ...options,
  });
  const data = await resp.json().catch(() => ({}));
  if (!resp.ok) {
    const detail = data.detail;
    const msg = detail?.errors ? detail.errors.join("；") : (detail?.message || "请求失败");
    const err = new Error(msg);
    err.status = resp.status;
    throw err;
  }
  return data;
}

function fmtBytes(n) {
  if (n == null) return "-";
  if (n < 1024) return `${n} B`;
  const units = ["KB", "MB", "GB"];
  let v = n, i = -1;
  do { v /= 1024; i++; } while (v >= 1024 && i < units.length - 1);
  return `${v.toFixed(v >= 100 ? 0 : v >= 10 ? 1 : 2)} ${units[i]}`;
}

function familyLabel(f) {
  return {
    integer: "整数", float: "浮点数", decimal: "定点小数", string: "字符串",
    binary: "二进制", date: "日期", timestamp: "时间戳", boolean: "布尔",
    unsupported: "不支持",
  }[f] || f;
}

// ---------------------------------------------------------------- 主题

function initTheme() {
  const saved = localStorage.getItem("pq-theme");
  if (saved) document.documentElement.dataset.theme = saved;
  $("#btn-theme").addEventListener("click", () => {
    const cur = document.documentElement.dataset.theme === "dark" ? "light" : "dark";
    document.documentElement.dataset.theme = cur;
    localStorage.setItem("pq-theme", cur);
  });
}

// ---------------------------------------------------------------- 文件加载

async function loadSamples() {
  const data = await api("/api/samples");
  state.samples = data.samples;
  renderSamples();
}

async function loadFiles() {
  const data = await api("/api/files");
  state.files = data.files;
  renderFileList();
  renderComparePickers();
}

function renderSamples() {
  const box = $("#sample-list");
  box.innerHTML = "";
  for (const s of state.samples) {
    const card = document.createElement("div");
    card.className = "file-card";
    card.innerHTML = `
      <div><span class="src-tag">样本</span><span class="fname">${s.name}</span></div>
      <div class="fmeta">${fmtBytes(s.size)}</div>`;
    card.addEventListener("click", () => registerSample(s.name));
    box.appendChild(card);
  }
}

function renderFileList() {
  const box = $("#file-list");
  box.innerHTML = "";
  for (const f of state.files) {
    const card = document.createElement("div");
    card.className = "file-card" + (f.file_id === state.currentId ? " active" : "");
    card.innerHTML = `
      <div><span class="src-tag">${f.source === "sample" ? "样本" : "上传"}</span>
      <span class="fname">${f.name}</span></div>
      <div class="fmeta">${fmtBytes(f.size)} · 运行记录 ${f.runs}</div>`;
    card.addEventListener("click", () => selectFile(f.file_id));
    box.appendChild(card);
  }
}

async function registerSample(name) {
  try {
    const data = await api(`/api/files/register-sample/${encodeURIComponent(name)}`, { method: "POST" });
    await loadFiles();
    await selectFile(data.file_id);
    toast(`已载入样本 ${name}`);
  } catch (e) { toast(e.message, true); }
}

$("#file-input").addEventListener("change", async (ev) => {
  const file = ev.target.files[0];
  if (!file) return;
  const fd = new FormData();
  fd.append("file", file);
  try {
    const resp = await fetch("/api/files/upload", { method: "POST", body: fd });
    const data = await resp.json();
    if (!resp.ok) throw new Error(data.detail?.message || "上传失败");
    await loadFiles();
    await selectFile(data.file_id);
    toast(`已上传并解析 ${file.name}`);
  } catch (e) { toast(e.message, true); }
  ev.target.value = "";
});

$("#btn-refresh-samples").addEventListener("click", () => loadSamples().then(() => toast("样本列表已刷新")));

async function selectFile(fileId) {
  state.currentId = fileId;
  const md = await api(`/api/files/${fileId}/metadata`);
  state.current = md;
  state.filter = { kind: "group", logic: "and", children: [] };
  state.lastPlan = null;
  state.lastRuns = [];
  $("#empty-state").hidden = true;
  $("#metadata-view").hidden = false;
  $("#filter-section").hidden = false;
  $("#plan-view").hidden = true;
  $("#result-view").hidden = true;
  $("#compare-view").hidden = false;
  renderMetadata();
  renderFilterBuilder();
  renderFileList();
  renderComparePickers();
}

// ---------------------------------------------------------------- 元数据渲染

function renderMetadata() {
  const md = state.current;
  $("#md-title").textContent = "文件元数据";
  const tiles = [
    ["总行数", md.num_rows.toLocaleString()],
    ["行组数", md.num_row_groups],
    ["列数", md.num_columns],
    ["文件大小", fmtBytes(md.file_size)],
    ["Footer 大小", fmtBytes(md.serialized_footer_size)],
    ["格式", md.format_version],
  ];
  $("#md-summary").innerHTML = tiles.map(([l, v]) =>
    `<div class="stat-tile"><div class="label">${l}</div><div class="value small">${v}</div></div>`
  ).join("");

  const tbody = $("#column-table tbody");
  tbody.innerHTML = md.columns.map(c => `
    <tr><td>${c.name}</td><td>${c.arrow_type}</td>
    <td>${familyLabel(c.family)}</td><td>${c.nullable ? "是" : "否"}</td></tr>`).join("");

  const box = $("#rg-container");
  box.innerHTML = "";
  md.row_groups.forEach((rg, i) => {
    const card = document.createElement("div");
    card.className = "rg-card" + (i === 0 ? " open" : "");
    const rows = rg.columns.map(c => {
      const st = c.statistics;
      const stat = st ? `
        <span class="stat-mini">min=${st.has_min_max ? st.min : "∅"} ·
        max=${st.has_min_max ? st.max : "∅"} · NULL=${st.null_count ?? "?"}
        （非空 ${st.num_values ?? "?"}）</span>` : '<span class="stat-mini">无统计块</span>';
      return `<tr><td>${c.name}</td><td>${c.physical_type}</td>
        <td>${fmtBytes(c.compressed_size)}</td><td class="wrap">${stat}</td></tr>`;
    }).join("");
    card.innerHTML = `
      <div class="rg-head">
        <span class="badge neutral">行组 ${rg.row_group_index}</span>
        <strong>${rg.num_rows.toLocaleString()} 行</strong>
        <span class="stat-mini">编码后 ${fmtBytes(
          rg.columns.reduce((a, c) => a + c.compressed_size, 0))}</span>
        <span class="stat-mini">${rg.sorting_columns.length ? "带排序元数据" : "无排序声明"}</span>
      </div>
      <div class="rg-body">
        <div class="table-wrap"><table class="data-table">
          <thead><tr><th>列</th><th>物理类型</th><th>压缩大小</th><th>统计信息</th></tr></thead>
          <tbody>${rows}</tbody></table></div>
      </div>`;
    card.querySelector(".rg-head").addEventListener("click", () => card.classList.toggle("open"));
    box.appendChild(card);
  });
}

// ---------------------------------------------------------------- 筛选构造器

function columnFamilies() {
  return Object.fromEntries(state.current.columns.map(c => [c.name, c]));
}

$("#btn-add-leaf").addEventListener("click", () => {
  const first = state.current.columns.find(c => c.family !== "unsupported") || state.current.columns[0];
  state.filter.children.push({
    kind: "leaf", column: first.name, op: "eq", value: defaultInputValue(first),
  });
  renderFilterBuilder();
});

$("#btn-clear-filter").addEventListener("click", () => {
  state.filter = { kind: "group", logic: "and", children: [] };
  renderFilterBuilder();
});

function defaultInputValue(col) {
  if (col.family === "boolean") return "true";
  if (col.family === "integer") return "0";
  if (col.family === "float") return "0";
  if (col.family === "date") return "2024-01-01";
  return "";
}

function renderFilterBuilder() {
  const cols = state.current?.columns || [];
  $("#filter-hint").textContent =
    `共 ${cols.length} 列；类型不符的输入会在预览时直接报错，不会静默转换。AND 组中任一条件可排除整组即跳过；OR 组需全部条件都可排除才跳过。`;
  const root = $("#filter-builder");
  root.innerHTML = "";
  const group = state.filter;
  const gEl = document.createElement("div");
  gEl.className = "filter-group " + group.logic;
  gEl.innerHTML = `
    <div class="group-head">
      <label>组合逻辑
        <select data-role="logic">
          <option value="and" ${group.logic === "and" ? "selected" : ""}>AND（全部满足）</option>
          <option value="or" ${group.logic === "or" ? "selected" : ""}>OR（任一满足）</option>
        </select>
      </label>
      <span class="stat-mini">${group.children.length} 个条件</span>
    </div>`;
  gEl.querySelector('[data-role="logic"]').addEventListener("change", (e) => {
    group.logic = e.target.value;
    renderFilterBuilder();
  });

  group.children.forEach((leaf, idx) => {
    const row = document.createElement("div");
    row.className = "leaf-row";
    const colOpts = cols.map(c => {
      const dis = c.family === "unsupported" ? "（不支持筛选）" : "";
      return `<option value="${c.name}" ${leaf.column === c.name ? "selected" : ""}>${c.name} · ${familyLabel(c.family)}${dis}</option>`;
    }).join("");
    const opOpts = OPS.map(([v, l]) =>
      `<option value="${v}" ${leaf.op === v ? "selected" : ""}>${l}</option>`).join("");
    row.innerHTML = `
      <select data-role="col">${colOpts}</select>
      <select data-role="op">${opOpts}</select>
      <input data-role="val" value="${escapeAttr(String(leaf.value ?? ""))}" placeholder="输入筛选值">
      <button class="leaf-del" title="删除条件" type="button">✕</button>`;
    row.querySelector('[data-role="col"]').addEventListener("change", (e) => {
      leaf.column = e.target.value;
      const col = cols.find(c => c.name === leaf.column);
      leaf.value = defaultInputValue(col);
      renderFilterBuilder();
    });
    row.querySelector('[data-role="op"]').addEventListener("change", (e) => { leaf.op = e.target.value; });
    row.querySelector('[data-role="val"]').addEventListener("input", (e) => { leaf.value = e.target.value; });
    row.querySelector(".leaf-del").addEventListener("click", () => {
      group.children.splice(idx, 1);
      renderFilterBuilder();
    });
    gEl.appendChild(row);
  });
  root.appendChild(gEl);
  $("#filter-errors").innerHTML = "";
}

function escapeAttr(s) {
  return s.replace(/&/g, "&amp;").replace(/"/g, "&quot;").replace(/</g, "&lt;");
}

function ensureFilterPresent() {
  if (!state.filter.children.length) {
    $("#btn-add-leaf").click();
    throw new Error("请先添加至少一个筛选条件");
  }
}

// ---------------------------------------------------------------- 计划预览

$("#btn-plan").addEventListener("click", async () => {
  try {
    ensureFilterPresent();
    const data = await api(`/api/files/${state.currentId}/plan`, {
      method: "POST", body: JSON.stringify({ filter: state.filter }),
    });
    state.lastPlan = data;
    renderPlan(data);
  } catch (e) { renderFilterError(e); }
});

function renderFilterError(e) {
  const box = $("#filter-errors");
  box.innerHTML = `<div class="err-item">类型错误：${e.message}</div>`;
  toast(e.message, true);
}

function renderPlan(plan) {
  $("#plan-view").hidden = false;
  const tiles = [
    ["总行组数", plan.num_row_groups],
    ["保留（候选）行组", plan.kept_row_groups.length],
    ["安全排除行组", plan.excluded_row_groups.length],
    ["候选行覆盖行数", plan.kept_rows.toLocaleString()],
    ["计划读取量", fmtBytes(plan.plan_bytes)],
  ];
  $("#plan-summary").innerHTML = tiles.map(([l, v]) =>
    `<div class="stat-tile"><div class="label">${l}</div><div class="value">${v}</div></div>`).join("");

  const box = $("#plan-decisions");
  box.innerHTML = "";
  plan.decisions.forEach((d, i) => {
    const card = document.createElement("div");
    card.className = "decision-card " + (d.excluded ? "excluded" : "kept") + (i === 0 ? " open" : "");
    card.innerHTML = `
      <div class="decision-head">
        <span class="badge ${d.excluded ? "excluded" : "kept"}">${d.decision}</span>
        <strong>行组 ${d.row_group_index}</strong>
        <span class="stat-mini">${d.num_rows.toLocaleString()} 行 · 若保留计划读取 ${fmtBytes(d.plan_bytes_if_kept)}</span>
      </div>
      <div class="decision-body">${renderTrace(d.trace)}</div>`;
    card.querySelector(".decision-head").addEventListener("click", () => card.classList.toggle("open"));
    box.appendChild(card);
  });
  $("#plan-view").scrollIntoView({ behavior: "smooth", block: "start" });
}

function renderTrace(node) {
  if (node.kind === "leaf") {
    const cls = node.excluded ? "leaf-excluded" : "leaf-kept";
    const verdict = node.excluded ? "excluded" : "kept";
    const val = node.value == null ? "∅" : String(node.value);
    return `<div class="trace-node ${cls}">
      <span class="trace-verdict ${verdict}">[${node.decision}]</span>
      <code class="pill">${node.column} ${node.op_label} ${escapeAttr(val)}</code>
      <span class="stat-mini">（${node.family_label}）</span>
      <div class="trace-reason">${node.reason}</div>
    </div>`;
  }
  const children = node.children.map(renderTrace).join("");
  return `<div class="trace-node">
    <span class="trace-verdict ${node.excluded ? "excluded" : "kept"}">[${node.decision}]</span>
    <code class="pill">${node.logic.toUpperCase()}</code>
    <div class="trace-reason">${node.reason}</div>
    ${children}
  </div>`;
}

// ---------------------------------------------------------------- 执行查询

$("#btn-run-prune").addEventListener("click", () => runQuery(true));
$("#btn-run-full").addEventListener("click", () => runQuery(false));

async function runQuery(pruning) {
  try {
    ensureFilterPresent();
    const run = await api(`/api/files/${state.currentId}/query`, {
      method: "POST",
      body: JSON.stringify({ filter: state.filter, pruning, preview_limit: 100 }),
    });
    state.lastRuns = state.lastRuns.filter(r => r.mode !== run.mode);
    state.lastRuns.push(run);
    renderResult(run);
    await loadFiles();
  } catch (e) { renderFilterError(e); }
}

function renderResult(run) {
  $("#result-view").hidden = false;
  const modeLabel = run.mode === "pruning" ? "裁剪查询" : "完整扫描";
  const tiles = [
    ["执行模式", modeLabel],
    ["读取行组", `${run.read_row_groups.length} 个`],
    ["排除行组", `${run.excluded_row_groups.length} 个`],
    ["扫描行数", run.scanned_rows.toLocaleString()],
    ["结果行数", run.result_rows.toLocaleString()],
    ["实测数据读取", fmtBytes(run.measured_data_bytes)],
    ["实测总读取（含 footer）", fmtBytes(run.measured_total_bytes)],
    ["计划读取量", fmtBytes(run.planned_data_bytes)],
  ];
  $("#result-summary").innerHTML = tiles.map(([l, v]) =>
    `<div class="stat-tile"><div class="label">${l}</div><div class="value small">${v}</div></div>`).join("");

  $("#meter-note").innerHTML = `
    计量口径：<strong>实测</strong>来自对底层文件描述符 read() 字节的真实计数（footer ${fmtBytes(
      run.measured_footer_bytes)} 单列；数据列块 ${fmtBytes(run.measured_data_bytes)}），
    <strong>计划</strong>来自候选行组列块 total_compressed_size 之和。
    文件总大小 <strong>不会</strong>被当作实际读取量。结果摘要 <code class="pill">${run.result_digest.slice(0, 16)}…</code>
    按全部列排序后计算，与行顺序无关，可用于跨运行/跨文件比对。`;

  const table = $("#result-table");
  const rows = run.preview;
  if (!rows.length) {
    table.innerHTML = "<thead></thead><tbody><tr><td>结果为空（0 行）</td></tr></tbody>";
  } else {
    const cols = Object.keys(rows[0]);
    table.innerHTML = `
      <thead><tr>${cols.map(c => `<th>${c}</th>`).join("")}</tr></thead>
      <tbody>${rows.map(r => `<tr>${cols.map(c => `<td class="wrap">${r[c] == null ? "<span class='stat-mini'>NULL</span>" : escapeAttr(String(r[c]))}</td>`).join("")}</tr>`).join("")}</tbody>`;
  }
  $("#result-view").scrollIntoView({ behavior: "smooth", block: "start" });
}

// ---------------------------------------------------------------- 变体对比

function renderComparePickers() {
  const box = $("#compare-pickers");
  box.innerHTML = "";
  for (const f of state.files) {
    const lab = document.createElement("label");
    lab.innerHTML = `<input type="checkbox" value="${f.file_id}" checked> ${f.name}`;
    box.appendChild(lab);
  }
}

$("#btn-run-compare").addEventListener("click", async () => {
  const ids = [...$("#compare-pickers input:checked")].map(i => i.value);
  if (ids.length < 2) return toast("请至少勾选两个文件进行对比", true);
  const hasFilter = state.filter.children.length > 0;
  try {
    const data = await api("/api/compare", {
      method: "POST",
      body: JSON.stringify({
        file_ids: ids,
        filter: hasFilter ? state.filter : null,
        pruning: true,
      }),
    });
    renderCompare(data);
  } catch (e) { toast(e.message, true); }
});

function renderCompare(data) {
  const banner = $("#compare-banner");
  if (data.consistent_across_variants && data.pruning_matches_full_scan) {
    banner.innerHTML = `<div class="banner ok">✓ 所有变体结果一致（行数 ${data.shared_result_rows.toLocaleString()}，摘要 ${data.shared_digest.slice(0, 16)}…），且每个文件的裁剪结果与完整扫描完全相同。</div>`;
  } else {
    banner.innerHTML = `<div class="banner bad">✗ 检测到结果不一致，请检查摘要列。</div>`;
  }

  // 条形图：实测数据读取（裁剪 vs 完整扫描），统一量纲
  const all = [...data.pruning_runs, ...data.full_scan_runs];
  const maxBytes = Math.max(...all.map(r => r.measured_data_bytes), 1);
  const chart = $("#compare-chart");
  chart.innerHTML = `
    <div class="legend">
      <span><i style="background:var(--series-1)"></i>计划读取（列块压缩尺寸之和）</span>
      <span><i style="background:var(--series-2)"></i>实测数据读取（read() 计数）</span>
    </div>`;
  for (const group of [
    ["裁剪模式", data.pruning_runs, true],
    ["完整扫描对照", data.full_scan_runs, false],
  ]) {
    const [title, runs, isPrune] = group;
    const g = document.createElement("div");
    g.className = "chart-group";
    g.innerHTML = `<h4>${title}</h4>`;
    for (const r of runs) {
      const pctPlan = Math.max(1.5, 100 * r.planned_data_bytes / maxBytes);
      const pctMeasured = Math.max(1.5, 100 * r.measured_data_bytes / maxBytes);
      g.innerHTML += `
        <div class="bar-row">
          <div class="bar-label">${r.name}<br><span class="stat-mini">排除 ${r.num_excluded_groups}/${r.num_excluded_groups + r.num_candidate_groups} 行组</span></div>
          <div class="bar-track">
            <div class="bar-fill" style="width:${pctPlan}%" title="计划 ${fmtBytes(r.planned_data_bytes)}"></div>
          </div>
          <div class="bar-value">${fmtBytes(r.planned_data_bytes)}</div>
        </div>
        <div class="bar-row">
          <div class="bar-label stat-mini">实测（${isPrune ? "裁剪" : "全扫"}）</div>
          <div class="bar-track">
            <div class="bar-fill measured" style="width:${pctMeasured}%" title="实测 ${fmtBytes(r.measured_data_bytes)}"></div>
          </div>
          <div class="bar-value">${fmtBytes(r.measured_data_bytes)}</div>
        </div>`;
    }
    chart.appendChild(g);
  }

  const tbody = $("#compare-table tbody");
  tbody.innerHTML = all.map(r => `
    <tr>
      <td>${r.name}</td>
      <td>${r.mode === "pruning" ? "裁剪" : "完整扫描"}</td>
      <td>${r.num_candidate_groups}</td>
      <td>${r.num_excluded_groups}</td>
      <td>${r.read_row_groups.length}</td>
      <td>${r.planned_data_bytes.toLocaleString()}</td>
      <td>${r.measured_data_bytes.toLocaleString()}</td>
      <td>${r.measured_total_bytes.toLocaleString()}</td>
      <td>${r.result_rows.toLocaleString()}</td>
      <td><code class="pill">${r.result_digest.slice(0, 12)}…</code></td>
    </tr>`).join("");
}

// ---------------------------------------------------------------- 启动

initTheme();
loadSamples().catch(e => toast(e.message, true));
loadFiles().catch(e => toast(e.message, true));
