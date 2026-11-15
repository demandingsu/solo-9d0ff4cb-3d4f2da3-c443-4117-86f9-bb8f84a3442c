"use strict";

const POLL_MS = 600;
const PREVIEW_MS = 700;

const els = {
  statusbar: document.getElementById("statusbar"),
  link: document.getElementById("m-link"),
  lastSeq: document.getElementById("m-lastseq"),
  contiguous: document.getElementById("m-contiguous"),
  total: document.getElementById("m-total"),
  breaks: document.getElementById("m-breaks"),
  ready: document.getElementById("m-ready"),
  lastSample: document.getElementById("last-sample"),
  streamAlert: document.getElementById("stream-alert"),
  recordBtn: document.getElementById("btn-record"),
  refreshBtn: document.getElementById("btn-refresh"),
  hint: document.getElementById("record-hint"),
  progress: document.getElementById("progress"),
  progressText: document.getElementById("progress-text"),
  recordAlert: document.getElementById("record-alert"),
  recsBody: document.getElementById("recs-body"),
  wave: document.getElementById("wave"),
  previewNote: document.getElementById("preview-note"),
  toast: document.getElementById("toast"),
};

const state = {
  latest: null,
  /** recordings started from this tab: id -> {downloaded:boolean} */
  mine: new Map(),
  activeId: null,
  busy: false,
  failures: 0,
};

// ---------------------------------------------------------------- helpers
function esc(value) {
  return String(value).replace(/[&<>"']/g, (c) => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
  }[c]));
}

function fmtTime(iso) {
  if (!iso) return "—";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  return d.toLocaleTimeString("zh-CN", { hour12: false });
}

let toastTimer = 0;
function toast(message, kind = "") {
  els.toast.textContent = message;
  els.toast.className = "toast show " + kind;
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => { els.toast.className = "toast " + kind; }, 4200);
}

function pill(text, dotClass) {
  return `<span class="pill"><span class="dot ${dotClass}"></span>${esc(text)}</span>`;
}

function progressClass(rec) {
  if (rec.status === "failed") return "progress fail";
  if (rec.status === "completed") return "progress done";
  return "progress";
}

function progressBar(rec) {
  const pct = Math.min(100, Math.round((rec.received / rec.total) * 100));
  return `<div class="${progressClass(rec)}" style="min-width:90px;"><i style="width:${pct}%"></i></div>`;
}

// ---------------------------------------------------------------- rendering
function renderStatus(snap) {
  state.latest = snap;
  const { link, stream } = snap;

  const linkOk = link.state === "connected";
  const linkText = linkOk ? "仪器已连接" : "仪器未连接";
  const dot = linkOk ? "ok" : "bad";
  const parts = [pill(linkText, dot)];
  parts.push(pill(stream.can_record ? "可录波" : `需积累 ${stream.pre_target} 条`, stream.can_record ? "ok" : "warn"));
  els.statusbar.innerHTML = parts.join("");

  els.link.innerHTML = linkOk
    ? `<span class="ok">● 已连接</span> <span class="muted mono" style="font-size:12px">${esc(fmtTime(link.since))}</span>`
    : `<span class="bad">● 未连接</span>`;
  els.lastSeq.textContent = stream.last_sample ? stream.last_sample.seq : "—";
  els.contiguous.textContent = `${stream.contiguous_run} / ${stream.pre_target}`;
  els.contiguous.className = "v " + (stream.can_record ? "ok" : "warn");
  els.total.textContent = stream.received_total;
  els.breaks.textContent = stream.discontinuities;
  els.breaks.className = "v" + (stream.discontinuities > 0 ? " warn" : "");
  els.ready.textContent = stream.can_record ? "是" : "否";
  els.ready.className = "v small " + (stream.can_record ? "ok" : "warn");

  if (stream.last_sample) {
    const s = stream.last_sample;
    els.lastSample.textContent =
      `#${s.seq}  t=${s.ts}  a=${Number(s.accel).toFixed(4)} g  (接收于 ${fmtTime(s.received_at)})`;
  } else {
    els.lastSample.textContent = "等待仪器数据…";
  }

  const err = stream.last_error;
  if (err) {
    els.streamAlert.hidden = false;
    els.streamAlert.textContent = `${err.error} · ${fmtTime(err.at)}`;
  } else {
    els.streamAlert.hidden = true;
  }

  els.recordBtn.disabled = state.busy || !stream.can_record;
  if (!state.activeId) {
    els.hint.textContent = stream.can_record
      ? "以服务端当前最后一条采样为界，保留含该条的前 100 条；完成后自动继续保留后 200 条。"
      : `连续采样不足 ${stream.pre_target} 条（当前 ${stream.contiguous_run} 条），暂不能录波。`;
  }

  renderRecordings(snap.recordings, stream);
}

function renderRecordings(recordings, stream) {
  if (!recordings.length) {
    els.recsBody.innerHTML =
      '<tr><td colspan="6" class="muted" style="font-family:var(--sans)">暂无录波</td></tr>';
    return;
  }
  // newest first
  const rows = [...recordings].reverse().map((rec) => {
    const mine = state.mine.has(rec.id);
    const label = mine ? `${esc(rec.id)} <span class="muted" style="font-family:var(--sans);font-size:11px">(本页)</span>` : esc(rec.id);
    const file = rec.downloadable
      ? `<a class="link-btn" href="/api/recordings/${encodeURIComponent(rec.id)}/download" download>下载 CSV</a>`
      : rec.status === "failed"
        ? '<span class="muted" style="font-family:var(--sans);font-size:12px">不提供下载</span>'
        : '<span class="muted" style="font-family:var(--sans);font-size:12px">等待完成</span>';
    const progress = `${rec.pre_count}/${rec.pre_target} + ${rec.post_count}/${rec.post_target}`;
    const note = rec.error ? `<span style="color:#ffb0b0">${esc(rec.error)}</span>` : "—";
    return `<tr>
      <td>${label}</td>
      <td>${rec.anchor_seq}</td>
      <td><span class="tag ${rec.status}">${esc(rec.status_label)}</span></td>
      <td style="font-size:12px">${progress}${progressBar(rec)}</td>
      <td class="err">${note}</td>
      <td>${file}</td>
    </tr>`;
  });
  els.recsBody.innerHTML = rows.join("");
}

function renderActive(rec) {
  state.activeId = rec.status === "recording" ? rec.id : null;
  els.progress.className = progressClass(rec);
  const pct = Math.min(100, Math.round((rec.received / rec.total) * 100));
  els.progress.querySelector("i").style.width = pct + "%";

  if (rec.status === "recording") {
    els.hint.textContent = "录波中；完成后将自动下载 CSV。关闭本页不影响服务端继续录波。";
    els.progressText.textContent =
      `录波 ${rec.id} · 前段 ${rec.pre_count}/${rec.pre_target} · 后段 ${rec.post_count}/${rec.post_target}（共 ${rec.received}/${rec.total}）`;
  } else if (rec.status === "completed") {
    els.hint.textContent = `录波完成：基准序号 ${rec.anchor_seq}，共 ${rec.total} 条。`;
    els.progressText.textContent = `录波 ${rec.id} 已完成（${rec.total} 条），可下载 CSV。`;
  } else {
    els.hint.textContent = "录波失败：窗口跨越断点，不提供下载。需重新积累连续 100 条后再录。";
    els.progressText.textContent = `录波 ${rec.id} 失败：${rec.error || "未知原因"}`;
  }
}

// ---------------------------------------------------------------- waveform
function drawWave(samples, preTarget) {
  const canvas = els.wave;
  const dpr = Math.min(window.devicePixelRatio || 1, 2);
  const cssW = canvas.clientWidth || 800;
  const cssH = canvas.clientHeight || 190;
  if (canvas.width !== Math.round(cssW * dpr) || canvas.height !== Math.round(cssH * dpr)) {
    canvas.width = Math.round(cssW * dpr);
    canvas.height = Math.round(cssH * dpr);
  }
  const ctx = canvas.getContext("2d");
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  ctx.clearRect(0, 0, cssW, cssH);

  if (!samples.length) {
    ctx.fillStyle = "rgba(139,155,181,.8)";
    ctx.font = "13px system-ui, sans-serif";
    ctx.fillText("暂无采样数据", 16, cssH / 2);
    return;
  }

  const padL = 46;
  const padR = 10;
  const padT = 12;
  const padB = 20;
  const w = cssW - padL - padR;
  const h = cssH - padT - padB;

  let min = Infinity;
  let max = -Infinity;
  for (const s of samples) { if (s.accel < min) min = s.accel; if (s.accel > max) max = s.accel; }
  if (!Number.isFinite(min) || !Number.isFinite(max)) { min = -1; max = 1; }
  const span = Math.max(max - min, 0.2);
  const pad = span * 0.15;
  min -= pad; max += pad;

  const x = (i) => padL + (samples.length <= 1 ? 0 : (i / (samples.length - 1)) * w);
  const y = (v) => padT + (1 - (v - min) / (max - min)) * h;

  // axes
  ctx.strokeStyle = "rgba(139,155,181,.35)";
  ctx.lineWidth = 1;
  ctx.beginPath();
  ctx.moveTo(padL, padT);
  ctx.lineTo(padL, padT + h);
  ctx.lineTo(padL + w, padT + h);
  ctx.stroke();

  ctx.fillStyle = "rgba(139,155,181,.85)";
  ctx.font = "11px ui-monospace, monospace";
  ctx.fillText(max.toFixed(2), 6, padT + 9);
  ctx.fillText(min.toFixed(2), 6, padT + h - 1);
  ctx.fillText("g", 6, padT + h / 2);

  // marker for the "last 100" boundary
  if (samples.length > preTarget) {
    const idx = samples.length - preTarget;
    const mx = x(idx);
    ctx.strokeStyle = "rgba(255,193,94,.35)";
    ctx.setLineDash([4, 4]);
    ctx.beginPath();
    ctx.moveTo(mx, padT);
    ctx.lineTo(mx, padT + h);
    ctx.stroke();
    ctx.setLineDash([]);
  }

  // waveform
  ctx.strokeStyle = "#6ed3ff";
  ctx.lineWidth = 1.4;
  ctx.lineJoin = "round";
  ctx.beginPath();
  samples.forEach((s, i) => {
    const px = x(i);
    const py = y(s.accel);
    if (i === 0) ctx.moveTo(px, py); else ctx.lineTo(px, py);
  });
  ctx.stroke();

  // seq labels
  const first = samples[0].seq;
  const last = samples[samples.length - 1].seq;
  ctx.fillStyle = "rgba(139,155,181,.85)";
  ctx.fillText(`#${first}`, padL, cssH - 5);
  const lastLabel = `#${last}`;
  ctx.fillText(lastLabel, padL + w - ctx.measureText(lastLabel).width, cssH - 5);
}

async function fetchPreview() {
  try {
    const res = await fetch("/api/preview?count=240", { cache: "no-store" });
    if (!res.ok) return;
    const data = await res.json();
    drawWave(data.samples, data.pre_target);
  } catch { /* transient */ }
}

// ---------------------------------------------------------------- polling
async function pollStatus() {
  try {
    const res = await fetch("/api/status", { cache: "no-store" });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const snap = await res.json();
    state.failures = 0;
    renderStatus(snap);
    reconcileMine(snap.recordings);
    if (state.activeId) {
      const rec = snap.recordings.find((r) => r.id === state.activeId);
      if (rec) renderActive(rec);
    } else {
      els.progress.querySelector("i").style.width = "0%";
      els.progress.className = "progress";
      els.progressText.textContent = "尚未发起录波";
    }
  } catch (err) {
    state.failures += 1;
    els.statusbar.innerHTML = pill("录波服务连接中断", "bad");
    if (state.failures === 3) toast("无法连接录波服务，请检查应用是否启动", "err");
  }
}

function reconcileMine(recordings) {
  for (const rec of recordings) {
    const track = state.mine.get(rec.id);
    if (!track) continue;
    if (rec.status === "recording") {
      if (state.activeId === rec.id) renderActive(rec);
      continue;
    }
    if (rec.status === "completed") {
      if (state.activeId === rec.id) renderActive(rec);
      if (!track.downloaded) {
        track.downloaded = true;
        triggerDownload(rec);
      }
    } else if (rec.status === "failed") {
      if (state.activeId === rec.id) renderActive(rec);
      if (!track.announced) {
        track.announced = true;
        els.recordAlert.hidden = false;
        els.recordAlert.textContent = `录波 ${rec.id} 失败：${rec.error || "未知原因"}（不提供下载）`;
        toast(`录波 ${rec.id} 失败：窗口跨越断点`, "err");
      }
    }
  }
}

function triggerDownload(rec) {
  const a = document.createElement("a");
  a.href = `/api/recordings/${encodeURIComponent(rec.id)}/download`;
  a.download = "";
  document.body.appendChild(a);
  a.click();
  a.remove();
  toast(`录波 ${rec.id} 已完成，正在下载 CSV`, "ok");
}

// ---------------------------------------------------------------- actions
async function startRecording() {
  if (state.busy) return;
  state.busy = true;
  els.recordBtn.disabled = true;
  els.recordAlert.hidden = true;
  els.recordBtn.textContent = "正在发起…";
  try {
    const res = await fetch("/api/recordings", { method: "POST" });
    const body = await res.json().catch(() => ({}));
    if (!res.ok) {
      const msg = body.detail || `发起录波失败（HTTP ${res.status}）`;
      els.recordAlert.hidden = false;
      els.recordAlert.textContent = msg;
      toast(msg, "err");
      return;
    }
    state.mine.set(body.id, { downloaded: false, announced: false });
    renderActive(body);
    toast(`已发起录波 ${body.id}，基准序号 ${body.anchor_seq}`, "");
    await pollStatus();
  } catch (err) {
    toast("发起录波失败：" + err.message, "err");
  } finally {
    state.busy = false;
    els.recordBtn.textContent = "发起录波";
    if (state.latest) {
      els.recordBtn.disabled = !state.latest.stream.can_record;
    } else {
      await pollStatus();
    }
  }
}

els.recordBtn.addEventListener("click", startRecording);
els.refreshBtn.addEventListener("click", () => { pollStatus(); fetchPreview(); });

setInterval(pollStatus, POLL_MS);
setInterval(fetchPreview, PREVIEW_MS);
pollStatus();
fetchPreview();
window.addEventListener("resize", fetchPreview);
