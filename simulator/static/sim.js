"use strict";

const els = {
  statusbar: document.getElementById("statusbar"),
  conn: document.getElementById("s-conn"),
  sent: document.getElementById("s-sent"),
  dropped: document.getElementById("s-dropped"),
  next: document.getElementById("s-next"),
  skips: document.getElementById("s-skips"),
  disconnects: document.getElementById("s-disconnects"),
  detail: document.getElementById("s-detail"),
  alert: document.getElementById("s-alert"),
  sendBtn: document.getElementById("btn-send"),
  batchLabel: document.getElementById("batch-label"),
  batchInput: document.getElementById("batch-input"),
  gapInput: document.getElementById("gap-input"),
  skipBtn: document.getElementById("btn-skip"),
  disconnectBtn: document.getElementById("btn-disconnect"),
  autoBtn: document.getElementById("btn-auto"),
  autoRate: document.getElementById("auto-rate"),
  autoBatch: document.getElementById("auto-batch"),
  autoState: document.getElementById("auto-state"),
  constValue: document.getElementById("const-value"),
  constCount: document.getElementById("const-count"),
  constBtn: document.getElementById("btn-constant"),
  constState: document.getElementById("const-state"),
  toast: document.getElementById("toast"),
};

let autoEnabled = false;
let busy = false;

function esc(v) {
  return String(v).replace(/[&<>"']/g, (c) => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
  }[c]));
}

let toastTimer = 0;
function toast(msg, kind = "") {
  els.toast.textContent = msg;
  els.toast.className = "toast show " + kind;
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => { els.toast.className = "toast " + kind; }, 3600);
}

function pill(text, dotClass) {
  return `<span class="pill"><span class="dot ${dotClass}"></span>${esc(text)}</span>`;
}

function fmtTime(iso) {
  if (!iso) return "—";
  const d = new Date(iso);
  return Number.isNaN(d.getTime()) ? iso : d.toLocaleTimeString("zh-CN", { hour12: false });
}

function render(s) {
  autoEnabled = s.auto;
  els.statusbar.innerHTML = [
    pill(s.connected ? "录波服务已连接" : "无录波服务连接", s.connected ? "ok" : "bad"),
    pill(s.auto ? `连续发送 ${s.auto_rate}/秒` : "手动发样", s.auto ? "warn" : ""),
  ].join("");

  els.conn.innerHTML = s.connected
    ? `<span class="ok">● 已连接</span>`
    : `<span class="bad">● 未连接</span>`;
  els.sent.textContent = s.sent_total;
  els.dropped.textContent = s.dropped_total;
  els.dropped.className = "v" + (s.dropped_total > 0 ? " warn" : "");
  els.next.textContent = s.next_seq;
  els.skips.textContent = s.skips;
  els.disconnects.textContent = s.disconnects;
  els.alert.hidden = s.connected;

  const bits = [];
  if (s.last_seq != null) bits.push(`最后发送序号 #${s.last_seq}`);
  if (s.connections_served) bits.push(`累计接入 ${s.connections_served} 次`);
  if (s.last_batch_at) bits.push(`最近操作 ${fmtTime(s.last_batch_at)}`);
  els.detail.textContent = bits.length ? bits.join(" · ") : "等待操作…";

  els.autoBtn.textContent = s.auto ? "停止连续发送" : "开始连续发送";
  els.autoBtn.style.borderColor = s.auto ? "rgba(255,193,94,.55)" : "";
  els.autoState.textContent = s.auto
    ? `连续发送中：约 ${s.auto_rate} 批/秒`
    : "连续发送未开启";
  els.constState.textContent = `累计恒值发送：${s.constant_total} 条`;
}

async function refresh() {
  try {
    const res = await fetch("/api/state", { cache: "no-store" });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    render(await res.json());
  } catch (err) {
    els.statusbar.innerHTML = pill("模拟器服务异常", "bad");
  }
}

async function post(path, body) {
  const res = await fetch(path, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body || {}),
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data.detail || `HTTP ${res.status}`);
  return data;
}

function withBusy(button, fn) {
  return async () => {
    if (busy) return;
    busy = true;
    button.disabled = true;
    try { await fn(); } catch (err) { toast(err.message || String(err), "err"); }
    finally { busy = false; button.disabled = false; await refresh(); }
  };
}

async function send(count) {
  const out = await post("/api/send", { count });
  if (out.connected) {
    toast(`已发送 ${out.sent} 条（#${out.first_seq}–#${out.last_seq}）`, "ok");
  } else {
    toast(`无录波服务连接，${out.dropped} 条被丢弃（序号推进至 #${out.last_seq}）`, "err");
  }
}

els.batchInput.addEventListener("input", () => {
  const v = Math.max(1, Math.min(5000, Number(els.batchInput.value) || 1));
  els.batchLabel.textContent = v;
});

els.sendBtn.addEventListener("click", withBusy(els.sendBtn, () => {
  const count = Math.max(1, Math.min(5000, Number(els.batchInput.value) || 1));
  return send(count);
}));

document.getElementById("btn-send-10").addEventListener("click", withBusy(
  document.getElementById("btn-send-10"), () => send(10)));
document.getElementById("btn-send-100").addEventListener("click", withBusy(
  document.getElementById("btn-send-100"), () => send(100)));
document.getElementById("btn-send-220").addEventListener("click", withBusy(
  document.getElementById("btn-send-220"), () => send(220)));

els.skipBtn.addEventListener("click", withBusy(els.skipBtn, async () => {
  const gap = Math.max(1, Math.min(100000, Number(els.gapInput.value) || 1));
  const out = await post("/api/skip", { gap });
  if (out.connected) {
    toast(`已发送跳号采样 #${out.seq}（跳过 ${out.gap} 个序号）`, "warn");
  } else {
    toast(`无连接，跳号采样 #${out.seq} 被丢弃`, "err");
  }
}));

els.disconnectBtn.addEventListener("click", withBusy(els.disconnectBtn, async () => {
  const out = await post("/api/disconnect", {});
  toast(out.was_connected ? "已断开 TCP 连接，等待录波服务重连" : "当前没有可断开的连接", "");
}));

async function sendConstant(count) {
  const value = Number(els.constValue.value);
  if (!Number.isFinite(value)) {
    toast("恒值必须是有限数字", "err");
    return;
  }
  const out = await post("/api/constant", { count, value });
  if (out.connected) {
    toast(`已发送 ${out.sent} 条恒值 ${value} g（#${out.first_seq}–#${out.last_seq}）`, "ok");
  } else {
    toast(`无录波服务连接，${out.dropped} 条恒值被丢弃（序号推进至 #${out.last_seq}）`, "err");
  }
}

els.constBtn.addEventListener("click", withBusy(els.constBtn, () => {
  const count = Math.max(1, Math.min(5000, Number(els.constCount.value) || 1));
  return sendConstant(count);
}));

document.getElementById("btn-constant-64").addEventListener("click", withBusy(
  document.getElementById("btn-constant-64"), () => sendConstant(64)));
document.getElementById("btn-constant-63").addEventListener("click", withBusy(
  document.getElementById("btn-constant-63"), () => sendConstant(63)));

els.autoBtn.addEventListener("click", withBusy(els.autoBtn, async () => {
  if (autoEnabled) {
    await post("/api/auto", { enabled: false });
    toast("已停止连续发送", "");
    return;
  }
  const rate = Math.max(0.1, Math.min(50, Number(els.autoRate.value) || 5));
  const batch = Math.max(1, Math.min(1000, Number(els.autoBatch.value) || 20));
  await post("/api/auto", { enabled: true, rate, batch });
  toast(`开始连续发送：${rate} 批/秒 × ${batch} 条`, "ok");
}));

setInterval(refresh, 700);
refresh();
