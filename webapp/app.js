const tg = window.Telegram?.WebApp;
if (tg) {
  tg.ready();
  tg.expand();
}

const RUNNER_STATE_LABELS = {
  active: "active",
  inactive: "stopped",
  failed: "failed",
  activating: "starting",
  deactivating: "stopping",
  reloading: "reloading",
  unknown: "unknown",
};

const FILL_STATUS_LABELS = {
  FILLED: "filled",
  PARTIALLY_FILLED: "partially filled",
  NEW: "new",
  CANCELED: "canceled",
  CANCELLED: "canceled",
  REJECTED: "rejected",
  EXPIRED: "expired",
};

const els = {
  runnerState: document.getElementById("runnerState"),
  serviceName: document.getElementById("serviceName"),
  symbol: document.getElementById("symbol"),
  tickCount: document.getElementById("tickCount"),
  exchange: document.getElementById("exchange"),
  ticksPerSec: document.getElementById("ticksPerSec"),
  savedAt: document.getElementById("savedAt"),
  lastUpdate: document.getElementById("lastUpdate"),
  quoteBalance: document.getElementById("quoteBalance"),
  quoteAvailable: document.getElementById("quoteAvailable"),
  baseBalance: document.getElementById("baseBalance"),
  baseAvailable: document.getElementById("baseAvailable"),
  baseAsset: document.getElementById("baseAsset"),
  totalBalance: document.getElementById("totalBalance"),
  pnlDay: document.getElementById("pnlDay"),
  pnlDayPct: document.getElementById("pnlDayPct"),
  openOrders: document.getElementById("openOrders"),
  refreshBtn: document.getElementById("refreshBtn"),
  actionOutput: document.getElementById("actionOutput"),
  tradesSource: document.getElementById("tradesSource"),
  tradesTableBody: document.getElementById("tradesTableBody"),
  ordersList: document.getElementById("ordersList"),
  actionButtons: Array.from(document.querySelectorAll("[data-action]")),
  tabButtons: Array.from(document.querySelectorAll("[data-tab]")),
  tabPanels: Array.from(document.querySelectorAll("[data-panel]")),
};

let latestStatus = null;
let latestTrades = null;
let activeTab = "activity";

function esc(input) {
  return String(input ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function isMissing(v) {
  if (v === null || v === undefined) {
    return true;
  }
  const s = String(v).trim();
  return s === "" || s === "?" || s === "None" || s === "null" || s === "NaN";
}

function fmt(v) {
  if (isMissing(v)) {
    return "-";
  }
  return String(v);
}

function fmtCount(v) {
  if (isMissing(v)) {
    return "-";
  }
  const num = Number(v);
  if (!Number.isFinite(num)) {
    return fmt(v);
  }
  return new Intl.NumberFormat("en-US").format(num);
}

function fmtNumber(v, max = 8) {
  if (isMissing(v)) {
    return "-";
  }
  const num = Number(v);
  if (!Number.isFinite(num)) {
    return fmt(v);
  }
  return new Intl.NumberFormat("en-US", {
    maximumFractionDigits: max,
    minimumFractionDigits: 0,
  }).format(num);
}

function fmtMoney(v) {
  if (isMissing(v)) {
    return "-";
  }
  const num = Number(v);
  if (!Number.isFinite(num)) {
    return fmt(v);
  }
  return new Intl.NumberFormat("en-US", {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  }).format(num);
}

function fmtDateTime(v) {
  if (isMissing(v)) {
    return "-";
  }
  const d = new Date(String(v));
  if (Number.isNaN(d.getTime())) {
    return fmt(v);
  }
  return new Intl.DateTimeFormat("en-US", {
    day: "2-digit",
    month: "2-digit",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  }).format(d);
}

function runnerStateRu(stateRaw) {
  const key = String(stateRaw || "unknown").trim().toLowerCase();
  return RUNNER_STATE_LABELS[key] || key || "unknown";
}

function fillStatusRu(statusRaw) {
  const key = String(statusRaw || "").trim().toUpperCase();
  return FILL_STATUS_LABELS[key] || key || "-";
}

function setRunnerPill(stateRaw) {
  const key = String(stateRaw || "unknown").trim().toLowerCase();
  els.runnerState.textContent = runnerStateRu(key);
  els.runnerState.className = "pill";
  if (key === "active") {
    els.runnerState.classList.add("pill-ok");
    return;
  }
  if (key === "failed" || key === "inactive") {
    els.runnerState.classList.add("pill-danger");
    return;
  }
  if (key === "activating" || key === "deactivating" || key === "reloading") {
    els.runnerState.classList.add("pill-warn");
    return;
  }
  els.runnerState.classList.add("pill-neutral");
}

function setBusy(flag) {
  for (const btn of els.actionButtons) {
    btn.disabled = flag;
  }
  if (els.refreshBtn) {
    els.refreshBtn.disabled = flag;
  }
}

async function api(path, options = {}) {
  const headers = new Headers(options.headers || {});
  if (tg?.initData) {
    headers.set("X-Telegram-Init-Data", tg.initData);
  }
  const res = await fetch(path, {
    ...options,
    headers,
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok || data?.ok === false) {
    const msg = data?.error || `HTTP ${res.status}`;
    throw new Error(msg);
  }
  return data;
}

function detectBaseAsset(symbol) {
  const s = String(symbol || "").trim().toUpperCase();
  if (!s) {
    return "BASE";
  }
  const known = ["USDT", "USDC", "FDUSD", "BTC", "ETH", "BNB", "BUSD"];
  for (const q of known) {
    if (s.endsWith(q) && s.length > q.length) {
      return s.slice(0, -q.length);
    }
  }
  return "BASE";
}

function setPnlStyles(valueRaw) {
  const v = Number(valueRaw);
  els.pnlDay.classList.remove("pnl-pos", "pnl-neg");
  els.pnlDayPct.classList.remove("pnl-pos", "pnl-neg");
  if (!Number.isFinite(v)) {
    return;
  }
  if (v > 0) {
    els.pnlDay.classList.add("pnl-pos");
    els.pnlDayPct.classList.add("pnl-pos");
  } else if (v < 0) {
    els.pnlDay.classList.add("pnl-neg");
    els.pnlDayPct.classList.add("pnl-neg");
  }
}

function renderStatus(payload) {
  latestStatus = payload;
  const service = payload?.service || {};
  const st = payload?.state || {};
  const symbol = fmt(st.symbol);
  const baseAsset = detectBaseAsset(symbol);

  setRunnerPill(service.state);
  els.serviceName.textContent = fmt(service.name);
  els.symbol.textContent = symbol;
  els.exchange.textContent = fmt(st.exchange);
  els.tickCount.textContent = fmtCount(st.tick_count);
  els.ticksPerSec.textContent = isMissing(st.ticks_ps) ? "-" : `${fmtNumber(st.ticks_ps, 2)} /s`;
  els.savedAt.textContent = fmtDateTime(st.saved_at);
  els.lastUpdate.textContent = fmtDateTime(st.last_update_ts);

  els.quoteBalance.textContent = fmtMoney(st.quote_balance);
  els.quoteAvailable.textContent = fmtMoney(st.quote_balance_free);
  els.baseBalance.textContent = fmtNumber(st.base_balance);
  els.baseAvailable.textContent = fmtNumber(st.base_balance_free);
  els.baseAsset.textContent = baseAsset;

  els.totalBalance.textContent = fmtMoney(st.total_balance_usdt);
  const dayValue = Number(st.pnl_day_usdt);
  const dayPct = Number(st.pnl_day_pct);
  if (Number.isFinite(dayValue)) {
    const sign = dayValue > 0 ? "+" : "";
    els.pnlDay.textContent = `${sign}${fmtMoney(dayValue)} USDT`;
  } else {
    els.pnlDay.textContent = "-";
  }
  if (Number.isFinite(dayPct)) {
    const signPct = dayPct > 0 ? "+" : "";
    els.pnlDayPct.textContent = `${signPct}${fmtNumber(dayPct, 2)}%`;
  } else {
    els.pnlDayPct.textContent = "-";
  }
  setPnlStyles(st.pnl_day_usdt);

  els.openOrders.textContent = fmtCount(st.open_orders_count);

  renderOrdersTab();
}

function renderTrades(payload) {
  latestTrades = payload;
  const source = payload?.source ? String(payload.source) : "-";
  els.tradesSource.textContent = `source: ${source}`;
  renderActivityTab();
}

function renderActivityTab() {
  const trades = Array.isArray(latestTrades?.trades) ? latestTrades.trades : [];
  if (!trades.length) {
    els.tradesTableBody.innerHTML = '<tr><td colspan="6">No executed trades yet.</td></tr>';
    return;
  }

  const rows = trades
    .map((t) => {
      const side = String(t.side || "?").toUpperCase();
      const sideLabel = side === "BUY" ? "BUY" : side === "SELL" ? "SELL" : esc(side);
      const sideCls = side === "BUY" ? "side-buy" : side === "SELL" ? "side-sell" : "";
      const status = fillStatusRu(t.status);
      const qty = fmtNumber(t.qty);
      const price = fmtNumber(t.price);
      const fee = fmtNumber(t.fee_quote);
      const ts = fmtDateTime(t.ts);

      return `
        <tr>
          <td data-label="Side"><span class="td-pill ${sideCls}">${sideLabel}</span></td>
          <td data-label="Qty">${esc(qty)}</td>
          <td data-label="Price">${esc(price)}</td>
          <td data-label="Fee">${esc(fee)}</td>
          <td data-label="Status">${esc(status)}</td>
          <td data-label="Time">${esc(ts)}</td>
        </tr>
      `;
    })
    .join("");
  els.tradesTableBody.innerHTML = rows;
}

function renderOrdersTab() {
  const rows = Array.isArray(latestStatus?.state?.open_orders_rows)
    ? latestStatus.state.open_orders_rows
    : [];
  if (!rows.length) {
    els.ordersList.innerHTML = '<div class="item">No open orders.</div>';
    return;
  }
  const html = rows
    .map((o) => {
      const side = String(o.side || "?").toUpperCase();
      const sideCls = side === "BUY" ? "side-buy" : side === "SELL" ? "side-sell" : "";
      return `
        <article class="item">
          <div class="item-head">
            <strong>Order #${esc(fmt(o.order_id))}</strong>
            <span class="order-side ${sideCls}">${esc(side)}</span>
          </div>
          <div class="item-grid">
            <div>Price<strong>${esc(fmtNumber(o.price))}</strong></div>
            <div>Qty<strong>${esc(fmtNumber(o.qty))}</strong></div>
          </div>
        </article>
      `;
    })
    .join("");
  els.ordersList.innerHTML = html;
}

function setActiveTab(tabName) {
  const nextTab = els.tabButtons.some((btn) => btn.dataset.tab === tabName)
    ? tabName
    : "activity";
  activeTab = nextTab;
  for (const btn of els.tabButtons) {
    btn.classList.toggle("active", btn.dataset.tab === nextTab);
  }
  for (const panel of els.tabPanels) {
    panel.classList.toggle("active", panel.dataset.panel === nextTab);
  }
}

async function refreshAll() {
  const [status, trades] = await Promise.all([
    api("/api/status"),
    api("/api/trades?limit=20"),
  ]);
  renderStatus(status);
  renderTrades(trades);
  setActiveTab(activeTab);
}

async function runAction(action) {
  setBusy(true);
  try {
    const data = await api(`/api/runner/${action}`, { method: "POST" });
    const lines = [
      `action: ${fmt(data.action)}`,
      `service: ${fmt(data.service)}`,
      `exit code: ${fmt(data.exit_code)}`,
      `state now: ${runnerStateRu(data.state_now)}`,
      "",
      fmt(data.output) || "(empty output)",
    ];
    els.actionOutput.textContent = lines.join("\n");
    await refreshAll();
  } catch (err) {
    els.actionOutput.textContent = `Command error: ${err?.message || String(err)}`;
  } finally {
    setBusy(false);
  }
}

for (const btn of els.actionButtons) {
  btn.addEventListener("click", () => {
    const action = btn.getAttribute("data-action");
    if (!action) {
      return;
    }
    runAction(action);
  });
}

for (const btn of els.tabButtons) {
  btn.addEventListener("click", () => {
    const tabName = btn.dataset.tab;
    if (!tabName) {
      return;
    }
    setActiveTab(tabName);
  });
}

if (els.refreshBtn) {
  els.refreshBtn.addEventListener("click", async () => {
    setBusy(true);
    try {
      await refreshAll();
    } catch (err) {
      els.actionOutput.textContent = `Refresh error: ${err?.message || String(err)}`;
    } finally {
      setBusy(false);
    }
  });
}

async function boot() {
  setBusy(true);
  try {
    setActiveTab("activity");
    await refreshAll();
    els.actionOutput.textContent = "Ready.";
  } catch (err) {
    els.actionOutput.textContent = `Load error: ${err?.message || String(err)}`;
  } finally {
    setBusy(false);
  }
}

boot();
setInterval(() => {
  refreshAll().catch(() => {});
}, 8000);
