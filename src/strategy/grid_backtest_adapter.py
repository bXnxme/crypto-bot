

from __future__ import annotations

from decimal import Decimal, ROUND_DOWN
from datetime import datetime, timedelta, timezone
import os
import re
from typing import Any, Optional, Callable

from .grid_core import GridCore
from .grid_types import GridConfig, GridLot, GridState
from loguru import logger



# NOTE:
# This adapter is intentionally self-contained and duck-typed.
# It avoids importing concrete broker/order enums to keep integration simple.
# Expected broker methods (duck typing):
#   - place_limit(side=<str|enum>, qty=Decimal, limit_price=Decimal)
# Optional broker attrs/methods used only for diagnostics:
#   - quote_balance / cash_quote / quote
#   - base_balance / base
#   - open_orders (iterable)
#
# Expected quote input to on_quote(): object or dict with bid/ask and optional ts.
# Expected fill input to on_fill(): object or dict with side/qty/price and optional fee_quote/ts.


def _d(x: Any) -> Decimal:
    return Decimal(str(x))


def _round_step(value: Decimal, step: Decimal) -> Decimal:
    if step == 0:
        return value
    steps = (value / step).to_integral_value(rounding=ROUND_DOWN)
    return steps * step

def _round_tick(value: Decimal, tick: Decimal) -> Decimal:
    if tick == 0:
        return value
    ticks = (value / tick).to_integral_value(rounding=ROUND_DOWN)
    return ticks * tick

def _get(obj: Any, *names: str, default: Any = None) -> Any:
    if obj is None:
        return default
    if isinstance(obj, dict):
        for n in names:
            if n in obj and obj[n] is not None:
                return obj[n]
        return default
    for n in names:
        if hasattr(obj, n):
            v = getattr(obj, n)
            if v is not None:
                return v
    return default


_INTERVAL_RE = re.compile(r"^(\d+)([smhdwM])$")


def _normalize_interval(interval: str) -> str:
    raw = str(interval or "").strip()
    m = _INTERVAL_RE.match(raw)
    if m is None:
        raise ValueError(
            f"Invalid interval {interval!r}. Expected Binance format like 1m/5m/15m/1h/4h/1d/1w."
        )
    qty = int(m.group(1))
    unit = m.group(2)
    if qty <= 0:
        raise ValueError(f"Interval quantity must be > 0, got {interval!r}")
    return f"{qty}{unit}"




class GridBacktestAdapter:
    """Paper/live adapter that mirrors the core infinity-grid mechanics from backtest.

    Design goals:
    - keep strategy state in adapter (anchor, buy levels, open lots)
    - react on every quote (possible marketable orders via paper broker)
    - optionally react on bar close for reanchor logic parity
    - be resilient to different broker/fill/quote object shapes (duck typing)

    This adapter does not depend on backtest CSV code and is safe to run in live/paper loops.
    """

    def __init__(
        self,
        *,
        symbol: str,
        interval: str = "15m",
        grid_step_pct: Decimal = Decimal("0.005"),
        grid_n_buy: int = 5,
        spend_quote: Decimal = Decimal("2"),
        spend_pct_of_quote: Optional[Decimal] = Decimal("0.1"),
        maker_fee_rate: Decimal = Decimal("0.001"),
        taker_fee_rate: Decimal = Decimal("0.001"),
        step_size: Decimal = Decimal("0.000001"),
        min_qty: Decimal = Decimal("0.000001"),
        min_notional: Decimal = Decimal("5"),
        grid_reanchor_up: bool = True,
        grid_reanchor_down: bool = True,
        grid_reanchor_trigger_steps: int = 2,
        grid_sell_only_above_cost: bool = True,
        grid_min_sell_markup_pct: Decimal = Decimal("0"),
        slippage_pct: Decimal = Decimal("0"),
        fill_epsilon_pct: Decimal = Decimal("0"),
        qty_precision_step: Optional[Decimal] = None,
        execution: Any = None,
    ) -> None:
        self.symbol = symbol
        self.interval = _normalize_interval(interval)
        self.grid_step_pct = _d(grid_step_pct)
        self.grid_n_buy = int(grid_n_buy)

        # Cap only BUY orders. SELL orders are 1:1 with open lots.
        self.max_active_buy_orders = int(grid_n_buy)
        # Cap TOTAL active orders (BUY + SELL). Default: keep exactly `grid_n_buy` total.
        self.max_total_orders = int(grid_n_buy)
        self.spend_quote = _d(spend_quote)
        self.spend_pct_of_quote = (
            _d(spend_pct_of_quote) if spend_pct_of_quote is not None else None
        )
        if self.spend_pct_of_quote is not None:
            if self.spend_pct_of_quote <= 0:
                raise ValueError("spend_pct_of_quote must be > 0 when provided")
            if self.spend_pct_of_quote > 1:
                raise ValueError("spend_pct_of_quote must be a fraction (e.g. 0.1 for 10%)")
        self.maker_fee_rate = _d(maker_fee_rate)

        # Warn if grid_step_pct is too small vs fee and below-cost SELLs may be allowed
        try:
            if self.grid_sell_only_above_cost and self.grid_step_pct <= (self.maker_fee_rate * Decimal("2")):
                logger.warning(
                    "Config warning: grid_step_pct={} is too small vs maker_fee_rate={} (step <= 2*fee). "
                    "SELL orders would be below-cost after fees; adapter will allow below-cost SELLs to avoid deadlock.",
                    self.grid_step_pct,
                    self.maker_fee_rate,
                )
        except Exception:
            pass
        self.taker_fee_rate = _d(taker_fee_rate)
        self.step_size = _d(qty_precision_step if qty_precision_step is not None else step_size)
        self.tick_size: Decimal = Decimal("0")
        self.min_qty = _d(min_qty)
        self.min_notional = _d(min_notional)
        self.grid_reanchor_up = bool(grid_reanchor_up)
        self.grid_reanchor_down = bool(grid_reanchor_down)
        self.grid_reanchor_trigger_steps = int(grid_reanchor_trigger_steps)
        self.grid_sell_only_above_cost = bool(grid_sell_only_above_cost)
        self.grid_min_sell_markup_pct = _d(grid_min_sell_markup_pct)
        self.slippage_pct = _d(slippage_pct)
        self.fill_epsilon_pct = _d(fill_epsilon_pct)

        # Optional execution hook (demo/live bridge, journaling, telemetry).
        # Duck-typed methods if present:
        #   - on_order_placed(order=<broker-order>, side=<str>, qty=Decimal, limit_price=Decimal, symbol=<str>)
        #   - on_fill(fill=<broker-fill>, symbol=<str>)
        self.execution = execution

        # State
        self.anchor: Optional[Decimal] = None
        self.last_bid: Optional[Decimal] = None
        self.last_ask: Optional[Decimal] = None
        self.last_mid: Optional[Decimal] = None
        self.last_quote_ts: Optional[datetime] = None
        self.last_bar_close: Optional[Decimal] = None
        self.last_bar_ts: Optional[datetime] = None

        self.buy_orders: set[Decimal] = set()  # target price levels (strategy-side), not broker ids
        self.open_lots: list[GridLot] = []
        self._lot_open_ts_by_key: dict[str, Optional[datetime]] = {}

        # Pending placements keyed by normalized level. Filled via on_fill callback.
        self.pending_buy_levels: dict[str, Decimal] = {}
        self.pending_sell_levels: dict[str, Decimal] = {}
        self._pending_buy_seen_at: dict[str, datetime] = {}
        self._pending_sell_seen_at: dict[str, datetime] = {}
        # BUY snapshots on demo/live can lag for tens of seconds around reconnects;
        # keep local BUY pending keys longer to avoid overplacing duplicate ladder levels.
        self._pending_buy_reconcile_grace_seconds: int = 120
        # If a BUY pending key is not visible on broker open orders for too long, release it.
        # This prevents BUY ladder underfill (e.g. 4/5 levels) after stale local pending refs.
        self._pending_buy_stale_release_seconds: int = 45
        # SELL reconciliation can stay tighter: stale SELL pending keys should be released quickly.
        self._pending_sell_reconcile_grace_seconds: int = 2
        # Do not let stale closed fill-accumulators block lot recovery forever.
        self._recover_fill_accum_block_seconds: int = 15
        # After a SELL cancel-replace request, wait a bit before retrying replacement on the same level.
        # This avoids cancel/place storms while exchange open_orders snapshot is still converging.
        self._sell_replace_retry_cooldown_seconds: int = 5
        self._sell_replace_cooldown_until_by_key: dict[str, datetime] = {}
        # If exchange snapshot lags and we over-accumulate BUY orders, trim BUYs to free slots for SELLs.
        self._order_slot_rebalance_cooldown_seconds: int = 2
        # Right after a fill, broker open_orders snapshot can still contain the already-filled BUY.
        # Do not rebalance slots immediately, otherwise we may cancel a valid BUY unnecessarily.
        self._order_slot_rebalance_after_fill_cooldown_seconds: int = 8
        self._last_order_slot_rebalance_ts: Optional[datetime] = None
        # Broker balance snapshots can lag immediately after fills; require persistent mismatch
        # before trimming lots against wallet base.
        self._drop_excess_confirm_seconds: int = 8
        self._drop_excess_cooldown_after_fill_seconds: int = 15
        self._drop_excess_candidate_since: Optional[datetime] = None
        # Keep strictly one BUY order per price level on exchange.
        self._buy_price_dedupe_cooldown_seconds: int = 2
        self._last_buy_price_dedupe_ts: Optional[datetime] = None
        # Enforce BUY ladder shape/cap even if broker snapshot lags behind true exchange state.
        self._buy_ladder_prune_cooldown_seconds: int = 2
        self._last_buy_ladder_prune_ts: Optional[datetime] = None
        self._buy_freeze_log_every_seconds: int = 5
        self._last_buy_freeze_log_ts: Optional[datetime] = None

        # Bind broker order ids to strategy levels so partial fills keep the same mapping.
        self._buy_level_by_order_ref: dict[str, Decimal] = {}
        self._sell_level_by_order_ref: dict[str, Decimal] = {}

        # Dedupe repeated fill callbacks (can happen on reconnect/polling bridges).
        self._seen_fill_exec_refs: set[str] = set()
        # Extra protection for polling bridges: sometimes the same final fill can be replayed
        # without a stable exec/trade id. We finalize each broker order only once.
        self._finalized_fill_order_refs: set[str] = set()

        # Aggregate partial fills by order id so strategy state transitions happen
        # only when the full order is completed (prevents premature BUY->SELL / SELL->BUY placement).
        # Stored payload:
        # {"side": str, "qty": Decimal, "quote_sum": Decimal, "fee_quote": Decimal, "fee_base": Decimal, "ts": Optional[datetime]}
        self._fill_accum_by_order_ref: dict[str, dict[str, Any]] = {}
        # Timestamp of the most recent processed fill (used to avoid aggressive lot recovery races).
        self._last_fill_ts: Optional[datetime] = None

        # Planned order qty captured at placement time (by broker order id).
        # Used to safely finalize partial fills even if broker open_orders snapshot lags.
        # Key: broker order id / client order id; Value: planned qty (step-rounded).
        self._planned_qty_by_order_ref: dict[str, Decimal] = {}

        # Recovery safety: wallet-based lot synthesis can race with delayed trade polling.
        # Disabled by default: synthesized lots can produce off-grid SELL orders.
        # Opt-in via env RUN_DEMO_GRID_ENABLE_LOT_RECOVERY=1.
        self._enable_runtime_lot_recovery: bool = str(
            os.getenv("RUN_DEMO_GRID_ENABLE_LOT_RECOVERY", "0")
        ).strip().lower() in {"1", "true", "yes", "on"}
        self._lot_recovery_cooldown_after_fill_seconds: int = 30
        # If an order is already closed on exchange but we only saw part of its trade fragments,
        # wait a short grace period for remaining fragments before forced accumulator finalization.
        self._fill_accum_finalize_grace_seconds: int = 3
        # Guard against tiny BUY orders when free quote is temporarily stale/low.
        # Example: 0.20 means spend must be at least 20% of intended spend.
        try:
            self._buy_spend_degrade_min_ratio: Decimal = _d(
                os.getenv("RUN_DEMO_GRID_BUY_SPEND_MIN_RATIO", "0.20")
            )
        except Exception:
            self._buy_spend_degrade_min_ratio = Decimal("0.20")
        if self._buy_spend_degrade_min_ratio < 0:
            self._buy_spend_degrade_min_ratio = Decimal("0")
        if self._buy_spend_degrade_min_ratio > 1:
            self._buy_spend_degrade_min_ratio = Decimal("1")
        # Strict mode: percent-based BUY spend is either fully placeable or skipped.
        # Prevents undersized BUY orders when wallet/open-order snapshots are temporarily out of sync.
        self._strict_spend_pct: bool = str(
            os.getenv("RUN_DEMO_GRID_STRICT_SPEND_PCT", "1")
        ).strip().lower() in {"1", "true", "yes", "on"}

        # Diagnostics
        self.fills_buy = 0
        self.fills_sell = 0
        self.reanchors = 0
        self.cap_blocked = 0
        self.min_notional_blocked = 0
        self.sell_profit_blocked = 0
        self.place_limit_failures = 0
        self.duplicate_place_skips = 0  # backward-compatible alias/counter name in logs/metrics
        self.duplicate_pending_skips = 0  # actual duplicate skip counter
        self.unknown_fill_side = 0
        # Throttle noisy duplicate-skip debug logs (they can fire on every quote loop).
        self._dup_skip_log_every = 50
        self._dup_skip_log_last_buy = 0
        self._dup_skip_log_last_sell = 0
        # Throttle repeated BUY retries after insufficient-balance rejects.
        self._buy_reject_cooldown_seconds = 15
        self._buy_reject_until_by_level: dict[str, datetime] = {}
        self._last_place_limit_error: Optional[str] = None
        self._last_place_limit_side: Optional[str] = None
        self.init_done = False
        self._peak_equity: Optional[Decimal] = None
        self._max_dd_abs = Decimal("0")
        self._max_dd_pct = Decimal("0")

        # Cashflow accounting mirrors (paper/runtime can call register_* methods).
        self.manual_deposits_total = Decimal("0")

        # GridCore-backed state for ladder/lots (broker balances remain source of truth for equity).
        self._core = GridCore(
            GridConfig(
                step_pct=self.grid_step_pct,
                n_buy_levels=self.grid_n_buy,
                spend_quote=self.spend_quote,
                maker_fee_rate=self.maker_fee_rate,
                taker_fee_rate=self.taker_fee_rate,
                qty_step=self.step_size,
                min_qty=self.min_qty,
                min_notional=self.min_notional,
                init_base_frac=Decimal("0"),
            ),
            GridState(quote=Decimal("0"), base=Decimal("0")),
        )

    # ---------------------------
    # Public API for run_paper.py
    # ---------------------------
    def on_quote(self, broker: Any, quote: Any) -> None:
        bid = _d(_get(quote, "bid", "best_bid", "b"))
        ask = _d(_get(quote, "ask", "best_ask", "a"))
        ts = self._parse_ts(_get(quote, "ts", "timestamp", "time", default=None))

        if ask <= 0 or bid <= 0:
            return

        self.last_bid = bid
        self.last_ask = ask
        self.last_mid = (bid + ask) / Decimal("2")
        self.last_quote_ts = ts
        self._sync_fee_rates_from_runtime(broker)
        self._sync_core_balances_from_broker(broker)
        self._finalize_fill_accumulators_from_broker(broker)
        self._drop_excess_open_lots_from_base(broker)
        self._recover_missing_open_lots_from_base(broker)
        self._reconcile_pending_levels_from_broker(broker)

        if not self.init_done:
            self._bootstrap_grid()
            self._sync_broker_orders(broker)
            self.init_done = True
        else:
            # on quote we keep the broker populated with current desired orders
            self._sync_broker_orders(broker)

        self._update_drawdown(broker)

    def on_bar_close(self, broker: Any, bar: Any) -> None:
        close_px = _d(_get(bar, "close", "c"))
        ts = self._parse_ts(_get(bar, "close_time", "ts", "timestamp", "time", "open_time", default=None))

        self.last_bar_close = close_px
        self.last_bar_ts = ts

        if self.anchor is None:
            self.anchor = close_px
            self._rebuild_initial_buys(self.anchor)
            self._sync_broker_orders(broker)
            return

        # Reanchor logic parity with backtest (bar-close driven).
        reanchor_pct = self.grid_step_pct * Decimal(int(self.grid_reanchor_trigger_steps))

        up_trig = self.anchor * (Decimal("1") + reanchor_pct)
        dn_trig = self.anchor * (Decimal("1") - reanchor_pct)

        did_reanchor = False
        if self.grid_reanchor_up and close_px >= up_trig:
            self.anchor = close_px
            self._rebuild_initial_buys(self.anchor)
            did_reanchor = True
        if self.grid_reanchor_down and close_px <= dn_trig:
            self.anchor = close_px
            self._rebuild_initial_buys(self.anchor)
            did_reanchor = True

        if did_reanchor:
            self.reanchors += 1
            # Re-anchor changes desired ladder levels. In live/demo we must cancel old
            # resting orders, otherwise the exchange keeps the old ladder and the UI
            # will show the "old anchor" levels.
            self._cancel_all_strategy_orders(broker)
            self._sync_broker_orders(broker)

    def on_fill(self, broker: Any, fill: Any) -> None:
        side_raw = _get(fill, "side", default=None)
        side = self._norm_side(side_raw)
        if side not in ("BUY", "SELL"):
            self.unknown_fill_side += 1
            return

        qty = _d(_get(fill, "qty", "quantity", default=0))
        px = _d(_get(fill, "price", "px", default=0))
        fee_quote = _d(_get(fill, "fee_quote", "fee", default=0))
        fee_base = _d(_get(fill, "fee_base", default=0))
        ts = self._parse_ts(_get(fill, "ts", "timestamp", "time", default=None))
        order_ref = self._fill_order_ref(fill)
        client_ref = self._fill_client_ref(fill)
        force_finalize = bool(_get(fill, "force_finalize", default=False))

        # Bridge LOCAL-* refs to exchange order id once we see both.
        # This keeps planned qty / level maps stable even when subsequent events omit clientOrderId.
        if order_ref is not None and client_ref is not None:
            try:
                order_ref_s = str(order_ref).strip()
                client_ref_s = str(client_ref).strip()
                if order_ref_s and client_ref_s:
                    if order_ref_s not in self._planned_qty_by_order_ref:
                        planned_from_client = self._planned_qty_by_order_ref.get(client_ref_s)
                        if planned_from_client is not None:
                            planned_dec = _d(planned_from_client)
                            if planned_dec > 0:
                                self._planned_qty_by_order_ref[order_ref_s] = planned_dec
                    if order_ref_s not in self._buy_level_by_order_ref:
                        buy_lvl = self._buy_level_by_order_ref.get(client_ref_s)
                        if buy_lvl is not None:
                            self._buy_level_by_order_ref[order_ref_s] = _d(buy_lvl)
                    if order_ref_s not in self._sell_level_by_order_ref:
                        sell_lvl = self._sell_level_by_order_ref.get(client_ref_s)
                        if sell_lvl is not None:
                            self._sell_level_by_order_ref[order_ref_s] = _d(sell_lvl)
            except Exception:
                pass
        self._refresh_broker_state_after_fill(broker)
        
        # Normalize fill qty to exchange LOT_SIZE step to prevent SELL deadlocks
        if self.step_size > 0:
            qty = _round_step(qty, self.step_size)

        exec_ref = self._fill_exec_ref(fill)
        # If a polling bridge replays a completed order fill and there is no stable exec id,
        # do not let the strategy transition the same EXCHANGE order twice.
        # When exec_ref exists, prefer exec-level dedupe so late fragments for the same order
        # can still reach the strategy after an early partial finalization.
        if order_ref is not None and str(order_ref) in self._finalized_fill_order_refs and exec_ref is None:
            return

        if qty <= 0 or px <= 0:
            return

        fill_seen_ts = ts or self.last_quote_ts or datetime.now(timezone.utc)
        if fill_seen_ts.tzinfo is None:
            fill_seen_ts = fill_seen_ts.replace(tzinfo=timezone.utc)
        else:
            fill_seen_ts = fill_seen_ts.astimezone(timezone.utc)
        self._last_fill_ts = fill_seen_ts

        # Dedupe repeated fill callbacks (reconnect/polling bridges may replay the same fill)
        if exec_ref is not None:
            if exec_ref in self._seen_fill_exec_refs:
                return
            self._seen_fill_exec_refs.add(exec_ref)

        # Aggregate partial fills by order id and process strategy transitions only
        # when the order is fully completed. This avoids placing the opposite order
        # on partial fills.
        if order_ref is not None:
            # In demo/live polling bridges, partial fills can arrive without a reliable final status.
            # We treat a fill as final only when either:
            #   1) the fill payload explicitly says so, or
            #   2) the order is no longer present in broker open orders (after a refresh attempt).
            is_final_fill = self._fill_is_final(fill)
            current_added_to_acc = False
            # Guard against premature finalization: some polling bridges emit early trade fragments
            # with status='?' and without `partial`. If we know the planned qty, do NOT finalize
            # until we have accumulated at least planned_qty (within one step).
            try:
                planned_qty = self._planned_qty_by_order_ref.get(str(order_ref))
                if (planned_qty is None or planned_qty <= 0) and client_ref is not None:
                    planned_qty = self._planned_qty_by_order_ref.get(str(client_ref))
                if planned_qty is not None and planned_qty > 0:
                    eps = self.step_size if self.step_size > 0 else Decimal("0")
                    if (not force_finalize) and is_final_fill and qty < (planned_qty - eps):
                        is_final_fill = False
            except Exception:
                pass
            if not is_final_fill:
                self._accumulate_fill_fragment(
                    order_ref,
                    side=side,
                    qty=qty,
                    px=px,
                    fee_quote=fee_quote,
                    fee_base=fee_base,
                    ts=fill_seen_ts,
                )
                current_added_to_acc = True
                # If we know the planned qty for this order (captured on placement), we can
                # finalize once accumulated qty reaches planned qty (within one step).
                try:
                    planned_qty = self._planned_qty_by_order_ref.get(str(order_ref))
                    if (planned_qty is None or planned_qty <= 0) and client_ref is not None:
                        planned_qty = self._planned_qty_by_order_ref.get(str(client_ref))
                    if planned_qty is not None:
                        acc_now = self._fill_accum_by_order_ref.get(str(order_ref)) or {}
                        acc_qty_now = _d(acc_now.get("qty", 0))
                        eps = self.step_size if self.step_size > 0 else Decimal("0")
                        if acc_qty_now > 0 and acc_qty_now >= (planned_qty - eps):
                            is_final_fill = True
                except Exception:
                    pass

                self._refresh_broker_state_after_fill(broker)

                # If the order is no longer present in broker open orders after a refresh,
                # we treat the aggregated fills as FINAL even if the polling bridge keeps
                # reporting `partial=True` / `PARTIALLY_FILLED`.
                if not is_final_fill:
                    if self._broker_order_ref_is_open(broker, order_ref):
                        return
                    # Order disappeared from open orders. If we still have less than planned qty,
                    # keep accumulator for a short grace window to collect late fragments from polling.
                    try:
                        planned_qty = self._planned_qty_by_order_ref.get(str(order_ref))
                        if (planned_qty is None or planned_qty <= 0) and client_ref is not None:
                            planned_qty = self._planned_qty_by_order_ref.get(str(client_ref))
                        acc_now = self._fill_accum_by_order_ref.get(str(order_ref)) or {}
                        acc_qty_now = _d(acc_now.get("qty", 0))
                        eps = self.step_size if self.step_size > 0 else Decimal("0")
                        if (
                            (not force_finalize)
                            and planned_qty is not None
                            and planned_qty > 0
                            and acc_qty_now < (planned_qty - eps)
                        ):
                            logger.debug(
                                "GRID_FILL wait_more_fragments | side={} order_ref={} acc_qty={} planned_qty={} reason=order_closed_snapshot",
                                side,
                                order_ref,
                                acc_qty_now,
                                planned_qty,
                            )
                            return
                    except Exception:
                        pass
                    # Enough qty accumulated (or forced) -> finalize now.
                    is_final_fill = True

            try:
                if is_final_fill and self._broker_order_ref_is_open(broker, order_ref):
                    logger.debug(
                        "GRID_FILL finalize_by_planned_qty | side={} order_ref={} qty={} px={} planned_qty={} reason=order_still_open_in_snapshot",
                        side,
                        order_ref,
                        qty,
                        px,
                        (
                            self._planned_qty_by_order_ref.get(str(order_ref))
                            if self._planned_qty_by_order_ref.get(str(order_ref)) is not None
                            else self._planned_qty_by_order_ref.get(str(client_ref)) if client_ref is not None else None
                        ),
                    )
            except Exception:
                pass
            acc = self._fill_accum_by_order_ref.pop(order_ref, None)
            if acc is not None:
                acc_qty = _d(acc.get("qty", 0))
                if acc_qty > 0:
                    acc_quote = _d(acc.get("quote_sum", 0))
                    acc_fee = _d(acc.get("fee_quote", 0))
                    acc_fee_base = _d(acc.get("fee_base", 0))

                    if current_added_to_acc:
                        # current fragment already included in accumulator
                        qty = acc_qty
                        px = (acc_quote / acc_qty) if acc_qty > 0 else px
                        fee_quote = acc_fee
                        fee_base = acc_fee_base
                    else:
                        # accumulator has only previous fragments, add current now
                        curr_quote = qty * px
                        total_qty = acc_qty + qty
                        total_quote = acc_quote + curr_quote
                        qty = total_qty
                        px = (total_quote / total_qty) if total_qty > 0 else px
                        fee_quote = acc_fee + fee_quote
                        fee_base = acc_fee_base + fee_base

                    acc_ts = acc.get("ts")
                    if ts is None and acc_ts is not None:
                        ts = acc_ts

            if is_final_fill:
                self._finalized_fill_order_refs.add(str(order_ref))
                # Order is finalized -> planned qty no longer needed.
                self._planned_qty_by_order_ref.pop(str(order_ref), None)
                if client_ref is not None:
                    self._planned_qty_by_order_ref.pop(str(client_ref), None)

        self._notify_execution_fill(fill)
        # Pull freshest broker snapshot before processing the fill. In demo/live mode
        # balance/order polling can lag a bit, so we also refresh explicitly when the
        # execution bridge exposes these methods.
        self._refresh_broker_state_after_fill(broker)
        self._sync_core_balances_from_broker(broker)
        # Match by nearest expected level (strategy-side level, not fill px)
        if side == "BUY":
            buy_level = None

            # Prefer stable mapping by broker order id/client order id
            for ref in (order_ref, client_ref):
                if ref is None:
                    continue
                buy_level = self._buy_level_by_order_ref.get(ref)
                if buy_level is not None:
                    break

            if buy_level is None:
                # Do not infer BUY level from pending ladder here:
                # after restarts LOCAL-* client ids can be reused and pending maps can be stale,
                # which may map a fill to a wrong level and collapse many lots to one sell price.
                buy_level = px / (Decimal("1") + self.slippage_pct) if self.slippage_pct > 0 else px

            # Remove pending only on final fill state (keep mapping for partials)
            final_seen = False
            for ref in (order_ref, client_ref):
                if ref is not None and str(ref) in self._finalized_fill_order_refs:
                    final_seen = True
                    break
            if order_ref is None or final_seen:
                for ref in (order_ref, client_ref):
                    if ref is not None:
                        self._buy_level_by_order_ref.pop(ref, None)

                # pop exact key by price if possible, otherwise nearest
                buy_key = self._k(buy_level)
                if buy_key in self.pending_buy_levels:
                    self.pending_buy_levels.pop(buy_key, None)
                else:
                    self._pop_nearest_pending(self.pending_buy_levels, px)
            expected_sell_level = buy_level * (Decimal("1") + self.grid_step_pct)

            # If commission is charged in base asset (common for BUY when no BNB discount),
            # actual credited base is less than executed qty by `fee_base`.
            if fee_base > 0:
                qty -= fee_base
                if qty < 0:
                    qty = Decimal("0")

            qty = _round_step(qty, self.step_size) if self.step_size > 0 else qty
            if qty <= 0:
                logger.warning(
                    "GRID_FILL buy_skip_after_base_fee | symbol={} order_ref={} px={} fee_base={}",
                    self.symbol,
                    order_ref,
                    px,
                    fee_base,
                )
                self._refresh_broker_state_after_fill(broker)
                self._sync_broker_orders(broker)
                self._update_drawdown(broker)
                return
            self._lot_open_ts_by_key[self._lot_key(expected_sell_level, qty)] = ts

            # use qty_net everywhere дальше (и в lot_key тоже!)
            self._core.on_buy_fill(
                qty=qty,
                price=px,
                fee_quote=fee_quote,
                level_price=buy_level,
            )
            self.fills_buy += 1

            self._mirror_local_state_from_core()
            self._ensure_buy_ladder()

        else:  # SELL
            sell_level = None

            # Prefer stable mapping by broker order id/client order id
            for ref in (order_ref, client_ref):
                if ref is None:
                    continue
                sell_level = self._sell_level_by_order_ref.get(ref)
                if sell_level is not None:
                    break

            if sell_level is None:
                # Same rationale as BUY fallback above: prefer fill-derived level over
                # potentially stale pending maps to avoid mismatched lot attribution.
                sell_level = px / (Decimal("1") - self.slippage_pct) if self.slippage_pct > 0 else px

            # Remove pending only on final fill state (keep mapping for partials)
            final_seen = False
            for ref in (order_ref, client_ref):
                if ref is not None and str(ref) in self._finalized_fill_order_refs:
                    final_seen = True
                    break
            if order_ref is None or final_seen:
                for ref in (order_ref, client_ref):
                    if ref is not None:
                        self._sell_level_by_order_ref.pop(ref, None)

                # SELL pending keys are composite "price|open_seq", so remove nearest
                self._pop_nearest_pending(self.pending_sell_levels, px)
            if not self._core.state.open_lots:
                logger.debug(
                    "GRID_FILL ignore | side=SELL qty={} px={} reason=no_open_lots",
                    qty,
                    px,
                )
                self._sync_broker_orders(broker)
                self._update_drawdown(broker)
                return

            self._core.on_sell_fill(qty=qty, price=px, fee_quote=fee_quote, level_price=sell_level)
            self.fills_sell += 1
            self._mirror_local_state_from_core()
            self._ensure_buy_ladder()

        # Re-sync broker desired orders after any fill
        self._refresh_broker_state_after_fill(broker)
        self._sync_broker_orders(broker)
        self._update_drawdown(broker)

    def on_cancel(self, broker: Any, order_id: Any = None, client_order_id: Any = None) -> None:
        """Best-effort cleanup for canceled orders reported by runtime loop."""
        refs: list[str] = []
        for raw in (order_id, client_order_id):
            if raw is None:
                continue
            s = str(raw).strip()
            if s:
                refs.append(s)
        if not refs:
            return

        for ref in refs:
            buy_px = self._buy_level_by_order_ref.pop(ref, None)
            if buy_px is not None:
                key = self._k(_round_tick(_d(buy_px), self.tick_size))
                self.pending_buy_levels.pop(key, None)
                self._pending_buy_seen_at.pop(key, None)
                self._buy_reject_until_by_level.pop(key, None)

            sell_px = self._sell_level_by_order_ref.pop(ref, None)
            if sell_px is not None:
                key = self._k(_round_tick(_d(sell_px), self.tick_size))
                self.pending_sell_levels.pop(key, None)
                self._pending_sell_seen_at.pop(key, None)
                self._sell_replace_cooldown_until_by_key.pop(key, None)

            self._planned_qty_by_order_ref.pop(ref, None)
            self._fill_accum_by_order_ref.pop(ref, None)

    def _refresh_broker_state_after_fill(self, broker: Any) -> None:
        def _maybe_call(x: Any) -> None:
            try:
                out = x()
            except Exception:
                return
            # If broker uses async refresh methods, schedule them.
            try:
                import asyncio as _asyncio
                if _asyncio.iscoroutine(out):
                    try:
                        loop = _asyncio.get_running_loop()
                        loop.create_task(out)
                    except RuntimeError:
                        # No running loop; best effort: drop.
                        return
            except Exception:
                return

        try:
            rb = getattr(broker, "refresh_balances", None)
            if callable(rb):
                _maybe_call(rb)
        except Exception:
            pass

        try:
            roo = getattr(broker, "refresh_open_orders", None)
            if callable(roo):
                _maybe_call(roo)
        except Exception:
            pass

    def _finalize_fill_accumulators_from_broker(self, broker: Any) -> None:
        """Finalize accumulated partial fills when the corresponding order disappears from broker open orders.

        Если мы получили PARTIALLY_FILLED/partial=True и вернулись из on_fill (потому что снапшот брокера
        всё ещё показывал ордер как open), то BUY->SELL переход может никогда не произойти.
        Этот метод на каждом on_quote проверяет аккумуляторы и финализирует те, чьи ордера уже пропали из open_orders.
        """
        if not self._fill_accum_by_order_ref:
            return

        now_ts = self.last_quote_ts or datetime.now(timezone.utc)
        if now_ts.tzinfo is None:
            now_ts = now_ts.replace(tzinfo=timezone.utc)
        else:
            now_ts = now_ts.astimezone(timezone.utc)

        # Идём по копии, потому что будем мутировать dict
        for order_ref, acc in list(self._fill_accum_by_order_ref.items()):
            try:
                if not order_ref:
                    continue
                order_ref_s = str(order_ref).strip()
                if not order_ref_s:
                    continue

                # Пока брокер считает ордер открытым — ждём
                if self._broker_order_ref_is_open(broker, order_ref_s):
                    continue

                side = str(acc.get("side", "")).upper()
                qty = _d(acc.get("qty", 0))
                quote_sum = _d(acc.get("quote_sum", 0))
                fee_quote = _d(acc.get("fee_quote", 0))
                fee_base = _d(acc.get("fee_base", 0))
                ts = acc.get("ts")

                if qty <= 0 or quote_sum <= 0:
                    self._fill_accum_by_order_ref.pop(order_ref, None)
                    continue

                force_finalize = False
                planned_qty = self._planned_qty_by_order_ref.get(order_ref_s)
                eps = self.step_size if self.step_size > 0 else Decimal("0")
                if planned_qty is not None and planned_qty > 0 and qty < (planned_qty - eps):
                    age_sec = None
                    try:
                        acc_ts = ts if isinstance(ts, datetime) else None
                        if acc_ts is not None:
                            acc_utc = acc_ts if acc_ts.tzinfo is not None else acc_ts.replace(tzinfo=timezone.utc)
                            age_sec = (now_ts - acc_utc).total_seconds()
                    except Exception:
                        age_sec = None

                    grace_sec = int(getattr(self, "_fill_accum_finalize_grace_seconds", 3))
                    if grace_sec > 0 and age_sec is not None and age_sec < grace_sec:
                        continue
                    force_finalize = True
                    logger.warning(
                        "GRID_FILL finalize_from_acc_partial | order_ref={} side={} acc_qty={} planned_qty={} age_s={} reason=grace_expired",
                        order_ref_s,
                        side,
                        qty,
                        planned_qty,
                        age_sec,
                    )

                px = (quote_sum / qty) if qty > 0 else Decimal("0")

                # ВАЖНО: удаляем accumulator ДО вызова on_fill, иначе задвоим
                self._fill_accum_by_order_ref.pop(order_ref, None)

                synth_fill = {
                    "side": side,
                    "qty": str(qty),
                    "price": str(px),
                    "fee_quote": str(fee_quote),
                    "fee_base": str(fee_base),
                    "ts": ts.isoformat() if isinstance(ts, datetime) else ts,
                    "order_id": order_ref_s,
                    "status": "FILLED",
                    "partial": False,
                    "trade_id": f"ACCUM-{order_ref_s}",
                    "force_finalize": force_finalize,
                }
                logger.debug(
                    "GRID_FILL finalize_from_acc | order_ref={} side={} qty={} px={} fee_quote={} fee_base={} reason=order_not_open",
                    order_ref_s, side, qty, px, fee_quote, fee_base
                )
                # Прогоняем как финальный fill -> появится open_lot -> _sync_broker_orders поставит SELL
                self.on_fill(broker, synth_fill)

            except Exception:
                # Нельзя ломать quote-loop
                continue    

    def heartbeat_fields(self, broker: Any) -> dict[str, Any]:
        quote_cash = self._broker_quote_cash(broker)
        base_qty = self._broker_base_qty(broker)
        quote_total = self._broker_quote_total(broker)
        base_total = self._broker_base_total(broker)
        mid = self.last_mid or Decimal("0")
        base_notional = base_qty * mid
        equity = quote_cash + base_notional
        self._sync_fee_rates_from_runtime(broker)

        reserved_pending_buy_quote = self._reserved_quote_in_pending_buys(broker)
        available_quote_for_new_buy = quote_cash - reserved_pending_buy_quote
        if available_quote_for_new_buy < 0:
            available_quote_for_new_buy = Decimal("0")
        need_quote_per_order = self._resolve_spend_quote(broker) * (Decimal("1") + self.maker_fee_rate)

        if self._peak_equity is None:
            dd_now_abs = Decimal("0")
            dd_now_pct = Decimal("0")
        else:
            dd_now_abs = self._peak_equity - equity
            if dd_now_abs < 0:
                dd_now_abs = Decimal("0")
            dd_now_pct = (dd_now_abs / self._peak_equity) if self._peak_equity > 0 else Decimal("0")

        return {
            "symbol": self.symbol,
            "interval": self.interval,
            "anchor": str(self.anchor) if self.anchor is not None else None,
            "quote": quote_cash,
            "quote_cash_seen_by_adapter": quote_cash,
            "reserved_pending_buy_quote": reserved_pending_buy_quote,
            "available_quote_for_new_buy": available_quote_for_new_buy,
            "need_quote_per_order": need_quote_per_order,
            "maker_fee_rate": self.maker_fee_rate,
            "taker_fee_rate": self.taker_fee_rate,
            "base": base_qty,
            "base_notional": base_notional,
            "equity": equity,
            "manual_deposits_total": self.manual_deposits_total,
            "core_initial_quote": _d(getattr(self._core.state, "initial_quote", Decimal("0"))),
            "core_deposits_total": _d(getattr(self._core.state, "deposits_total", Decimal("0"))),
            "open_orders": self._broker_open_orders_count(broker),
            "broker_open_orders_total": self._broker_open_orders_count(broker),
            "strategy_open_orders": self._broker_strategy_open_orders_count(broker),
            "strategy_pending_orders": len(self.pending_buy_levels) + len(self.pending_sell_levels),
            "max_active_buy_orders": getattr(self, "max_active_buy_orders", self.grid_n_buy),
            "grid_open_lots": len(self.open_lots),
            "grid_buy_levels": len(self.buy_orders),
            "fills_buy": self.fills_buy,
            "fills_sell": self.fills_sell,
            "reanchors": self.reanchors,
            "cap_blocked": self.cap_blocked,
            "min_notional_blocked": self.min_notional_blocked,
            "sell_profit_blocked": self.sell_profit_blocked,
            "duplicate_place_skips": self.duplicate_place_skips,
            "duplicate_pending_skips": self.duplicate_pending_skips,
            "buy_reject_cooldowns": len(self._buy_reject_until_by_level),            
            "place_limit_failures": self.place_limit_failures,
            "adapter_dd_now_abs": dd_now_abs,
            "adapter_dd_now_pct": dd_now_pct,
            "adapter_max_dd_abs": self._max_dd_abs,
            "adapter_max_dd_pct": self._max_dd_pct,
            "last_bid": self.last_bid,
            "last_ask": self.last_ask,
            "last_mid": self.last_mid,
            "quote_total": quote_total,
            "base_total": base_total,
            "equity_total": (quote_total + (base_total * mid)),
        }

    # ---------------------------
    # Internal strategy mechanics
    # ---------------------------
    def _bootstrap_grid(self) -> None:
        if self.last_mid is None:
            return
        if self.anchor is not None:
            self._core.state.anchor = self.anchor
        self._core.bootstrap(self.last_mid)
        self._mirror_local_state_from_core()
        self._ensure_buy_ladder()

    def _ensure_buy_ladder(self) -> None:
        """Force BUY ladder to be exactly `grid_n_buy` levels based on anchor.

        This guarantees:
        - target N BUY orders (N = grid_n_buy)
        - each filled BUY -> exactly one SELL (lot exit)
        - after BUY fills, we replenish BUY ladder back to N
        """
        if self.grid_n_buy <= 0:
            self.buy_orders = set()
            try:
                self._core.state.buy_levels = set()
            except Exception:
                pass
            return

        anchor = self.anchor or self.last_mid
        if anchor is None or anchor <= 0:
            return

        step = self.grid_step_pct
        if step <= 0:
            return

        levels: list[Decimal] = []
        for i in range(1, int(self.grid_n_buy) + 1):
            px = anchor * (Decimal("1") - step * Decimal(i))
            if px > 0:
                levels.append(px)

        # Keep exactly N highest buy levels (closest to anchor)
        levels = sorted(set(levels), reverse=True)[: int(self.grid_n_buy)]
        self.buy_orders = set(levels)

        # Keep core in sync (so restore/export stays coherent)
        try:
            self._core.state.buy_levels = set(self.buy_orders)
        except Exception:
            pass    

    def _fill_is_final(self, fill: Any) -> bool:
        """Best-effort decision whether a fill fragment should be treated as final.

        Priority:
        1) explicit `partial` flag if present
        2) order `status` if present
        3) unknown -> treat as non-final (safer for polling bridges; planned-qty/open-orders logic will finalize)
        """
        # 1) partial flag is authoritative if present
        partial_flag = _get(fill, "partial", "is_partial", default=None)
        if partial_flag is not None:
            return not bool(partial_flag)

        # 2) fall back to status
        status = _get(fill, "status", "order_status", default=None)
        if status is None:
            # If nothing is known -> assume final (trade-level only bridges)
            return True

        s = str(status).strip().upper()
        if not s or s == "?":
            # Unknown status: safer to treat as NON-final.
            # Planned-qty accumulation / open-orders disappearance will finalize.
            return False

        if s in {"FILLED", "FULLY_FILLED", "DONE", "CLOSED", "COMPLETED"}:
            return True

        if s in {
            "NEW",
            "PARTIALLY_FILLED",
            "PARTIALLYFILLED",
            "OPEN",
            "WORKING",
            "PENDING",
            "EXPIRED",
            "CANCELED",
            "CANCELLED",
            "REJECTED",
        }:
            return False

        # Unknown status -> non-final (safer)
        return False

    def _order_ref(self, obj: Any) -> Optional[str]:
        raw = _get(
            obj,
            "order_id","orderId","orderID",
            "client_order_id","clientOrderId","origClientOrderId",
            "id",
            default=None,
        )
        if raw is None:
            return None
        s = str(raw).strip()
        return s or None

    def _broker_order_ref_is_open(self, broker: Any, order_ref: str) -> bool:
        """Best-effort check whether a broker order is still open."""
        try:
            open_orders = getattr(broker, "open_orders", None)
            if not open_orders:
                return False

            order_ref_s = str(order_ref)
            for o in open_orders:
                ref = self._order_ref(o)
                if ref is not None and str(ref) == order_ref_s:
                    return True
        except Exception:
            return False
        return False    

    def _rebuild_initial_buys(self, anchor: Decimal) -> None:
        self._core.rebuild_buy_levels(anchor)
        self._mirror_local_state_from_core()
        self._ensure_buy_ladder()   

    def _build_sell_targets_from_open_lots(self) -> list[tuple[Decimal, Decimal]]:
        """Aggregate SELL intents by price so one level has one order with summed qty."""
        buckets: dict[str, dict[str, Decimal]] = {}

        for lot in sorted(self.open_lots, key=lambda x: (x.sell_price, x.open_seq)):
            if lot.qty <= 0:
                continue

            sell_px_place = _round_tick(_d(lot.sell_price), self.tick_size)
            if sell_px_place <= 0:
                continue

            lot_qty = _round_step(_d(lot.qty), self.step_size)
            if lot_qty <= 0 or lot_qty < self.min_qty:
                self.min_notional_blocked += 1
                continue
            if (lot_qty * sell_px_place) < self.min_notional:
                self.min_notional_blocked += 1
                continue

            if self.grid_sell_only_above_cost:
                proceeds_net = (lot_qty * sell_px_place) * (Decimal("1") - self.maker_fee_rate)
                if proceeds_net <= lot.cost_quote:
                    self.sell_profit_blocked += 1
                    step_too_small = self.grid_step_pct <= (self.maker_fee_rate * Decimal("2"))
                    if not step_too_small:
                        continue

            if self.grid_min_sell_markup_pct > 0:
                if sell_px_place < lot.buy_price * (Decimal("1") + self.grid_min_sell_markup_pct):
                    continue

            key = self._k(sell_px_place)
            bucket = buckets.get(key)
            if bucket is None:
                buckets[key] = {"price": sell_px_place, "qty": lot_qty}
            else:
                bucket["qty"] = bucket["qty"] + lot_qty

        out: list[tuple[Decimal, Decimal]] = []
        for key in sorted(buckets.keys(), key=lambda k: buckets[k]["price"]):
            price = buckets[key]["price"]
            qty = _round_step(_d(buckets[key]["qty"]), self.step_size)
            if qty <= 0 or qty < self.min_qty:
                continue
            if (qty * price) < self.min_notional:
                continue
            out.append((price, qty))
        return out

    def _sync_broker_orders(self, broker: Any) -> None:
        """Place desired levels as limit orders via broker.

        We intentionally do not cancel stale broker orders here because current paper broker API in project
        appears to be append-only in the snippets. We dedupe by local pending maps and rely on fills to clear.
        """
        # NOTE: fills can arrive via polling before we receive a fresh quote snapshot.
        # We still must be able to place SELL exits for existing open lots.
        if self.last_bid is None or self.last_ask is None:
            if not self.open_lots:
                return
            logger.debug(
                "GRID_SYNC warn | missing_bid_ask placing_sells_only | symbol={} last_mid={} open_lots={}",
                self.symbol,
                self.last_mid,
                len(self.open_lots),
            )

        self._reconcile_pending_levels_from_broker(broker)
        self._ensure_buy_ladder()
        desired_buy_keys_norm = {
            self._k(_round_tick(_d(px), self.tick_size))
            for px in self.buy_orders
            if _round_tick(_d(px), self.tick_size) > 0
        }
        max_buy_cfg = max(int(getattr(self, "max_active_buy_orders", self.grid_n_buy)), 0)
        self._enforce_buy_ladder_cap(
            broker,
            desired_buy_keys=desired_buy_keys_norm,
            max_buy=max_buy_cfg,
        )
        # Keep TOTAL active orders (BUY+SELL) capped.
        # SELL side is aggregated by price level; remaining slots are used for BUY ladder.
        max_total = int(getattr(self, "max_total_orders", self.grid_n_buy))
        if max_total < 0:
            max_total = 0

        sell_targets = self._build_sell_targets_from_open_lots()
        desired_sell_cnt = len(sell_targets)
        desired_buy_cnt = max_total - desired_sell_cnt
        if desired_buy_cnt < 0:
            desired_buy_cnt = 0

        # Cancel stale SELL orders that are no longer represented by current lot targets.
        # Otherwise base can stay locked on orphan prices and block correct SELL placement.
        desired_sell_keys = {self._k(px) for px, _ in sell_targets}
        stale_sell_cancelled = 0
        try:
            for meta in self._broker_active_strategy_orders(broker):
                if meta.get("side") != "SELL":
                    continue
                px = _d(meta.get("price", Decimal("0")))
                if px <= 0:
                    continue
                key = self._k(px)
                if key in desired_sell_keys:
                    continue
                oid_s = str(meta.get("order_id") or "").strip()
                coid_s = str(meta.get("client_order_id") or "").strip()
                if not self._cancel_order_refs_best_effort(broker, order_id=oid_s, client_order_id=coid_s):
                    continue
                stale_sell_cancelled += 1
                self.pending_sell_levels.pop(key, None)
                self._pending_sell_seen_at.pop(key, None)
                self._sell_replace_cooldown_until_by_key.pop(key, None)
                for ref in (oid_s, coid_s):
                    if not ref:
                        continue
                    self._sell_level_by_order_ref.pop(ref, None)
                    self._planned_qty_by_order_ref.pop(ref, None)
                    self._fill_accum_by_order_ref.pop(ref, None)
        except Exception:
            pass
        if stale_sell_cancelled > 0:
            logger.warning(
                "GRID_SYNC stale_sell_cleanup | symbol={} cancelled={} desired_sell_cnt={} open_lots={}",
                self.symbol,
                stale_sell_cancelled,
                desired_sell_cnt,
                len(self.open_lots),
            )
            self._refresh_broker_state_after_fill(broker)

        # Priority rule: if slots are saturated while SELL exits are missing, trim extra BUYs first.
        self._trim_buy_orders_for_sell_priority(
            broker,
            desired_sell_cnt=desired_sell_cnt,
            max_total=max_total,
        )
        self._dedupe_duplicate_buy_prices(broker)
        # Refresh local pending maps after potential cancellations.
        self._reconcile_pending_levels_from_broker(broker)
        broker_open_sig_counts = self._broker_open_order_sig_counts(broker)

        def _active_total_orders() -> int:
            """Best-effort count of currently active strategy orders (excluding LOCAL-*)"""
            try:
                return int(self._broker_strategy_open_orders_count(broker))
            except Exception:
                return 0

        def _total_slots_left() -> int:
            active = _active_total_orders()
            # pending_* can be ahead of broker snapshot; take the max to be safe.
            pending = len(self.pending_buy_levels) + len(self.pending_sell_levels)
            active_eff = active if active > pending else pending
            left = max_total - active_eff
            return left if left > 0 else 0

        logger.trace(
            "GRID_SYNC start | symbol={} anchor={} buy_levels={} open_lots={} pending_buy={} pending_sell={} broker_open_orders_total={} broker_strategy_open_orders={} quote={} base={}",
            self.symbol,
            self.anchor,
            len(self.buy_orders),
            len(self.open_lots),
            len(self.pending_buy_levels),
            len(self.pending_sell_levels),
            self._broker_open_orders_count(broker),
            self._broker_strategy_open_orders_count(broker),
            self._broker_quote_cash(broker),
            self._broker_base_qty(broker),
        )
        sync_now_ts = self.last_quote_ts or datetime.now(timezone.utc)
        if sync_now_ts.tzinfo is None:
            sync_now_ts = sync_now_ts.replace(tzinfo=timezone.utc)
        else:
            sync_now_ts = sync_now_ts.astimezone(timezone.utc)

        # Release stale BUY pending keys that have no broker-visible order for too long.
        # Without this, one stale key can block a full BUY slot and leave ladder underfilled.
        stale_release_sec = int(getattr(self, "_pending_buy_stale_release_seconds", 12))
        if stale_release_sec > 0 and self.pending_buy_levels:
            broker_open_buy_keys: set[str] = set()
            try:
                for o in list(getattr(broker, "open_orders", []) or []):
                    if not self._order_is_active(o):
                        continue
                    side = self._norm_side(_get(o, "side", default=None))
                    if side != "BUY":
                        continue
                    status_raw = _get(o, "status", "state", default=None)
                    if status_raw is not None:
                        st = str(status_raw).upper()
                        if st in {"FILLED", "CANCELED", "CANCELLED", "REJECTED", "EXPIRED", "CLOSED"}:
                            continue
                    px_raw = _get(o, "limit_price", "price", "limit", "px", default=None)
                    if px_raw is None:
                        continue
                    px = _round_tick(_d(px_raw), self.tick_size)
                    if px <= 0:
                        continue
                    broker_open_buy_keys.add(self._k(px))
            except Exception:
                broker_open_buy_keys = set()

            released_pending = 0
            for k in list(self.pending_buy_levels.keys()):
                if k in broker_open_buy_keys:
                    continue

                seen_at = self._pending_buy_seen_at.get(k)
                should_release = False
                if seen_at is None:
                    should_release = True
                else:
                    seen_utc = seen_at if seen_at.tzinfo is not None else seen_at.replace(tzinfo=timezone.utc)
                    if (sync_now_ts - seen_utc).total_seconds() >= stale_release_sec:
                        should_release = True

                if not should_release:
                    continue

                self.pending_buy_levels.pop(k, None)
                self._pending_buy_seen_at.pop(k, None)
                self._buy_reject_until_by_level.pop(k, None)
                released_pending += 1

            if released_pending > 0:
                logger.warning(
                    "GRID_SYNC pending_buy_released | symbol={} released={} stale_after_s={} broker_open_buy={}",
                    self.symbol,
                    released_pending,
                    stale_release_sec,
                    len(broker_open_buy_keys),
                )

        def _active_buy_slots_left() -> int:
            """How many BUY orders we can still place this tick."""
            max_buy_cfg = max(int(getattr(self, "max_active_buy_orders", self.grid_n_buy)), 0)
            max_buy = min(max_buy_cfg, int(desired_buy_cnt))
            if max_buy <= 0:
                return 0

            broker_open_buy = 0
            try:
                for o in list(getattr(broker, "open_orders", []) or []):
                    if not self._order_is_active(o):
                        continue
                    side = self._norm_side(_get(o, "side", default=None))
                    if side != "BUY":
                        continue
                    status_raw = _get(o, "status", "state", default=None)
                    if status_raw is not None:
                        st = str(status_raw).upper()
                        if st in {"FILLED", "CANCELED", "CANCELLED", "REJECTED", "EXPIRED", "CLOSED"}:
                            continue
                    broker_open_buy += 1
            except Exception:
                broker_open_buy = 0

            local_pending_buy = len(self.pending_buy_levels)
            active_buy = broker_open_buy if broker_open_buy > local_pending_buy else local_pending_buy
            left = max_buy - active_buy
            logger.debug(
                "ACTIVE_BUY_SLOTS | broker_open_buy={} pending_buy={} desired_buy_cnt={} max_buy={} left={}",
                broker_open_buy, local_pending_buy, desired_buy_cnt, max_buy, left
            )
            return left if left > 0 else 0

        def _free_base_for_new_sells() -> Decimal:
            try:
                base_total_now = _d(self._broker_base_total(broker))
            except Exception:
                base_total_now = Decimal("0")
            try:
                reserved_now = _d(self._reserved_base_in_open_sells(broker))
            except Exception:
                reserved_now = Decimal("0")
            free_now = base_total_now - reserved_now
            if free_now < 0:
                free_now = Decimal("0")
            if self.step_size and self.step_size > 0:
                free_now = _round_step(free_now, self.step_size)
            return free_now

        

        # 1) Ensure SELLs for aggregated open-lot targets (one order per price level).
        active_sells_by_price: dict[str, list[dict[str, Any]]] = {}
        try:
            for meta in self._broker_active_strategy_orders(broker):
                if meta.get("side") != "SELL":
                    continue
                px = _d(meta.get("price", Decimal("0")))
                if px <= 0:
                    continue
                active_sells_by_price.setdefault(self._k(px), []).append(meta)
        except Exception:
            active_sells_by_price = {}

        eps = (self.step_size * Decimal("2")) if self.step_size and self.step_size > 0 else Decimal("0")

        for sell_px_place, sell_qty in sell_targets:
            if sell_qty <= 0:
                continue
            if sell_qty < self.min_qty:
                self.min_notional_blocked += 1
                continue
            if (sell_qty * sell_px_place) < self.min_notional:
                self.min_notional_blocked += 1
                continue

            key = self._k(sell_px_place)
            existing = active_sells_by_price.get(key, [])
            existing_qty = Decimal("0")
            for row in existing:
                existing_qty += _d(row.get("qty", Decimal("0")))

            # If there is no visible SELL on this level and free base is currently below
            # full target qty, skip placement now. This avoids partial/duplicate churn while
            # broker snapshots converge after recent fills/cancels.
            if not existing:
                free_now = _free_base_for_new_sells()
                if free_now + eps < sell_qty:
                    self.pending_sell_levels[key] = sell_px_place
                    self._pending_sell_seen_at[key] = sync_now_ts
                    continue

            qty_delta = sell_qty - existing_qty
            tiny_unsellable_gap = False
            if qty_delta > 0:
                gap_qty = _round_step(qty_delta, self.step_size) if self.step_size > 0 else qty_delta
                if gap_qty <= 0:
                    tiny_unsellable_gap = True
                elif self.min_qty > 0 and gap_qty < self.min_qty:
                    tiny_unsellable_gap = True
                elif self.min_notional > 0 and (gap_qty * sell_px_place) < self.min_notional:
                    tiny_unsellable_gap = True

            # If we still have only local pending marker and broker snapshot has no SELL row yet,
            # avoid duplicate placement in this sync pass.
            if key in self.pending_sell_levels and not existing:
                self.duplicate_pending_skips += 1
                self.duplicate_place_skips += 1
                if (self.duplicate_pending_skips - self._dup_skip_log_last_sell) >= self._dup_skip_log_every:
                    self._dup_skip_log_last_sell = self.duplicate_pending_skips
                    logger.debug(
                        "GRID_PLACE skip summary | side=SELL reason=pending_duplicate total_dup_pending={} total_dup_place={} sell_targets={} pending_sell={}",
                        self.duplicate_pending_skips,
                        self.duplicate_place_skips,
                        len(sell_targets),
                        len(self.pending_sell_levels),
                    )
                continue

            # One exact active SELL at this price means level is already covered.
            if len(existing) == 1 and (abs(existing_qty - sell_qty) <= eps or tiny_unsellable_gap):
                self.pending_sell_levels[key] = sell_px_place
                self._pending_sell_seen_at[key] = sync_now_ts
                self._sell_replace_cooldown_until_by_key.pop(key, None)
                continue

            # Rebuild any ambiguous SELL set on this price:
            # - quantity mismatch
            # - multiple active rows on one grid level (duplicates)
            if existing and (len(existing) > 1 or abs(existing_qty - sell_qty) > eps):
                # If existing SELL qty is below target but there is not enough currently free base
                # to top up this level, keep current order and wait for snapshots/fills to converge.
                if existing_qty < sell_qty:
                    needed = sell_qty - existing_qty
                    free_now = _free_base_for_new_sells()
                    if free_now + eps < needed:
                        self.pending_sell_levels[key] = sell_px_place
                        self._pending_sell_seen_at[key] = sync_now_ts
                        continue

                cooldown_until = self._sell_replace_cooldown_until_by_key.get(key)
                if cooldown_until is not None:
                    cooldown_utc = cooldown_until if cooldown_until.tzinfo is not None else cooldown_until.replace(tzinfo=timezone.utc)
                    if sync_now_ts < cooldown_utc:
                        continue
                    self._sell_replace_cooldown_until_by_key.pop(key, None)

                cancelled_here = 0
                for meta in existing:
                    oid_s = str(meta.get("order_id") or "").strip()
                    coid_s = str(meta.get("client_order_id") or "").strip()
                    if self._cancel_order_refs_best_effort(broker, order_id=oid_s, client_order_id=coid_s):
                        cancelled_here += 1
                    for ref in (oid_s, coid_s):
                        if not ref:
                            continue
                        self._sell_level_by_order_ref.pop(ref, None)
                        self._planned_qty_by_order_ref.pop(ref, None)
                        self._fill_accum_by_order_ref.pop(ref, None)
                if cancelled_here > 0:
                    logger.warning(
                        "GRID_SYNC sell_price_replace | symbol={} px={} cancelled={} existing_qty={} target_qty={} reason={}",
                        self.symbol,
                        sell_px_place,
                        cancelled_here,
                        existing_qty,
                        sell_qty,
                        ("duplicate_rows" if len(existing) > 1 else "qty_mismatch"),
                    )

                # Do not place replacement in the same sync pass:
                # broker cancels are async and balance/order snapshots may still be stale.
                # Keep this level pending and retry on a later tick after a short cooldown.
                self.pending_sell_levels[key] = sell_px_place
                self._pending_sell_seen_at[key] = sync_now_ts
                retry_cooldown_sec = int(getattr(self, "_sell_replace_retry_cooldown_seconds", 5))
                if retry_cooldown_sec > 0:
                    self._sell_replace_cooldown_until_by_key[key] = sync_now_ts + timedelta(seconds=retry_cooldown_sec)
                else:
                    self._sell_replace_cooldown_until_by_key.pop(key, None)
                active_sells_by_price.pop(key, None)
                continue

            # If exact signature is present, reuse it and avoid duplicate placement.
            sell_sig = self._order_sig("SELL", sell_qty, sell_px_place)
            if broker_open_sig_counts.get(sell_sig, 0) > 0:
                broker_open_sig_counts[sell_sig] = int(broker_open_sig_counts.get(sell_sig, 0)) - 1
                self.pending_sell_levels[key] = sell_px_place
                self._pending_sell_seen_at[key] = sync_now_ts
                self._sell_replace_cooldown_until_by_key.pop(key, None)
                continue

            logger.trace(
                "GRID_PLACE try | side=SELL px={} qty={} notional={} key={}",
                sell_px_place,
                sell_qty,
                (sell_qty * sell_px_place),
                key,
            )
            ok = self._place_limit_safe(broker, side="SELL", qty=sell_qty, limit_price=sell_px_place)
            logger.trace(
                "GRID_PLACE result | side=SELL px={} qty={} ok={}",
                sell_px_place,
                sell_qty,
                ok,
            )
            if ok:
                self.pending_sell_levels[key] = sell_px_place
                self._pending_sell_seen_at[key] = sync_now_ts
                self._sell_replace_cooldown_until_by_key.pop(key, None)
                self._refresh_broker_state_after_fill(broker)
            else:
                logger.warning(
                    "GRID_PLACE failed | side=SELL px={} qty={} free_base={} err={}",
                    sell_px_place,
                    sell_qty,
                    self._broker_base_qty(broker),
                    self._last_place_limit_error,
                )

        # If we don't have a bid/ask snapshot, we cannot safely place the BUY ladder.
        # (SELL exits above were handled already.)
        if self.last_bid is None or self.last_ask is None:
            return

        # Freeze BUY ladder while strategy-tracked base is not fully covered by open SELL orders.
        try:
            tracked_open_base = Decimal("0")
            for lot in self.open_lots:
                try:
                    tracked_open_base += _d(getattr(lot, "qty", 0))
                except Exception:
                    continue
            base_total_now = _round_step(tracked_open_base, self.step_size) if self.step_size > 0 else tracked_open_base
            reserved_sell_now = _round_step(self._reserved_base_in_open_sells(broker), self.step_size) if self.step_size > 0 else _d(self._reserved_base_in_open_sells(broker))
            uncovered_base = base_total_now - reserved_sell_now
            if uncovered_base > eps:
                # Do not freeze BUY ladder on unsellable base dust:
                # if uncovered base cannot pass exchange SELL filters (min_qty/min_notional),
                # blocking BUYs would deadlock the grid with no actionable exit.
                uncovered_for_check = _round_step(uncovered_base, self.step_size) if self.step_size > 0 else uncovered_base
                sell_ref_px = self.last_bid if (self.last_bid is not None and self.last_bid > 0) else (self.last_mid or Decimal("0"))
                dust_unsellable = False
                if uncovered_for_check <= 0:
                    dust_unsellable = True
                elif self.min_qty > 0 and uncovered_for_check < self.min_qty:
                    dust_unsellable = True
                elif sell_ref_px > 0 and self.min_notional > 0 and (uncovered_for_check * sell_ref_px) < self.min_notional:
                    dust_unsellable = True

                if not dust_unsellable:
                    now_ts = sync_now_ts
                    last_ts = self._last_buy_freeze_log_ts
                    should_log = False
                    if last_ts is None:
                        should_log = True
                    else:
                        last_utc = last_ts if last_ts.tzinfo is not None else last_ts.replace(tzinfo=timezone.utc)
                        if (now_ts - last_utc).total_seconds() >= int(getattr(self, "_buy_freeze_log_every_seconds", 5)):
                            should_log = True

                    if should_log:
                        self._last_buy_freeze_log_ts = now_ts
                        logger.warning(
                            "GRID_SYNC buy_freeze | symbol={} uncovered_base={} base_total={} reserved_sell={} sell_targets={}",
                            self.symbol,
                            uncovered_base,
                            base_total_now,
                            reserved_sell_now,
                            len(sell_targets),
                        )
                    return
        except Exception:
            pass

        # 2) Ensure BUY ladder
        buy_levels_sorted = sorted(self.buy_orders, reverse=True)
        buy_levels_sorted = buy_levels_sorted[: int(desired_buy_cnt)]
        for bp in buy_levels_sorted:
            if _active_buy_slots_left() <= 0:
                break
            if _total_slots_left() <= 0:
                break

            bp_place = _round_tick(_d(bp), self.tick_size)
            if bp_place <= 0:
                continue
            # Spend is a fraction of total equity, but it must gracefully degrade
            # to currently placeable cash and remaining BUY slots.
            spend_target = self._resolve_spend_quote(
                broker,
                desired_orders=max(1, _active_buy_slots_left()),
            )
            intended_spend = self._target_spend_quote_uncapped(broker)
            min_ratio = _d(getattr(self, "_buy_spend_degrade_min_ratio", Decimal("0")))
            if (
                intended_spend > 0
                and min_ratio > 0
                and spend_target < (intended_spend * min_ratio)
            ):
                self.cap_blocked += 1
                logger.debug(
                    "GRID_PLACE skip | side=BUY px={} reason=spend_degraded spend_target={} intended_spend={} min_ratio={}",
                    bp_place,
                    spend_target,
                    intended_spend,
                    min_ratio,
                )
                continue

            # If target spend is non-positive -> no order.
            if spend_target <= 0:
                self.cap_blocked += 1
                logger.debug(
                    "GRID_PLACE skip | side=BUY px={} reason=spend_target_non_positive spend_target={} total_capital_quote={}",
                    bp_place,
                    spend_target,
                    self._broker_quote_total(broker),
                )
                continue

            # Compute BUY qty from FULL spend_target (no downscaling).
            buy_fill_px = bp_place * (Decimal("1") + self.slippage_pct)
            if buy_fill_px <= 0:
                self.min_notional_blocked += 1
                logger.debug(
                    "GRID_PLACE skip | side=BUY px={} reason=invalid_buy_fill_px buy_fill_px={} slippage_pct={} spend_target={}",
                    bp_place,
                    buy_fill_px,
                    self.slippage_pct,
                    spend_target,
                )
                continue

            qty = _round_step(spend_target / buy_fill_px, self.step_size)
            if qty <= 0 or qty < self.min_qty:
                self.min_notional_blocked += 1
                logger.debug(
                    "GRID_PLACE skip | side=BUY px={} qty={} reason=min_qty_or_zero min_qty={} spend_target={} buy_fill_px={}",
                    bp_place,
                    qty,
                    self.min_qty,
                    spend_target,
                    buy_fill_px,
                )
                continue

            if (qty * bp_place) < self.min_notional:
                self.min_notional_blocked += 1
                logger.debug(
                    "GRID_PLACE skip | side=BUY px={} qty={} notional={} reason=min_notional min_notional={} spend_target={}",
                    bp_place,
                    qty,
                    (qty * bp_place),
                    self.min_notional,
                    spend_target,
                )
                continue

            key = self._k(bp_place)
            if key in self.pending_buy_levels:
                self.duplicate_pending_skips += 1
                self.duplicate_place_skips += 1
                if (self.duplicate_pending_skips - self._dup_skip_log_last_buy) >= self._dup_skip_log_every:
                    self._dup_skip_log_last_buy = self.duplicate_pending_skips
                    logger.debug(
                        "GRID_PLACE skip summary | side=BUY reason=pending_duplicate total_dup_pending={} total_dup_place={} buy_levels={} pending_buy={}",
                        self.duplicate_pending_skips,
                        self.duplicate_place_skips,
                        len(self.buy_orders),
                        len(self.pending_buy_levels),
                    )
                continue

            # Respect per-level cooldown after insufficient-balance rejects.
            now_ts = self.last_quote_ts or datetime.now(timezone.utc)
            reject_until = self._buy_reject_until_by_level.get(key)
            if reject_until is not None:
                reject_until_utc = reject_until if reject_until.tzinfo is not None else reject_until.replace(tzinfo=timezone.utc)
                now_ts = now_ts if now_ts.tzinfo is not None else now_ts.replace(tzinfo=timezone.utc)
                now_ts = now_ts.astimezone(timezone.utc)
                if now_ts < reject_until_utc:
                    self.cap_blocked += 1
                    continue
                self._buy_reject_until_by_level.pop(key, None)

            buy_sig = self._order_sig("BUY", qty, bp_place)
            if broker_open_sig_counts.get(buy_sig, 0) > 0:
                broker_open_sig_counts[buy_sig] = int(broker_open_sig_counts.get(buy_sig, 0)) - 1
                self.pending_buy_levels[key] = bp_place
                continue

            logger.trace(
                "GRID_PLACE try | side=BUY px={} qty={} notional={} key={} spend_target={} total_capital_quote={}",
                bp_place,
                qty,
                (qty * bp_place),
                key,
                spend_target,
                self._broker_quote_total(broker),
            )
            if _total_slots_left() <= 0:
                break

            ok = self._place_limit_safe(broker, side="BUY", qty=qty, limit_price=bp_place)       

            logger.trace(
                "GRID_PLACE result | side=BUY px={} qty={} ok={}",
                bp_place,
                qty,
                ok,
            )

            if ok:
                self.pending_buy_levels[key] = bp_place

                now_ts = self.last_quote_ts or datetime.now(timezone.utc)
                now_ts = now_ts if now_ts.tzinfo is not None else now_ts.replace(tzinfo=timezone.utc)
                self._pending_buy_seen_at[key] = now_ts

                self._buy_reject_until_by_level.pop(key, None)
                self._refresh_broker_state_after_fill(broker)    
            else:
                err = (self._last_place_limit_error or "").lower()
                if self._last_place_limit_side == "BUY" and ("insufficient" in err or "-2010" in err):
                    now_ts = self.last_quote_ts or datetime.now(timezone.utc)
                    now_ts = now_ts if now_ts.tzinfo is not None else now_ts.replace(tzinfo=timezone.utc)
                    now_ts = now_ts.astimezone(timezone.utc)
                    self._buy_reject_until_by_level[key] = now_ts + timedelta(seconds=self._buy_reject_cooldown_seconds)
                    self.cap_blocked += 1

    def _sync_core_balances_from_broker(self, broker: Any) -> None:
        """Keep GridCore balances synchronized with broker balances.

        Also seeds `initial_quote` once from the broker wallet if it has not been set yet,
        so profit-gate accounting reflects real starting capital in live/demo runs.
        """
        quote_cash = self._broker_quote_cash(broker)
        base_qty = self._broker_base_qty(broker)

        self._core.state.quote = quote_cash
        self._core.state.base = base_qty
        st = self._core.state
        try:
            if hasattr(st, "initial_quote"):
                initial_quote_now = _d(getattr(st, "initial_quote", Decimal("0")))
                if initial_quote_now <= 0:
                    quote_total = self._broker_quote_total(broker)
                    seed = quote_total if quote_total > 0 else quote_cash
                    if seed > 0:
                        st.initial_quote = seed
            if hasattr(st, "deposits_total"):
                deposits_now = _d(getattr(st, "deposits_total", Decimal("0")))
                if deposits_now < 0:
                    st.deposits_total = Decimal("0")
        except Exception:
            pass

    def _drop_excess_open_lots_from_base(self, broker: Any) -> None:
        """Drop stale strategy lots when tracked base exceeds broker base snapshot.

        This can happen when SELL fills were missed during restart/startup sync:
        strategy still thinks lots are open, but wallet base is already gone.
        """
        lots = list(self._core.state.open_lots or [])
        if not lots:
            self._drop_excess_candidate_since = None
            return

        eps = self.step_size if self.step_size and self.step_size > 0 else Decimal("0")
        tolerance = (eps * Decimal("2")) if eps > 0 else Decimal("0")

        now_ts = self.last_quote_ts or datetime.now(timezone.utc)
        if now_ts.tzinfo is None:
            now_ts = now_ts.replace(tzinfo=timezone.utc)
        else:
            now_ts = now_ts.astimezone(timezone.utc)

        # Don't trim lots while partial fill accumulators are active.
        if self._fill_accum_by_order_ref:
            return

        # Cooldown after any fill: wallet/base snapshots can be stale for several seconds.
        last_fill_ts = self._last_fill_ts
        if last_fill_ts is not None:
            last_fill_utc = last_fill_ts if last_fill_ts.tzinfo is not None else last_fill_ts.replace(tzinfo=timezone.utc)
            cooldown_sec = int(getattr(self, "_drop_excess_cooldown_after_fill_seconds", 15))
            if cooldown_sec > 0 and (now_ts - last_fill_utc).total_seconds() < cooldown_sec:
                return

        base_total_raw = self._broker_base_total(broker)
        base_total = _round_step(_d(base_total_raw), self.step_size) if eps > 0 else _d(base_total_raw)
        if base_total < 0:
            base_total = Decimal("0")

        tracked_open_base = Decimal("0")
        for lot in lots:
            try:
                tracked_open_base += _d(getattr(lot, "qty", 0))
            except Exception:
                continue
        if eps > 0:
            tracked_open_base = _round_step(tracked_open_base, self.step_size)

        if tracked_open_base <= (base_total + tolerance):
            self._drop_excess_candidate_since = None
            return

        # Require mismatch to persist for a short period before trimming lots.
        candidate_since = self._drop_excess_candidate_since
        if candidate_since is None:
            self._drop_excess_candidate_since = now_ts
            return

        candidate_utc = candidate_since if candidate_since.tzinfo is not None else candidate_since.replace(tzinfo=timezone.utc)
        confirm_sec = int(getattr(self, "_drop_excess_confirm_seconds", 8))
        if confirm_sec > 0 and (now_ts - candidate_utc).total_seconds() < confirm_sec:
            return

        excess = tracked_open_base - base_total
        removed_lots = 0
        reduced_lots = 0
        trimmed_qty = Decimal("0")

        # Newest lots are more likely to be stale after restart gaps.
        for idx in range(len(self._core.state.open_lots) - 1, -1, -1):
            if excess <= tolerance:
                break

            lot = self._core.state.open_lots[idx]
            lot_qty = _d(getattr(lot, "qty", 0))
            if lot_qty <= 0:
                continue

            if lot_qty <= (excess + tolerance):
                trimmed_qty += lot_qty
                excess -= lot_qty
                removed_lots += 1
                self._core.state.open_lots.pop(idx)
                continue

            new_qty = lot_qty - excess
            if eps > 0:
                new_qty = _round_step(new_qty, self.step_size)

            if new_qty <= 0:
                trimmed_qty += lot_qty
                excess -= lot_qty
                removed_lots += 1
                self._core.state.open_lots.pop(idx)
                continue

            sold_part = lot_qty - new_qty
            if sold_part <= 0:
                continue

            try:
                lot.cost_quote = _d(getattr(lot, "cost_quote", Decimal("0"))) * (new_qty / lot_qty)
            except Exception:
                pass
            lot.qty = new_qty
            trimmed_qty += sold_part
            excess -= sold_part
            reduced_lots += 1

        if removed_lots <= 0 and reduced_lots <= 0:
            return

        tracked_after = Decimal("0")
        for lot in self._core.state.open_lots:
            try:
                tracked_after += _d(getattr(lot, "qty", 0))
            except Exception:
                continue
        if eps > 0:
            tracked_after = _round_step(tracked_after, self.step_size)

        logger.warning(
            "GRID_RECONCILE drop_excess_open_lots | symbol={} removed_lots={} reduced_lots={} trimmed_qty={} tracked_before={} tracked_after={} base_total={}",
            self.symbol,
            removed_lots,
            reduced_lots,
            trimmed_qty,
            tracked_open_base,
            tracked_after,
            base_total,
        )
        self._drop_excess_candidate_since = None
        self._mirror_local_state_from_core()
        self._ensure_buy_ladder()

    def _recover_missing_open_lots_from_base(self, broker: Any) -> None:
        """Recover strategy lots when broker base is present but lots are missing.

        This protects live/demo runs from prior desyncs where open lots were dropped
        while base remained on the wallet, leaving the strategy with BUY-only ladder
        and no SELL exits.
        """
        # Lot synthesis from wallet balances is risky (external/locked base, snapshot lag).
        # Keep it fully opt-in.
        if not bool(getattr(self, "_enable_runtime_lot_recovery", False)):
            return

        now_ts = self.last_quote_ts or datetime.now(timezone.utc)
        if now_ts.tzinfo is None:
            now_ts = now_ts.replace(tzinfo=timezone.utc)
        else:
            now_ts = now_ts.astimezone(timezone.utc)

        # Avoid racing with still-finalizing partial fills, but do not block forever
        # on stale closed accumulators (can happen after reconnects/id reuse).
        if self._fill_accum_by_order_ref:
            try:
                self._finalize_fill_accumulators_from_broker(broker)
            except Exception:
                pass
        if self._fill_accum_by_order_ref:
            block_sec = int(getattr(self, "_recover_fill_accum_block_seconds", 15))
            has_open_acc = False
            has_fresh_closed_acc = False
            stale_closed_refs: list[str] = []
            for order_ref, acc in list(self._fill_accum_by_order_ref.items()):
                order_ref_s = str(order_ref).strip()
                if not order_ref_s:
                    continue
                if self._broker_order_ref_is_open(broker, order_ref_s):
                    has_open_acc = True
                    continue
                acc_ts = acc.get("ts") if isinstance(acc, dict) else None
                age_sec: Optional[float] = None
                if isinstance(acc_ts, datetime):
                    acc_utc = acc_ts if acc_ts.tzinfo is not None else acc_ts.replace(tzinfo=timezone.utc)
                    age_sec = (now_ts - acc_utc).total_seconds()
                if age_sec is None or age_sec < float(block_sec):
                    has_fresh_closed_acc = True
                else:
                    stale_closed_refs.append(order_ref_s)
            if has_open_acc or has_fresh_closed_acc:
                return
            if stale_closed_refs:
                for ref in list(self._fill_accum_by_order_ref.keys()):
                    self._fill_accum_by_order_ref.pop(ref, None)
                logger.warning(
                    "GRID_RECOVER cleared_stale_fill_accum | symbol={} refs={} block_s={}",
                    self.symbol,
                    len(stale_closed_refs),
                    block_sec,
                )

        last_fill_ts = self._last_fill_ts
        if last_fill_ts is not None:
            last_fill_utc = last_fill_ts if last_fill_ts.tzinfo is not None else last_fill_ts.replace(tzinfo=timezone.utc)
            cooldown_sec = int(getattr(self, "_lot_recovery_cooldown_after_fill_seconds", 30))
            if cooldown_sec > 0 and (now_ts - last_fill_utc).total_seconds() < cooldown_sec:
                return

        if self.last_mid is None or self.last_mid <= 0:
            return

        eps = self.step_size if self.step_size and self.step_size > 0 else Decimal("0")
        base_total = _round_step(self._broker_base_total(broker), self.step_size) if eps > 0 else _d(self._broker_base_total(broker))
        base_free = _round_step(self._broker_base_qty(broker), self.step_size) if eps > 0 else _d(self._broker_base_qty(broker))
        if base_total <= 0:
            return
        # Do not recover from locked base (e.g. exchange reserved / external constraints).
        if (base_total - base_free) > (eps * Decimal("2") if eps > 0 else Decimal("0")):
            return

        tracked_open_base = Decimal("0")
        for lot in self._core.state.open_lots:
            try:
                tracked_open_base += _d(getattr(lot, "qty", 0))
            except Exception:
                continue
        if eps > 0:
            tracked_open_base = _round_step(tracked_open_base, self.step_size)

        # If exchange already has active SELLs, do not synthesize new lots from that reserved base.
        reserved_in_open_sells = Decimal("0")
        try:
            reserved_in_open_sells = self._reserved_base_in_open_sells(broker)
            if eps > 0:
                reserved_in_open_sells = _round_step(reserved_in_open_sells, self.step_size)
        except Exception:
            reserved_in_open_sells = Decimal("0")

        # Untracked base on wallet (base not represented by strategy lots).
        # If broker has orphan SELLs not linked to lots, their reserved qty should not be recovered.
        orphan_reserved = reserved_in_open_sells - tracked_open_base
        if orphan_reserved < 0:
            orphan_reserved = Decimal("0")
        recover_qty = base_total - tracked_open_base - orphan_reserved
        if recover_qty <= (eps * Decimal("2") if eps > 0 else Decimal("0")):
            return

        ref_px = self.last_mid
        est_buy_price = ref_px / (Decimal("1") + self.grid_step_pct) if self.grid_step_pct > 0 else ref_px
        if est_buy_price <= 0:
            return
        est_sell_price = est_buy_price * (Decimal("1") + self.grid_step_pct)
        lot_qty = recover_qty
        if eps > 0:
            lot_qty = _round_step(lot_qty, self.step_size)
        if lot_qty < self.min_qty:
            return
        if (lot_qty * est_sell_price) < self.min_notional:
            return

        self._core.state.seq += 1
        lot = GridLot(
            buy_price=est_buy_price,
            sell_price=est_sell_price,
            qty=lot_qty,
            cost_quote=(lot_qty * est_buy_price) * (Decimal("1") + self.maker_fee_rate),
            open_seq=self._core.state.seq,
        )
        self._core.state.open_lots.append(lot)
        self._lot_open_ts_by_key[self._lot_key(lot.sell_price, lot.qty)] = now_ts

        if lot_qty > 0:
            logger.warning(
                "GRID_RECOVER synthesized_open_lot | symbol={} recovered_qty={} base_total={} base_free={} tracked_open_base={} reserved_sell_base={}",
                self.symbol,
                lot_qty,
                base_total,
                base_free,
                tracked_open_base,
                reserved_in_open_sells,
            )
            self._mirror_local_state_from_core()
            self._ensure_buy_ladder()

    def _mirror_local_state_from_core(self) -> None:
        """Mirror GridCore ladder/lots into adapter fields used by broker sync + serialization."""
        self.anchor = self._core.state.anchor
        self.buy_orders = set(self._core.state.buy_levels)

        mirrored: list[GridLot] = []
        for lot in self._core.state.open_lots:
            key = self._lot_key(lot.sell_price, lot.qty)
            open_ts = self._lot_open_ts_by_key.get(key)
            mirrored.append(
                GridLot(
                    buy_price=lot.buy_price,
                    sell_price=lot.sell_price,
                    qty=lot.qty,
                    cost_quote=lot.cost_quote,
                    open_seq=lot.open_seq,
                )
            )
            if open_ts is not None:
                # `grid_types.GridLot` has no `open_ts`; keep timestamp only in adapter bridge map.
                self._lot_open_ts_by_key[key] = open_ts

        live_keys = {self._lot_key(l.sell_price, l.qty) for l in mirrored}
        self._lot_open_ts_by_key = {k: v for k, v in self._lot_open_ts_by_key.items() if k in live_keys}
        self.open_lots = mirrored


    def _lot_key(self, sell_price: Decimal, qty: Decimal) -> str:
        return f"{self._k(sell_price)}|{format(qty.quantize(Decimal('0.00000001')), 'f')}"

    def _fill_order_ref(self, fill: Any) -> Optional[str]:
        raw = _get(
            fill,
            "order_id",
            "orderId",
            "orderID",
            "client_order_id",
            "clientOrderId",
            "origClientOrderId",
            default=None,
        )
        if raw is None:
            return None
        s = str(raw).strip()
        return s or None

    def _fill_client_ref(self, fill: Any) -> Optional[str]:
        raw = _get(
            fill,
            "client_order_id",
            "clientOrderId",
            "origClientOrderId",
            default=None,
        )
        if raw is None:
            return None
        s = str(raw).strip()
        return s or None

    def _fill_exec_ref(self, fill: Any) -> Optional[str]:
        raw = _get(
            fill,
            "trade_id",
            "tradeId",
            "execution_id",
            "executionId",
            "exec_id",
            "execId",
            "id",
            default=None,
        )
        if raw is None:
            return None
        order_ref = self._fill_order_ref(fill) or ""
        qty_raw = _get(fill, "qty", "quantity", "origQty", "orig_qty", default=None)
        px_raw = _get(fill, "price", "px", default=None)
        return f"{order_ref}|{raw}|{qty_raw}|{px_raw}"

    def _accumulate_fill_fragment(
        self,
        order_ref: str,
        *,
        side: str,
        qty: Decimal,
        px: Decimal,
        fee_quote: Decimal,
        fee_base: Decimal,
        ts: Optional[datetime],
    ) -> None:
        if qty <= 0 or px <= 0:
            return
        quote_sum = qty * px
        cur = self._fill_accum_by_order_ref.get(order_ref)
        if cur is None:
            self._fill_accum_by_order_ref[order_ref] = {
                "side": side,
                "qty": qty,
                "quote_sum": quote_sum,
                "fee_quote": fee_quote,
                "fee_base": fee_base,
                "ts": ts,
            }
            return

        # If side changes unexpectedly for the same order id, reset accumulator to avoid corruption.
        if str(cur.get("side", "")) != side:
            self._fill_accum_by_order_ref[order_ref] = {
                "side": side,
                "qty": qty,
                "quote_sum": quote_sum,
                "fee_quote": fee_quote,
                "fee_base": fee_base,
                "ts": ts,
            }
            return

        cur["qty"] = _d(cur.get("qty", 0)) + qty
        cur["quote_sum"] = _d(cur.get("quote_sum", 0)) + quote_sum
        cur["fee_quote"] = _d(cur.get("fee_quote", 0)) + fee_quote
        cur["fee_base"] = _d(cur.get("fee_base", 0)) + fee_base
        if cur.get("ts") is None and ts is not None:
            cur["ts"] = ts


    def _peek_nearest_pending(self, pending: dict[str, Decimal], fill_px: Decimal) -> Optional[Decimal]:
        if not pending:
            return None
        best_level: Optional[Decimal] = None
        best_dist: Optional[Decimal] = None
        for _, lvl in pending.items():
            try:
                lvl_d = _d(lvl)
            except Exception:
                continue
            dist = abs(lvl_d - fill_px)
            if best_dist is None or dist < best_dist:
                best_dist = dist
                best_level = lvl_d
        return best_level


    def _calc_order_qty(self, level_price: Decimal, broker: Any = None) -> Decimal:
        spend_now = self._resolve_spend_quote(broker)
        if spend_now <= 0:
            return Decimal("0")
        buy_fill_px = level_price * (Decimal("1") + self.slippage_pct)
        if buy_fill_px <= 0:
            return Decimal("0")
        qty = _round_step(spend_now / buy_fill_px, self.step_size)
        return qty if qty > 0 else Decimal("0")

    def _reserved_quote_in_pending_buys(self, broker: Any = None) -> Decimal:
        """Best-effort estimate of quote reserved by currently tracked pending BUY orders.

        If broker open orders are available, prefer summing actual BUY qty*price from broker state
        (more accurate and avoids recursive estimation when spend is percent-based).
        """
        if broker is not None and hasattr(broker, "open_orders"):
            try:
                total = Decimal("0")
                for o in list(getattr(broker, "open_orders")):
                    if not self._order_is_active(o):
                        continue
                    side = self._norm_side(_get(o, "side", default=None))
                    if side != "BUY":
                        continue

                    px_raw = _get(o, "limit_price", "price", "limit", "px", default=None)
                    qty_raw = _get(o, "qty", "quantity", "origQty", "orig_qty", default=None)
                    if px_raw is None or qty_raw is None:
                        continue

                    px = _d(px_raw)
                    qty = _d(qty_raw)
                    if px <= 0 or qty <= 0:
                        continue
                    total += (px * qty)
                return total
            except Exception:
                pass

        total = Decimal("0")
        for px in self.pending_buy_levels.values():
            try:
                price = _d(px)
            except Exception:
                continue
            if price <= 0:
                continue
            qty = self._calc_order_qty(price, broker=None)
            if qty <= 0:
                continue
            total += qty * price
        return total

    def _resolve_spend_quote(self, broker: Any = None, desired_orders: Optional[int] = None) -> Decimal:
        if self.spend_pct_of_quote is None:
            return self.spend_quote
        if broker is None:
            return self.spend_quote
        try:
            desired_orders_i = int(desired_orders) if desired_orders is not None else 1
        except Exception:
            desired_orders_i = 1
        if desired_orders_i <= 0:
            desired_orders_i = 1

        target_spend_net = self._target_spend_quote_uncapped(broker)
        if target_spend_net <= 0:
            return Decimal("0")

        # Use free quote cash only; execution adapter already exposes free balance here.
        available_quote = self._broker_quote_cash(broker)
        if available_quote < 0:
            available_quote = Decimal("0")

        if available_quote <= 0:
            return Decimal("0")

        gross_to_net = Decimal("1") + self.maker_fee_rate
        if gross_to_net <= 0:
            gross_to_net = Decimal("1")

        max_spend_net_placeable = available_quote / gross_to_net
        if max_spend_net_placeable <= 0:
            return Decimal("0")

        if self._strict_spend_pct:
            tol = target_spend_net * Decimal("0.000001")
            if tol < Decimal("0.00000001"):
                tol = Decimal("0.00000001")
            if max_spend_net_placeable + tol < target_spend_net:
                return Decimal("0")
            return target_spend_net

        per_order_cap = max_spend_net_placeable / Decimal(desired_orders_i)
        if per_order_cap <= 0:
            return Decimal("0")

        if target_spend_net > per_order_cap:
            return per_order_cap
        return target_spend_net

    def _spend_capital_basis_in_quote(self, broker: Any = None) -> Decimal:
        """Prefer contributed-capital basis for percent spend sizing when available."""
        if broker is None:
            return Decimal("0")

        try:
            st = self._core.state
            initial_quote = _d(getattr(st, "initial_quote", Decimal("0")))
            deposits_total = _d(getattr(st, "deposits_total", Decimal("0")))
            basis = initial_quote + deposits_total
            if basis > 0:
                return basis
        except Exception:
            pass

        manual_basis = _d(getattr(self, "manual_deposits_total", Decimal("0")))
        if manual_basis > 0:
            return manual_basis

        return self._total_capital_in_quote(broker)

    def _target_spend_quote_uncapped(self, broker: Any = None) -> Decimal:
        """Configured per-order spend before free-quote capping."""
        if self.spend_pct_of_quote is None:
            return self.spend_quote
        if broker is None:
            return Decimal("0")
        basis_quote = self._spend_capital_basis_in_quote(broker)
        if basis_quote <= 0:
            return Decimal("0")
        target = basis_quote * self.spend_pct_of_quote
        return target if target > 0 else Decimal("0")


    def _reserved_base_in_open_sells(self, broker: Any) -> Decimal:
        """Best-effort base qty already reserved in broker SELL open orders."""
        raw_orders = getattr(broker, "open_orders", None)
        if raw_orders is None:
            return Decimal("0")

        try:
            orders_iter = list(raw_orders)
        except Exception:
            return Decimal("0")

        total = Decimal("0")
        for o in orders_iter:
            side = self._norm_side(_get(o, "side", default=None))
            if side != "SELL":
                continue

            status_raw = _get(o, "status", "state", default=None)
            if status_raw is not None:
                if not self._order_is_active(o):
                    continue

            qty_raw = _get(o, "qty", "quantity", "origQty", "orig_qty", default=None)
            if qty_raw is None:
                continue
            try:
                qty = _d(qty_raw)
            except Exception:
                continue
            if qty <= 0:
                continue
            total += qty

        return total

    def _calc_placeable_sell_qty(self, broker: Any, desired_qty: Decimal) -> Decimal:
        """Cap SELL qty to currently free base, but tolerate stale broker balances.

        In demo/live polling mode we sometimes receive fills before local broker balances are
        refreshed. In that short window `base_balance` can still look like zero, which blocks
        SELL placement right after a BUY fill. If strategy lots indicate more base than the broker
        snapshot currently reports, we treat the broker balance as stale and allow placement using
        the strategy lot qty (exchange-side validation still protects from true oversell).
        """
        want = _d(desired_qty)
        if want <= 0:
            return Decimal("0")

        base_total = self._broker_base_total(broker)
        base_reserved = self._reserved_base_in_open_sells(broker)
        base_free = base_total - base_reserved

        # Detect stale broker balance snapshot (fills already processed by strategy, balances not yet refreshed).
        strategy_open_base = Decimal("0")
        try:
            for lot in self.open_lots:
                strategy_open_base += _d(getattr(lot, "qty", 0))
        except Exception:
            strategy_open_base = Decimal("0")

        stale_balance_snapshot = strategy_open_base > (base_total + (self.step_size * Decimal("2")))
        if stale_balance_snapshot:
            qty = _round_step(want, self.step_size)
            if qty < 0:
                return Decimal("0")
            return qty

        if base_free <= 0:
            return Decimal("0")

        qty = want if want <= base_free else base_free
        qty = _round_step(qty, self.step_size)
        if qty < 0:
            return Decimal("0")
        return qty

    def _can_add_base(self, broker: Any) -> bool:
        spend_target = self._target_spend_quote_uncapped(broker)

        if spend_target <= 0:
            return False

        available_quote = self._broker_quote_cash(broker)
        if available_quote < 0:
            available_quote = Decimal("0")

        need = spend_target * (Decimal("1") + self.maker_fee_rate)
        return available_quote >= need

    def _pick_sell_lot_index(self, sell_level: Decimal, fill_px: Decimal) -> Optional[int]:
        # Prefer exact/nearest sell_price match among lots that are actually eligible to be sold.
        if not self.open_lots:
            return None

        candidates: list[tuple[int, Decimal]] = []
        for i, lot in enumerate(self.open_lots):
            if lot.qty <= 0:
                continue
            if self.grid_sell_only_above_cost:
                proceeds_net = (lot.qty * lot.sell_price) * (Decimal("1") - self.maker_fee_rate)
                if proceeds_net <= lot.cost_quote:
                    continue
            if self.grid_min_sell_markup_pct > 0:
                if lot.sell_price < lot.buy_price * (Decimal("1") + self.grid_min_sell_markup_pct):
                    continue
            candidates.append((i, abs(lot.sell_price - sell_level)))

        if candidates:
            candidates.sort(key=lambda x: x[1])
            return candidates[0][0]

        # Fallback: choose nearest by actual fill price if all candidates were filtered out.
        best_i: Optional[int] = None
        best_dist: Optional[Decimal] = None
        for i, lot in enumerate(self.open_lots):
            dist = abs(lot.sell_price - fill_px)
            if best_dist is None or dist < best_dist:
                best_dist = dist
                best_i = i
        return best_i

    def _sync_fee_rates_from_runtime(self, broker: Any) -> None:
        """Best-effort fee sync from runtime execution/broker/account snapshots.

        Supported inputs (duck-typed):
        - execution.account_commission_rates = {"maker": "0.001", "taker": "0.001"}
        - execution.maker_fee_rate / execution.taker_fee_rate (already normalized fractions)
        - broker.maker_fee_rate / broker.taker_fee_rate
        - Binance raw integer commission units (e.g. 10 => 0.001) via makerCommission/takerCommission
        """
        candidates = []

        ex = getattr(self, "execution", None)
        if ex is not None:
            candidates.append(ex)

        if broker is not None:
            candidates.append(broker)

        for src in candidates:
            # Preferred source: normalized commissionRates payload from /api/v3/account
            rates = getattr(src, "account_commission_rates", None)
            if isinstance(rates, dict):
                maker = self._coerce_fee_rate(rates.get("maker"))
                taker = self._coerce_fee_rate(rates.get("taker"))
                changed = False
                if maker is not None and maker >= 0:
                    self.maker_fee_rate = maker
                    changed = True
                if taker is not None and taker >= 0:
                    self.taker_fee_rate = taker
                    changed = True
                if changed:
                    continue

            # Direct normalized fee attrs
            maker = self._coerce_fee_rate(getattr(src, "maker_fee_rate", None))
            taker = self._coerce_fee_rate(getattr(src, "taker_fee_rate", None))
            changed = False
            if maker is not None and maker >= 0:
                self.maker_fee_rate = maker
                changed = True
            if taker is not None and taker >= 0:
                self.taker_fee_rate = taker
                changed = True
            if changed:
                continue

            # Binance raw account fields (integer-like units, e.g. 10 -> 0.001)
            maker_raw = self._coerce_binance_commission_units(getattr(src, "makerCommission", None))
            taker_raw = self._coerce_binance_commission_units(getattr(src, "takerCommission", None))
            changed = False
            if maker_raw is not None and maker_raw >= 0:
                self.maker_fee_rate = maker_raw
                changed = True
            if taker_raw is not None and taker_raw >= 0:
                self.taker_fee_rate = taker_raw
                changed = True
            if changed:
                continue
            
        # --- also sync symbol filters from BinanceDemoExecution ---
        for src in candidates:
            try:
                tick = _get(src, "tick_size", "price_tick_size", "_price_tick_size", default=None)
                if tick is not None:
                    t = _d(tick)
                    if t > 0:
                        self.tick_size = t
            except Exception:
                pass

            try:
                step = _get(src, "step_size", "qty_step_size", "_qty_step_size", default=None)
                if step is not None:
                    s = _d(step)
                    if s > 0:
                        self.step_size = s
            except Exception:
                pass

            try:
                mnq = _get(src, "min_qty", "_min_qty", default=None)
                if mnq is not None:
                    q = _d(mnq)
                    if q > 0:
                        self.min_qty = q
            except Exception:
                pass

            try:
                mnn = _get(src, "min_notional", "_min_notional", default=None)
                if mnn is not None:
                    n = _d(mnn)
                    if n > 0:
                        self.min_notional = n
            except Exception:
                pass

    @staticmethod
    def _coerce_fee_rate(v: Any) -> Optional[Decimal]:
        if v is None:
            return None
        try:
            d = _d(v)
        except Exception:
            return None
        if d < 0:
            return None

        # If value is clearly in percent units (e.g. 0.1 for 0.1%), convert to fraction.
        if d > 1:
            # Could be Binance integer commission units too (handled elsewhere), but
            # this fallback keeps weird sources usable.
            return d / Decimal("100")
        return d

    @staticmethod
    def _coerce_binance_commission_units(v: Any) -> Optional[Decimal]:
        """Convert Binance /account makerCommission/takerCommission to normalized fraction.

        Binance spot often returns integer-like units where 10 means 0.1% (0.001).
        That is basis points scaled by 10, so divide by 10000.
        """
        if v is None:
            return None
        try:
            d = _d(v)
        except Exception:
            return None
        if d < 0:
            return None
        return d / Decimal("10000")

    # ---------------------------
    # Diagnostics / utils
    # ---------------------------
    def _reconcile_pending_levels_from_broker(self, broker: Any) -> None:
        """Best-effort reconciliation of local pending maps with broker open orders.

        This is a live-safety bridge for restarts and transient desyncs: we rebuild local
        `pending_buy_levels` / `pending_sell_levels` from broker open orders so the adapter
        does not spam duplicate placements after resume.
        """
        raw_orders = getattr(broker, "open_orders", None)
        if raw_orders is None:
            return

        try:
            orders_iter = list(raw_orders)
        except Exception:
            return

        broker_buy: dict[str, Decimal] = {}
        broker_sell_prices: dict[str, Decimal] = {}

        for o in orders_iter:
            side = self._norm_side(_get(o, "side", default=None))
            if side not in ("BUY", "SELL"):
                continue

            if not self._order_is_active(o):
                continue

            px_raw = _get(o, "limit_price", "price", "limit", "px", default=None)
            if px_raw is None:
                continue
            try:
                px = _d(px_raw)
            except Exception:
                continue
            if px <= 0:
                continue

            if side == "SELL":
                # For SELL we keep broker snapshot keyed by PRICE only.
                # Later we fan out that price to local composite keys "price|open_seq".
                key = self._k(px)
            else:
                key = self._k(px)

            if side == "BUY":
                broker_buy[key] = px
            else:
                broker_sell_prices[key] = px

        # SELL pending keys are normalized prices (one SELL target per price).
        desired_sell_keys: set[str] = set()
        for lot in self.open_lots:
            if getattr(lot, "qty", Decimal("0")) <= 0:
                continue
            px = _round_tick(_d(getattr(lot, "sell_price", Decimal("0"))), self.tick_size)
            if px <= 0:
                continue
            desired_sell_keys.add(self._k(px))

        # IMPORTANT:
        # Broker open_orders snapshot can lag right after placement.
        # If we hard-replace local pending maps from broker snapshot, we can "forget"
        # a just-placed order and place it again on the next sync tick.
        # So we MERGE broker-visible orders into local pending instead of replacing.

        # BUY keys are simple normalized prices
        desired_buy_keys: set[str] = set()
        for px_raw in self.buy_orders:
            px = _round_tick(_d(px_raw), self.tick_size)
            if px <= 0:
                continue
            desired_buy_keys.add(self._k(px))

        now_ts = self.last_quote_ts or datetime.now(timezone.utc)
        now_ts = now_ts if now_ts.tzinfo is not None else now_ts.replace(tzinfo=timezone.utc)
        buy_grace = timedelta(seconds=int(getattr(self, "_pending_buy_reconcile_grace_seconds", 120)))
        sell_grace = timedelta(seconds=int(getattr(self, "_pending_sell_reconcile_grace_seconds", 15)))

        # брокерные BUY считаем “увиденными сейчас”
        for k in broker_buy.keys():
            self._pending_buy_seen_at[k] = now_ts

        merged_buy: dict[str, Decimal] = {}

        # 1) берём локальные pending, но только если они ещё желаемые и не устарели
        for k, v in (self.pending_buy_levels or {}).items():
            if k not in desired_buy_keys:
                continue
            if k in broker_buy:
                merged_buy[k] = broker_buy[k]
                continue
            seen_at = self._pending_buy_seen_at.get(k)
            if seen_at is not None and (now_ts - seen_at) <= buy_grace:
                merged_buy[k] = v
                continue
            # stale -> дропаем
            self._pending_buy_seen_at.pop(k, None)

        # 2) добавляем все broker-visible BUY (которые нужны стратегии)
        for k, v in broker_buy.items():
            if k in desired_buy_keys:
                merged_buy[k] = v

        merged_sell: dict[str, Decimal] = {}

        for k in broker_sell_prices.keys():
            self._pending_sell_seen_at[k] = now_ts

        for k, v in (self.pending_sell_levels or {}).items():
            if k not in desired_sell_keys:
                continue
            if k in broker_sell_prices:
                merged_sell[k] = broker_sell_prices[k]
                continue
            seen_at = self._pending_sell_seen_at.get(k)
            if seen_at is not None and (now_ts - seen_at) <= sell_grace:
                merged_sell[k] = v
                continue
            self._pending_sell_seen_at.pop(k, None)

        for k, v in broker_sell_prices.items():
            if k in desired_sell_keys:
                merged_sell[k] = v

        self.pending_sell_levels = merged_sell

        self._buy_reject_until_by_level = {k: v for k, v in self._buy_reject_until_by_level.items() if k in desired_buy_keys}


        self.pending_buy_levels = merged_buy
        self._pending_buy_seen_at = {k: t for k, t in self._pending_buy_seen_at.items() if k in desired_buy_keys}
        self._pending_sell_seen_at = {k: t for k, t in self._pending_sell_seen_at.items() if k in desired_sell_keys}
        self._sell_replace_cooldown_until_by_key = {
            k: t for k, t in self._sell_replace_cooldown_until_by_key.items() if k in desired_sell_keys
        }

    def _order_sig(self, side: str, qty: Decimal, price: Decimal) -> str:
        s = self._norm_side(side)
        q = _d(qty)
        p = _d(price)
        if self.step_size and self.step_size > 0:
            q = _round_step(q, self.step_size)
        if self.tick_size and self.tick_size > 0:
            p = _round_tick(p, self.tick_size)
        q_s = format(q.quantize(Decimal("0.00000001")), "f")
        p_s = format(p.quantize(Decimal("0.00000001")), "f")
        return f"{s}|{q_s}|{p_s}"

    def _broker_open_order_sig_counts(self, broker: Any) -> dict[str, int]:
        sig_counts: dict[str, int] = {}
        raw = getattr(broker, "open_orders", None)
        if raw is None:
            return sig_counts
        try:
            orders = list(raw)
        except Exception:
            return sig_counts

        for o in orders:
            try:
                # только биржевые (не LOCAL placeholders)
                if not self._order_is_active(o):
                    continue

                st_raw = _get(o, "status", "state", default=None)
                if st_raw is not None:
                    st = str(st_raw).upper()
                    if st in {"FILLED", "CANCELED", "CANCELLED", "REJECTED", "EXPIRED", "CLOSED"}:
                        continue

                side = self._norm_side(_get(o, "side", default=None))
                if side not in {"BUY", "SELL"}:
                    continue

                px_raw = _get(o, "limit_price", "price", "limit", "px", default=None)
                qty_raw = _get(o, "qty", "quantity", "origQty", "orig_qty", default=None)
                if px_raw is None or qty_raw is None:
                    continue

                px = _d(px_raw)
                qty = _d(qty_raw)
                if px <= 0 or qty <= 0:
                    continue

                sig = self._order_sig(side, qty, px)
                sig_counts[sig] = int(sig_counts.get(sig, 0)) + 1
            except Exception:
                continue

        return sig_counts

    def _schedule_awaitable(self, maybe_awaitable: Any) -> None:
        try:
            import asyncio as _asyncio
            if _asyncio.iscoroutine(maybe_awaitable):
                try:
                    loop = _asyncio.get_running_loop()
                    loop.create_task(maybe_awaitable)
                except RuntimeError:
                    return
        except Exception:
            return

    def _broker_active_strategy_orders(self, broker: Any) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        raw = getattr(broker, "open_orders", None)
        if raw is None:
            return out
        try:
            orders = list(raw)
        except Exception:
            return out

        for o in orders:
            try:
                if not self._order_is_active(o):
                    continue
                if self._is_local_broker_order(o):
                    continue
                oid = _get(o, "order_id", "orderId", "id", default=None)
                coid = _get(o, "client_order_id", "clientOrderId", "origClientOrderId", default=None)
                oid_s = str(oid).strip() if oid is not None else ""
                coid_s = str(coid).strip() if coid is not None else ""
                # If we already finalized a fill for this exchange order id, treat broker snapshot row
                # as stale and exclude it from active-order math (slot rebalance, dedupe, etc.).
                if oid_s and oid_s in self._finalized_fill_order_refs:
                    continue
                side = self._norm_side(_get(o, "side", default=None))
                if side not in {"BUY", "SELL"}:
                    continue
                px = _d(_get(o, "limit_price", "price", "limit", "px", default=0))
                qty = _d(_get(o, "qty", "quantity", "origQty", "orig_qty", default=0))
                out.append(
                    {
                        "order": o,
                        "side": side,
                        "price": px,
                        "qty": qty,
                        "order_id": oid_s,
                        "client_order_id": coid_s,
                    }
                )
            except Exception:
                continue
        return out

    def _cancel_order_refs_best_effort(self, broker: Any, *, order_id: str = "", client_order_id: str = "") -> bool:
        oid_s = str(order_id or "").strip()
        coid_s = str(client_order_id or "").strip()
        if not oid_s and not coid_s:
            return False

        cancel_fns: list[Any] = []
        for name in ("cancel_order", "cancel", "cancel_limit", "cancel_open_order"):
            fn = getattr(broker, name, None)
            if callable(fn):
                cancel_fns.append(fn)
        if not cancel_fns:
            return False

        for fn in cancel_fns:
            if oid_s:
                try:
                    out = fn(order_id=oid_s, symbol=self.symbol)
                    self._schedule_awaitable(out)
                    return True
                except TypeError:
                    pass
                except Exception:
                    pass
                try:
                    out = fn(order_id=oid_s)
                    self._schedule_awaitable(out)
                    return True
                except TypeError:
                    pass
                except Exception:
                    pass
                try:
                    out = fn(oid_s)
                    self._schedule_awaitable(out)
                    return True
                except Exception:
                    pass

            if coid_s:
                try:
                    out = fn(client_order_id=coid_s, symbol=self.symbol)
                    self._schedule_awaitable(out)
                    return True
                except TypeError:
                    pass
                except Exception:
                    pass
                try:
                    out = fn(origClientOrderId=coid_s, symbol=self.symbol)
                    self._schedule_awaitable(out)
                    return True
                except TypeError:
                    pass
                except Exception:
                    pass
        return False

    def _dedupe_duplicate_buy_prices(self, broker: Any) -> None:
        """Cancel extra BUY orders that share the same price level."""
        if broker is None:
            return

        now_ts = self.last_quote_ts or datetime.now(timezone.utc)
        if now_ts.tzinfo is None:
            now_ts = now_ts.replace(tzinfo=timezone.utc)
        else:
            now_ts = now_ts.astimezone(timezone.utc)

        last_ts = self._last_buy_price_dedupe_ts
        if last_ts is not None:
            last_utc = last_ts if last_ts.tzinfo is not None else last_ts.replace(tzinfo=timezone.utc)
            if (now_ts - last_utc).total_seconds() < int(getattr(self, "_buy_price_dedupe_cooldown_seconds", 2)):
                return

        active = self._broker_active_strategy_orders(broker)
        if not active:
            return

        by_price: dict[str, list[dict[str, Any]]] = {}
        for meta in active:
            if meta.get("side") != "BUY":
                continue
            px = _d(meta.get("price", Decimal("0")))
            if px <= 0:
                continue
            by_price.setdefault(self._k(px), []).append(meta)

        to_cancel: list[dict[str, Any]] = []
        dup_levels = 0
        for _, items in by_price.items():
            if len(items) <= 1:
                continue
            dup_levels += 1
            # Keep one order with max qty, cancel the rest.
            sorted_items = sorted(
                items,
                key=lambda m: (
                    _d(m.get("qty", Decimal("0"))),
                    str(m.get("order_id") or m.get("client_order_id") or ""),
                ),
                reverse=True,
            )
            to_cancel.extend(sorted_items[1:])

        if not to_cancel:
            return

        cancelled_oids: set[str] = set()
        cancelled_coids: set[str] = set()
        cancelled_cnt = 0
        for meta in to_cancel:
            oid_s = str(meta.get("order_id") or "").strip()
            coid_s = str(meta.get("client_order_id") or "").strip()
            if not self._cancel_order_refs_best_effort(broker, order_id=oid_s, client_order_id=coid_s):
                continue

            cancelled_cnt += 1
            if oid_s:
                cancelled_oids.add(oid_s)
            if coid_s:
                cancelled_coids.add(coid_s)

            px = _d(meta.get("price", Decimal("0")))
            if px > 0:
                key = self._k(px)
                self.pending_buy_levels.pop(key, None)
                self._pending_buy_seen_at.pop(key, None)
                self._buy_reject_until_by_level.pop(key, None)

            for ref in (oid_s, coid_s):
                if not ref:
                    continue
                self._buy_level_by_order_ref.pop(ref, None)
                self._planned_qty_by_order_ref.pop(ref, None)
                self._fill_accum_by_order_ref.pop(ref, None)

        if cancelled_cnt <= 0:
            return

        self._last_buy_price_dedupe_ts = now_ts
        logger.warning(
            "GRID_SYNC buy_price_dedupe | symbol={} cancelled={} duplicate_levels={}",
            self.symbol,
            cancelled_cnt,
            dup_levels,
        )

        # Optimistically update local broker cache so same tick can re-place missing levels.
        try:
            raw_orders = getattr(broker, "open_orders", None)
            if raw_orders is not None:
                kept: list[Any] = []
                for o in list(raw_orders):
                    try:
                        if self._is_local_broker_order(o):
                            kept.append(o)
                            continue
                        oid = _get(o, "order_id", "orderId", "id", default=None)
                        coid = _get(o, "client_order_id", "clientOrderId", "origClientOrderId", default=None)
                        oid_s = str(oid).strip() if oid is not None else ""
                        coid_s = str(coid).strip() if coid is not None else ""
                        if oid_s and oid_s in cancelled_oids:
                            continue
                        if coid_s and coid_s in cancelled_coids:
                            continue
                        kept.append(o)
                    except Exception:
                        kept.append(o)
                broker.open_orders = kept
        except Exception:
            pass

        try:
            ro = getattr(broker, "refresh_open_orders", None)
            if callable(ro):
                self._schedule_awaitable(ro())
        except Exception:
            pass

    def _enforce_buy_ladder_cap(self, broker: Any, *, desired_buy_keys: set[str], max_buy: int) -> None:
        """Cancel BUY orders outside ladder and trim excess BUY count."""
        if broker is None or max_buy < 0:
            return

        now_ts = self.last_quote_ts or datetime.now(timezone.utc)
        if now_ts.tzinfo is None:
            now_ts = now_ts.replace(tzinfo=timezone.utc)
        else:
            now_ts = now_ts.astimezone(timezone.utc)

        last_ts = self._last_buy_ladder_prune_ts
        if last_ts is not None:
            last_utc = last_ts if last_ts.tzinfo is not None else last_ts.replace(tzinfo=timezone.utc)
            if (now_ts - last_utc).total_seconds() < int(getattr(self, "_buy_ladder_prune_cooldown_seconds", 2)):
                return

        active = self._broker_active_strategy_orders(broker)
        if not active:
            return

        buys: list[dict[str, Any]] = []
        for meta in active:
            if meta.get("side") != "BUY":
                continue
            px = _d(meta.get("price", Decimal("0")))
            if px <= 0:
                continue
            meta = dict(meta)
            meta["price_key"] = self._k(px)
            buys.append(meta)
        if not buys:
            return

        to_cancel: list[dict[str, Any]] = []
        for meta in buys:
            if desired_buy_keys and str(meta.get("price_key")) not in desired_buy_keys:
                to_cancel.append(meta)

        # If still above cap after removing non-ladder buys, keep highest BUY levels.
        if max_buy >= 0:
            survivors = [m for m in buys if m not in to_cancel]
            if len(survivors) > max_buy:
                survivors_sorted = sorted(survivors, key=lambda m: _d(m.get("price", Decimal("0"))), reverse=True)
                keep_ids: set[str] = set()
                for row in survivors_sorted[:max_buy]:
                    oid_s = str(row.get("order_id") or "").strip()
                    coid_s = str(row.get("client_order_id") or "").strip()
                    if oid_s:
                        keep_ids.add(f"oid:{oid_s}")
                    if coid_s:
                        keep_ids.add(f"coid:{coid_s}")
                for row in survivors_sorted[max_buy:]:
                    oid_s = str(row.get("order_id") or "").strip()
                    coid_s = str(row.get("client_order_id") or "").strip()
                    marker_oid = f"oid:{oid_s}" if oid_s else ""
                    marker_coid = f"coid:{coid_s}" if coid_s else ""
                    if marker_oid and marker_oid in keep_ids:
                        continue
                    if marker_coid and marker_coid in keep_ids:
                        continue
                    to_cancel.append(row)

        if not to_cancel:
            return

        seen_cancel_refs: set[str] = set()
        cancelled_oids: set[str] = set()
        cancelled_coids: set[str] = set()
        cancelled_cnt = 0
        for meta in to_cancel:
            oid_s = str(meta.get("order_id") or "").strip()
            coid_s = str(meta.get("client_order_id") or "").strip()
            uniq = f"{oid_s}|{coid_s}"
            if uniq in seen_cancel_refs:
                continue
            seen_cancel_refs.add(uniq)

            if not self._cancel_order_refs_best_effort(broker, order_id=oid_s, client_order_id=coid_s):
                continue

            cancelled_cnt += 1
            if oid_s:
                cancelled_oids.add(oid_s)
            if coid_s:
                cancelled_coids.add(coid_s)

            px = _d(meta.get("price", Decimal("0")))
            if px > 0:
                key = self._k(px)
                self.pending_buy_levels.pop(key, None)
                self._pending_buy_seen_at.pop(key, None)
                self._buy_reject_until_by_level.pop(key, None)

            for ref in (oid_s, coid_s):
                if not ref:
                    continue
                self._buy_level_by_order_ref.pop(ref, None)
                self._planned_qty_by_order_ref.pop(ref, None)
                self._fill_accum_by_order_ref.pop(ref, None)

        if cancelled_cnt <= 0:
            return

        self._last_buy_ladder_prune_ts = now_ts
        logger.warning(
            "GRID_SYNC buy_ladder_prune | symbol={} cancelled={} max_buy={} desired_levels={}",
            self.symbol,
            cancelled_cnt,
            max_buy,
            len(desired_buy_keys),
        )

        try:
            raw_orders = getattr(broker, "open_orders", None)
            if raw_orders is not None:
                kept: list[Any] = []
                for o in list(raw_orders):
                    try:
                        if self._is_local_broker_order(o):
                            kept.append(o)
                            continue
                        oid = _get(o, "order_id", "orderId", "id", default=None)
                        coid = _get(o, "client_order_id", "clientOrderId", "origClientOrderId", default=None)
                        oid_s = str(oid).strip() if oid is not None else ""
                        coid_s = str(coid).strip() if coid is not None else ""
                        if oid_s and oid_s in cancelled_oids:
                            continue
                        if coid_s and coid_s in cancelled_coids:
                            continue
                        kept.append(o)
                    except Exception:
                        kept.append(o)
                broker.open_orders = kept
        except Exception:
            pass

        try:
            ro = getattr(broker, "refresh_open_orders", None)
            if callable(ro):
                self._schedule_awaitable(ro())
        except Exception:
            pass

    def _trim_buy_orders_for_sell_priority(self, broker: Any, *, desired_sell_cnt: int, max_total: int) -> None:
        if broker is None or max_total <= 0:
            return

        now_ts = self.last_quote_ts or datetime.now(timezone.utc)
        if now_ts.tzinfo is None:
            now_ts = now_ts.replace(tzinfo=timezone.utc)
        else:
            now_ts = now_ts.astimezone(timezone.utc)

        # Right after fill handling we can still see stale open_orders rows from broker snapshot.
        # In this window, slot rebalance may cancel a BUY even though natural fill removal
        # would already free the required slot for SELL.
        last_fill_ts = self._last_fill_ts
        if last_fill_ts is not None:
            last_fill_utc = last_fill_ts if last_fill_ts.tzinfo is not None else last_fill_ts.replace(tzinfo=timezone.utc)
            after_fill_cooldown = int(getattr(self, "_order_slot_rebalance_after_fill_cooldown_seconds", 8))
            if after_fill_cooldown > 0 and (now_ts - last_fill_utc).total_seconds() < after_fill_cooldown:
                return

        last_ts = self._last_order_slot_rebalance_ts
        if last_ts is not None:
            last_utc = last_ts if last_ts.tzinfo is not None else last_ts.replace(tzinfo=timezone.utc)
            if (now_ts - last_utc).total_seconds() < int(getattr(self, "_order_slot_rebalance_cooldown_seconds", 2)):
                return

        active = self._broker_active_strategy_orders(broker)
        if not active:
            return

        active_buys = [m for m in active if m.get("side") == "BUY"]
        active_sells = [m for m in active if m.get("side") == "SELL"]
        active_total = len(active)

        sell_coverage = len(active_sells)
        if len(self.pending_sell_levels) > sell_coverage:
            sell_coverage = len(self.pending_sell_levels)
        missing_sells = desired_sell_cnt - sell_coverage
        if missing_sells < 0:
            missing_sells = 0

        # Need to free slots so missing SELL exits can be placed while respecting max_total.
        cancel_needed = active_total + missing_sells - max_total
        if cancel_needed <= 0:
            return

        if not active_buys:
            logger.debug(
                "GRID_SYNC slot_rebalance skipped | symbol={} reason=no_active_buys active_total={} max_total={} missing_sells={}",
                self.symbol,
                active_total,
                max_total,
                missing_sells,
            )
            return

        cancel_needed = min(cancel_needed, len(active_buys))
        buys_sorted = sorted(active_buys, key=lambda m: _d(m.get("price", Decimal("0"))))
        to_cancel = buys_sorted[:cancel_needed]

        cancelled_oids: set[str] = set()
        cancelled_coids: set[str] = set()
        cancelled_cnt = 0
        for meta in to_cancel:
            oid_s = str(meta.get("order_id") or "").strip()
            coid_s = str(meta.get("client_order_id") or "").strip()
            if not self._cancel_order_refs_best_effort(broker, order_id=oid_s, client_order_id=coid_s):
                continue

            cancelled_cnt += 1
            if oid_s:
                cancelled_oids.add(oid_s)
            if coid_s:
                cancelled_coids.add(coid_s)

            px = _d(meta.get("price", Decimal("0")))
            if px > 0:
                key = self._k(px)
                self.pending_buy_levels.pop(key, None)
                self._pending_buy_seen_at.pop(key, None)
                self._buy_reject_until_by_level.pop(key, None)

            for ref in (oid_s, coid_s):
                if not ref:
                    continue
                self._buy_level_by_order_ref.pop(ref, None)
                self._planned_qty_by_order_ref.pop(ref, None)
                self._fill_accum_by_order_ref.pop(ref, None)

        if cancelled_cnt <= 0:
            return

        self._last_order_slot_rebalance_ts = now_ts
        logger.warning(
            "GRID_SYNC slot_rebalance | symbol={} cancelled_buys={} active_total={} max_total={} desired_sell_cnt={} active_sells={} pending_sells={}",
            self.symbol,
            cancelled_cnt,
            active_total,
            max_total,
            desired_sell_cnt,
            len(active_sells),
            len(self.pending_sell_levels),
        )

        # Optimistically free slots in local broker cache for same-tick resync.
        try:
            raw_orders = getattr(broker, "open_orders", None)
            if raw_orders is not None:
                kept: list[Any] = []
                for o in list(raw_orders):
                    try:
                        if self._is_local_broker_order(o):
                            kept.append(o)
                            continue
                        oid = _get(o, "order_id", "orderId", "id", default=None)
                        coid = _get(o, "client_order_id", "clientOrderId", "origClientOrderId", default=None)
                        oid_s = str(oid).strip() if oid is not None else ""
                        coid_s = str(coid).strip() if coid is not None else ""
                        if oid_s and oid_s in cancelled_oids:
                            continue
                        if coid_s and coid_s in cancelled_coids:
                            continue
                        kept.append(o)
                    except Exception:
                        kept.append(o)
                broker.open_orders = kept
        except Exception:
            pass

        try:
            ro = getattr(broker, "refresh_open_orders", None)
            if callable(ro):
                self._schedule_awaitable(ro())
        except Exception:
            pass


    def _update_drawdown(self, broker: Any) -> None:
        mid = self.last_mid
        if mid is None:
            return
        equity = self._broker_quote_cash(broker) + (self._broker_base_qty(broker) * mid)
        if self._peak_equity is None or equity > self._peak_equity:
            self._peak_equity = equity
        if self._peak_equity is None or self._peak_equity <= 0:
            return
        dd_abs = self._peak_equity - equity
        if dd_abs < 0:
            dd_abs = Decimal("0")
        dd_pct = dd_abs / self._peak_equity
        if dd_abs > self._max_dd_abs:
            self._max_dd_abs = dd_abs
        if dd_pct > self._max_dd_pct:
            self._max_dd_pct = dd_pct

    def _is_duplicate_order_error(self, err: Any) -> bool:
        try:
            s = str(err).strip().lower()
        except Exception:
            return False
        return "duplicate order sent" in s

    def _place_limit_safe(self, broker: Any, *, side: str, qty: Decimal, limit_price: Decimal) -> bool:
        """Try common broker signatures without hard dependency on enum classes."""
        if qty <= 0 or limit_price <= 0:
            return False
        self._last_place_limit_error = None
        self._last_place_limit_side = side
        order_obj = None

        # Prevent exact duplicate placements in same sync pass.
        # (If broker already has this order and we don't track ids, we still may duplicate after restart;
        # state restore in run_paper should mitigate that.)
        try:
            order_obj = broker.place_limit(side=side, qty=qty, limit_price=limit_price)
            self._notify_execution_order_placed(order_obj, side=side, qty=qty, limit_price=limit_price)
            return True
        except TypeError:
            pass
        except Exception as e:
            self._last_place_limit_error = str(e)
            if self._is_duplicate_order_error(e):
                logger.debug(
                    "GRID_PLACE duplicate treated as already_open | side={} qty={} px={} err={}",
                    side,
                    qty,
                    limit_price,
                    e,
                )
                return True
            logger.warning(
                "GRID_PLACE broker.place_limit failed | sig=side,qty,limit_price side={} qty={} px={} err={}",
                side,
                qty,
                limit_price,
                e,
            )

        # common alternative parameter names
        for kwargs in (
            {"side": side, "qty": qty, "limit": limit_price},
            {"side": side, "quantity": qty, "limit_price": limit_price},
            {"side": side, "quantity": qty, "limit": limit_price},
        ):
            try:
                order_obj = broker.place_limit(**kwargs)
                self._notify_execution_order_placed(order_obj, side=side, qty=qty, limit_price=limit_price)
                return True
            except Exception as e:
                self._last_place_limit_error = str(e)
                if self._is_duplicate_order_error(e):
                    logger.debug(
                        "GRID_PLACE duplicate treated as already_open | side={} qty={} px={} kwargs={}",
                        side,
                        qty,
                        limit_price,
                        tuple(sorted(kwargs.keys())),
                    )
                    return True
                logger.warning(
                    "GRID_PLACE broker.place_limit failed | kwargs={} side={} qty={} px={} err= {}",
                    tuple(sorted(kwargs.keys())),
                    side,
                    qty,
                    limit_price,
                    e,
                )
                continue

        # positional fallback
        try:
            order_obj = broker.place_limit(side, qty, limit_price)
            self._notify_execution_order_placed(order_obj, side=side, qty=qty, limit_price=limit_price)
            return True
        except Exception as e:
            self._last_place_limit_error = str(e)
            if self._is_duplicate_order_error(e):
                logger.debug(
                    "GRID_PLACE duplicate treated as already_open | side={} qty={} px={} path=positional",
                    side,
                    qty,
                    limit_price,
                )
                return True
            self.place_limit_failures += 1
            logger.warning(
                "GRID_PLACE broker.place_limit failed | positional side={} qty={} px={} err={}",
                side,
                qty,
                limit_price,
                e,
            )
            return False


    def _notify_execution_order_placed(
        self,
        order: Any,
        *,
        side: str,
        qty: Decimal,
        limit_price: Decimal,
    ) -> None:
        """Best-effort callback for external execution/journaling layer when an order is placed."""

        # Bind broker order refs to strategy level for stable fill matching (partial fills, reconnects)
        try:
            order_ref = _get(
                order,
                "order_id",
                "orderId",
                "orderID",
                "client_order_id",
                "clientOrderId",
                "origClientOrderId",
                "id",
                default=None,
            )
            if order_ref is not None:
                order_ref = str(order_ref).strip()
                if order_ref:
                    side_norm = self._norm_side(side)
                    if side_norm == "BUY":
                        self._buy_level_by_order_ref[order_ref] = limit_price
                    elif side_norm == "SELL":
                        self._sell_level_by_order_ref[order_ref] = limit_price

                    # Also capture the planned qty for safe partial-fill finalization.
                    # IMPORTANT: prefer broker-returned (possibly adjusted) qty over the requested qty.
                    try:
                        qty_raw_actual = _get(
                            order,
                            "qty", "quantity",
                            "origQty", "orig_qty", "origQuantity",
                            default=None,
                        )
                        q_src = _d(qty_raw_actual) if qty_raw_actual is not None else _d(qty)
                        q_planned = _round_step(q_src, self.step_size) if self.step_size > 0 else q_src
                        if q_planned > 0:
                            self._planned_qty_by_order_ref[order_ref] = q_planned
                    except Exception:
                        # Fallback to requested qty if order object has no qty fields
                        try:
                            q_planned = _round_step(_d(qty), self.step_size) if self.step_size > 0 else _d(qty)
                            if q_planned > 0:
                                self._planned_qty_by_order_ref[order_ref] = q_planned
                        except Exception:
                            pass
        except Exception:
            pass

        ex = getattr(self, "execution", None)
        if ex is None:
            return

        cb = getattr(ex, "on_order_placed", None)
        if not callable(cb):
            return

        try:
            cb(
                order=order,
                side=side,
                qty=qty,
                limit_price=limit_price,
                symbol=self.symbol,
            )
        except Exception:
            # Strategy flow must not break on diagnostics/journal hooks.
            return

    def _notify_execution_fill(self, fill: Any) -> None:
        """Best-effort callback for external execution/journaling layer when a fill is observed."""
        ex = getattr(self, "execution", None)
        if ex is None:
            return

        cb = getattr(ex, "on_fill", None)
        if not callable(cb):
            return

        try:
            cb(fill=fill, symbol=self.symbol)
        except Exception:
            # Strategy flow must not break on diagnostics/journal hooks.
            return

    def export_state(self) -> dict[str, Any]:
        """Serializable adapter state for run_paper resume."""
        return {
            "symbol": self.symbol,
            "interval": self.interval,
            "maker_fee_rate": str(self.maker_fee_rate),
            "taker_fee_rate": str(self.taker_fee_rate),
            "anchor": str(self.anchor) if self.anchor is not None else None,
            "last_bid": str(self.last_bid) if self.last_bid is not None else None,
            "last_ask": str(self.last_ask) if self.last_ask is not None else None,
            "last_mid": str(self.last_mid) if self.last_mid is not None else None,
            "buy_orders": [str(x) for x in sorted(self.buy_orders)],
            "open_lots": [
                {
                    "buy_price": str(l.buy_price),
                    "sell_price": str(l.sell_price),
                    "qty": str(l.qty),
                    "cost_quote": str(l.cost_quote),
                    "open_seq": l.open_seq,
                    "open_ts": (
                        self._lot_open_ts_by_key.get(self._lot_key(l.sell_price, l.qty)).isoformat()
                        if self._lot_open_ts_by_key.get(self._lot_key(l.sell_price, l.qty)) is not None
                        else None
                    ),
                }
                for l in self.open_lots
            ],
            "pending_buy_levels": {k: str(v) for k, v in self.pending_buy_levels.items()},
            "pending_sell_levels": {k: str(v) for k, v in self.pending_sell_levels.items()},
            "buy_level_by_order_ref": {str(k): str(v) for k, v in self._buy_level_by_order_ref.items()},
            "sell_level_by_order_ref": {str(k): str(v) for k, v in self._sell_level_by_order_ref.items()},
            "seen_fill_exec_refs": list(self._seen_fill_exec_refs),
            "fills_buy": self.fills_buy,
            "fills_sell": self.fills_sell,
            "reanchors": self.reanchors,
            "cap_blocked": self.cap_blocked,
            "min_notional_blocked": self.min_notional_blocked,
            "duplicate_place_skips": self.duplicate_place_skips,
            "duplicate_pending_skips": self.duplicate_pending_skips,
            "buy_reject_cooldown_seconds": self._buy_reject_cooldown_seconds,
            "buy_reject_until_by_level": {k: (v.isoformat() if v is not None else None) for k, v in self._buy_reject_until_by_level.items()},            
            "pending_sell_seen_at": {k: (v.isoformat() if v is not None else None) for k, v in self._pending_sell_seen_at.items()},
            "place_limit_failures": self.place_limit_failures,
            "unknown_fill_side": self.unknown_fill_side,
            "init_done": self.init_done,
            "peak_equity": str(self._peak_equity) if self._peak_equity is not None else None,
            "max_dd_abs": str(self._max_dd_abs),
            "max_dd_pct": str(self._max_dd_pct),
            "manual_deposits_total": str(self.manual_deposits_total),
            "core_initial_quote": str(_d(getattr(self._core.state, "initial_quote", Decimal("0")))),
            "core_deposits_total": str(_d(getattr(self._core.state, "deposits_total", Decimal("0")))),
            "last_quote_ts": self.last_quote_ts.isoformat() if self.last_quote_ts is not None else None,
            "last_bar_close": str(self.last_bar_close) if self.last_bar_close is not None else None,
            "last_bar_ts": self.last_bar_ts.isoformat() if self.last_bar_ts is not None else None,
            "planned_qty_by_order_ref": {str(k): str(v) for k, v in self._planned_qty_by_order_ref.items()},
        }

    def restore_state(self, state: dict[str, Any]) -> None:
        """Restore adapter state from `export_state()` output (best-effort, backward-compatible)."""
        if not isinstance(state, dict):
            return

        self.maker_fee_rate = _d(state["maker_fee_rate"]) if state.get("maker_fee_rate") is not None else self.maker_fee_rate
        self.taker_fee_rate = _d(state["taker_fee_rate"]) if state.get("taker_fee_rate") is not None else self.taker_fee_rate
        self.anchor = _d(state["anchor"]) if state.get("anchor") is not None else self.anchor
        self.last_bid = _d(state["last_bid"]) if state.get("last_bid") is not None else self.last_bid
        self.last_ask = _d(state["last_ask"]) if state.get("last_ask") is not None else self.last_ask
        self.last_mid = _d(state["last_mid"]) if state.get("last_mid") is not None else self.last_mid
        self.last_bar_close = _d(state["last_bar_close"]) if state.get("last_bar_close") is not None else self.last_bar_close

        self.last_quote_ts = self._parse_ts(state.get("last_quote_ts"))
        self.last_bar_ts = self._parse_ts(state.get("last_bar_ts"))

        buy_orders_raw = state.get("buy_orders") or []
        self.buy_orders = { _d(x) for x in buy_orders_raw }

        self.open_lots = []
        self._lot_open_ts_by_key = {}
        for row in (state.get("open_lots") or []):
            if not isinstance(row, dict):
                continue
            try:
                lot = GridLot(
                    buy_price=_d(row["buy_price"]),
                    sell_price=_d(row["sell_price"]),
                    qty=_d(row["qty"]),
                    cost_quote=_d(row["cost_quote"]),
                    open_seq=int(row.get("open_seq", 0)),
                )
                self.open_lots.append(lot)
                open_ts = self._parse_ts(row.get("open_ts"))
                if open_ts is not None:
                    self._lot_open_ts_by_key[self._lot_key(lot.sell_price, lot.qty)] = open_ts
            except Exception:
                continue

        # Rebuild GridCore state from restored adapter state
        self._core.state.anchor = self.anchor
        self._core.state.buy_levels = set(self.buy_orders)
        self._core.state.open_lots = []
        for l in self.open_lots:
            self._core.state.open_lots.append(
                GridLot(
                    buy_price=l.buy_price,
                    sell_price=l.sell_price,
                    qty=l.qty,
                    cost_quote=l.cost_quote,
                    open_seq=l.open_seq,
                )
            )

        self.pending_buy_levels = {
            str(k): _d(v) for k, v in (state.get("pending_buy_levels") or {}).items()
        }
        self.pending_sell_levels = {
            str(k): _d(v) for k, v in (state.get("pending_sell_levels") or {}).items()
        }
        self._pending_sell_seen_at = {}
        for k, v in (state.get("pending_sell_seen_at") or {}).items():
            ts = self._parse_ts(v)
            if ts is not None:
                self._pending_sell_seen_at[str(k)] = ts
        self._buy_level_by_order_ref = {
            str(k): _d(v) for k, v in (state.get("buy_level_by_order_ref") or {}).items()
        }
        self._sell_level_by_order_ref = {
            str(k): _d(v) for k, v in (state.get("sell_level_by_order_ref") or {}).items()
        }
        self._seen_fill_exec_refs = {
            str(x) for x in (state.get("seen_fill_exec_refs") or [])
        }
        self.fills_buy = int(state.get("fills_buy", self.fills_buy))
        self.fills_sell = int(state.get("fills_sell", self.fills_sell))
        self.reanchors = int(state.get("reanchors", self.reanchors))
        self.cap_blocked = int(state.get("cap_blocked", self.cap_blocked))
        self.min_notional_blocked = int(state.get("min_notional_blocked", self.min_notional_blocked))
        self.duplicate_place_skips = int(state.get("duplicate_place_skips", self.duplicate_place_skips))
        self.duplicate_pending_skips = int(state.get("duplicate_pending_skips", self.duplicate_pending_skips))
        self.place_limit_failures = int(state.get("place_limit_failures", self.place_limit_failures))
        self._buy_reject_cooldown_seconds = int(state.get("buy_reject_cooldown_seconds", self._buy_reject_cooldown_seconds))
        self._buy_reject_until_by_level = {}
        for k, v in (state.get("buy_reject_until_by_level") or {}).items():
            ts = self._parse_ts(v)
            if ts is not None:
                self._buy_reject_until_by_level[str(k)] = ts
        self.unknown_fill_side = int(state.get("unknown_fill_side", self.unknown_fill_side))
        self.init_done = bool(state.get("init_done", self.init_done))

        self._peak_equity = _d(state["peak_equity"]) if state.get("peak_equity") is not None else self._peak_equity
        self._max_dd_abs = _d(state.get("max_dd_abs", self._max_dd_abs))
        self._max_dd_pct = _d(state.get("max_dd_pct", self._max_dd_pct))
        self.manual_deposits_total = _d(state.get("manual_deposits_total", self.manual_deposits_total))

        # Restore cashflow fields into GridState if the current GridState version supports them.
        try:
            if hasattr(self._core.state, "initial_quote") and state.get("core_initial_quote") is not None:
                self._core.state.initial_quote = _d(state.get("core_initial_quote"))
            if hasattr(self._core.state, "deposits_total") and state.get("core_deposits_total") is not None:
                self._core.state.deposits_total = _d(state.get("core_deposits_total"))
        except Exception:
            pass
        # LOCAL-* client ids are session-scoped and can be reused after restart.
        # Keeping them from a persisted snapshot can poison level mapping for new orders.
        self._buy_level_by_order_ref = {
            k: v for k, v in self._buy_level_by_order_ref.items()
            if k and not str(k).upper().startswith("LOCAL-")
        }
        self._sell_level_by_order_ref = {
            k: v for k, v in self._sell_level_by_order_ref.items()
            if k and not str(k).upper().startswith("LOCAL-")
        }
        self._pending_sell_seen_at = {k: t for k, t in self._pending_sell_seen_at.items() if k in self.pending_sell_levels}

        # Restore planned order qty map (used to finalize partial fills safely)
        self._planned_qty_by_order_ref = {
            str(k): _d(v)
            for k, v in (state.get("planned_qty_by_order_ref") or {}).items()
            if str(k) and not str(k).upper().startswith("LOCAL-")
        }

        self._mirror_local_state_from_core()
        self._ensure_buy_ladder()

    def register_deposit(self, amount: Any) -> dict[str, Any]:
        """Register external quote deposit for contributed-capital accounting (paper/live runtime hook)."""
        amt = _d(amount)
        if amt <= 0:
            return {"ok": False, "status": "invalid_amount", "amount": amt}

        self.manual_deposits_total += amt

        st = self._core.state
        try:
            if hasattr(st, "initial_quote") and _d(getattr(st, "initial_quote", Decimal("0"))) <= 0:
                st.initial_quote = amt
            elif hasattr(st, "deposits_total"):
                st.deposits_total = _d(getattr(st, "deposits_total", Decimal("0"))) + amt
        except Exception:
            pass

        return {
            "ok": True,
            "status": "deposit_registered",
            "amount": amt,
            "manual_deposits_total": self.manual_deposits_total,
        }

    def on_deposit(self, amount: Any) -> dict[str, Any]:
        return self.register_deposit(amount)


    def _broker_quote_total(self, broker: Any) -> Decimal:
        """Best-effort TOTAL quote balance (free + locked)."""
        for name in (
            "quote_balance_total",
            "quote_total",
            "balance_quote_total",
            "total_quote",
        ):
            if hasattr(broker, name):
                try:
                    return _d(getattr(broker, name))
                except Exception:
                    pass

        # If broker exposes balances dict like {"USDT": {"free": ..., "locked": ...}}
        for name in ("balances", "account_balances", "wallet", "portfolio"):
            if hasattr(broker, name):
                try:
                    obj = getattr(broker, name)
                    if isinstance(obj, dict):
                        usdt = obj.get("USDT") or obj.get("usdt")
                        if isinstance(usdt, dict):
                            free = _d(usdt.get("free", 0))
                            locked = _d(usdt.get("locked", 0))
                            return free + locked
                except Exception:
                    pass

        # Fallback to free-only quote cash
        return self._broker_quote_cash(broker)

    def _broker_base_total(self, broker: Any) -> Decimal:
        """Best-effort TOTAL base balance (free + locked)."""
        for name in (
            "base_balance_total",
            "base_total",
            "position_base_total",
            "total_base",
        ):
            if hasattr(broker, name):
                try:
                    return _d(getattr(broker, name))
                except Exception:
                    pass

        base_asset = ""
        try:
            base_asset = str(self.symbol).split("USDT", 1)[0]
        except Exception:
            base_asset = ""
        if base_asset:
            for name in ("balances", "account_balances", "wallet"):
                if hasattr(broker, name):
                    try:
                        obj = getattr(broker, name)
                        if isinstance(obj, dict):
                            row = obj.get(base_asset) or obj.get(base_asset.upper()) or obj.get(base_asset.lower())
                            if isinstance(row, dict):
                                free = _d(row.get("free", 0))
                                locked = _d(row.get("locked", 0))
                                return free + locked
                    except Exception:
                        pass

        return self._broker_base_qty(broker)

    def _total_capital_in_quote(self, broker: Any) -> Decimal:
        """Best-effort total portfolio value in quote (USDT).
        Priority:
        1) broker.equity_total / total_equity_quote if provided
        2) quote_total + base_total * mid
        """
        for name in ("equity_total", "total_equity", "total_equity_quote", "equity_quote_total"):
            if hasattr(broker, name):
                try:
                    v = _d(getattr(broker, name))
                    if v > 0:
                        return v
                except Exception:
                    pass

        mid = self.last_mid or Decimal("0")
        q_total = self._broker_quote_total(broker)
        b_total = self._broker_base_total(broker)
        total = q_total + (b_total * mid)
        return total if total > 0 else q_total

    def _broker_quote_cash(self, broker: Any) -> Decimal:
        for name in ("quote_balance", "cash_quote", "quote", "balance_quote"):
            if hasattr(broker, name):
                try:
                    return _d(getattr(broker, name))
                except Exception:
                    pass
        if hasattr(broker, "portfolio"):
            pf = getattr(broker, "portfolio")
            for name in ("quote", "cash_quote"):
                if hasattr(pf, name):
                    try:
                        return _d(getattr(pf, name))
                    except Exception:
                        pass
        return Decimal("0")

    def _broker_base_qty(self, broker: Any) -> Decimal:
        for name in ("base_balance", "base", "position_base", "qty_base"):
            if hasattr(broker, name):
                try:
                    return _d(getattr(broker, name))
                except Exception:
                    pass
        if hasattr(broker, "portfolio"):
            pf = getattr(broker, "portfolio")
            for name in ("base", "base_qty"):
                if hasattr(pf, name):
                    try:
                        return _d(getattr(pf, name))
                    except Exception:
                        pass
        return Decimal("0")

    def _broker_open_orders_count(self, broker: Any) -> int:
        raw_orders = getattr(broker, "open_orders", None)
        if raw_orders is None:
            return 0
        try:
            orders = list(raw_orders)
        except Exception:
            return 0

        active = 0
        for o in orders:
            if self._order_is_active(o):
                active += 1
        return active

    def _is_local_broker_order(self, order: Any) -> bool:
        """True только для локальных плейсхолдеров LOCAL-*.

        ВАЖНО: clientOrderId может быть LOCAL-* и у РЕАЛЬНОГО биржевого ордера,
        поэтому client_order_id/clientOrderId здесь НЕ используем.
        """
        try:
            raw = _get(
                order,
                "order_id",
                "orderId",
                "id",
                default=None,
            )
            if raw is None:
                return False
            s = str(raw).strip().upper()
            return s.startswith("LOCAL-")
        except Exception:
            return False
        

    def _order_is_active(self, order: Any) -> bool:
        """True если ордер реально активный (open). Фильтруем ghost orders из open_orders."""
        try:
            if self._is_local_broker_order(order):
                return False

            status_raw = _get(order, "status", "state", "order_status", default=None)
            if status_raw is not None:
                st = str(status_raw).strip().upper()
                if st in {"FILLED", "CANCELED", "CANCELLED", "REJECTED", "EXPIRED", "CLOSED"}:
                    return False

            # Если брокер дает remaining/leaves qty — ноль значит закрыт
            rem_raw = _get(order, "remaining_qty", "remaining", "leavesQty", "leaves_qty", default=None)
            if rem_raw is not None:
                rem = _d(rem_raw)
                eps = (self.step_size if self.step_size and self.step_size > 0 else Decimal("0"))
                if rem <= eps:
                    return False

            # Если можно сравнить executed vs original — полностью исполнен => закрыт
            orig_raw = _get(order, "origQty", "orig_qty", "orig_quantity", "qty", "quantity", default=None)
            exe_raw  = _get(order, "executedQty", "executed_qty", "filled_qty", "filled", "cumQty", default=None)
            if orig_raw is not None and exe_raw is not None:
                orig = _d(orig_raw)
                exe = _d(exe_raw)
                eps = (self.step_size if self.step_size and self.step_size > 0 else Decimal("0"))
                if orig > 0 and exe >= (orig - eps):
                    return False

            return True
        except Exception:
            # safer default: считаем активным, чтобы не поставить дубликат (но обычно сюда не попадём)
            return True


    def _broker_strategy_open_orders_count(self, broker: Any) -> int:
        """Count only active broker orders that belong to the strategy (exclude LOCAL-*)."""
        if broker is None or not hasattr(broker, "open_orders"):
            return 0
        try:
            cnt = 0
            for o in list(getattr(broker, "open_orders")):
                if self._order_is_active(o):
                    cnt += 1
            return cnt
        except Exception:
            return 0

    def _pop_nearest_pending(self, dct: dict[str, Decimal], px: Decimal) -> Optional[Decimal]:
        if not dct:
            return None
        best_key: Optional[str] = None
        best_val: Optional[Decimal] = None
        best_dist: Optional[Decimal] = None
        for k, v in dct.items():
            dist = abs(v - px)
            if best_dist is None or dist < best_dist:
                best_dist = dist
                best_key = k
                best_val = v
        if best_key is not None:
            dct.pop(best_key, None)
        return best_val

    def _k(self, price: Decimal) -> str:
        # stable string key to avoid Decimal exponent representation issues
        return format(price.quantize(Decimal("0.00000001")), "f")

    def _norm_side(self, side: Any) -> str:
        if side is None:
            return ""
        if isinstance(side, str):
            return side.upper()
        # enum-like repr: Side.BUY / Side.SELL
        name = getattr(side, "name", None)
        if isinstance(name, str):
            return name.upper()
        s = str(side).upper()
        if "BUY" in s:
            return "BUY"
        if "SELL" in s:
            return "SELL"
        return s

    def _parse_ts(self, v: Any) -> Optional[datetime]:
        if v is None:
            return None
        try:
            if isinstance(v, datetime):
                return v if v.tzinfo is not None else v.replace(tzinfo=timezone.utc)
            if isinstance(v, (int, float)):
                fv = float(v)
                if fv > 1e12:
                    return datetime.fromtimestamp(fv / 1000.0, tz=timezone.utc)
                return datetime.fromtimestamp(fv, tz=timezone.utc)
            s = str(v).strip()
            if s.isdigit():
                fv = float(s)
                if fv > 1e12:
                    return datetime.fromtimestamp(fv / 1000.0, tz=timezone.utc)
                return datetime.fromtimestamp(fv, tz=timezone.utc)
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            return dt if dt.tzinfo is not None else dt.replace(tzinfo=timezone.utc)
        except Exception:
            return None

    def _cancel_all_strategy_orders(self, broker: Any) -> None:
        """Best-effort cancel of all active strategy orders on the broker/exchange.

        IMPORTANT: in demo/live bridges cancellations are often async. If we cancel and then
        immediately call `_sync_broker_orders`, the broker snapshot may still show old orders
        and the max_total cap will block placing the new ladder.

        Therefore we:
        - clear adapter local pending maps first
        - prefer broker-level cancel-all if available
        - fall back to per-order cancel attempts
        - schedule async cancels if returned
        - trigger a broker refresh_open_orders() best-effort
        - optimistically drop canceled exchange orders from broker.open_orders cache (best-effort)
          to free slots for the next sync tick.
        """
        if broker is None:
            return

        # Clear local pending tracking first to prevent immediate re-dedupe against stale keys.
        try:
            self.pending_buy_levels.clear()
            self.pending_sell_levels.clear()
            self._pending_buy_seen_at.clear()
            self._pending_sell_seen_at.clear()
            self._sell_replace_cooldown_until_by_key.clear()
            self._buy_level_by_order_ref.clear()
            self._sell_level_by_order_ref.clear()
            self._planned_qty_by_order_ref.clear()
            self._fill_accum_by_order_ref.clear()
        except Exception:
            pass

        # Helper: schedule awaitables without breaking sync flow.
        def _schedule(maybe_awaitable: Any) -> None:
            try:
                import asyncio as _asyncio

                if _asyncio.iscoroutine(maybe_awaitable):
                    try:
                        loop = _asyncio.get_running_loop()
                        loop.create_task(maybe_awaitable)
                    except RuntimeError:
                        # No running loop; ignore.
                        return
            except Exception:
                return

        # 1) Prefer cancel-all entrypoints when available.
        for name in (
            "cancel_all_open_orders",
            "cancel_open_orders",
            "cancel_all_orders",
            "cancel_all",
        ):
            fn = getattr(broker, name, None)
            if callable(fn):
                try:
                    out = fn(symbol=self.symbol)
                except TypeError:
                    try:
                        out = fn(self.symbol)
                    except Exception:
                        out = None
                except Exception:
                    out = None
                _schedule(out)
                # Best-effort refresh after initiating cancel-all
                try:
                    ro = getattr(broker, "refresh_open_orders", None)
                    if callable(ro):
                        _schedule(ro())
                except Exception:
                    pass
                # Optimistic local cache cleanup: drop non-local active exchange orders
                try:
                    raw_orders = getattr(broker, "open_orders", None)
                    if raw_orders is not None:
                        kept: list[Any] = []
                        for o in list(raw_orders):
                            # keep LOCAL placeholders only; exchange orders expected to be gone soon
                            try:
                                if self._is_local_broker_order(o):
                                    kept.append(o)
                            except Exception:
                                continue
                        broker.open_orders = kept
                except Exception:
                    pass
                logger.debug("GRID_CANCEL_ALL | symbol={} reason=reanchor", self.symbol)
                return

        # 2) Fallback: per-order cancel.
        raw_orders = getattr(broker, "open_orders", None)
        if raw_orders is None:
            return

        try:
            orders = list(raw_orders)
        except Exception:
            return

        cancel_fns: list[Any] = []
        for name in ("cancel_order", "cancel", "cancel_limit", "cancel_open_order"):
            fn = getattr(broker, name, None)
            if callable(fn):
                cancel_fns.append(fn)

        if not cancel_fns:
            return

        cancelled_ids: set[str] = set()
        for o in orders:
            try:
                if not self._order_is_active(o):
                    continue

                # Skip local placeholders (LOCAL-*): they are not real exchange orders.
                if self._is_local_broker_order(o):
                    continue

                oid = _get(o, "order_id", "orderId", "id", default=None)
                coid = _get(o, "client_order_id", "clientOrderId", "origClientOrderId", default=None)

                oid_s = str(oid).strip() if oid is not None else ""
                coid_s = str(coid).strip() if coid is not None else ""

                if not oid_s and not coid_s:
                    continue

                cancelled = False
                for fn in cancel_fns:
                    # Prefer cancel by exchange orderId if available.
                    if oid_s:
                        try:
                            out = fn(order_id=oid_s, symbol=self.symbol)
                            _schedule(out)
                            cancelled = True
                            break
                        except TypeError:
                            pass
                        except Exception:
                            pass

                        try:
                            out = fn(order_id=oid_s)
                            _schedule(out)
                            cancelled = True
                            break
                        except TypeError:
                            pass
                        except Exception:
                            pass

                        try:
                            out = fn(oid_s)
                            _schedule(out)
                            cancelled = True
                            break
                        except Exception:
                            pass

                    # Fallback: cancel by clientOrderId when exposed.
                    if coid_s:
                        try:
                            out = fn(client_order_id=coid_s, symbol=self.symbol)
                            _schedule(out)
                            cancelled = True
                            break
                        except TypeError:
                            pass
                        except Exception:
                            pass

                        try:
                            out = fn(origClientOrderId=coid_s, symbol=self.symbol)
                            _schedule(out)
                            cancelled = True
                            break
                        except TypeError:
                            pass
                        except Exception:
                            pass

                if cancelled:
                    if oid_s:
                        cancelled_ids.add(oid_s)
                    logger.debug(
                        "GRID_CANCEL | symbol={} order_id={} client_order_id={} reason=reanchor",
                        self.symbol,
                        oid_s or None,
                        coid_s or None,
                    )
            except Exception:
                continue

        # Best-effort refresh after initiating cancels
        try:
            ro = getattr(broker, "refresh_open_orders", None)
            if callable(ro):
                _schedule(ro())
        except Exception:
            pass

        # Optimistic broker cache cleanup: remove the canceled exchange orders by id
        # (so `_sync_broker_orders` is not blocked by max_total cap on the same tick).
        try:
            if cancelled_ids and hasattr(broker, "open_orders"):
                new_list: list[Any] = []
                for o in list(getattr(broker, "open_orders") or []):
                    try:
                        if self._is_local_broker_order(o):
                            new_list.append(o)
                            continue
                        oid = _get(o, "order_id", "orderId", "id", default=None)
                        oid_s = str(oid).strip() if oid is not None else ""
                        if oid_s and oid_s in cancelled_ids:
                            continue
                        new_list.append(o)
                    except Exception:
                        new_list.append(o)
                broker.open_orders = new_list
        except Exception:
            pass
