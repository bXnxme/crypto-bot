from __future__ import annotations

import asyncio
import os
import traceback
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Optional

from datetime import datetime, timezone

from loguru import logger


def _load_env_file_if_present() -> None:
    """Best-effort load of local .env into process environment (without deps)."""
    candidates = [
        Path.cwd() / ".env",
        Path(__file__).resolve().parents[2] / ".env",
    ]
    env_path = next((p for p in candidates if p.exists() and p.is_file()), None)
    if env_path is None:
        return

    try:
        for raw_line in env_path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("export "):
                line = line[len("export ") :].strip()
            if "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip()
            if not key:
                continue
            if (value.startswith('"') and value.endswith('"')) or (
                value.startswith("'") and value.endswith("'")
            ):
                value = value[1:-1]
            os.environ.setdefault(key, value)
    except Exception:
        return


def _normalize_rest_base_url(raw: str) -> str:
    v = (raw or "").strip().rstrip("/")
    if not v:
        return ""
    if v.lower().endswith("/api/v3"):
        v = v[:-7].rstrip("/")
    elif v.lower().endswith("/api"):
        v = v[:-4].rstrip("/")
    return v


def _pick_base_url() -> str:
    """Match the smoke script logic for REST endpoint selection."""
    for key in (
        "BINANCE_DEMO_REST_BASE_URL",
        "BINANCE_REST_BASE_URL",
        "BASE_URL",
        # tolerated fallback if user accidentally put REST URL here
        "BINANCE_DEMO_WS_BASE_URL",
    ):
        val = os.environ.get(key)
        if not val or not str(val).strip():
            continue
        return _normalize_rest_base_url(str(val))
    return "https://testnet.binance.vision"


def _pick_api_creds(base_url: str) -> tuple[str, str, str]:
    """Pick creds matching the selected endpoint (demo vs testnet), same idea as smoke."""
    key_demo = (os.environ.get("BINANCE_DEMO_API_KEY") or "").strip()
    sec_demo = (os.environ.get("BINANCE_DEMO_API_SECRET") or "").strip()
    key_std = (os.environ.get("BINANCE_API_KEY") or "").strip()
    sec_std = (os.environ.get("BINANCE_API_SECRET") or "").strip()

    base_l = str(base_url).lower()
    wants_demo = "demo-api.binance.com" in base_l
    wants_testnet = "testnet.binance.vision" in base_l

    if wants_demo and key_demo and sec_demo:
        return key_demo, sec_demo, "demo"
    if wants_testnet and key_std and sec_std:
        return key_std, sec_std, "standard"

    if key_demo and sec_demo:
        return key_demo, sec_demo, "fallback-demo"
    if key_std and sec_std:
        return key_std, sec_std, "fallback-standard"

    raise RuntimeError(
        "Missing Binance API credentials. Set BINANCE_DEMO_API_KEY/BINANCE_DEMO_API_SECRET "
        "(demo) or BINANCE_API_KEY/BINANCE_API_SECRET (testnet)."
    )



@dataclass
class DemoFill:
    """Minimal fill object compatible with simple strategy adapters."""

    # Binance exchange orderId
    order_id: str
    # Binance clientOrderId (we set it to LOCAL-*)
    client_order_id: Optional[str] = None

    symbol: str = ""
    side: str = ""
    qty: Decimal = Decimal("0")
    price: Decimal = Decimal("0")
    fee_quote: Decimal = Decimal("0")
    maker: bool = False


@dataclass
class DemoOrderEvent:
    """Normalized order-status event for demo exchange polling."""

    order_id: str
    symbol: str
    side: str
    status: str
    orig_qty: Decimal = Decimal("0")
    executed_qty: Decimal = Decimal("0")
    price: Decimal = Decimal("0")
    update_ts: Optional[datetime] = None

    @property
    def partial(self) -> bool:
        st = str(self.status or "").upper()
        return st in {"PARTIALLY_FILLED", "PARTIAL"}

@dataclass
class DemoPlacedOrder:
    """Минимальный объект ордера для совместимости с GridBacktestAdapter."""
    order_id: str
    symbol: str
    side: str
    qty: Decimal
    limit_price: Decimal
    status: str = "PENDING_SUBMIT"
    ts: Optional[datetime] = None

class BinanceDemoExecution:
    """
    Thin demo execution adapter for src.run_demo.

    Goals:
    - Be importable in any branch (even if python-binance is missing/broken).
    - Expose methods/attrs expected by run_demo:
      bootstrap(), refresh_balances(), refresh_open_orders(), poll_new_fills()
      + quote_balance/base_balance/open_orders
    - Not block startup if Binance client bootstrap fails.
    """

    def __init__(
        self,
        symbol: str = "ETHUSDT",
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
        testnet: bool = True,
        **_: Any,
    ) -> None:
        self.symbol = symbol.upper()
        # Respect explicit creds if passed, otherwise resolve from env based on endpoint.
        self.api_key = (api_key or "").strip()
        self.api_secret = (api_secret or "").strip()
        self.testnet = bool(testnet)

        _load_env_file_if_present()
        self.rest_base_url = _pick_base_url()
        self.endpoint_mode = (
            "DEMO"
            if "demo-api.binance.com" in self.rest_base_url.lower()
            else "TESTNET"
            if "testnet.binance.vision" in self.rest_base_url.lower()
            else "CUSTOM"
        )

        # Simple symbol parsing (works for ETHUSDT/BTCUSDT etc.)
        self.quote_asset = "USDT" if self.symbol.endswith("USDT") else self.symbol[-4:]
        self.base_asset = self.symbol[: -len(self.quote_asset)] if self.quote_asset else self.symbol

        self.client: Any = None

        # State expected by run_demo_grid heartbeat
        # Backward-compatible aliases: quote_balance/base_balance keep "free" balances.
        self.quote_balance: Decimal = Decimal("0")
        self.base_balance: Decimal = Decimal("0")
        self.open_orders: list[Any] = []

        # Explicit balance split (free / locked / total) for correct heartbeat reporting.
        self.quote_balance_free: Decimal = Decimal("0")
        self.quote_balance_locked: Decimal = Decimal("0")
        self.quote_balance_total: Decimal = Decimal("0")
        self.base_balance_free: Decimal = Decimal("0")
        self.base_balance_locked: Decimal = Decimal("0")
        self.base_balance_total: Decimal = Decimal("0")

        # Buffered fills (if strategy uses helper methods below)
        self._fills_buffer: list[Any] = []
        self._order_events_buffer: list[Any] = []
        self._seen_trade_keys: set[str] = set()
        self._order_state_cache: dict[str, dict[str, Any]] = {}
        self._bootstrapped = False
        self._client_lock: Optional[asyncio.Lock] = None
        self._local_order_seq = 0
        self._warned_no_place_limit_backend = False
        self._pending_local_orders: dict[str, DemoPlacedOrder] = {}
        self._price_tick_size: Decimal = Decimal("0")
        self._qty_step_size: Decimal = Decimal("0")
        self._min_qty: Decimal = Decimal("0")
        self._min_notional: Decimal = Decimal("0")        
        self._pending_submit_sigs: set[str] = set()
        self._local_order_sig: dict[str, str] = {}
        self._prime_trade_history_on_first_poll = True
        self._prime_order_history_on_first_poll = True

        self.exchange_open_orders_count: int = 0
        self.local_pending_open_orders_count: int = 0
        self._pending_local_order_ttl_sec: int = 15

        # Account commission info (pulled from /api/v3/account)
        self.account_commission_rates: dict[str, Any] = {}
        self.maker_fee_rate: Decimal = Decimal("0")
        self.taker_fee_rate: Decimal = Decimal("0")
        self.makerCommission: Optional[int] = None
        self.takerCommission: Optional[int] = None
        self._commissions_logged = False
        self._warned_sell_balance_adjust_sig: Optional[str] = None
        self._warned_buy_balance_adjust_sig: Optional[str] = None

    async def bootstrap(self) -> None:
        if self._bootstrapped:
            return

        try:
            from binance import AsyncClient  # type: ignore
        except Exception as e:
            logger.warning("python-binance import failed; execution in no-op mode: {}", e)
            self._bootstrapped = True
            return

        try:
            selected_key = self.api_key
            selected_secret = self.api_secret
            cred_source = "explicit"
            if not selected_key or not selected_secret:
                selected_key, selected_secret, cred_source = _pick_api_creds(self.rest_base_url)

            # Create client first (create() may ping on default host, which is OK), then point it to our REST base.
            self.client = await AsyncClient.create(selected_key, selected_secret)
            self._client_lock = asyncio.Lock()

            normalized_rest = _normalize_rest_base_url(self.rest_base_url)
            if normalized_rest:
                try:
                    # python-binance expects API_URL ending with /api
                    self.client.API_URL = f"{normalized_rest}/api"
                except Exception:
                    pass

            logger.info(
                "BinanceDemoExecution endpoint resolved | symbol={} mode={} rest_base={} cred_source= {}",
                self.symbol,
                self.endpoint_mode,
                normalized_rest or self.rest_base_url,
                cred_source,
            )

            await self.refresh_balances()
            await self.refresh_open_orders()
            await self._load_symbol_filters()

            logger.info(
                "BinanceDemoExecution bootstrapped | symbol={} mode={} quote_balance={} base_balance={}",
                self.symbol,
                self.endpoint_mode,
                self.quote_balance,
                self.base_balance,
            )
        except Exception as e:
            err_detail = self._format_exc(e)
            logger.warning(
                "BinanceDemoExecution bootstrap failed; fallback no-op mode | err={} detail={}",
                (str(e) or "").strip() or err_detail,
                err_detail,
            )
            try:
                logger.debug(
                    "BinanceDemoExecution bootstrap traceback:\n{}",
                    "".join(traceback.format_exception(type(e), e, e.__traceback__)),
                )
            except Exception:
                pass
            try:
                await self.close()
            except Exception:
                pass
            self.client = None
            self._client_lock = None
        finally:
            self._bootstrapped = True

    async def close(self) -> None:
        if self.client is None:
            return

        try:
            close_connection = getattr(self.client, "close_connection", None)
            if callable(close_connection):
                out = close_connection()
                if asyncio.iscoroutine(out):
                    await out
        except Exception as e:
            logger.warning("BinanceDemoExecution close_connection failed: {}", e)

        # Extra safety for python-binance versions exposing raw aiohttp session
        try:
            session = getattr(self.client, "session", None)
            if session is not None and not session.closed:
                await session.close()
        except Exception as e:
            logger.warning("BinanceDemoExecution session close failed: {}", e)

    @staticmethod
    def _d(v: Any, default: str = "0") -> Decimal:
        try:
            return Decimal(str(v))
        except Exception:
            return Decimal(default)

    @staticmethod
    def _safe_dt_ms(ms: Any) -> Optional[datetime]:
        try:
            iv = int(ms)
            if iv <= 0:
                return None
            return datetime.fromtimestamp(iv / 1000, tz=timezone.utc)
        except Exception:
            return None

    @staticmethod
    def _fmt_dec(v: Decimal) -> str:
        s = format(Decimal(str(v)), "f")
        if "." in s:
            s = s.rstrip("0").rstrip(".")
        return s or "0"
    
    @staticmethod
    def _format_exc(e: BaseException) -> str:
        """Best-effort stringify for Binance/python-binance exceptions."""
        try:
            parts: list[str] = [f"type={type(e).__name__}", f"repr={repr(e)}"]

            for attr in ("status_code", "code", "message"):
                try:
                    if hasattr(e, attr):
                        v = getattr(e, attr)
                        if v is not None and str(v) != "":
                            parts.append(f"{attr}={v}")
                except Exception:
                    pass

            for attr in ("response", "body"):
                try:
                    if hasattr(e, attr):
                        v = getattr(e, attr)
                        if v is None:
                            continue

                        # response might have status/text
                        try:
                            status = getattr(v, "status", None)
                            if status is not None:
                                parts.append(f"{attr}.status={status}")
                        except Exception:
                            pass

                        s = str(v)
                        if s and s not in ("None", ""):
                            parts.append(f"{attr}={s}")
                except Exception:
                    pass

            return " | ".join(parts)
        except Exception:
            return f"type={type(e).__name__} | repr={repr(e)}"    

    @staticmethod
    def _round_down_step(value: Decimal, step: Decimal) -> Decimal:
        v = Decimal(str(value))
        s = Decimal(str(step))
        if s <= 0:
            return v
        return (v // s) * s

    def _norm_side(self, side: Any) -> str:
        return str(getattr(side, "value", side)).upper()

    def _norm_qty_px_for_sig(self, qty: Decimal, price: Decimal) -> tuple[Decimal, Decimal]:
        q = Decimal(str(qty))
        p = Decimal(str(price))
        if self._qty_step_size > 0:
            q = self._round_down_step(q, self._qty_step_size)
        if self._price_tick_size > 0:
            p = self._round_down_step(p, self._price_tick_size)
        # normalize textual representation to avoid 1.2300 vs 1.23 mismatches
        q = self._d(self._fmt_dec(q))
        p = self._d(self._fmt_dec(p))
        return q, p

    def _make_order_sig(self, side: Any, qty: Decimal, price: Decimal) -> str:
        side_s = self._norm_side(side)
        q, p = self._norm_qty_px_for_sig(qty, price)
        return f"{side_s}|{self._fmt_dec(q)}|{self._fmt_dec(p)}"

    async def _load_symbol_filters(self) -> None:
        """Load PRICE_FILTER / LOT_SIZE / MIN_NOTIONAL (or NOTIONAL) for the symbol."""
        # Fallback defaults (если exchangeInfo не ответит)
        self._price_tick_size = Decimal("0.01")
        self._qty_step_size = Decimal("0.000001")
        self._min_qty = Decimal("0")
        self._min_notional = Decimal("5")

        if self.client is None:
            return

        try:
            info = await self.client.get_symbol_info(self.symbol)
            if not isinstance(info, dict):
                logger.warning(
                    "_load_symbol_filters: get_symbol_info returned non-dict for {}: {}",
                    self.symbol,
                    type(info),
                )
                return

            filters = info.get("filters", []) or []
            for f in filters:
                if not isinstance(f, dict):
                    continue
                ft = str(f.get("filterType", "")).upper()

                if ft == "PRICE_FILTER":
                    self._price_tick_size = self._d(
                        f.get("tickSize", self._price_tick_size),
                        str(self._price_tick_size),
                    )
                elif ft == "LOT_SIZE":
                    self._qty_step_size = self._d(
                        f.get("stepSize", self._qty_step_size),
                        str(self._qty_step_size),
                    )
                    self._min_qty = self._d(
                        f.get("minQty", self._min_qty),
                        str(self._min_qty),
                    )
                elif ft in {"MIN_NOTIONAL", "NOTIONAL"}:
                    mn = f.get("minNotional")
                    if mn is None:
                        mn = f.get("notional")
                    if mn is not None:
                        self._min_notional = self._d(mn, str(self._min_notional))

            logger.info(
                "BinanceDemoExecution symbol filters | symbol={} tick_size={} step_size={} min_qty={} min_notional={}",
                self.symbol,
                self._price_tick_size,
                self._qty_step_size,
                self._min_qty,
                self._min_notional,
            )
        except Exception as e:
            logger.warning("_load_symbol_filters failed for {}: {}", self.symbol, e)

    def _next_local_order_id(self) -> str:
        self._local_order_seq += 1
        return f"LOCAL-{self._local_order_seq}"

    def _append_local_open_order(self, *, order_id: str, side: str, qty: Decimal, limit_price: Decimal) -> DemoPlacedOrder:
        order = DemoPlacedOrder(
            order_id=str(order_id),
            symbol=self.symbol,
            side=str(side).upper(),
            qty=Decimal(str(qty)),
            limit_price=Decimal(str(limit_price)),
            status="PENDING_SUBMIT",
            ts=datetime.now(timezone.utc),
        )
        self._pending_local_orders[order.order_id] = order
        self.open_orders.append(order)
        self._local_order_sig[order.order_id] = self._make_order_sig(order.side, order.qty, order.limit_price)
        return order

    def _order_price_for_match(self, row: Any) -> Decimal:
        if isinstance(row, DemoPlacedOrder):
            return self._d(row.limit_price)
        if isinstance(row, dict):
            return self._d(row.get("price", "0"))
        return Decimal("0")

    def _prune_stale_local_placeholders(self) -> None:
        """Drop stale local placeholders that never appeared on the exchange snapshot.

        This prevents LOCAL-* placeholders from accumulating and inflating open_orders
        when submit/ack timing is unlucky or the exchange rejected the order.
        """
        if not self._pending_local_orders:
            self.local_pending_open_orders_count = 0
            return

        now = datetime.now(timezone.utc)
        kept: dict[str, DemoPlacedOrder] = {}

        for local_id, order in list(self._pending_local_orders.items()):
            ts = getattr(order, "ts", None)
            age_sec = None
            try:
                if isinstance(ts, datetime):
                    dt = ts if ts.tzinfo is not None else ts.replace(tzinfo=timezone.utc)
                    age_sec = (now - dt).total_seconds()
            except Exception:
                age_sec = None

            status = str(getattr(order, "status", "") or "").upper()
            is_terminal = status in {"REJECTED", "CANCELED", "CANCELLED", "FILLED", "EXPIRED", "DUPLICATE_SKIPPED"}
            too_old = (age_sec is not None and age_sec > self._pending_local_order_ttl_sec)

            if is_terminal or too_old:
                sig = self._local_order_sig.pop(local_id, None)
                if sig:
                    self._pending_submit_sigs.discard(sig)
                continue

            kept[local_id] = order

        self._pending_local_orders = kept
        self.local_pending_open_orders_count = len(self._pending_local_orders)

    def _rebuild_open_orders_with_pending(self, broker_rows: list[Any]) -> None:
        """Merge broker open orders with local placeholders to avoid duplicate re-placement.

        Local placeholders are kept only until we can match them to a broker order by
        (side, rounded qty, rounded price).
        """
        # Match and clear placeholders that have reached broker open_orders snapshot.
        pending_items = list(self._pending_local_orders.items())
        matched_local_ids: set[str] = set()
        used_broker_idx: set[int] = set()

        for local_id, local in pending_items:
            local_side = str(getattr(local, "side", "")).upper()
            local_qty = self._d(getattr(local, "qty", "0"))
            local_px = self._d(getattr(local, "limit_price", "0"))

            # Compare with exchange-rounded values to avoid tiny formatting mismatches.
            if self._qty_step_size > 0:
                local_qty = self._round_down_step(local_qty, self._qty_step_size)
            if self._price_tick_size > 0:
                local_px = self._round_down_step(local_px, self._price_tick_size)

            for idx, row in enumerate(broker_rows):
                if idx in used_broker_idx or not isinstance(row, dict):
                    continue

                side_b = str(row.get("side", "")).upper()
                qty_b = self._d(row.get("origQty", row.get("orig_qty", "0")))
                px_b = self._d(row.get("price", "0"))

                if self._qty_step_size > 0:
                    qty_b = self._round_down_step(qty_b, self._qty_step_size)
                if self._price_tick_size > 0:
                    px_b = self._round_down_step(px_b, self._price_tick_size)

                if side_b == local_side and qty_b == local_qty and px_b == local_px:
                    matched_local_ids.add(local_id)
                    used_broker_idx.add(idx)
                    break

        for local_id in matched_local_ids:
            self._pending_local_orders.pop(local_id, None)
            sig = self._local_order_sig.pop(local_id, None)
            if sig:
                self._pending_submit_sigs.discard(sig)

        # Prune stale local placeholders
        self._prune_stale_local_placeholders()

        # Expose broker orders + still-pending placeholders (for adapter dedupe).
        broker_rows = self._filter_active_open_order_rows(list(broker_rows))
        self.exchange_open_orders_count = len(broker_rows)
        self.local_pending_open_orders_count = len(self._pending_local_orders)
        self.open_orders = list(broker_rows) + list(self._pending_local_orders.values())

    def _normalize_order_row(self, row: dict[str, Any]) -> dict[str, Any]:
        order_id = str(row.get("orderId", row.get("order_id", "")))
        status = str(row.get("status", "")).upper()
        side = str(row.get("side", "")).upper()
        client_oid = row.get("clientOrderId") or row.get("client_order_id") or row.get("origClientOrderId")
        client_oid_s = str(client_oid).strip() if client_oid is not None else ""
        return {
            "order_id": order_id,
            "client_order_id": client_oid_s or None,
            "symbol": str(row.get("symbol", self.symbol)).upper(),
            "side": side,
            "status": status,
            "orig_qty": self._d(row.get("origQty", row.get("orig_qty", "0"))),
            "executed_qty": self._d(row.get("executedQty", row.get("executed_qty", "0"))),
            "price": self._d(row.get("price", "0")),
            "update_ts": self._safe_dt_ms(row.get("updateTime") or row.get("transactTime") or row.get("time")),
        }

    @staticmethod
    def _is_terminal_order_status(status: Any) -> bool:
        st = str(status or "").upper()
        return st in {"FILLED", "CANCELED", "CANCELLED", "REJECTED", "EXPIRED", "CLOSED"}

    def _remaining_qty_from_order_row(self, row: dict[str, Any]) -> Decimal:
        orig = self._d(row.get("origQty", row.get("orig_qty", row.get("qty", "0"))))
        execd = self._d(row.get("executedQty", row.get("executed_qty", row.get("filled_qty", "0"))))
        rem = orig - execd
        return rem if rem > 0 else Decimal("0")

    def _filter_active_open_order_rows(self, rows: list[Any]) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for raw in rows:
            if not isinstance(raw, dict):
                continue
            if str(raw.get("symbol", self.symbol)).upper() != self.symbol:
                continue
            if self._is_terminal_order_status(raw.get("status")):
                continue
            if self._remaining_qty_from_order_row(raw) <= 0:
                continue
            out.append(raw)
        return out

    def refresh_balances(self):
        """Sync/async-compatible balance refresh.

        - If called from async code: returns an asyncio.Task (awaitable).
        - If called from sync code inside a running event loop: schedules the task immediately.
        - If no loop is running: returns the coroutine for the caller to await/run.
        """
        coro = self._refresh_balances_async()
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return coro
        return loop.create_task(coro)

    async def _refresh_balances_async(self) -> None:
        if self.client is None:
            return
        try:
            account = await self.client.get_account()

            # Pull account-level commissions so strategy adapters can use real rates.
            if isinstance(account, dict):
                try:
                    raw_maker_comm = account.get("makerCommission")
                    raw_taker_comm = account.get("takerCommission")
                    self.makerCommission = int(raw_maker_comm) if raw_maker_comm is not None else None
                    self.takerCommission = int(raw_taker_comm) if raw_taker_comm is not None else None
                except Exception:
                    # Keep previous values if parsing fails.
                    pass

                rates_obj = account.get("commissionRates")
                if isinstance(rates_obj, dict):
                    self.account_commission_rates = dict(rates_obj)
                    maker_rate_raw = rates_obj.get("maker")
                    taker_rate_raw = rates_obj.get("taker")
                    if maker_rate_raw is not None:
                        self.maker_fee_rate = self._d(maker_rate_raw, str(self.maker_fee_rate))
                    if taker_rate_raw is not None:
                        self.taker_fee_rate = self._d(taker_rate_raw, str(self.taker_fee_rate))
                else:
                    # Fallback for older schemas: raw makerCommission/takerCommission are usually
                    # integer-like values (e.g. 10 => 0.001 = 0.1%). Convert to decimal rate.
                    if self.makerCommission is not None:
                        self.maker_fee_rate = self._d(self.makerCommission) / Decimal("10000")
                    if self.takerCommission is not None:
                        self.taker_fee_rate = self._d(self.takerCommission) / Decimal("10000")

                if not self._commissions_logged:
                    logger.info(
                        "BinanceDemoExecution account commissions | symbol={} maker_fee_rate={} taker_fee_rate={} raw_makerCommission={} raw_takerCommission={}",
                        self.symbol,
                        self.maker_fee_rate,
                        self.taker_fee_rate,
                        self.makerCommission,
                        self.takerCommission,
                    )
                    self._commissions_logged = True

            balances = {row.get("asset"): row for row in account.get("balances", [])}
            q = balances.get(self.quote_asset, {})
            b = balances.get(self.base_asset, {})

            q_free = Decimal(str(q.get("free", "0")))
            q_locked = Decimal(str(q.get("locked", "0")))
            b_free = Decimal(str(b.get("free", "0")))
            b_locked = Decimal(str(b.get("locked", "0")))

            self.quote_balance_free = q_free
            self.quote_balance_locked = q_locked
            self.quote_balance_total = q_free + q_locked
            self.base_balance_free = b_free
            self.base_balance_locked = b_locked
            self.base_balance_total = b_free + b_locked

            # Backward compatibility: legacy fields remain "free" balances.
            self.quote_balance = self.quote_balance_free
            self.base_balance = self.base_balance_free
        except Exception as e:
            logger.warning("refresh_balances failed: {}", e)
            self._commissions_logged = False

    def refresh_open_orders(self):
        """Sync/async-compatible open-orders refresh (see refresh_balances)."""
        coro = self._refresh_open_orders_async()
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return coro
        return loop.create_task(coro)

    async def _refresh_open_orders_async(self) -> None:
        if self.client is None:
            self.open_orders = []
            return
        try:
            raw_rows = list(await self.client.get_open_orders(symbol=self.symbol))
            rows = self._filter_active_open_order_rows(raw_rows)
            self.exchange_open_orders_count = len(rows)
            self._rebuild_open_orders_with_pending(rows)

            # During startup sync, do not emit historical order events from the current
            # open-orders snapshot. We only warm the cache to prevent log spam.
            suppress_startup_order_events = bool(self._prime_order_history_on_first_poll)

            # Keep cache in sync for currently open orders (NEW / PARTIALLY_FILLED on exchange side)
            for row in rows:
                norm = self._normalize_order_row(row if isinstance(row, dict) else {})
                oid = norm.get("order_id")
                if not oid:
                    continue
                prev = self._order_state_cache.get(oid)
                self._order_state_cache[oid] = norm
                if suppress_startup_order_events:
                    continue

                if prev is None:
                    self._order_events_buffer.append(
                        DemoOrderEvent(
                            order_id=oid,
                            symbol=str(norm["symbol"]),
                            side=str(norm["side"]),
                            status=str(norm["status"] or "NEW"),
                            orig_qty=norm["orig_qty"],
                            executed_qty=norm["executed_qty"],
                            price=norm["price"],
                            update_ts=norm["update_ts"],
                        )
                    )
                else:
                    # Emit event if status or executed qty changed (e.g. partial fill progression)
                    if (
                        str(prev.get("status", "")) != str(norm.get("status", ""))
                        or Decimal(str(prev.get("executed_qty", "0"))) != Decimal(str(norm.get("executed_qty", "0")))
                    ):
                        self._order_events_buffer.append(
                            DemoOrderEvent(
                                order_id=oid,
                                symbol=str(norm["symbol"]),
                                side=str(norm["side"]),
                                status=str(norm["status"] or "NEW"),
                                orig_qty=norm["orig_qty"],
                                executed_qty=norm["executed_qty"],
                                price=norm["price"],
                                update_ts=norm["update_ts"],
                            )
                        )
        except Exception as e:
            self.exchange_open_orders_count = 0
            self.local_pending_open_orders_count = len(self._pending_local_orders)
            logger.warning("refresh_open_orders failed: {}", e)

    async def poll_new_fills(self) -> list[Any]:
        """Poll user trades and return only new fills since last poll.

        Falls back to local simulated buffer if no real trades API data is available.
        """
        out: list[Any] = []

        if self.client is not None:
            trades = list(await self.client.get_my_trades(symbol=self.symbol, limit=100))

            # Make ordering deterministic: oldest -> newest
            try:
                trades.sort(
                    key=lambda x: int(x.get("time", 0)) if isinstance(x, dict) and x.get("time") is not None else 0
                )
            except Exception:
                pass

            skipped_startup_trades = 0
            for tr in trades:
                if not isinstance(tr, dict):
                    continue
                trade_id = tr.get("id")
                order_id = tr.get("orderId")
                key = f"{self.symbol}:{order_id}:{trade_id}"
                if key in self._seen_trade_keys:
                    continue
                self._seen_trade_keys.add(key)
                if self._prime_trade_history_on_first_poll:
                    skipped_startup_trades += 1
                    continue

                qty = self._d(tr.get("qty", "0"))
                price = self._d(tr.get("price", "0"))
                fee = self._d(tr.get("commission", "0"))
                commission_asset = str(tr.get("commissionAsset", "") or "").strip().upper()
                side = "BUY" if bool(tr.get("isBuyer", False)) else "SELL"
                maker = bool(tr.get("isMaker", False))

                fee_quote = Decimal("0")
                fee_base = Decimal("0")
                try:
                    quote_asset_u = str(self.quote_asset or "").strip().upper()
                    base_asset_u = str(self.base_asset or "").strip().upper()
                    if commission_asset and commission_asset == quote_asset_u:
                        fee_quote = fee
                    elif commission_asset and commission_asset == base_asset_u:
                        fee_base = fee
                except Exception:
                    fee_quote = Decimal("0")
                    fee_base = Decimal("0")

                oid_s = str(order_id) if order_id is not None else ""
                cached = self._order_state_cache.get(oid_s) if oid_s else None

                # Update executed_qty in cache from trades so we can emit FILLED without waiting for /allOrders refresh
                if oid_s and isinstance(cached, dict) and qty > 0:
                    try:
                        prev_exec = self._d(cached.get("executed_qty", "0"))
                        orig_qty = self._d(cached.get("orig_qty", cached.get("origQty", "0")))
                        new_exec = prev_exec + qty

                        if orig_qty > 0 and new_exec > orig_qty:
                            new_exec = orig_qty

                        cached["executed_qty"] = new_exec
                        cached["update_ts"] = datetime.now(timezone.utc)

                        eps = self._qty_step_size if self._qty_step_size and self._qty_step_size > 0 else Decimal("0")
                        if orig_qty > 0:
                            cached["status"] = "PARTIALLY_FILLED" if (new_exec + eps < orig_qty) else "FILLED"

                        self._order_state_cache[oid_s] = cached
                    except Exception:
                        pass

                # Best-effort map exchange orderId -> clientOrderId from cache
                client_oid = None
                try:
                    if isinstance(cached, dict):
                        client_oid = cached.get("client_order_id")
                except Exception:
                    client_oid = None

                # Derive ORDER finality from (updated) cached order state.
                status_fill: Any = "?"
                partial_flag: Optional[bool] = None
                try:
                    if isinstance(cached, dict):
                        orig_qty = self._d(cached.get("orig_qty", cached.get("origQty", "0")))
                        exec_qty = self._d(cached.get("executed_qty", cached.get("executedQty", "0")))
                        eps = self._qty_step_size if self._qty_step_size and self._qty_step_size > 0 else Decimal("0")
                        if orig_qty > 0:
                            partial_flag = bool(exec_qty + eps < orig_qty)
                            status_fill = "PARTIALLY_FILLED" if partial_flag else "FILLED"
                        else:
                            status_fill = str(cached.get("status", "?") or "?")
                except Exception:
                    status_fill = "?"
                    partial_flag = None

                fill_obj = DemoFill(
                    order_id=str(order_id if order_id is not None else trade_id),
                    client_order_id=str(client_oid).strip() if client_oid else None,
                    symbol=self.symbol,
                    side=side,
                    qty=qty,
                    price=price,
                    fee_quote=fee_quote,
                    maker=maker,
                )

                try:
                    if commission_asset:
                        setattr(fill_obj, "commission_asset", commission_asset)
                    if fee_base > 0:
                        setattr(fill_obj, "fee_base", fee_base)
                except Exception:
                    pass

                try:
                    setattr(fill_obj, "status", status_fill)
                except Exception:
                    pass
                try:
                    if partial_flag is not None:
                        setattr(fill_obj, "partial", partial_flag)
                except Exception:
                    pass

                out.append(fill_obj)
            if self._prime_trade_history_on_first_poll:
                # после первого прохода просто "прогрели" seen keys
                self._prime_trade_history_on_first_poll = False
                return []

            # если здесь оказались — значит уже не prime, возвращаем новые fills
            return out

        # fallback: если client is None — вернуть буфер/пусто
        if self._fills_buffer:
            out = list(self._fills_buffer)
            self._fills_buffer.clear()
            return out
        return []

    async def poll_order_updates(self) -> list[Any]:
        """Poll order status updates (NEW/PARTIALLY_FILLED/FILLED/CANCELED) and return deltas.

        This is a best-effort REST polling implementation for demo mode. It compares the
        latest `/allOrders` snapshot to a local cache and emits events when status or
        executed quantity changes.
        """
        if self.client is None:
            if not self._order_events_buffer:
                return []
            out = list(self._order_events_buffer)
            self._order_events_buffer.clear()
            return out

        try:
            rows = list(await self.client.get_all_orders(symbol=self.symbol, limit=200))
            if self._prime_order_history_on_first_poll:
                primed_count = 0
                for row in rows:
                    if not isinstance(row, dict):
                        continue
                    norm = self._normalize_order_row(row)
                    oid = norm.get("order_id")
                    if not oid:
                        continue
                    self._order_state_cache[oid] = norm
                    primed_count += 1
                # Purge any events already queued during startup
                self._order_events_buffer.clear()
                self._prime_order_history_on_first_poll = False
                if primed_count:
                    logger.info(
                        "BinanceDemoExecution startup history suppressed | order_events_skipped={}",
                        primed_count,
                    )
                return []
            for row in rows:
                if not isinstance(row, dict):
                    continue
                norm = self._normalize_order_row(row)
                oid = norm.get("order_id")
                if not oid:
                    continue

                prev = self._order_state_cache.get(oid)
                self._order_state_cache[oid] = norm

                if prev is None:
                    self._order_events_buffer.append(
                        DemoOrderEvent(
                            order_id=oid,
                            symbol=str(norm["symbol"]),
                            side=str(norm["side"]),
                            status=str(norm["status"]),
                            orig_qty=norm["orig_qty"],
                            executed_qty=norm["executed_qty"],
                            price=norm["price"],
                            update_ts=norm["update_ts"],
                        )
                    )
                    continue

                prev_status = str(prev.get("status", ""))
                prev_exec = self._d(prev.get("executed_qty", "0"))
                new_status = str(norm.get("status", ""))
                new_exec = self._d(norm.get("executed_qty", "0"))

                if prev_status != new_status or prev_exec != new_exec:
                    self._order_events_buffer.append(
                        DemoOrderEvent(
                            order_id=oid,
                            symbol=str(norm["symbol"]),
                            side=str(norm["side"]),
                            status=new_status,
                            orig_qty=norm["orig_qty"],
                            executed_qty=norm["executed_qty"],
                            price=norm["price"],
                            update_ts=norm["update_ts"],
                        )
                    )

                # Remove from open_orders if terminal
                if self._is_terminal_order_status(norm.get("status")):
                    filtered_open: list[Any] = []
                    for existing in self.open_orders:
                        try:
                            if isinstance(existing, dict):
                                ex_oid = str(existing.get("orderId", existing.get("order_id", "")))
                                ex_status = str(existing.get("status", "")).upper()
                                if ex_oid == oid or self._is_terminal_order_status(ex_status):
                                    continue
                                if self._remaining_qty_from_order_row(existing) <= 0:
                                    continue
                                filtered_open.append(existing)
                            else:
                                ex_oid = str(getattr(existing, "order_id", ""))
                                ex_status = str(getattr(existing, "status", "")).upper()
                                if ex_oid == oid or self._is_terminal_order_status(ex_status):
                                    sig = self._local_order_sig.pop(ex_oid, None)
                                    if sig:
                                        self._pending_submit_sigs.discard(sig)
                                    self._pending_local_orders.pop(ex_oid, None)
                                    continue
                                filtered_open.append(existing)
                        except Exception:
                            filtered_open.append(existing)
                    self.open_orders = filtered_open
                    self.local_pending_open_orders_count = len(self._pending_local_orders)
                    self.exchange_open_orders_count = sum(1 for x in self.open_orders if isinstance(x, dict))
        except Exception as e:
            logger.warning("poll_order_updates failed (get_all_orders): {}", e)

        if not self._order_events_buffer:
            return []
        out = list(self._order_events_buffer)
        self._order_events_buffer.clear()
        return out

    def place_limit(self, side: Any, *, qty: Decimal, limit_price: Decimal) -> DemoPlacedOrder:
        """
        Sync API для GridBacktestAdapter.

        Адаптер вызывает place_limit синхронно, поэтому здесь мы:
        1) сразу создаём локальный placeholder-ордер (чтобы работал dedupe в адаптере)
        2) асинхронно отправляем реальный LIMIT ордер в Binance
        """
        side_s = self._norm_side(side)
        qty_d = Decimal(str(qty))
        px_d = Decimal(str(limit_price))

        if qty_d <= 0:
            raise ValueError("qty must be > 0")
        if px_d <= 0:
            raise ValueError("limit_price must be > 0")

        # IMPORTANT: apply exchange rounding early (same as async submit)
        # so dedupe/signatures don't drift and leak.
        try:
            if self._qty_step_size > 0:
                qty_d = self._round_down_step(qty_d, self._qty_step_size)
            if self._price_tick_size > 0:
                px_d = self._round_down_step(px_d, self._price_tick_size)
            qty_d = self._d(self._fmt_dec(qty_d))
            px_d = self._d(self._fmt_dec(px_d))
        except Exception:
            # keep original values if rounding fails for any reason
            qty_d = Decimal(str(qty))
            px_d = Decimal(str(limit_price))

        if qty_d <= 0:
            raise ValueError("qty rounded to 0 after LOT_SIZE step")
        if px_d <= 0:
            raise ValueError("limit_price rounded to 0 after PRICE_FILTER tick")

        # Hard execution-layer dedupe: if the same side/qty/price is already pending/open,
        # return the existing local placeholder instead of submitting another order.
        sig = self._make_order_sig(side_s, qty_d, px_d)
        if sig in self._pending_submit_sigs:
            for o in self.open_orders:
                try:
                    if isinstance(o, dict):
                        o_side = str(o.get("side", "")).upper()
                        o_qty = self._d(o.get("origQty", o.get("orig_qty", o.get("qty", "0"))))
                        o_px = self._d(o.get("price", o.get("limit_price", "0")))
                    else:
                        o_side = self._norm_side(getattr(o, "side", ""))
                        o_qty = self._d(getattr(o, "qty", "0"))
                        o_px = self._d(getattr(o, "limit_price", "0"))
                    if self._make_order_sig(o_side, o_qty, o_px) == sig:
                        return o
                except Exception:
                    continue

            # Pending signature exists but no matching open order.
            # This means dedupe cache is stale; clear it and proceed with fresh placement.
            stale_local_ids: list[str] = []
            for local_id, local_sig in list(self._local_order_sig.items()):
                if local_sig == sig:
                    stale_local_ids.append(str(local_id))
            for local_id in stale_local_ids:
                self._local_order_sig.pop(local_id, None)
                self._pending_local_orders.pop(local_id, None)
            if stale_local_ids:
                stale_local_set = set(stale_local_ids)
                try:
                    self.open_orders = [
                        o
                        for o in self.open_orders
                        if not (
                            not isinstance(o, dict)
                            and str(getattr(o, "order_id", "")) in stale_local_set
                        )
                    ]
                except Exception:
                    pass
            self.local_pending_open_orders_count = len(self._pending_local_orders)
            self._pending_submit_sigs.discard(sig)
            logger.warning(
                "DEMO PLACE_LIMIT stale dedupe signature cleared | symbol={} side={} qty={} price={} stale_local_ids={}",
                self.symbol,
                side_s,
                qty_d,
                px_d,
                stale_local_ids,
            )

        # Also guard against already-confirmed broker open order duplicates.
        for o in self.open_orders:
            try:
                if isinstance(o, dict):
                    o_side = str(o.get("side", "")).upper()
                    o_qty = self._d(o.get("origQty", o.get("orig_qty", o.get("qty", "0"))))
                    o_px = self._d(o.get("price", "0"))
                else:
                    o_side = self._norm_side(getattr(o, "side", ""))
                    o_qty = self._d(getattr(o, "qty", "0"))
                    o_px = self._d(getattr(o, "limit_price", "0"))
                if self._make_order_sig(o_side, o_qty, o_px) == sig:
                    return o
            except Exception:
                continue

        local_id = self._next_local_order_id()
        order = self._append_local_open_order(order_id=local_id, side=side_s, qty=qty_d, limit_price=px_d)
        self._pending_submit_sigs.add(sig)

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            # Нет loop — оставляем локальный placeholder (для no-op/тестового сценария)
            if not self._warned_no_place_limit_backend:
                logger.warning("place_limit called without running asyncio loop; keeping local placeholder orders only")
                self._warned_no_place_limit_backend = True
            return order

        loop.create_task(
            self._place_limit_async(
                local_order=order,
                side=side_s,
                qty=qty_d,
                limit_price=px_d,
            )
        )
        return order

    # -----------------
    # Order cancellation
    # -----------------
    def cancel_order(
        self,
        *,
        order_id: Any = None,
        client_order_id: Any = None,
        symbol: Optional[str] = None,
    ):
        """Sync/async-compatible cancel."""
        coro = self._cancel_order_async(order_id=order_id, client_order_id=client_order_id, symbol=symbol)
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return coro
        return loop.create_task(coro)

    async def _cancel_order_async(
        self,
        *,
        order_id: Any = None,
        client_order_id: Any = None,
        symbol: Optional[str] = None,
    ) -> bool:
        sym = (symbol or self.symbol or "").upper()
        oid = (str(order_id).strip() if order_id is not None else "")
        coid = (str(client_order_id).strip() if client_order_id is not None else "")

        # 1) LOCAL-* placeholder cleanup (ONLY if it's order_id)
        if oid.upper().startswith("LOCAL-"):
            try:
                self._pending_local_orders.pop(oid, None)
                sig = self._local_order_sig.pop(oid, None)
                if sig:
                    self._pending_submit_sigs.discard(sig)
            except Exception:
                pass
            try:
                self.open_orders = [o for o in self.open_orders if str(getattr(o, "order_id", "")) != oid]
            except Exception:
                pass
            self.local_pending_open_orders_count = len(self._pending_local_orders)
            return True

        if self.client is None:
            return False

        try:
            if self._client_lock is None:
                self._client_lock = asyncio.Lock()

            async with self._client_lock:
                kwargs: dict[str, Any] = {"symbol": sym}
                if oid:
                    kwargs["orderId"] = oid
                elif coid:
                    kwargs["origClientOrderId"] = coid
                else:
                    return False

                resp = await self.client.cancel_order(**kwargs)

            # best-effort remove from local open_orders cache
            try:
                filtered: list[Any] = []
                for existing in self.open_orders:
                    if isinstance(existing, dict):
                        ex_oid = str(existing.get("orderId", existing.get("order_id", "")) or "")
                        ex_coid = str(
                            existing.get("clientOrderId")
                            or existing.get("client_order_id")
                            or existing.get("origClientOrderId")
                            or ""
                        )
                        if oid and ex_oid == oid:
                            continue
                        if coid and ex_coid == coid:
                            continue
                        filtered.append(existing)
                    else:
                        ex_oid = str(getattr(existing, "order_id", "") or "")
                        if oid and ex_oid == oid:
                            continue
                        filtered.append(existing)
                self.open_orders = filtered
            except Exception:
                pass

            logger.info(
                "DEMO CANCEL ok | symbol={} order_id={} client_order_id={} resp_type={}",
                sym, oid or None, coid or None, type(resp).__name__,
            )
            return True

        except Exception as e:
            err_detail = self._format_exc(e)
            logger.warning(
                "DEMO CANCEL failed | symbol={} order_id={} client_order_id={} err={} detail={}",
                sym,
                oid or None,
                coid or None,
                (str(e) or "").strip() or err_detail,
                err_detail,
            )
            try:
                logger.debug(
                    "DEMO CANCEL traceback:\n{}",
                    "".join(traceback.format_exception(type(e), e, e.__traceback__)),
                )
            except Exception:
                pass
            return False

    def cancel_all_open_orders(self, *, symbol: Optional[str] = None):
        coro = self._cancel_all_open_orders_async(symbol=symbol)
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return coro
        return loop.create_task(coro)

    async def _cancel_all_open_orders_async(self, *, symbol: Optional[str] = None) -> int:
        sym = (symbol or self.symbol or "").upper()

        # cancel local placeholders first
        try:
            for local_id in list(self._pending_local_orders.keys()):
                await self._cancel_order_async(order_id=local_id, symbol=sym)
        except Exception:
            pass

        if self.client is None:
            return 0

        # Prefer exchange endpoint if python-binance exposes it
        try:
            if hasattr(self.client, "cancel_open_orders"):
                await self.client.cancel_open_orders(symbol=sym)
                try:
                    await self.refresh_open_orders()
                except Exception:
                    pass
                logger.info("DEMO CANCEL_ALL ok | symbol={} mode=endpoint", sym)
                return 1
        except Exception as e:
            err_detail = self._format_exc(e)
            logger.warning(
                "DEMO CANCEL_ALL failed | symbol={} err={} detail={}",
                sym, (str(e) or "").strip() or err_detail, err_detail,
            )

        # Fallback: iterate snapshot
        attempts = 0
        try:
            snapshot = list(self.open_orders)
        except Exception:
            snapshot = []

        for o in snapshot:
            if isinstance(o, dict):
                ex_oid = o.get("orderId") or o.get("order_id")
                ex_coid = o.get("clientOrderId") or o.get("client_order_id") or o.get("origClientOrderId")
                if ex_oid is None and ex_coid is None:
                    continue
                await self._cancel_order_async(order_id=ex_oid, client_order_id=ex_coid, symbol=sym)
                attempts += 1
            else:
                ex_oid = getattr(o, "order_id", None)
                if ex_oid is None:
                    continue
                await self._cancel_order_async(order_id=ex_oid, symbol=sym)
                attempts += 1

        try:
            await self.refresh_open_orders()
        except Exception:
            pass

        logger.info("DEMO CANCEL_ALL done | symbol={} attempts={}", sym, attempts)
        return attempts        

    async def _place_limit_async(self, *, local_order: DemoPlacedOrder, side: str, qty: Decimal, limit_price: Decimal) -> None:
        if self.client is None:
            # no-op режим: локальный ордер останется в open_orders
            return

        try:
            if self._client_lock is None:
                self._client_lock = asyncio.Lock()

            qty_send = Decimal(str(qty))
            px_send = Decimal(str(limit_price))

            # Приводим к биржевым фильтрам
            if self._qty_step_size > 0:
                qty_send = self._round_down_step(qty_send, self._qty_step_size)
            if self._price_tick_size > 0:
                px_send = self._round_down_step(px_send, self._price_tick_size)

            qty_send = self._d(self._fmt_dec(qty_send))
            px_send = self._d(self._fmt_dec(px_send))

            # SELL safety:
            # - allow tiny clamp (dust-level) to absorb rounding/commission tails
            # - but reject significant shortfall instead of placing partial SELL,
            #   otherwise adapter enters cancel/replace churn on qty mismatch
            if str(side).upper() == "SELL":
                try:
                    await self.refresh_balances()
                except Exception:
                    pass

                free_base = self._d(self.base_balance)
                if self._qty_step_size > 0:
                    free_base = self._round_down_step(free_base, self._qty_step_size)
                free_base = self._d(self._fmt_dec(free_base))

                if qty_send > free_base:
                    shortfall = qty_send - free_base
                    tiny_shortfall = Decimal("0")
                    if self._qty_step_size and self._qty_step_size > 0:
                        tiny_shortfall = self._qty_step_size * Decimal("2")

                    if shortfall <= tiny_shortfall and free_base > 0:
                        old_qty = qty_send
                        qty_send = free_base
                        sig_warn = f"{self.symbol}|SELL|{self._fmt_dec(old_qty)}|{self._fmt_dec(qty_send)}|{self._fmt_dec(px_send)}"
                        if getattr(self, "_warned_sell_balance_adjust_sig", None) != sig_warn:
                            logger.info(
                                "DEMO PLACE_LIMIT sell qty adjusted to free balance | symbol={} requested_qty={} adjusted_qty={} free_base={} price={}",
                                self.symbol,
                                old_qty,
                                qty_send,
                                free_base,
                                px_send,
                            )
                            self._warned_sell_balance_adjust_sig = sig_warn
                    else:
                        raise RuntimeError(
                            f"SELL_QTY_EXCEEDS_FREE_BASE requested={self._fmt_dec(qty_send)} "
                            f"free_base={self._fmt_dec(free_base)} shortfall={self._fmt_dec(shortfall)} "
                            f"price={self._fmt_dec(px_send)}"
                        )

            # BUY safety: clamp to free quote balance to avoid -2010 insufficient balance.
            elif str(side).upper() == "BUY":
                try:
                    await self.refresh_balances()
                except Exception:
                    pass

                free_quote = self._d(self.quote_balance)
                free_quote = self._d(self._fmt_dec(free_quote))

                if free_quote > 0 and px_send > 0:
                    # Max affordable base qty = free_quote / price
                    max_qty = free_quote / px_send
                    if self._qty_step_size > 0:
                        max_qty = self._round_down_step(max_qty, self._qty_step_size)
                    max_qty = self._d(self._fmt_dec(max_qty))

                    if max_qty > 0 and qty_send > max_qty:
                        old_qty = qty_send
                        qty_send = max_qty
                        sig_warn = f"{self.symbol}|BUY|{self._fmt_dec(old_qty)}|{self._fmt_dec(qty_send)}|{self._fmt_dec(px_send)}"
                        if getattr(self, "_warned_buy_balance_adjust_sig", None) != sig_warn:
                            logger.info(
                                "DEMO PLACE_LIMIT buy qty adjusted to free quote | symbol={} requested_qty={} adjusted_qty={} free_quote={} price={}",
                                self.symbol,
                                old_qty,
                                qty_send,
                                free_quote,
                                px_send,
                            )
                            self._warned_buy_balance_adjust_sig = sig_warn

            # --- keep dedupe signatures consistent if qty/price changed after rounding/clamps ---
            local_id = str(getattr(local_order, "order_id", ""))
            old_sig = self._local_order_sig.get(local_id)
            new_sig = self._make_order_sig(side, qty_send, px_send)
            if old_sig != new_sig:
                if old_sig:
                    self._pending_submit_sigs.discard(old_sig)
                self._pending_submit_sigs.add(new_sig)
            else:
                # ensure sig is present in pending set (defensive)
                if new_sig:
                    self._pending_submit_sigs.add(new_sig)
            self._local_order_sig[local_id] = new_sig

            if qty_send <= 0:
                raise ValueError("qty rounded to 0 after LOT_SIZE step")
            if self._min_qty > 0 and qty_send < self._min_qty:
                raise ValueError(f"qty {qty_send} < min_qty {self._min_qty}")
            if self._min_notional > 0 and (qty_send * px_send) < self._min_notional:
                raise ValueError(f"notional {(qty_send * px_send)} < min_notional {self._min_notional}")

            # Синхронизируем локальный placeholder с фактическими значениями
            local_order.qty = qty_send
            local_order.limit_price = px_send

            resp = await self.client.create_order(
                symbol=self.symbol,
                side=side,
                type="LIMIT",
                timeInForce="GTC",
                quantity=self._fmt_dec(qty_send),
                price=self._fmt_dec(px_send),
                # важно: стабильная связка биржевого orderId ↔ наш LOCAL-*
                newClientOrderId=str(local_order.order_id),
            )

            # Keep local placeholder id stable until refresh_open_orders sees the broker order.
            local_order.status = "ACK_PENDING_SYNC"

            # Seed order state cache immediately so poll_new_fills can derive FILLED/PARTIALLY_FILLED
            # without waiting for open_orders/all_orders refresh.
            try:
                if isinstance(resp, dict):
                    ex_oid = resp.get("orderId") or resp.get("order_id")
                    if ex_oid is not None:
                        seed_row = {
                            "orderId": ex_oid,
                            "clientOrderId": resp.get("clientOrderId") or resp.get("client_order_id") or str(local_order.order_id),
                            "symbol": resp.get("symbol") or self.symbol,
                            "side": resp.get("side") or side,
                            "status": resp.get("status") or "NEW",
                            "origQty": resp.get("origQty") or str(qty_send),
                            "executedQty": resp.get("executedQty") or "0",
                            "price": resp.get("price") or str(px_send),
                            "updateTime": resp.get("updateTime") or resp.get("transactTime") or resp.get("time"),
                        }
                        norm = self._normalize_order_row(seed_row)
                        oid_s = str(norm.get("order_id") or ex_oid)
                        if oid_s:
                            self._order_state_cache[oid_s] = norm
            except Exception:
                pass

            logger.debug(
                "DEMO PLACE_LIMIT submitted | symbol={} side={} qty={} price={} order_id={}",
                self.symbol,
                side,
                qty_send,
                px_send,
                local_order.order_id,
            )

        except Exception as e:
            err_detail = self._format_exc(e)
            err_s = (str(e) or "").strip()
            if not err_s:
                err_s = err_detail
            err_l = err_s.lower()

            # Exchange already has an equivalent order. Keep placeholder as ACK-pending and
            # resync snapshot so adapter/order-dedupe converges without repeated re-submits.
            if "duplicate order sent" in err_l:
                local_order.status = "ACK_PENDING_SYNC"
                try:
                    await self.refresh_open_orders()
                except Exception:
                    pass
                logger.info(
                    "DEMO PLACE_LIMIT duplicate treated as already_open | symbol={} side={} qty={} price={} local_order_id={} err={}",
                    self.symbol,
                    side,
                    qty,
                    limit_price,
                    getattr(local_order, "order_id", None),
                    err_s,
                )
                return

            local_order.status = "REJECTED"
            try:
                local_id = str(getattr(local_order, "order_id", ""))
                sig = self._local_order_sig.pop(local_id, None)
                if sig:
                    self._pending_submit_sigs.discard(sig)
            except Exception:
                pass
            try:
                self._pending_local_orders.pop(str(getattr(local_order, "order_id", "")), None)
                self.open_orders = [o for o in self.open_orders if o is not local_order]
            except Exception:
                pass
            self.local_pending_open_orders_count = len(self._pending_local_orders)

            if "insufficient balance" in err_s.lower() and str(side).upper() == "SELL":
                logger.debug(
                    "DEMO PLACE_LIMIT sell rejected (insufficient balance) | symbol={} side={} qty={} price={} err={} detail={}",
                    self.symbol,
                    side,
                    qty,
                    limit_price,
                    err_s,
                    err_detail,
                )
            else:
                logger.warning(
                    "DEMO PLACE_LIMIT failed | symbol={} side={} qty={} price={} err={} detail={}",
                    self.symbol,
                    side,
                    qty,
                    limit_price,
                    err_s,
                    err_detail,
                )
            try:
                logger.debug(
                    "DEMO PLACE_LIMIT traceback:\n{}",
                    "".join(traceback.format_exception(type(e), e, e.__traceback__)),
                )
            except Exception:
                pass


    async def place_market_buy(self, qty: Decimal, price_hint: Optional[Decimal] = None) -> DemoFill:
        return await self._buffer_sim_fill("BUY", qty, price_hint)

    async def place_market_sell(self, qty: Decimal, price_hint: Optional[Decimal] = None) -> DemoFill:
        return await self._buffer_sim_fill("SELL", qty, price_hint)

    async def _buffer_sim_fill(self, side: str, qty: Decimal, price_hint: Optional[Decimal]) -> DemoFill:
        fill = DemoFill(
            order_id=f"SIM-{side}-{len(self._fills_buffer) + 1}",
            client_order_id=None,
            symbol=self.symbol,
            side=side,
            qty=Decimal(str(qty)),
            price=Decimal(str(price_hint if price_hint is not None else 0)),
            fee_quote=Decimal("0"),
            maker=False,
        )
        self._fills_buffer.append(fill)
        return fill


BinanceDemoExecutor = BinanceDemoExecution
DemoExecution = BinanceDemoExecution
BinanceExecution = BinanceDemoExecution
def _call_strategy_hook(strategy: Any, hook_name: str, **kwargs: Any):
    fn = getattr(strategy, hook_name, None)
    if not callable(fn):
        return None

    try:
        return fn(**kwargs)
    except TypeError:
        pass

    if hook_name == "on_quote" and "bid" in kwargs and "ask" in kwargs:
        try:
            return fn(kwargs["bid"], kwargs["ask"])
        except TypeError:
            pass

        class _QuoteCompat:
            __slots__ = ("bid", "ask", "b", "a", "best_bid", "best_ask")

            def __init__(self, bid, ask):
                self.bid = bid
                self.ask = ask
                self.b = bid
                self.a = ask
                self.best_bid = bid
                self.best_ask = ask

        return fn(_QuoteCompat(kwargs["bid"], kwargs["ask"]))

    return fn(*kwargs.values())
