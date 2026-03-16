from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import os
import re
import urllib.parse
from collections import deque
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from aiohttp import web
from dotenv import load_dotenv
from loguru import logger


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(str(raw).strip())
    except Exception:
        return default


def _allowed_user_ids() -> set[int]:
    raw = str(os.getenv("TELEGRAM_ALLOWED_USER_IDS", "")).strip()
    if not raw:
        return set()
    out: set[int] = set()
    for part in raw.split(","):
        p = part.strip()
        if not p:
            continue
        try:
            out.add(int(p))
        except Exception:
            continue
    return out


def _is_missing(v: Any) -> bool:
    if v is None:
        return True
    s = str(v).strip()
    return s in {"", "?", "None", "null"}


def _to_decimal(v: Any) -> Decimal:
    try:
        return Decimal(str(v))
    except Exception:
        return Decimal("0")


class AuthError(RuntimeError):
    def __init__(self, message: str, *, status: int = 401) -> None:
        super().__init__(message)
        self.status = status


class TelegramMiniAppServer:
    def __init__(self) -> None:
        load_dotenv()

        token = str(os.getenv("TELEGRAM_BOT_TOKEN", "")).strip()
        if not token:
            raise RuntimeError("Missing TELEGRAM_BOT_TOKEN")
        self.bot_token = token

        self.allowed_user_ids = _allowed_user_ids()
        self.runner_service = str(os.getenv("TELEGRAM_RUNNER_SERVICE", "crypto-bot")).strip() or "crypto-bot"
        self.state_file = Path(str(os.getenv("RUN_DEMO_GRID_STATE_FILE", "data/run_demo_grid_state.json")).strip())
        self.log_dir = Path(str(os.getenv("RUN_DEMO_GRID_LOG_DIR", "logs")).strip() or "logs")
        self.log_prefix = str(os.getenv("RUN_DEMO_GRID_LOG_PREFIX", "demo_grid")).strip() or "demo_grid"
        self.log_symbol = str(os.getenv("TELEGRAM_NOTIFY_SYMBOL", "")).strip().upper()
        self.cmd_timeout_sec = max(_env_int("TELEGRAM_CMD_TIMEOUT_SEC", 20), 3)

        self.host = str(os.getenv("TELEGRAM_MINIAPP_HOST", "0.0.0.0")).strip() or "0.0.0.0"
        self.port = max(_env_int("TELEGRAM_MINIAPP_PORT", 8080), 1)
        self.allow_runner_control = _env_bool("TELEGRAM_MINIAPP_ALLOW_RUNNER_CONTROL", default=True)
        self.auth_max_age_sec = max(_env_int("TELEGRAM_MINIAPP_AUTH_MAX_AGE_SEC", 86400), 300)
        self.dev_mode = _env_bool("TELEGRAM_MINIAPP_DEV_MODE", default=False)
        self.dev_user_id = max(_env_int("TELEGRAM_MINIAPP_DEV_USER_ID", 0), 0)
        tz_name = str(os.getenv("TELEGRAM_MINIAPP_TIMEZONE", "Europe/Moscow")).strip() or "Europe/Moscow"
        try:
            self.display_tz = ZoneInfo(tz_name)
            self.display_tz_name = tz_name
        except Exception:
            self.display_tz = timezone.utc
            self.display_tz_name = "UTC"

        self._metrics_cache_key: tuple[str, int, int] | None = None
        self._metrics_cache_value: dict[str, Any] | None = None

        static_dir_raw = str(os.getenv("TELEGRAM_MINIAPP_STATIC_DIR", "")).strip()
        if static_dir_raw:
            self.static_dir = Path(static_dir_raw).expanduser().resolve()
        else:
            self.static_dir = (Path(__file__).resolve().parents[1] / "webapp").resolve()

        index_file = self.static_dir / "index.html"
        if not index_file.exists():
            raise RuntimeError(f"Mini app static file not found: {index_file}")

    async def _run_cmd(self, *argv: str) -> tuple[int, str]:
        proc = await asyncio.create_subprocess_exec(
            *argv,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            out_b, err_b = await asyncio.wait_for(proc.communicate(), timeout=self.cmd_timeout_sec)
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
            return 124, f"timeout after {self.cmd_timeout_sec}s: {' '.join(argv)}"

        out = (out_b or b"").decode("utf-8", errors="replace").strip()
        err = (err_b or b"").decode("utf-8", errors="replace").strip()
        merged = out if not err else (f"{out}\n{err}" if out else err)
        return int(proc.returncode or 0), merged.strip()

    async def _service_state(self) -> str:
        code, out = await self._run_cmd("systemctl", "is-active", self.runner_service)
        if code == 0 and out:
            return out.strip()
        if out:
            return out.strip()
        return "unknown"

    @staticmethod
    def _extract_fill_field(fill_repr: str, name: str) -> str:
        if not fill_repr:
            return "?"
        if name in {"qty", "price", "fee_quote"}:
            m = re.search(rf"{name}=Decimal\\('([^']+)'\\)", fill_repr)
            if m:
                return m.group(1)
            return "?"
        m = re.search(rf"{name}='([^']+)'", fill_repr)
        if m:
            return m.group(1)
        return "?"

    def _effective_symbol(self) -> str:
        if self.log_symbol:
            return self.log_symbol
        if self.state_file.exists():
            try:
                raw = json.loads(self.state_file.read_text(encoding="utf-8"))
                sym = str(raw.get("symbol", "")).strip().upper()
                if sym:
                    return sym
            except Exception:
                pass
        return ""

    def _discover_trades_file(self) -> Path | None:
        symbol = self._effective_symbol()
        if symbol:
            candidate = self.log_dir / f"{self.log_prefix}_{symbol.lower()}_trades.jsonl"
            if candidate.exists():
                return candidate

        if not self.log_dir.exists():
            return None
        pattern = f"{self.log_prefix}_*_trades.jsonl"
        files = sorted(self.log_dir.glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True)
        return files[0] if files else None

    def _discover_metrics_file(self, symbol: str = "") -> Path | None:
        sym = str(symbol or "").strip().upper() or self._effective_symbol()
        if sym:
            candidate = self.log_dir / f"{self.log_prefix}_{sym.lower()}_metrics.jsonl"
            if candidate.exists():
                return candidate

        if not self.log_dir.exists():
            return None
        pattern = f"{self.log_prefix}_*_metrics.jsonl"
        files = sorted(self.log_dir.glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True)
        return files[0] if files else None

    @staticmethod
    def _parse_ts_utc(v: Any) -> datetime | None:
        if v is None:
            return None
        try:
            s = str(v).strip()
            if not s:
                return None
            if s.endswith("Z"):
                s = s[:-1] + "+00:00"
            dt = datetime.fromisoformat(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except Exception:
            return None

    @staticmethod
    def _parse_order_rows(open_orders: Any) -> list[dict[str, Any]]:
        if not isinstance(open_orders, list):
            return []
        rows: list[dict[str, Any]] = []
        for o in open_orders:
            if not isinstance(o, dict):
                continue
            side = str(o.get("side", "?")).upper()
            price = _to_decimal(o.get("price", o.get("limit_price", o.get("limit", o.get("px", "0")))))
            qty = _to_decimal(o.get("origQty", o.get("orig_qty", o.get("qty", o.get("quantity", "0")))))
            oid = str(o.get("orderId", o.get("order_id", o.get("id", "?"))))
            if qty <= 0:
                continue
            rows.append(
                {
                    "order_id": oid,
                    "side": side if side in {"BUY", "SELL"} else "?",
                    "price": str(price),
                    "qty": str(qty),
                    "_price": price,
                }
            )

        rows.sort(
            key=lambda r: (
                0 if r["side"] == "SELL" else 1 if r["side"] == "BUY" else 2,
                float(r["_price"]) if r["side"] == "SELL" else -float(r["_price"]) if r["side"] == "BUY" else 0.0,
            )
        )
        for r in rows:
            r.pop("_price", None)
        return rows

    @staticmethod
    def _step_guess(value: Any) -> Decimal:
        raw = str(value or "").strip()
        if not raw:
            return Decimal("0")
        if "." not in raw:
            return Decimal("1")
        frac = raw.split(".", 1)[1].rstrip("0")
        if not frac:
            return Decimal("1")
        try:
            return Decimal("1").scaleb(-len(frac))
        except Exception:
            return Decimal("0")

    @classmethod
    def _match_active_sell_order(
        cls,
        *,
        sell_orders: list[dict[str, Any]],
        used_indexes: set[int],
        lot_qty: Decimal,
        target_sell_price: Decimal,
    ) -> dict[str, Any] | None:
        best_idx: int | None = None
        best_score: tuple[Decimal, Decimal] | None = None

        for idx, order in enumerate(sell_orders):
            if idx in used_indexes:
                continue
            order_qty = _to_decimal(order.get("origQty", order.get("orig_qty", order.get("qty", "0"))))
            order_price = _to_decimal(order.get("price", order.get("limit_price", order.get("limit", order.get("px", "0")))))
            if order_qty <= 0 or order_price <= 0:
                continue

            qty_tol = cls._step_guess(order.get("origQty", order.get("orig_qty", order.get("qty", "0"))))
            if qty_tol <= 0:
                qty_tol = cls._step_guess(str(lot_qty))
            if qty_tol <= 0:
                qty_tol = Decimal("0.0001")

            price_tol = cls._step_guess(order.get("price", order.get("limit_price", order.get("limit", order.get("px", "0")))))
            if price_tol <= 0:
                price_tol = cls._step_guess(str(target_sell_price))
            if price_tol <= 0:
                price_tol = Decimal("0.01")

            qty_diff = abs(order_qty - lot_qty)
            price_diff = abs(order_price - target_sell_price)
            if qty_diff > (qty_tol * Decimal("2")):
                continue
            if price_diff > (price_tol * Decimal("2")):
                continue

            score = (price_diff, qty_diff)
            if best_score is None or score < best_score:
                best_idx = idx
                best_score = score

        if best_idx is None:
            return None

        used_indexes.add(best_idx)
        return sell_orders[best_idx]

    @classmethod
    def _parse_position_rows(cls, open_lots: Any, mark_price: Decimal, open_orders: Any) -> list[dict[str, Any]]:
        if not isinstance(open_lots, list):
            return []
        sell_orders = [
            o for o in (open_orders or [])
            if isinstance(o, dict) and str(o.get("side", "")).upper() == "SELL"
        ]
        used_sell_indexes: set[int] = set()
        rows: list[dict[str, Any]] = []
        for idx, lot in enumerate(open_lots):
            if not isinstance(lot, dict):
                continue
            qty = _to_decimal(lot.get("qty", "0"))
            buy_price = _to_decimal(lot.get("buy_price", "0"))
            sell_price = _to_decimal(lot.get("sell_price", "0"))
            cost_quote = _to_decimal(lot.get("cost_quote", "0"))
            if qty <= 0:
                continue

            market_value = (qty * mark_price) if mark_price > 0 else Decimal("0")
            pnl = market_value - cost_quote if market_value > 0 else Decimal("0")
            pnl_pct = Decimal("0")
            if cost_quote > 0:
                pnl_pct = (pnl / cost_quote) * Decimal("100")
            active_sell = cls._match_active_sell_order(
                sell_orders=sell_orders,
                used_indexes=used_sell_indexes,
                lot_qty=qty,
                target_sell_price=sell_price,
            )

            rows.append(
                {
                    "id": idx + 1,
                    "qty": str(qty),
                    "buy_price": str(buy_price),
                    "target_sell_price": str(sell_price),
                    "cost_quote": str(cost_quote),
                    "mark_price": str(mark_price) if mark_price > 0 else None,
                    "market_value": str(market_value) if market_value > 0 else None,
                    "pnl_quote": str(pnl) if market_value > 0 else None,
                    "pnl_pct": str(pnl_pct) if market_value > 0 else None,
                    "exit_order_active": active_sell is not None,
                    "exit_order_id": (
                        str(active_sell.get("orderId", active_sell.get("order_id", active_sell.get("id", "?"))))
                        if active_sell is not None else None
                    ),
                    "exit_order_price": (
                        str(_to_decimal(active_sell.get("price", active_sell.get("limit_price", active_sell.get("limit", active_sell.get("px", "0"))))))
                        if active_sell is not None else None
                    ),
                }
            )
        return rows

    def _load_metrics_summary(self, symbol: str = "") -> dict[str, Any]:
        source = self._discover_metrics_file(symbol)
        if source is None or not source.exists():
            return {
                "source": None,
                "first_today_ts": None,
                "last_today_ts": None,
                "ticks_ps": None,
                "equity_first": None,
                "equity_last": None,
                "pnl_day": None,
                "bid": None,
                "ask": None,
                "mid": None,
            }

        try:
            st = source.stat()
            cache_key = (str(source), int(st.st_mtime), int(st.st_size))
        except Exception:
            cache_key = (str(source), 0, 0)

        if self._metrics_cache_key == cache_key and self._metrics_cache_value is not None:
            return dict(self._metrics_cache_value)

        now_local = datetime.now(self.display_tz)
        day_local = now_local.date()

        first_today_ts: datetime | None = None
        last_today_ts: datetime | None = None
        first_today_equity: Decimal | None = None
        last_today_equity: Decimal | None = None
        last_ticks_ps: float | None = None
        last_bid: Decimal | None = None
        last_ask: Decimal | None = None

        try:
            with source.open("r", encoding="utf-8") as f:
                for line in f:
                    ln = line.strip()
                    if not ln:
                        continue
                    try:
                        row = json.loads(ln)
                    except Exception:
                        continue
                    if str(row.get("kind", "")).strip().lower() != "heartbeat":
                        continue

                    ts_utc = self._parse_ts_utc(row.get("ts"))
                    if ts_utc is None:
                        continue
                    ts_local = ts_utc.astimezone(self.display_tz)
                    if ts_local.date() != day_local:
                        continue

                    eq = _to_decimal(row.get("equity", "0"))
                    if first_today_ts is None:
                        first_today_ts = ts_utc
                        first_today_equity = eq

                    if last_today_ts is None or ts_utc >= last_today_ts:
                        last_today_ts = ts_utc
                        last_today_equity = eq
                        try:
                            last_ticks_ps = float(row.get("ticks_ps"))
                        except Exception:
                            last_ticks_ps = None
                        bid_v = _to_decimal(row.get("bid", "0"))
                        ask_v = _to_decimal(row.get("ask", "0"))
                        last_bid = bid_v if bid_v > 0 else None
                        last_ask = ask_v if ask_v > 0 else None
        except Exception:
            pass

        pnl_day: Decimal | None = None
        if first_today_equity is not None and last_today_equity is not None:
            pnl_day = last_today_equity - first_today_equity

        mid: Decimal | None = None
        if last_bid is not None and last_ask is not None:
            mid = (last_bid + last_ask) / Decimal("2")
        elif last_bid is not None:
            mid = last_bid
        elif last_ask is not None:
            mid = last_ask

        out = {
            "source": str(source),
            "first_today_ts": first_today_ts.isoformat() if first_today_ts is not None else None,
            "last_today_ts": last_today_ts.isoformat() if last_today_ts is not None else None,
            "ticks_ps": last_ticks_ps,
            "equity_first": str(first_today_equity) if first_today_equity is not None else None,
            "equity_last": str(last_today_equity) if last_today_equity is not None else None,
            "pnl_day": str(pnl_day) if pnl_day is not None else None,
            "bid": str(last_bid) if last_bid is not None else None,
            "ask": str(last_ask) if last_ask is not None else None,
            "mid": str(mid) if mid is not None else None,
        }

        self._metrics_cache_key = cache_key
        self._metrics_cache_value = dict(out)
        return out

    @staticmethod
    def _tail_lines(path: Path, limit: int) -> list[str]:
        bucket: deque[str] = deque(maxlen=max(limit, 1))
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                ln = line.strip()
                if ln:
                    bucket.append(ln)
        return list(bucket)

    def _load_state_snapshot(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "exists": self.state_file.exists(),
            "path": str(self.state_file),
        }
        if not self.state_file.exists():
            return out

        try:
            raw = json.loads(self.state_file.read_text(encoding="utf-8"))
        except Exception as e:
            out["error"] = f"state parse error: {e}"
            return out

        strategy = raw.get("strategy", {}) if isinstance(raw.get("strategy"), dict) else {}
        execution = raw.get("execution", {}) if isinstance(raw.get("execution"), dict) else {}
        runtime = raw.get("runtime_stats", {}) if isinstance(raw.get("runtime_stats"), dict) else {}
        symbol = str(raw.get("symbol", "")).strip().upper()

        quote_balance = _to_decimal(execution.get("quote_balance", "0"))
        quote_free = _to_decimal(execution.get("quote_balance_free", "0"))
        base_balance = _to_decimal(execution.get("base_balance", "0"))
        base_free = _to_decimal(execution.get("base_balance_free", "0"))
        open_orders_raw = execution.get("open_orders", []) or []
        open_lots_raw = strategy.get("open_lots", []) or []

        metrics = self._load_metrics_summary(symbol)
        bid = _to_decimal(metrics.get("bid", "0"))
        ask = _to_decimal(metrics.get("ask", "0"))
        mid = _to_decimal(metrics.get("mid", "0"))
        if mid <= 0 and bid > 0 and ask > 0:
            mid = (bid + ask) / Decimal("2")
        if mid <= 0:
            mid = bid if bid > 0 else ask if ask > 0 else Decimal("0")

        total_balance_usdt = None
        eq_last = _to_decimal(metrics.get("equity_last", "0"))
        if eq_last > 0:
            total_balance_usdt = eq_last
        elif mid > 0:
            total_balance_usdt = quote_balance + (base_balance * mid)
        elif quote_balance > 0:
            total_balance_usdt = quote_balance

        pnl_day = None
        pnl_day_pct = None
        eq_first = _to_decimal(metrics.get("equity_first", "0"))
        day_delta = _to_decimal(metrics.get("pnl_day", "0"))
        if metrics.get("pnl_day") is not None:
            pnl_day = day_delta
            if eq_first > 0:
                pnl_day_pct = (day_delta / eq_first) * Decimal("100")

        order_rows = self._parse_order_rows(open_orders_raw)
        position_rows = self._parse_position_rows(open_lots_raw, mid, open_orders_raw)

        out.update(
            {
                "saved_at": raw.get("saved_at"),
                "symbol": raw.get("symbol"),
                "tick_count": raw.get("tick_count"),
                "exchange": "Binance",
                "quote_balance": str(quote_balance),
                "quote_balance_free": str(quote_free),
                "base_balance": str(base_balance),
                "base_balance_free": str(base_free),
                "open_orders_count": len(order_rows),
                "open_lots_count": len(position_rows),
                "open_orders_rows": order_rows,
                "open_positions_rows": position_rows,
                "total_balance_usdt": (str(total_balance_usdt) if total_balance_usdt is not None else None),
                "pnl_day_usdt": (str(pnl_day) if pnl_day is not None else None),
                "pnl_day_pct": (str(pnl_day_pct) if pnl_day_pct is not None else None),
                "ticks_ps": metrics.get("ticks_ps"),
                "last_update_ts": metrics.get("last_today_ts"),
                "day_first_ts": metrics.get("first_today_ts"),
                "market_bid": (str(bid) if bid > 0 else None),
                "market_ask": (str(ask) if ask > 0 else None),
                "market_mid": (str(mid) if mid > 0 else None),
                "timezone": self.display_tz_name,
                "metrics_source": metrics.get("source"),
                "fills_total": runtime.get("fills_total"),
                "bars_total": runtime.get("bars_total"),
            }
        )
        return out

    def _verify_telegram_init_data(self, init_data: str) -> dict[str, Any]:
        params = dict(urllib.parse.parse_qsl(init_data, keep_blank_values=True))
        hash_value = str(params.pop("hash", "")).strip()
        if not hash_value:
            raise AuthError("missing initData hash", status=401)

        data_check_string = "\n".join(f"{k}={v}" for k, v in sorted(params.items()))
        secret_key = hmac.new(b"WebAppData", self.bot_token.encode("utf-8"), hashlib.sha256).digest()
        calc_hash = hmac.new(secret_key, data_check_string.encode("utf-8"), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(calc_hash, hash_value):
            raise AuthError("invalid initData signature", status=401)

        auth_date_raw = str(params.get("auth_date", "")).strip()
        if auth_date_raw:
            try:
                auth_date = int(auth_date_raw)
            except Exception as e:
                raise AuthError(f"invalid auth_date: {e}", status=401) from e
            now_ts = int(datetime.now(timezone.utc).timestamp())
            if now_ts - auth_date > self.auth_max_age_sec:
                raise AuthError("initData expired", status=401)

        user_raw = str(params.get("user", "")).strip()
        if not user_raw:
            raise AuthError("missing user payload", status=401)
        try:
            user = json.loads(user_raw)
        except Exception as e:
            raise AuthError(f"invalid user payload: {e}", status=401) from e

        try:
            user_id = int(user.get("id"))
        except Exception as e:
            raise AuthError(f"invalid user id: {e}", status=401) from e

        if self.allowed_user_ids and user_id not in self.allowed_user_ids:
            raise AuthError("access denied", status=403)

        return {
            "id": user_id,
            "username": str(user.get("username", "")).strip(),
            "first_name": str(user.get("first_name", "")).strip(),
        }

    def _authenticate_request(self, request: web.Request) -> dict[str, Any]:
        init_data = str(request.headers.get("X-Telegram-Init-Data", "")).strip()
        if not init_data:
            init_data = str(request.query.get("initData", "")).strip()

        if not init_data:
            if not self.dev_mode:
                raise AuthError("missing Telegram initData", status=401)

            user_id = self.dev_user_id
            if user_id <= 0 and self.allowed_user_ids:
                user_id = sorted(self.allowed_user_ids)[0]
            if user_id <= 0:
                raise AuthError("dev mode enabled but TELEGRAM_MINIAPP_DEV_USER_ID is not set", status=401)
            if self.allowed_user_ids and user_id not in self.allowed_user_ids:
                raise AuthError("dev user is not in TELEGRAM_ALLOWED_USER_IDS", status=403)
            return {"id": user_id, "username": "dev", "first_name": "Dev"}

        return self._verify_telegram_init_data(init_data)

    @web.middleware
    async def _auth_middleware(self, request: web.Request, handler):  # type: ignore[no-untyped-def]
        if request.path.startswith("/api/"):
            try:
                request["auth_user"] = self._authenticate_request(request)
            except AuthError as e:
                return web.json_response({"ok": False, "error": str(e)}, status=e.status)
        return await handler(request)

    async def handle_health(self, _: web.Request) -> web.Response:
        return web.json_response({"ok": True, "ts": datetime.now(timezone.utc).isoformat()})

    async def handle_index(self, _: web.Request) -> web.Response:
        response = web.FileResponse(self.static_dir / "index.html")
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
        return response

    async def handle_status(self, request: web.Request) -> web.Response:
        user = request.get("auth_user", {})
        state = await self._service_state()
        snapshot = self._load_state_snapshot()
        return web.json_response(
            {
                "ok": True,
                "user": user,
                "service": {"name": self.runner_service, "state": state},
                "state": snapshot,
                "runner_control_enabled": self.allow_runner_control,
            }
        )

    async def handle_trades(self, request: web.Request) -> web.Response:
        try:
            limit = int(request.query.get("limit", "20"))
        except Exception:
            limit = 20
        limit = min(max(limit, 1), 100)

        source = self._discover_trades_file()
        if source is None or not source.exists():
            return web.json_response({"ok": True, "source": None, "trades": []})

        # Pull a bit more lines than requested because JSONL may include non-fill events.
        raw_lines = self._tail_lines(source, limit * 10)
        trades: list[dict[str, Any]] = []
        for line in reversed(raw_lines):
            try:
                row = json.loads(line)
            except Exception:
                continue
            if str(row.get("kind", "")).strip().lower() != "fill":
                continue

            fill_repr = str(row.get("fill", ""))
            side = str(row.get("side", "?"))
            qty = str(row.get("qty", "?"))
            price = str(row.get("price", "?"))
            fee_quote = str(row.get("fee_quote", row.get("fee", "?")))
            maker_val: Any = row.get("maker", "?")
            if isinstance(maker_val, bool):
                maker = "True" if maker_val else "False"
            else:
                maker = str(maker_val)

            if _is_missing(side):
                side = self._extract_fill_field(fill_repr, "side")
            if _is_missing(qty):
                qty = self._extract_fill_field(fill_repr, "qty")
            if _is_missing(price):
                price = self._extract_fill_field(fill_repr, "price")
            if _is_missing(fee_quote):
                fee_quote = self._extract_fill_field(fill_repr, "fee_quote")
            if _is_missing(maker):
                maker = self._extract_fill_field(fill_repr, "maker")

            trades.append(
                {
                    "symbol": str(row.get("symbol", "?")),
                    "ts": str(row.get("ts", "?")),
                    "order_id": str(row.get("order_id", "?")),
                    "status": str(row.get("fill_status", "?")),
                    "partial": bool(row.get("partial", False)),
                    "side": side if not _is_missing(side) else "?",
                    "qty": qty if not _is_missing(qty) else "?",
                    "price": price if not _is_missing(price) else "?",
                    "fee_quote": fee_quote if not _is_missing(fee_quote) else "?",
                    "maker": maker if not _is_missing(maker) else "?",
                }
            )
            if len(trades) >= limit:
                break

        return web.json_response({"ok": True, "source": str(source), "trades": trades})

    async def handle_runner_action(self, request: web.Request) -> web.Response:
        if not self.allow_runner_control:
            return web.json_response({"ok": False, "error": "runner control disabled"}, status=403)

        action = str(request.match_info.get("action", "")).strip().lower()
        if action not in {"start", "stop", "restart"}:
            return web.json_response({"ok": False, "error": f"unsupported action: {action}"}, status=400)

        code, out = await self._run_cmd("systemctl", action, self.runner_service)
        now_state = await self._service_state()
        return web.json_response(
            {
                "ok": code == 0,
                "action": action,
                "service": self.runner_service,
                "exit_code": code,
                "state_now": now_state,
                "output": out,
            }
        )

    def build_app(self) -> web.Application:
        app = web.Application(middlewares=[self._auth_middleware])
        app.router.add_get("/", self.handle_index)
        app.router.add_get("/health", self.handle_health)
        app.router.add_get("/api/status", self.handle_status)
        app.router.add_get("/api/trades", self.handle_trades)
        app.router.add_post("/api/runner/{action}", self.handle_runner_action)
        app.router.add_static("/static/", str(self.static_dir), show_index=False)
        return app

    def run(self) -> None:
        logger.info(
            "Mini app starting | host={} port={} static_dir={} runner_service={} allow_runner_control={} auth_max_age_sec={} allowed_users={} dev_mode={} tz={}",
            self.host,
            self.port,
            self.static_dir,
            self.runner_service,
            self.allow_runner_control,
            self.auth_max_age_sec,
            (sorted(self.allowed_user_ids) if self.allowed_user_ids else "ALL"),
            self.dev_mode,
            self.display_tz_name,
        )
        web.run_app(self.build_app(), host=self.host, port=self.port)


def main() -> int:
    srv = TelegramMiniAppServer()
    try:
        srv.run()
    except KeyboardInterrupt:
        logger.info("Mini app interrupted")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
