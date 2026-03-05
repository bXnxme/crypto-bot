from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import os
import re
import time
import urllib.parse
from decimal import Decimal
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import aiohttp
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


def _int_set_env(name: str) -> set[int]:
    raw = str(os.getenv(name, "")).strip()
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


def _normalize_rest_base_url(raw: str) -> str:
    v = (raw or "").strip().rstrip("/")
    if not v:
        return ""
    if v.lower().endswith("/api/v3"):
        v = v[:-7].rstrip("/")
    elif v.lower().endswith("/api"):
        v = v[:-4].rstrip("/")
    return v


def _pick_binance_rest_base() -> str:
    for key in (
        "BINANCE_DEMO_REST_BASE_URL",
        "BINANCE_REST_BASE_URL",
        "BASE_URL",
        "BINANCE_DEMO_WS_BASE_URL",
    ):
        val = os.getenv(key)
        if not val or not str(val).strip():
            continue
        return _normalize_rest_base_url(str(val))
    return "https://testnet.binance.vision"


def _pick_api_creds(base_url: str) -> tuple[str, str, str]:
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
    return "", "", "missing"


def _split_symbol_guess(symbol: str) -> tuple[str, str]:
    s = (symbol or "").strip().upper()
    if not s:
        return "", ""

    quote_assets = (
        "FDUSD",
        "USDT",
        "USDC",
        "USDP",
        "TUSD",
        "BUSD",
        "BTC",
        "ETH",
        "BNB",
        "TRY",
        "EUR",
        "GBP",
        "RUB",
        "JPY",
    )
    for quote in quote_assets:
        if s.endswith(quote) and len(s) > len(quote):
            return s[: -len(quote)], quote
    if len(s) > 4:
        return s[:-4], s[-4:]
    return s, ""


def _to_decimal(v: Any) -> Decimal:
    try:
        return Decimal(str(v))
    except Exception:
        return Decimal("0")


def _dec_to_str(v: Decimal) -> str:
    s = format(v, "f")
    if "." in s:
        s = s.rstrip("0").rstrip(".")
    return s or "0"


def _is_missing(v: Any) -> bool:
    if v is None:
        return True
    s = str(v).strip()
    return s in {"", "?", "None", "null"}


class TelegramRunnerBot:
    def __init__(self) -> None:
        load_dotenv()
        token = str(os.getenv("TELEGRAM_BOT_TOKEN", "")).strip()
        if not token:
            raise RuntimeError("Missing TELEGRAM_BOT_TOKEN")

        self.token = token
        self.api_base = f"https://api.telegram.org/bot{self.token}"
        self.allowed_user_ids = _allowed_user_ids()

        self.runner_service = str(os.getenv("TELEGRAM_RUNNER_SERVICE", "crypto-bot")).strip() or "crypto-bot"
        self.miniapp_url = str(os.getenv("TELEGRAM_MINIAPP_URL", "")).strip()
        self.state_file = Path(str(os.getenv("RUN_DEMO_GRID_STATE_FILE", "data/run_demo_grid_state.json")).strip())
        self.log_dir = Path(str(os.getenv("RUN_DEMO_GRID_LOG_DIR", "logs")).strip() or "logs")
        self.log_prefix = str(os.getenv("RUN_DEMO_GRID_LOG_PREFIX", "demo_grid")).strip() or "demo_grid"

        self.poll_timeout_sec = max(_env_int("TELEGRAM_POLL_TIMEOUT_SEC", 25), 5)
        self.cmd_timeout_sec = max(_env_int("TELEGRAM_CMD_TIMEOUT_SEC", 20), 3)
        self.log_lines_default = min(max(_env_int("TELEGRAM_LOG_LINES", 25), 5), 80)
        self.debug = _env_bool("TELEGRAM_DEBUG", default=False)
        self.sync_menu_commands = _env_bool("TELEGRAM_SYNC_MENU_COMMANDS", default=True)
        self.sync_menu_button = _env_bool("TELEGRAM_SYNC_MENU_BUTTON", default=True)
        self.menu_button_mode = str(os.getenv("TELEGRAM_MENU_BUTTON_MODE", "web_app")).strip().lower() or "web_app"
        if self.menu_button_mode not in {"web_app", "commands", "default"}:
            self.menu_button_mode = "web_app"
        self.menu_button_text = str(os.getenv("TELEGRAM_MENU_BUTTON_TEXT", "Mini App")).strip() or "Mini App"
        self.notify_trades = _env_bool("TELEGRAM_NOTIFY_TRADES", default=True)
        self.notify_poll_sec = max(_env_int("TELEGRAM_NOTIFY_POLL_SEC", 2), 1)
        self.notify_from_beginning = _env_bool("TELEGRAM_NOTIFY_FROM_BEGINNING", default=False)
        self.notify_symbol = str(os.getenv("TELEGRAM_NOTIFY_SYMBOL", "")).strip().upper()
        self.notify_enrich_binance = _env_bool("TELEGRAM_NOTIFY_ENRICH_BINANCE", default=True)
        self.notify_enrich_recv_window_ms = max(_env_int("TELEGRAM_NOTIFY_ENRICH_RECV_WINDOW_MS", 5000), 1000)
        self.notify_enrich_cache_max = max(_env_int("TELEGRAM_NOTIFY_ENRICH_CACHE_MAX", 2000), 100)
        self.notify_chat_ids = _int_set_env("TELEGRAM_NOTIFY_CHAT_IDS")
        if not self.notify_chat_ids and self.allowed_user_ids:
            # Private chat id for user usually equals user id.
            self.notify_chat_ids = set(self.allowed_user_ids)

        self.binance_rest_base = _pick_binance_rest_base()
        self.binance_api_key, self.binance_api_secret, self.binance_cred_source = _pick_api_creds(
            self.binance_rest_base
        )

        self._offset: Optional[int] = None
        self._session: Optional[aiohttp.ClientSession] = None
        self._notify_task: Optional[asyncio.Task[Any]] = None
        self._notify_trades_path: Optional[Path] = None
        self._notify_trades_pos: int = 0
        self._notify_fill_enrich_cache: dict[str, dict[str, Any]] = {}

    async def _api(self, method: str, payload: dict[str, Any]) -> dict[str, Any]:
        if self._session is None:
            raise RuntimeError("HTTP session not initialized")
        url = f"{self.api_base}/{method}"
        async with self._session.post(url, json=payload) as resp:
            text = await resp.text()
            if resp.status >= 400:
                raise RuntimeError(f"Telegram API {method} failed: HTTP {resp.status} | {text[:500]}")
            try:
                data = json.loads(text)
            except Exception as e:
                raise RuntimeError(f"Telegram API {method} invalid JSON: {e}") from e
            if not bool(data.get("ok", False)):
                raise RuntimeError(f"Telegram API {method} error: {data}")
            return data

    async def _send(self, chat_id: int, text: str, reply_markup: Optional[dict[str, Any]] = None) -> None:
        if len(text) > 3900:
            text = text[:3900] + "\n...\n(truncated)"
        payload: dict[str, Any] = {
            "chat_id": chat_id,
            "text": text,
            "disable_web_page_preview": True,
        }
        if reply_markup is not None:
            payload["reply_markup"] = reply_markup
        await self._api("sendMessage", payload)

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

    def _menu_commands_payload(self) -> list[dict[str, str]]:
        return [
            {"command": "app", "description": "Открыть мини-приложение"},
            {"command": "status", "description": "Статус сервиса и состояния"},
            {"command": "runner", "description": "Статус systemd раннера"},
            {"command": "start_runner", "description": "Запустить раннер"},
            {"command": "restart_runner", "description": "Перезапустить раннер"},
            {"command": "stop_runner", "description": "Остановить раннер"},
            {"command": "notify", "description": "Параметры уведомлений"},
            {"command": "logs", "description": "Последние логи раннера"},
            {"command": "ping", "description": "Проверка связи"},
            {"command": "help", "description": "Помощь по командам"},
        ]

    async def _sync_bot_menu(self) -> None:
        if self.sync_menu_commands:
            try:
                commands = self._menu_commands_payload()
                await self._api(
                    "setMyCommands",
                    {
                        "commands": commands,
                        "scope": {"type": "all_private_chats"},
                    },
                )
                logger.info("Telegram commands synced | count={}", len(commands))
            except Exception as e:
                logger.warning("Telegram setMyCommands failed: {}", e)

        if self.sync_menu_button:
            try:
                if self.menu_button_mode == "commands":
                    payload = {"menu_button": {"type": "commands"}}
                elif self.menu_button_mode == "default":
                    payload = {"menu_button": {"type": "default"}}
                else:
                    if not self.miniapp_url:
                        logger.warning(
                            "TELEGRAM_MENU_BUTTON_MODE=web_app but TELEGRAM_MINIAPP_URL is empty; fallback to commands"
                        )
                        payload = {"menu_button": {"type": "commands"}}
                    else:
                        payload = {
                            "menu_button": {
                                "type": "web_app",
                                "text": self.menu_button_text,
                                "web_app": {"url": self.miniapp_url},
                            }
                        }

                await self._api("setChatMenuButton", payload)
                logger.info("Telegram chat menu button synced | mode={} payload={}", self.menu_button_mode, payload)
            except Exception as e:
                logger.warning("Telegram setChatMenuButton failed: {}", e)

    @staticmethod
    def _extract_fill_field(fill_repr: str, name: str) -> str:
        if not fill_repr:
            return "?"
        key = re.escape(name)
        if name in {"qty", "price", "fee_quote"}:
            patterns = [
                rf"{key}=Decimal\('([^']+)'\)",
                rf'{key}=Decimal\("([^"]+)"\)',
                rf"{key}=([-+]?\d+(?:\.\d+)?)",
                rf"['\"]{key}['\"]\s*:\s*Decimal\('([^']+)'\)",
                rf'["\']{key}["\']\s*:\s*Decimal\("([^"]+)"\)',
                rf"['\"]{key}['\"]\s*:\s*['\"]([^'\"]+)['\"]",
                rf"['\"]{key}['\"]\s*:\s*([-+]?\d+(?:\.\d+)?)",
            ]
        elif name == "maker":
            patterns = [
                rf"{key}=([Tt]rue|[Ff]alse)",
                rf"['\"]{key}['\"]\s*:\s*([Tt]rue|[Ff]alse)",
            ]
        else:
            patterns = [
                rf"{key}='([^']+)'",
                rf'{key}="([^"]+)"',
                rf"['\"]{key}['\"]\s*:\s*['\"]([^'\"]+)['\"]",
            ]
        for pat in patterns:
            m = re.search(pat, fill_repr)
            if m:
                return m.group(1)
        return "?"

    @staticmethod
    def _extract_order_field(order_repr: str, name: str) -> str:
        if not order_repr:
            return "?"
        key = re.escape(name)
        patterns = [
            rf"{key}=Decimal\('([^']+)'\)",
            rf'{key}=Decimal\("([^"]+)"\)',
            rf"{key}=([-+]?\d+(?:\.\d+)?)",
            rf"{key}='([^']+)'",
            rf'{key}="([^"]+)"',
            rf"['\"]{key}['\"]\s*:\s*Decimal\('([^']+)'\)",
            rf'["\']{key}["\']\s*:\s*Decimal\("([^"]+)"\)',
            rf"['\"]{key}['\"]\s*:\s*['\"]([^'\"]+)['\"]",
            rf"['\"]{key}['\"]\s*:\s*([-+]?\d+(?:\.\d+)?)",
        ]
        for pat in patterns:
            m = re.search(pat, order_repr)
            if m:
                return m.group(1)
        return "?"

    def _parse_fill_row(self, row: dict[str, Any]) -> dict[str, Any]:
        out: dict[str, Any] = {
            "symbol": str(row.get("symbol", "?")),
            "ts": str(row.get("ts", "?")),
            "order_id": str(row.get("order_id", "?")),
            "status": str(row.get("fill_status", row.get("status", "?"))),
            "partial": bool(row.get("partial", False)),
            "side": str(row.get("side", "?")),
            "qty": str(row.get("qty", "?")),
            "price": str(row.get("price", "?")),
            "fee_quote": str(row.get("fee_quote", row.get("fee", "?"))),
            "maker": row.get("maker", "?"),
        }
        fill = row.get("fill")
        if isinstance(fill, dict):
            if _is_missing(out["side"]):
                out["side"] = str(fill.get("side", "?"))
            if _is_missing(out["qty"]):
                out["qty"] = str(fill.get("qty", fill.get("quantity", "?")))
            if _is_missing(out["price"]):
                out["price"] = str(fill.get("price", fill.get("px", "?")))
            if _is_missing(out["fee_quote"]):
                out["fee_quote"] = str(fill.get("fee_quote", fill.get("fee", fill.get("commission", "?"))))
            if _is_missing(out["maker"]):
                out["maker"] = fill.get("maker", fill.get("isMaker", "?"))
            if _is_missing(out["order_id"]):
                out["order_id"] = str(fill.get("order_id", fill.get("orderId", "?")))
        else:
            fill_repr = str(fill or "")
            if _is_missing(out["side"]):
                out["side"] = self._extract_fill_field(fill_repr, "side")
            if _is_missing(out["qty"]):
                out["qty"] = self._extract_fill_field(fill_repr, "qty")
            if _is_missing(out["price"]):
                out["price"] = self._extract_fill_field(fill_repr, "price")
            if _is_missing(out["fee_quote"]):
                out["fee_quote"] = self._extract_fill_field(fill_repr, "fee_quote")
            if _is_missing(out["maker"]):
                out["maker"] = self._extract_fill_field(fill_repr, "maker")

        maker_v = out.get("maker")
        if isinstance(maker_v, bool):
            out["maker"] = "True" if maker_v else "False"
        else:
            mk = str(maker_v).strip()
            if mk.lower() in {"true", "1", "yes", "on"}:
                out["maker"] = "True"
            elif mk.lower() in {"false", "0", "no", "off"}:
                out["maker"] = "False"
            elif _is_missing(mk):
                out["maker"] = "?"
            else:
                out["maker"] = mk

        for k in ("side", "qty", "price", "fee_quote", "order_id", "symbol", "status", "ts"):
            if _is_missing(out.get(k)):
                out[k] = "?"
            else:
                out[k] = str(out[k]).strip()
        return out

    def _sign_binance_params(self, params: dict[str, Any]) -> dict[str, Any]:
        query = urllib.parse.urlencode(params, doseq=True)
        signature = hmac.new(
            self.binance_api_secret.encode("utf-8"),
            query.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        out = dict(params)
        out["signature"] = signature
        return out

    async def _binance_get_signed(self, path: str, *, params: dict[str, Any]) -> Any:
        if self._session is None:
            raise RuntimeError("HTTP session not initialized")
        if not self.binance_api_key or not self.binance_api_secret:
            raise RuntimeError("Binance credentials are missing")

        payload = dict(params)
        payload["timestamp"] = int(time.time() * 1000)
        payload["recvWindow"] = int(self.notify_enrich_recv_window_ms)
        signed = self._sign_binance_params(payload)
        url = f"{self.binance_rest_base.rstrip('/')}{path}"
        headers = {"X-MBX-APIKEY": self.binance_api_key}

        async with self._session.get(url, params=signed, headers=headers) as resp:
            text = await resp.text()
            if resp.status >= 400:
                raise RuntimeError(f"HTTP {resp.status}: {text[:300]}")
            try:
                return json.loads(text)
            except Exception as e:
                raise RuntimeError(f"Invalid Binance JSON: {e}") from e

    async def _enrich_fill_from_binance(self, *, symbol: str, order_id: str) -> Optional[dict[str, Any]]:
        symbol_u = str(symbol or "").strip().upper()
        order_s = str(order_id or "").strip()
        if not symbol_u or not order_s.isdigit():
            return None
        if not self.notify_enrich_binance:
            return None
        if not self.binance_api_key or not self.binance_api_secret:
            return None

        cache_key = f"{symbol_u}:{order_s}"
        cached = self._notify_fill_enrich_cache.get(cache_key)
        if cached is not None:
            return dict(cached)

        try:
            rows = await self._binance_get_signed(
                "/api/v3/myTrades",
                params={"symbol": symbol_u, "orderId": int(order_s), "limit": 100},
            )
        except Exception as e:
            logger.debug("Trade enrich failed | symbol={} order_id={} err={}", symbol_u, order_s, e)
            return None
        if not isinstance(rows, list) or not rows:
            return None

        base_asset, quote_asset = _split_symbol_guess(symbol_u)
        side: str = "?"
        maker: Optional[bool] = None
        qty_total = Decimal("0")
        quote_total = Decimal("0")
        fee_quote_total = Decimal("0")

        for tr in rows:
            if not isinstance(tr, dict):
                continue

            qty = _to_decimal(tr.get("qty", "0"))
            px = _to_decimal(tr.get("price", "0"))
            quote_qty = _to_decimal(tr.get("quoteQty", tr.get("quoteQuantity", "0")))
            if quote_qty <= 0 and qty > 0 and px > 0:
                quote_qty = qty * px

            if qty > 0:
                qty_total += qty
            if quote_qty > 0:
                quote_total += quote_qty

            is_buyer = tr.get("isBuyer")
            if isinstance(is_buyer, bool):
                side = "BUY" if is_buyer else "SELL"

            is_maker = tr.get("isMaker")
            if isinstance(is_maker, bool):
                maker = is_maker if maker is None else (maker and is_maker)

            commission = _to_decimal(tr.get("commission", "0"))
            if commission > 0:
                commission_asset = str(tr.get("commissionAsset", "")).strip().upper()
                if quote_asset and commission_asset == quote_asset:
                    fee_quote_total += commission
                elif base_asset and commission_asset == base_asset and px > 0:
                    fee_quote_total += commission * px

        if qty_total <= 0:
            return None

        avg_price = (quote_total / qty_total) if quote_total > 0 else Decimal("0")
        out = {
            "side": side if side in {"BUY", "SELL"} else "?",
            "qty": _dec_to_str(qty_total),
            "price": _dec_to_str(avg_price) if avg_price > 0 else "?",
            "fee_quote": _dec_to_str(fee_quote_total) if fee_quote_total > 0 else "0",
            "maker": ("True" if maker else "False") if maker is not None else "?",
            "trades_count": len(rows),
            "source": "binance_myTrades",
        }

        self._notify_fill_enrich_cache[cache_key] = dict(out)
        if len(self._notify_fill_enrich_cache) > self.notify_enrich_cache_max:
            try:
                first_key = next(iter(self._notify_fill_enrich_cache.keys()))
                self._notify_fill_enrich_cache.pop(first_key, None)
            except Exception:
                pass
        return out

    def _effective_notify_symbol(self) -> str:
        if self.notify_symbol:
            return self.notify_symbol
        if self.state_file.exists():
            try:
                raw = json.loads(self.state_file.read_text(encoding="utf-8"))
                sym = str(raw.get("symbol", "")).strip().upper()
                if sym:
                    return sym
            except Exception:
                pass
        return ""

    def _discover_trades_file(self) -> Optional[Path]:
        sym = self._effective_notify_symbol()
        if sym:
            candidate = self.log_dir / f"{self.log_prefix}_{sym.lower()}_trades.jsonl"
            if candidate.exists():
                return candidate

        if not self.log_dir.exists():
            return None
        pattern = f"{self.log_prefix}_*_trades.jsonl"
        files = sorted(self.log_dir.glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True)
        return files[0] if files else None

    async def _send_trade_notify(self, row: dict[str, Any]) -> None:
        if not self.notify_chat_ids:
            return

        kind = str(row.get("kind", "")).strip().lower()
        if kind != "fill":
            return

        parsed = self._parse_fill_row(row)
        symbol = str(parsed.get("symbol", "?"))
        ts = str(parsed.get("ts", "?"))
        order_id = str(parsed.get("order_id", "?"))
        fill_status = str(parsed.get("status", "?"))
        partial = bool(parsed.get("partial", False))
        side = str(parsed.get("side", "?"))
        qty = str(parsed.get("qty", "?"))
        price = str(parsed.get("price", "?"))
        fee_quote = str(parsed.get("fee_quote", "?"))
        maker = str(parsed.get("maker", "?"))

        needs_enrich = any(_is_missing(v) for v in (side, qty, price, fee_quote, maker))
        if needs_enrich:
            enriched = await self._enrich_fill_from_binance(symbol=symbol, order_id=order_id)
            if enriched:
                if _is_missing(side):
                    side = str(enriched.get("side", side))
                if _is_missing(qty):
                    qty = str(enriched.get("qty", qty))
                if _is_missing(price):
                    price = str(enriched.get("price", price))
                if _is_missing(fee_quote):
                    fee_quote = str(enriched.get("fee_quote", fee_quote))
                if _is_missing(maker):
                    maker = str(enriched.get("maker", maker))

        text = (
            f"Сделка {symbol}\n"
            f"side: {side}\n"
            f"qty: {qty}\n"
            f"price: {price}\n"
            f"fee_quote: {fee_quote}\n"
            f"maker: {maker}\n"
            f"status: {fill_status}\n"
            f"partial: {partial}\n"
            f"order_id: {order_id}\n"
            f"ts: {ts}"
        )

        for chat_id in sorted(self.notify_chat_ids):
            try:
                await self._send(chat_id, text)
            except Exception as e:
                logger.warning("Trade notify send failed | chat_id={} err={}", chat_id, e)

    def _render_open_orders_summary(self, open_orders: Any, limit: int = 12) -> str:
        if not isinstance(open_orders, list) or not open_orders:
            return "open_orders_detail: -"

        rows: list[dict[str, Any]] = []
        for o in open_orders:
            side = "?"
            price = "?"
            qty = "?"
            oid = "?"
            if isinstance(o, dict):
                side = str(o.get("side", "?")).upper()
                price = str(o.get("price", o.get("limit_price", o.get("limit", o.get("px", "?")))))
                qty = str(o.get("origQty", o.get("orig_qty", o.get("qty", o.get("quantity", "?")))))
                oid = str(o.get("orderId", o.get("order_id", o.get("id", "?"))))
            else:
                repr_s = str(o)
                side = str(self._extract_order_field(repr_s, "side")).upper()
                price = self._extract_order_field(repr_s, "price")
                if _is_missing(price):
                    price = self._extract_order_field(repr_s, "limit_price")
                qty = self._extract_order_field(repr_s, "origQty")
                if _is_missing(qty):
                    qty = self._extract_order_field(repr_s, "qty")
                oid = self._extract_order_field(repr_s, "orderId")
                if _is_missing(oid):
                    oid = self._extract_order_field(repr_s, "order_id")

            side_norm = side if side in {"BUY", "SELL"} else "?"
            try:
                p_val = float(price)
            except Exception:
                p_val = -1.0
            rows.append(
                {
                    "side": side_norm,
                    "price": price,
                    "qty": qty,
                    "order_id": oid,
                    "_price_val": p_val,
                }
            )

        def _sort_key(x: dict[str, Any]) -> tuple[int, float]:
            side_v = str(x.get("side", "?"))
            p = float(x.get("_price_val", -1.0))
            if side_v == "SELL":
                return (0, p if p >= 0 else 0.0)
            if side_v == "BUY":
                return (1, -p if p >= 0 else 0.0)
            return (2, 0.0)

        rows.sort(key=_sort_key)
        show = rows[: max(limit, 1)]

        lines = ["open_orders_detail:"]
        for r in show:
            lines.append(f"- {r['side']} price={r['price']} qty={r['qty']} id={r['order_id']}")
        if len(rows) > len(show):
            lines.append(f"... +{len(rows) - len(show)} more")
        return "\n".join(lines)

    async def _poll_trade_notifications(self) -> None:
        while True:
            try:
                if not self.notify_trades:
                    await asyncio.sleep(self.notify_poll_sec)
                    continue
                if not self.notify_chat_ids:
                    await asyncio.sleep(self.notify_poll_sec)
                    continue

                path = self._discover_trades_file()
                if path is None or (not path.exists()):
                    await asyncio.sleep(self.notify_poll_sec)
                    continue

                if self._notify_trades_path is None or self._notify_trades_path != path:
                    self._notify_trades_path = path
                    if self.notify_from_beginning:
                        self._notify_trades_pos = 0
                    else:
                        try:
                            self._notify_trades_pos = int(path.stat().st_size)
                        except Exception:
                            self._notify_trades_pos = 0
                    logger.info(
                        "Trade notify source | path={} from_beginning={} start_pos={}",
                        path,
                        self.notify_from_beginning,
                        self._notify_trades_pos,
                    )

                try:
                    size_now = int(path.stat().st_size)
                except Exception:
                    await asyncio.sleep(self.notify_poll_sec)
                    continue

                if size_now < self._notify_trades_pos:
                    # log rotated/truncated
                    self._notify_trades_pos = 0

                if size_now == self._notify_trades_pos:
                    await asyncio.sleep(self.notify_poll_sec)
                    continue

                with path.open("r", encoding="utf-8") as f:
                    f.seek(self._notify_trades_pos)
                    for line in f:
                        ln = line.strip()
                        if not ln:
                            continue
                        try:
                            row = json.loads(ln)
                        except Exception:
                            continue
                        try:
                            await self._send_trade_notify(row)
                        except Exception as e:
                            logger.warning("Trade notify process failed: {}", e)
                    self._notify_trades_pos = int(f.tell())

                await asyncio.sleep(self.notify_poll_sec)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning("Trade notify poll error: {}", e)
                await asyncio.sleep(self.notify_poll_sec)

    def _load_state_summary(self) -> str:
        if not self.state_file.exists():
            return f"state: not found ({self.state_file})"

        try:
            raw = json.loads(self.state_file.read_text(encoding="utf-8"))
        except Exception as e:
            return f"state: failed to parse ({self.state_file}) | {e}"

        symbol = raw.get("symbol", "?")
        tick_count = raw.get("tick_count", "?")
        saved_at = raw.get("saved_at", "?")

        strategy = raw.get("strategy", {}) if isinstance(raw.get("strategy"), dict) else {}
        execution = raw.get("execution", {}) if isinstance(raw.get("execution"), dict) else {}
        runtime = raw.get("runtime_stats", {}) if isinstance(raw.get("runtime_stats"), dict) else {}

        quote = execution.get("quote_balance", "?")
        base = execution.get("base_balance", "?")
        open_orders = execution.get("open_orders", [])
        open_orders_cnt = len(open_orders) if isinstance(open_orders, list) else "?"
        open_orders_detail = self._render_open_orders_summary(open_orders, limit=12)

        open_lots = strategy.get("open_lots", [])
        open_lots_cnt = len(open_lots) if isinstance(open_lots, list) else "?"
        fills_total = runtime.get("fills_total", "?")
        bars_total = runtime.get("bars_total", "?")

        return (
            f"state_file: {self.state_file}\n"
            f"saved_at: {saved_at}\n"
            f"symbol: {symbol}\n"
            f"tick_count: {tick_count}\n"
            f"quote_balance: {quote}\n"
            f"base_balance: {base}\n"
            f"open_orders: {open_orders_cnt}\n"
            f"{open_orders_detail}\n"
            f"open_lots: {open_lots_cnt}\n"
            f"fills_total: {fills_total}\n"
            f"bars_total: {bars_total}"
        )

    async def _handle(self, chat_id: int, user_id: int, text: str) -> None:
        cmd = text.strip().split()[0].split("@")[0].lower()
        args = text.strip().split()[1:]

        if cmd in {"/start", "/help"}:
            await self._send(
                chat_id,
                (
                    "Crypto Bot control bot\n\n"
                    "Commands:\n"
                    "/ping - health check\n"
                    "/status - service + state snapshot\n"
                    "/runner - runner systemd status\n"
                    "/app - open Telegram Mini App\n"
                    "/notify - notification config/status\n"
                    "/start_runner - systemctl start runner\n"
                    "/stop_runner - systemctl stop runner\n"
                    "/restart_runner - systemctl restart runner\n"
                    "/logs [N] - latest journal lines (default 25)"
                ),
            )
            return

        if cmd == "/ping":
            now = datetime.now(timezone.utc).isoformat()
            await self._send(chat_id, f"pong\nutc: {now}\nuser_id: {user_id}")
            return

        if cmd == "/runner":
            state = await self._service_state()
            await self._send(chat_id, f"service: {self.runner_service}\nstate: {state}")
            return

        if cmd == "/app":
            if not self.miniapp_url:
                await self._send(chat_id, "mini app url is not configured (set TELEGRAM_MINIAPP_URL)")
                return
            await self._send(
                chat_id,
                "Открыть панель управления:",
                reply_markup={
                    "inline_keyboard": [
                        [
                            {
                                "text": "Open Mini App",
                                "web_app": {"url": self.miniapp_url},
                            }
                        ]
                    ]
                },
            )
            return

        if cmd == "/status":
            state = await self._service_state()
            summary = self._load_state_summary()
            await self._send(chat_id, f"service: {self.runner_service}\nstate: {state}\n\n{summary}")
            return

        if cmd == "/notify":
            source = self._discover_trades_file()
            await self._send(
                chat_id,
                (
                    f"notify_trades: {self.notify_trades}\n"
                    f"notify_chat_ids: {sorted(self.notify_chat_ids) if self.notify_chat_ids else '[]'}\n"
                    f"notify_symbol: {self._effective_notify_symbol() or '-'}\n"
                    f"notify_source: {source if source is not None else '-'}\n"
                    f"notify_poll_sec: {self.notify_poll_sec}\n"
                    f"from_beginning: {self.notify_from_beginning}\n"
                    f"notify_enrich_binance: {self.notify_enrich_binance}\n"
                    f"binance_rest_base: {self.binance_rest_base}\n"
                    f"binance_cred_source: {self.binance_cred_source}"
                ),
            )
            return

        if cmd in {"/start_runner", "/stop_runner", "/restart_runner"}:
            action = {
                "/start_runner": "start",
                "/stop_runner": "stop",
                "/restart_runner": "restart",
            }[cmd]
            code, out = await self._run_cmd("systemctl", action, self.runner_service)
            state = await self._service_state()
            await self._send(
                chat_id,
                (
                    f"runner action: {action}\n"
                    f"service: {self.runner_service}\n"
                    f"exit_code: {code}\n"
                    f"state_now: {state}\n"
                    f"output:\n{out or '(empty)'}"
                ),
            )
            return

        if cmd == "/logs":
            lines = self.log_lines_default
            if args:
                try:
                    lines = int(args[0])
                except Exception:
                    lines = self.log_lines_default
            lines = min(max(lines, 5), 80)
            code, out = await self._run_cmd(
                "journalctl",
                "-u",
                self.runner_service,
                "--no-pager",
                "-n",
                str(lines),
                "-o",
                "cat",
            )
            await self._send(
                chat_id,
                f"logs service={self.runner_service} lines={lines} exit_code={code}\n\n{out or '(empty)'}",
            )
            return

        await self._send(chat_id, "unknown command. use /help")

    async def _handle_update(self, upd: dict[str, Any]) -> None:
        msg = upd.get("message") if isinstance(upd, dict) else None
        if not isinstance(msg, dict):
            return

        text = msg.get("text")
        if not isinstance(text, str) or not text.startswith("/"):
            return

        chat = msg.get("chat") if isinstance(msg.get("chat"), dict) else {}
        from_u = msg.get("from") if isinstance(msg.get("from"), dict) else {}

        chat_id = int(chat.get("id"))
        user_id = int(from_u.get("id"))

        if self.allowed_user_ids and user_id not in self.allowed_user_ids:
            await self._send(chat_id, "access denied")
            logger.warning("Telegram denied user_id={} chat_id={}", user_id, chat_id)
            return

        if self.debug:
            logger.info("Telegram cmd | user_id={} chat_id={} text={}", user_id, chat_id, text)

        await self._handle(chat_id=chat_id, user_id=user_id, text=text)

    async def run(self) -> None:
        timeout = aiohttp.ClientTimeout(total=35)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            self._session = session
            logger.info(
                "Telegram bot started | service={} state_file={} allowed_users={} notify_trades={} notify_chats={} notify_enrich_binance={} binance_cred_source={} sync_menu_commands={} sync_menu_button={} menu_button_mode={}",
                self.runner_service,
                self.state_file,
                (sorted(self.allowed_user_ids) if self.allowed_user_ids else "ALL"),
                self.notify_trades,
                (sorted(self.notify_chat_ids) if self.notify_chat_ids else "[]"),
                self.notify_enrich_binance,
                self.binance_cred_source,
                self.sync_menu_commands,
                self.sync_menu_button,
                self.menu_button_mode,
            )
            await self._sync_bot_menu()
            self._notify_task = asyncio.create_task(self._poll_trade_notifications())
            backoff = 1
            try:
                while True:
                    try:
                        payload: dict[str, Any] = {
                            "timeout": self.poll_timeout_sec,
                            "allowed_updates": ["message"],
                        }
                        if self._offset is not None:
                            payload["offset"] = self._offset

                        res = await self._api("getUpdates", payload)
                        updates = res.get("result", [])
                        if isinstance(updates, list):
                            for upd in updates:
                                if not isinstance(upd, dict):
                                    continue
                                upd_id = upd.get("update_id")
                                if isinstance(upd_id, int):
                                    self._offset = upd_id + 1
                                try:
                                    await self._handle_update(upd)
                                except Exception as e:
                                    logger.exception("Telegram update handler failed: {}", e)
                        backoff = 1
                    except asyncio.CancelledError:
                        raise
                    except Exception as e:
                        logger.warning("Telegram polling error: {}", e)
                        await asyncio.sleep(backoff)
                        backoff = min(backoff * 2, 15)
            finally:
                if self._notify_task is not None:
                    self._notify_task.cancel()
                    try:
                        await self._notify_task
                    except asyncio.CancelledError:
                        pass


def main() -> int:
    bot = TelegramRunnerBot()
    try:
        asyncio.run(bot.run())
    except KeyboardInterrupt:
        logger.info("Telegram bot interrupted")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
