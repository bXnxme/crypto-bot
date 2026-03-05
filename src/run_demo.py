from __future__ import annotations

import argparse
import asyncio
import contextlib
import importlib
import inspect
import os
import json
import re
import sys
from pathlib import Path
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from decimal import Decimal, InvalidOperation
from typing import Optional, Any

from loguru import logger


def _setup_console_logging() -> None:
    try:
        logger.remove()
    except Exception:
        pass

    log_level = os.getenv("RUN_DEMO_GRID_LOG_LEVEL", os.getenv("LOG_LEVEL", "INFO")).upper()
    logger.add(
        sys.stderr,
        level=log_level,
        colorize=True,
        backtrace=False,
        diagnose=False,
        enqueue=False,
        format=(
            "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
            "<level>{level:<8}</level> | "
            "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
            "<level>{message}</level>"
        ),
    )


def _setup_file_logging() -> None:
    path = os.getenv("RUN_DEMO_GRID_TEXT_LOG")
    if not path:
        return
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    logger.add(
        p,
        level=os.getenv("RUN_DEMO_GRID_FILE_LOG_LEVEL", "DEBUG").upper(),
        colorize=False,
        backtrace=False,
        diagnose=False,
        rotation=os.getenv("RUN_DEMO_GRID_FILE_LOG_ROTATION", "20 MB"),
        retention=os.getenv("RUN_DEMO_GRID_FILE_LOG_RETENTION", "10 days"),
        format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level:<8} | {name}:{function}:{line} - {message}",
    )

_setup_console_logging()
_setup_file_logging()

# Keep imports explicit so wiring is deterministic and logs stay readable.
try:
    from src.demo_execution import BinanceDemoExecution
except Exception as e:  # pragma: no cover
    BinanceDemoExecution = None  # type: ignore[assignment]
    _IMPORT_EXEC_ERR = e
else:
    _IMPORT_EXEC_ERR = None

try:
    from src.binance_ws import stream_book_ticker
except Exception as e:  # pragma: no cover
    stream_book_ticker = None  # type: ignore[assignment]
    _IMPORT_WS_ERR = e
else:
    _IMPORT_WS_ERR = None



# Dynamically resolve GridPaperAdapter class to avoid static imports.
def _resolve_grid_paper_adapter_class():
    """Resolve GridPaperAdapter without hard static imports (keeps IDE lint quieter)."""
    candidates = (
        "src.paper.grid_paper_adapter",
        "src.strategy.grid_paper_adapter",
    )
    errors: list[str] = []

    for mod_name in candidates:
        try:
            mod = importlib.import_module(mod_name)
        except Exception as e:  # pragma: no cover
            errors.append(f"{mod_name}: {e}")
            continue

        cls = getattr(mod, "GridPaperAdapter", None)
        if cls is None:
            errors.append(f"{mod_name}: GridPaperAdapter not found")
            continue
        return cls, None

    return None, " | ".join(errors)


@dataclass
class BBO:
    ts: datetime
    bid: Decimal
    ask: Decimal

    @property
    def mid(self) -> Decimal:
        return (self.bid + self.ask) / Decimal("2")


_INTERVAL_RE = re.compile(r"^(\d+)([smhdwM])$")


def _parse_interval(interval: str) -> tuple[int, str]:
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
    return qty, unit


def _normalize_interval(interval: str) -> str:
    qty, unit = _parse_interval(interval)
    return f"{qty}{unit}"


def _floor_to_interval(ts: datetime, interval: str) -> datetime:
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    ts = ts.astimezone(timezone.utc)

    qty, unit = _parse_interval(interval)
    if unit == "M":
        bucket_month = ((ts.month - 1) // qty) * qty + 1
        return ts.replace(month=bucket_month, day=1, hour=0, minute=0, second=0, microsecond=0)
    if unit == "w":
        anchor = datetime(1970, 1, 5, tzinfo=timezone.utc)  # Monday 00:00 UTC
        span = timedelta(weeks=qty)
        span_seconds = int(span.total_seconds())
        elapsed = int((ts - anchor).total_seconds())
        buckets = elapsed // span_seconds
        return anchor + timedelta(seconds=(buckets * span_seconds))

    unit_seconds = {
        "s": 1,
        "m": 60,
        "h": 3600,
        "d": 86400,
    }.get(unit)
    if unit_seconds is None:
        raise ValueError(f"Unsupported interval unit in {interval!r}")
    span_seconds = qty * unit_seconds
    bucket_epoch = (int(ts.timestamp()) // span_seconds) * span_seconds
    return datetime.fromtimestamp(bucket_epoch, tz=timezone.utc)


class CandleIntervalBuilder:
    """Builds closed interval candles from mid-price ticks.

    Uses mid as O/H/L/C and synthetic volume=0. This is fine for grid logic that
    only needs bar-close cadence, and can later be replaced by real klines.
    """

    def __init__(self, interval: str = "15m"):
        self.interval = _normalize_interval(interval)
        self._bucket_start: Optional[datetime] = None
        self._o: Optional[Decimal] = None
        self._h: Optional[Decimal] = None
        self._l: Optional[Decimal] = None
        self._c: Optional[Decimal] = None

    def _bucket(self, ts: datetime) -> datetime:
        return _floor_to_interval(ts, self.interval)

    def on_bbo(self, bbo: BBO) -> Optional[dict[str, Any]]:
        px = bbo.mid
        bucket = self._bucket(bbo.ts)

        # First tick
        if self._bucket_start is None:
            self._bucket_start = bucket
            self._o = self._h = self._l = self._c = px
            return None

        # Same candle -> update OHLC
        if bucket == self._bucket_start:
            assert self._h is not None and self._l is not None
            self._h = max(self._h, px)
            self._l = min(self._l, px)
            self._c = px
            return None

        # New bucket -> emit previous closed candle, start new one
        closed = {
            "ts": self._bucket_start,
            "open": self._o,
            "high": self._h,
            "low": self._l,
            "close": self._c,
            "volume": Decimal("0"),
        }

        self._bucket_start = bucket
        self._o = self._h = self._l = self._c = px
        return closed


# Backward-compatible alias for local references/log wording.
Candle15mBuilder = CandleIntervalBuilder


async def _maybe_await(x: Any) -> Any:
    if asyncio.iscoroutine(x):
        return await x
    return x


async def bootstrap_execution(symbol: str) -> Any:
    if BinanceDemoExecution is None:
        raise RuntimeError(f"Cannot import BinanceDemoExecution: {_IMPORT_EXEC_ERR}")

    api_key = os.getenv("BINANCE_DEMO_API_KEY")
    api_secret = os.getenv("BINANCE_DEMO_API_SECRET")
    if not api_key or not api_secret:
        raise RuntimeError("Missing BINANCE_DEMO_API_KEY / BINANCE_DEMO_API_SECRET")

    # Keep constructor probing small and explicit.
    ctor_errors: list[Exception] = []
    exec_obj = None

    for kwargs in (
        {"symbol": symbol, "api_key": api_key, "api_secret": api_secret},
        {"api_key": api_key, "api_secret": api_secret, "symbol": symbol},
        {"symbol": symbol},
        {},
    ):
        try:
            exec_obj = BinanceDemoExecution(**kwargs)
            break
        except TypeError as e:
            ctor_errors.append(e)
        except Exception as e:
            ctor_errors.append(e)

    if exec_obj is None:
        for args in (
            (symbol, api_key, api_secret),
            (api_key, api_secret),
            (symbol,),
            tuple(),
        ):
            try:
                exec_obj = BinanceDemoExecution(*args)
                break
            except TypeError as e:
                ctor_errors.append(e)
            except Exception as e:
                ctor_errors.append(e)

    if exec_obj is not None:
        exec_obj = await _maybe_await(exec_obj)

    if exec_obj is None:
        msgs = " | ".join(str(e) for e in ctor_errors[-6:])
        raise RuntimeError(
            "Unable to construct BinanceDemoExecution. Tried explicit signatures. "
            f"Errors: {msgs}"
        )

    # Optional bootstrap hooks
    for method_name in ("bootstrap", "refresh_balances", "refresh_open_orders"):
        fn = getattr(exec_obj, method_name, None)
        if callable(fn):
            await _maybe_await(fn())

    return exec_obj


def _call_strategy_hook(strategy: Any, hook_name: str, **kwargs: Any):
    fn = getattr(strategy, hook_name, None)
    if not callable(fn):
        return None

    # `on_quote` is the most ambiguous hook across adapters. Handle it explicitly.
    if hook_name == "on_quote" and "bid" in kwargs and "ask" in kwargs:
        bid = kwargs["bid"]
        ask = kwargs["ask"]
        ts = kwargs.get("ts")
        broker = kwargs.get("broker")

        quote_dict = {
            "ts": ts,
            "bid": bid,
            "ask": ask,
            "b": bid,
            "a": ask,
            "best_bid": bid,
            "best_ask": ask,
        }

        class _QuoteCompat:
            __slots__ = ("ts", "bid", "ask", "b", "a", "best_bid", "best_ask")

            def __init__(self, ts, bid, ask):
                self.ts = ts
                self.bid = bid
                self.ask = ask
                self.b = bid
                self.a = ask
                self.best_bid = bid
                self.best_ask = ask

            def get(self, key, default=None):
                return getattr(self, key, default)

            def __getitem__(self, key):
                return getattr(self, key)

        quote_obj = _QuoteCompat(ts, bid, ask)

        # Prefer quote-shaped payloads. Do NOT fall back to fn(bid, ask), because some
        # adapters expect a single quote object and will try to parse the first positional
        # arg as a quote container (which breaks on Decimal bid values).
        probe_calls = (
            lambda: fn(broker=broker, quote=quote_obj) if broker is not None else fn(quote=quote_obj),
            lambda: fn(broker=broker, quote=quote_dict) if broker is not None else fn(quote=quote_dict),
            lambda: fn(broker, quote_obj) if broker is not None else fn(quote_obj),
            lambda: fn(broker, quote_dict) if broker is not None else fn(quote_dict),
            lambda: fn(broker=broker, ts=ts, bid=bid, ask=ask) if broker is not None else fn(ts=ts, bid=bid, ask=ask),
            lambda: fn(broker=broker, bid=bid, ask=ask) if broker is not None else fn(bid=bid, ask=ask),
            lambda: fn(ts=ts, quote=quote_obj),
            lambda: fn(ts=ts, quote=quote_dict),
            lambda: fn(quote=quote_obj),
            lambda: fn(quote=quote_dict),
            lambda: fn(quote_obj),
            lambda: fn(quote_dict),
            lambda: fn(ts, quote_obj),
            lambda: fn(ts, quote_dict),
            lambda: fn(ts=ts, bid=bid, ask=ask),
            lambda: fn(bid=bid, ask=ask),
            lambda: fn(**kwargs),
        )

        last_err: Exception | None = None
        for call in probe_calls:
            try:
                return call()
            except (TypeError, InvalidOperation, ValueError) as e:
                last_err = e
                continue
            except Exception as e:
                last_err = e
                continue

        if last_err is not None:
            logger.warning("Strategy on_quote dispatch failed: {}", last_err)
        return None

    # Non-quote hooks: fast path via kwargs, then generic positional fallback.
    try:
        return fn(**kwargs)
    except (TypeError, InvalidOperation, ValueError):
        pass

    try:
        return fn(*kwargs.values())
    except (TypeError, InvalidOperation, ValueError):
        return None



def _json_default(v: Any) -> Any:
    if isinstance(v, Decimal):
        return str(v)
    if isinstance(v, datetime):
        return v.isoformat()
    if isinstance(v, Exception):
        return str(v)
    return str(v)


# --- Numeric formatting helper ---
def _fmt_num(x: Any, places: int = 2) -> str:
    try:
        return f"{float(x):.{places}f}"
    except Exception:
        return str(x)
    
def _fmt_qty(x: Any) -> str:
    """Format asset quantities for logs with higher precision (trims trailing zeros)."""
    try:
        v = float(x)
    except Exception:
        return str(x)
    s = f"{v:.8f}".rstrip("0").rstrip(".")
    return s if s else "0"
    

def _fmt_log_val(x: Any) -> str:
    """Format numbers for logs with 2 decimals; pass through non-numeric values."""
    if x is None:
        return "?"
    if isinstance(x, (int, float, Decimal)):
        return _fmt_num(x)
    try:
        return _fmt_num(x)
    except Exception:
        return str(x)


def _env_flag(name: str, default: bool = False) -> bool:
    """Parse bool-like env flags: 1/true/yes/on."""
    raw = str(os.getenv(name, "1" if default else "0")).strip().lower()
    return raw in {"1", "true", "yes", "y", "on"}


# --- Telemetry Writer ---
class JsonlTelemetryWriter:
    """Best-effort JSONL telemetry writer for quotes/trades/metrics."""

    def __init__(self, *, symbol: str):
        base_dir = Path(os.getenv("RUN_DEMO_GRID_LOG_DIR", "logs"))
        base_dir.mkdir(parents=True, exist_ok=True)
        safe_symbol = (symbol or "UNKNOWN").upper()
        prefix = os.getenv("RUN_DEMO_GRID_LOG_PREFIX", f"demo_grid_{safe_symbol.lower()}")
        self.quotes_path = base_dir / f"{prefix}_quotes.jsonl"
        self.trades_path = base_dir / f"{prefix}_trades.jsonl"
        self.metrics_path = base_dir / f"{prefix}_metrics.jsonl"

    def _append(self, path: Path, payload: dict[str, Any]) -> None:
        try:
            with path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(payload, ensure_ascii=False, default=_json_default) + "\n")
        except Exception as e:
            logger.warning("JSONL write failed [{}]: {}", path.name, e)

    def quote(self, **payload: Any) -> None:
        self._append(self.quotes_path, payload)

    def trade(self, **payload: Any) -> None:
        self._append(self.trades_path, payload)

    def metric(self, **payload: Any) -> None:
        self._append(self.metrics_path, payload)


@dataclass
class S3BackupUploader:
    """Optional S3 uploader for state/log backups."""

    bucket: str
    prefix: str
    symbol: str
    endpoint: str
    region: str
    _client: Any

    @classmethod
    def from_env(cls, *, symbol: str) -> "S3BackupUploader | None":
        enabled_raw = str(os.getenv("RUN_DEMO_GRID_S3_ENABLED", "0")).strip().lower()
        enabled = enabled_raw in {"1", "true", "yes", "y", "on"}
        if not enabled:
            return None

        endpoint = str(
            os.getenv("RUN_DEMO_GRID_S3_ENDPOINT", os.getenv("S3_ENDPOINT", ""))
        ).strip()
        region = str(os.getenv("RUN_DEMO_GRID_S3_REGION", os.getenv("S3_REGION", "ru-1"))).strip()
        bucket = str(os.getenv("RUN_DEMO_GRID_S3_BUCKET", os.getenv("S3_BUCKET", ""))).strip()
        access_key = str(
            os.getenv("RUN_DEMO_GRID_S3_ACCESS_KEY", os.getenv("S3_ACCESS_KEY", ""))
        ).strip()
        secret_key = str(
            os.getenv("RUN_DEMO_GRID_S3_SECRET_KEY", os.getenv("S3_SECRET_KEY", ""))
        ).strip()
        prefix = str(os.getenv("RUN_DEMO_GRID_S3_PREFIX", "demo-grid")).strip().strip("/")

        missing = []
        if not endpoint:
            missing.append("RUN_DEMO_GRID_S3_ENDPOINT or S3_ENDPOINT")
        if not bucket:
            missing.append("RUN_DEMO_GRID_S3_BUCKET or S3_BUCKET")
        if not access_key:
            missing.append("RUN_DEMO_GRID_S3_ACCESS_KEY or S3_ACCESS_KEY")
        if not secret_key:
            missing.append("RUN_DEMO_GRID_S3_SECRET_KEY or S3_SECRET_KEY")
        if missing:
            logger.warning(
                "S3 backup disabled: missing env vars: {}",
                ", ".join(missing),
            )
            return None

        try:
            import boto3  # type: ignore
        except Exception as e:
            logger.warning("S3 backup disabled: boto3 import failed: {}", e)
            return None

        try:
            session = boto3.session.Session(
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                region_name=region or None,
            )
            client = session.client(
                "s3",
                endpoint_url=endpoint,
                region_name=region or None,
            )
        except Exception as e:
            logger.warning("S3 backup disabled: cannot build S3 client: {}", e)
            return None

        return cls(
            bucket=bucket,
            prefix=prefix,
            symbol=str(symbol or "").strip().upper(),
            endpoint=endpoint,
            region=region,
            _client=client,
        )

    @property
    def _root(self) -> str:
        safe_symbol = re.sub(r"[^a-zA-Z0-9_-]+", "_", self.symbol.lower())
        if self.prefix:
            return f"{self.prefix}/{safe_symbol}"
        return safe_symbol

    def _state_key(self) -> str:
        return f"{self._root}/state/latest.json"

    def _quotes_key(self) -> str:
        return f"{self._root}/logs/quotes_latest.jsonl"

    def _trades_key(self) -> str:
        return f"{self._root}/logs/trades_latest.jsonl"

    def _metrics_key(self) -> str:
        return f"{self._root}/logs/metrics_latest.jsonl"

    def _upload_file_sync(self, *, local_path: Path, object_key: str) -> bool:
        if not local_path.exists():
            return False
        self._client.upload_file(str(local_path), self.bucket, object_key)
        return True

    async def upload_state(self, *, state_path: Path, reason: str, tick: int | None = None) -> None:
        object_key = self._state_key()
        try:
            ok = await asyncio.to_thread(
                self._upload_file_sync,
                local_path=state_path,
                object_key=object_key,
            )
            if ok:
                logger.debug(
                    "S3 upload state ok | bucket={} key={} reason={} tick={}",
                    self.bucket,
                    object_key,
                    reason,
                    tick if tick is not None else "?",
                )
        except Exception as e:
            logger.warning(
                "S3 upload state failed | bucket={} key={} reason={} err={}",
                self.bucket,
                object_key,
                reason,
                e,
            )

    async def upload_logs(
        self,
        *,
        quotes_path: Path,
        trades_path: Path,
        metrics_path: Path,
        reason: str,
    ) -> None:
        files = (
            (quotes_path, self._quotes_key()),
            (trades_path, self._trades_key()),
            (metrics_path, self._metrics_key()),
        )
        uploaded = 0
        for local_path, object_key in files:
            try:
                ok = await asyncio.to_thread(
                    self._upload_file_sync,
                    local_path=local_path,
                    object_key=object_key,
                )
                if ok:
                    uploaded += 1
            except Exception as e:
                logger.warning(
                    "S3 upload log failed | bucket={} key={} reason={} err={}",
                    self.bucket,
                    object_key,
                    reason,
                    e,
                )
        if uploaded > 0:
            logger.debug(
                "S3 upload logs ok | bucket={} uploaded={} reason={}",
                self.bucket,
                uploaded,
                reason,
            )

# --- Helper functions for telemetry/heartbeat ---
def _safe_strategy_fields(strategy: Any, execution: Any | None = None) -> dict[str, Any]:
    """Best-effort adapter telemetry for heartbeat/stop logs."""
    try:
        fn = getattr(strategy, "heartbeat_fields", None)
        if fn is None:
            return {}
        try:
            data = fn()
        except TypeError:
            if execution is None:
                return {}
            data = fn(execution)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


# --- Compact adapter preview and suffix helpers ---
def _compact_adapter_preview(fields: dict[str, Any]) -> dict[str, Any]:
    """Shrink verbose adapter heartbeat fields for terminal logs and JSON metrics."""
    if not isinstance(fields, dict) or not fields:
        return {}
    keep_keys = (
        "anchor",
        "grid_buy_levels",
        "open_orders",
        "grid_open_lots",
        "fills_buy",
        "fills_sell",
        "reanchors",
        "cap_blocked",
        "min_notional_blocked",
        "duplicate_place_skips",
        "duplicate_pending_skips",
        "place_limit_failures",
    )
    out: dict[str, Any] = {}
    for k in keep_keys:
        if k not in fields:
            continue
        v = fields.get(k)
        if isinstance(v, Decimal):
            if k == "anchor":
                out[k] = _fmt_num(v)
            else:
                out[k] = str(v)
        else:
            out[k] = v
    return out


def _compact_adapter_suffix(fields: dict[str, Any], *, hb_dt: float | None = None, runtime_stats: dict[str, Any] | None = None) -> str:
    """Render compact adapter metrics similar to run_paper heartbeat style."""
    if not isinstance(fields, dict) or not fields:
        return ""

    parts: list[str] = []
    for k in (
        "anchor",
        "grid_buy_levels",
        "open_orders",
        "fills_buy",
        "fills_sell",
        "reanchors",
        "cap_blocked",
        "min_notional_blocked",
    ):
        if k not in fields:
            continue
        v = fields[k]
        if k == "anchor" and isinstance(v, Decimal):
            parts.append(f"{k}={_fmt_num(v)}")
        else:
            parts.append(f"{k}={v}")

    dup_place_total = int(fields.get("duplicate_place_skips", 0) or 0)
    dup_pending_total = int(fields.get("duplicate_pending_skips", 0) or 0)

    dup_place_delta = 0
    dup_pending_delta = 0
    dup_place_rate = 0.0
    dup_pending_rate = 0.0

    if runtime_stats is not None and hb_dt is not None and hb_dt > 0:
        prev_place = int(runtime_stats.get("hb_prev_dup_place", 0) or 0)
        prev_pending = int(runtime_stats.get("hb_prev_dup_pending", 0) or 0)
        dup_place_delta = dup_place_total - prev_place
        dup_pending_delta = dup_pending_total - prev_pending
        dup_place_rate = float(dup_place_delta) / hb_dt
        dup_pending_rate = float(dup_pending_delta) / hb_dt
        runtime_stats["hb_prev_dup_place"] = dup_place_total
        runtime_stats["hb_prev_dup_pending"] = dup_pending_total

    parts.append(f"dup_place={dup_place_total}(+{dup_place_delta}, {dup_place_rate:.1f}/s)")
    parts.append(f"dup_pending={dup_pending_total}(+{dup_pending_delta}, {dup_pending_rate:.1f}/s)")

    return " | adapter=" + ", ".join(parts) if parts else ""

def _is_local_order_id(order_id: Any) -> bool:
    try:
        return str(order_id).startswith("LOCAL-")
    except Exception:
        return False

def _extract_order_id(obj: Any) -> str | None:
    """Extract a stable order identifier.

    IMPORTANT:
    - Prefer exchange-generated ids (order_id/orderId/id).
    - Only fall back to clientOrderId/client_order_id when no exchange id is present.

    This avoids treating real exchange orders as LOCAL-* just because clientOrderId
    uses a LOCAL-* scheme for correlation.
    """
    if obj is None:
        return None

    # dict-like first
    if isinstance(obj, dict):
        for k in ("order_id", "orderId", "id"):
            v = obj.get(k)
            if v is not None and str(v) != "":
                return str(v)
        for k in ("clientOrderId", "client_order_id"):
            v = obj.get(k)
            if v is not None and str(v) != "":
                return str(v)
        return None

    # object-like
    for k in ("order_id", "orderId", "id"):
        v = getattr(obj, k, None)
        if v is not None and str(v) != "":
            return str(v)
    for k in ("clientOrderId", "client_order_id"):
        v = getattr(obj, k, None)
        if v is not None and str(v) != "":
            return str(v)

    return None


def _extract_fill_qty(fill: Any) -> Decimal | None:
    raw = None
    if isinstance(fill, dict):
        raw = (
            fill.get("qty")
            or fill.get("executed_qty")
            or fill.get("executedQty")
            or fill.get("last_qty")
            or fill.get("lastQty")
        )
    else:
        raw = (
            getattr(fill, "qty", None)
            or getattr(fill, "executed_qty", None)
            or getattr(fill, "executedQty", None)
            or getattr(fill, "last_qty", None)
            or getattr(fill, "lastQty", None)
        )
    if raw is None:
        return None
    try:
        return Decimal(str(raw))
    except Exception:
        return None


def _extract_fill_status(fill: Any) -> str | None:
    if isinstance(fill, dict):
        for k in ("status", "execution_type", "exec_type", "x", "X"):
            v = fill.get(k)
            if v is not None:
                return str(v)
        return None
    for k in ("status", "execution_type", "exec_type", "x", "X"):
        v = getattr(fill, k, None)
        if v is not None:
            return str(v)
    return None


def _extract_open_order_ids(open_orders: Any) -> set[str]:
    ids: set[str] = set()
    if not isinstance(open_orders, list):
        return ids
    for o in open_orders:
        oid = _extract_order_id(o)
        if oid:
            ids.add(oid)
    return ids

def _get_quote_balance_total(execution: Any) -> Any:
    """Prefer total quote balance; otherwise derive free+locked when possible."""
    total = getattr(execution, "quote_balance_total", None)
    free = getattr(execution, "quote_balance", None)
    locked = getattr(execution, "quote_balance_locked", getattr(execution, "quote_locked", None))

    if total is not None:
        return total
    if free is None:
        return None
    if locked is None:
        return free
    try:
        return Decimal(str(free)) + Decimal(str(locked))
    except Exception:
        return free


def _get_base_balance_total(execution: Any) -> Any:
    """Prefer total base balance; otherwise derive free+locked when possible."""
    total = getattr(execution, "base_balance_total", None)
    free = getattr(execution, "base_balance", None)
    locked = getattr(execution, "base_balance_locked", getattr(execution, "base_locked", None))

    if total is not None:
        return total
    if free is None:
        return None
    if locked is None:
        return free
    try:
        return Decimal(str(free)) + Decimal(str(locked))
    except Exception:
        return free

# --- Event key for deduplication ---
def _make_event_key(obj: Any, *, kind: str) -> str:
    order_id = _extract_order_id(obj) or "?"
    status = (_extract_fill_status(obj) or "?").upper()
    qty = _extract_fill_qty(obj)
    if isinstance(obj, dict):
        ts = obj.get("ts") or obj.get("time") or obj.get("transactTime") or obj.get("updateTime")
        side = obj.get("side") or "?"
        price = obj.get("price") or obj.get("avgPrice") or obj.get("lastPrice") or "?"
        trade_id = obj.get("trade_id") or obj.get("tradeId") or obj.get("id") or obj.get("executionId")
    else:
        ts = getattr(obj, "ts", None) or getattr(obj, "time", None) or getattr(obj, "transactTime", None) or getattr(obj, "updateTime", None)
        side = getattr(obj, "side", "?")
        price = getattr(obj, "price", None) or getattr(obj, "avgPrice", None) or getattr(obj, "lastPrice", None) or "?"
        trade_id = getattr(obj, "trade_id", None) or getattr(obj, "tradeId", None) or getattr(obj, "id", None) or getattr(obj, "executionId", None)
    if isinstance(ts, datetime):
        ts = ts.isoformat()
    elif ts is None:
        ts = "?"
    qty_s = str(qty) if qty is not None else "?"
    return f"{kind}|{order_id}|{status}|{side}|{qty_s}|{price}|{trade_id or '?'}|{ts}"


def _capture_runtime_state(*, symbol: str, strategy: Any, execution: Any, runtime_stats: dict[str, Any], tick_count: int) -> dict[str, Any]:
    state: dict[str, Any] = {
        "version": 1,
        "saved_at": datetime.now(timezone.utc),
        "symbol": symbol,
        "tick_count": tick_count,
        "runtime_stats": dict(runtime_stats),
        "execution": {
            "quote_balance": _get_quote_balance_total(execution),
            "quote_balance_free": getattr(execution, "quote_balance", None),
            "base_balance": _get_base_balance_total(execution),
            "base_balance_free": getattr(execution, "base_balance", None),
            "open_orders": getattr(execution, "open_orders", None),
        },
    }

    # Strategy state (best effort)
    try:
        if hasattr(strategy, "snapshot_state") and callable(getattr(strategy, "snapshot_state")):
            state["strategy"] = strategy.snapshot_state()
        elif hasattr(strategy, "export_state") and callable(getattr(strategy, "export_state")):
            # GridBacktestAdapter / GridPaperAdapter expose export_state()
            state["strategy"] = strategy.export_state()
        elif hasattr(strategy, "get_state") and callable(getattr(strategy, "get_state")):
            state["strategy"] = strategy.get_state()
        elif hasattr(strategy, "state"):
            state["strategy"] = getattr(strategy, "state")
    except Exception as e:
        state["strategy_state_error"] = str(e)

    return state


def _apply_runtime_state(*, state: dict[str, Any], strategy: Any, runtime_stats: dict[str, Any]) -> None:
    # Restore only runner counters that are safe to resume.
    saved_stats = state.get("runtime_stats")
    if isinstance(saved_stats, dict):
        for k in (
            "bar_builder_fallback_uses",
            "ws_messages",
            "ws_reconnects",
            "ws_timeouts",
            "ws_errors",
        ):
            if k in saved_stats:
                runtime_stats[k] = saved_stats[k]

    strategy_state = state.get("strategy")
    if strategy_state is None:
        return

    try:
        if hasattr(strategy, "restore_state") and callable(getattr(strategy, "restore_state")):
            strategy.restore_state(strategy_state)
            return
        if hasattr(strategy, "import_state") and callable(getattr(strategy, "import_state")):
            strategy.import_state(strategy_state)
            return
        if hasattr(strategy, "load_state") and callable(getattr(strategy, "load_state")):
            strategy.load_state(strategy_state)
            return
        if hasattr(strategy, "state"):
            setattr(strategy, "state", strategy_state)
    except Exception as e:
        logger.warning("Strategy state restore failed: {}", e)


async def _periodic_state_flush_task(
    *,
    symbol: str,
    strategy: Any,
    execution: Any,
    runtime_stats: dict[str, Any],
    tick_count_ref: dict[str, int],
    path: Path,
    interval_sec: float,
    stop_evt: asyncio.Event,
    jsonl: JsonlTelemetryWriter,
    s3_uploader: S3BackupUploader | None = None,
    s3_log_sync_sec: float = 0.0,
) -> None:
    next_s3_logs_sync_mono = 0.0
    while not stop_evt.is_set():
        try:
            await asyncio.wait_for(stop_evt.wait(), timeout=interval_sec)
            break
        except asyncio.TimeoutError:
            pass

        try:
            payload = _capture_runtime_state(
                symbol=symbol,
                strategy=strategy,
                execution=execution,
                runtime_stats=runtime_stats,
                tick_count=int(tick_count_ref.get("tick_count", 0)),
            )
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(payload, ensure_ascii=False, default=_json_default, indent=2), encoding="utf-8")
            logger.debug("STATE: periodic flush completed")
            tick_no = int(tick_count_ref.get("tick_count", 0))
            jsonl.metric(
                kind="state_flush",
                ts=datetime.now(timezone.utc),
                symbol=symbol,
                tick=tick_no,
                path=str(path),
            )

            if s3_uploader is not None:
                await s3_uploader.upload_state(
                    state_path=path,
                    reason="periodic",
                    tick=tick_no,
                )
                if s3_log_sync_sec > 0:
                    now_mono = asyncio.get_running_loop().time()
                    if now_mono >= next_s3_logs_sync_mono:
                        await s3_uploader.upload_logs(
                            quotes_path=jsonl.quotes_path,
                            trades_path=jsonl.trades_path,
                            metrics_path=jsonl.metrics_path,
                            reason="periodic",
                        )
                        next_s3_logs_sync_mono = now_mono + float(s3_log_sync_sec)
        except Exception as e:
            logger.warning("STATE: periodic flush failed: {}", e)


def _emit_preflight_log(*, symbol: str, strategy: Any, execution: Any, state_file: Path | None = None, state_loaded: bool = False) -> None:
    q = _get_quote_balance_total(execution)
    b = _get_base_balance_total(execution)
    oo = getattr(execution, "open_orders", None)
    strategy_name = type(strategy).__name__ if strategy is not None else "None"
    strategy_interval = getattr(strategy, "interval", None)
    raw_adapter_fields = _safe_strategy_fields(strategy, execution=execution)
    adapter_quote_view = raw_adapter_fields.get("quote") if isinstance(raw_adapter_fields, dict) else None
    adapter_base_view = raw_adapter_fields.get("base") if isinstance(raw_adapter_fields, dict) else None
    adapter_preview = _compact_adapter_preview(raw_adapter_fields)
    logger.info(
        "PREFLIGHT mode=demo_live_grid symbol={} state_file={} state_loaded={} | strategy={} | quote_balance={} base_balance={} open_orders={} | "
        "strategy_interval={} | demo_rest={} demo_ws={} | adapter_quote_view={} adapter_base_view={} | adapter_preview={}",
        symbol,
        (str(state_file) if state_file is not None else "-"),
        state_loaded,
        strategy_name,
        _fmt_num(q) if q is not None else "?",
        _fmt_num(b) if b is not None else "?",
        len(oo) if isinstance(oo, list) else oo,
        strategy_interval or "?",
        os.environ.get("BINANCE_DEMO_REST_BASE_URL", "-"),
        os.environ.get("BINANCE_DEMO_WS_BASE_URL", "-"),
        (_fmt_num(adapter_quote_view) if adapter_quote_view is not None else "?"),
        (_fmt_num(adapter_base_view) if adapter_base_view is not None else "?"),
        adapter_preview,
    )


async def run_loop(*, symbol: str, strategy: Any, interval: str = "15m", max_ticks: int = 0) -> None:
    if stream_book_ticker is None:
        raise RuntimeError(f"Cannot import stream_book_ticker: {_IMPORT_WS_ERR}")
    interval = _normalize_interval(interval)

    # Force demo endpoints for this runner.
    # IMPORTANT: REST must be https://demo-api.binance.com, while WS is wss://demo-stream.binance.com.
    # If REST accidentally gets a wss:// URL, signed requests go to /api/v3 on a WS host and return 404.
    os.environ.setdefault("BINANCE_DEMO", "1")
    os.environ.setdefault("BINANCE_DEMO_MODE", "1")
    os.environ.setdefault("BINANCE_DEMO_REST_BASE_URL", "https://demo-api.binance.com")
    os.environ.setdefault("BINANCE_DEMO_WS_BASE_URL", "wss://demo-stream.binance.com")

    execution = await bootstrap_execution(symbol)
    # --- Hard rate-limit wrappers for execution REST refresh methods ---
    # Strategy adapter can call refresh_* multiple times per fill; without a guard this hits -1003 and triggers IP bans.

    def _parse_ban_until_utc_from_msg(msg: str) -> datetime | None:
        try:
            s = str(msg)
            marker = "banned until"
            if marker not in s:
                return None
            tail = s.split(marker, 1)[1].strip()
            num = ""
            for ch in tail:
                if ch.isdigit():
                    num += ch
                elif num:
                    break
            if not num:
                return None
            ms = int(num)
            return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)
        except Exception:
            return None

    def _wrap_refresh_method(exe: Any, method_name: str, *, min_interval_sec: float, backoff_max_sec: float) -> None:
        orig = getattr(exe, method_name, None)
        if not callable(orig):
            return

        state = {
            "next_allowed_mono": 0.0,
            "backoff_sec": 0.0,
            "banned_until_utc": None,
            "in_flight": False,
        }

        async def _call_orig_async() -> None:
            out = orig()
            if asyncio.iscoroutine(out):
                await out

        def wrapper():
            # Sync/async compatible wrapper: if in running loop, schedule; otherwise best-effort.
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                now = datetime.now(timezone.utc)
                banned_until = state.get("banned_until_utc")
                if isinstance(banned_until, datetime) and now < banned_until:
                    return None
                try:
                    return orig()
                except Exception:
                    return None

            now_mono = loop.time()
            now_utc = datetime.now(timezone.utc)

            banned_until = state.get("banned_until_utc")
            if isinstance(banned_until, datetime) and now_utc < banned_until:
                return None

            if state.get("in_flight"):
                return None

            if now_mono < float(state.get("next_allowed_mono", 0.0) or 0.0):
                return None

            state["in_flight"] = True
            gate = float(min_interval_sec) + float(state.get("backoff_sec", 0.0) or 0.0)
            state["next_allowed_mono"] = now_mono + max(gate, 0.0)

            async def run_and_handle_errors():
                try:
                    await _call_orig_async()
                    # success -> relax backoff slowly
                    if state["backoff_sec"] > 0:
                        state["backoff_sec"] = max(0.0, float(state["backoff_sec"]) - 1.0)
                except Exception as e:
                    msg = str(e)
                    ban_until = _parse_ban_until_utc_from_msg(msg)
                    if ban_until is not None:
                        state["banned_until_utc"] = ban_until

                    if "-1003" in msg or "Too much request weight" in msg or "Way too much request weight" in msg:
                        cur = float(state.get("backoff_sec", 0.0) or 0.0)
                        nxt = max(cur * 2.0, 5.0)
                        state["backoff_sec"] = min(nxt, float(backoff_max_sec))
                        state["next_allowed_mono"] = loop.time() + float(min_interval_sec) + float(state["backoff_sec"])

                    logger.warning("{} throttled wrapper error: {}", method_name, e)
                finally:
                    state["in_flight"] = False

            return loop.create_task(run_and_handle_errors())

        setattr(exe, method_name, wrapper)

    _wrap_refresh_method(
        execution,
        "refresh_balances",
        min_interval_sec=float(os.getenv("RUN_DEMO_GRID_REFRESH_BALANCES_MIN_SEC", "5.0")),
        backoff_max_sec=float(os.getenv("RUN_DEMO_GRID_REFRESH_BACKOFF_MAX_SEC", "300.0")),
    )
    _wrap_refresh_method(
        execution,
        "refresh_open_orders",
        min_interval_sec=float(os.getenv("RUN_DEMO_GRID_REFRESH_OPEN_ORDERS_MIN_SEC", "5.0")),
        backoff_max_sec=float(os.getenv("RUN_DEMO_GRID_REFRESH_BACKOFF_MAX_SEC", "300.0")),
    )    
    jsonl = JsonlTelemetryWriter(symbol=symbol)

    state_path = Path(os.getenv("RUN_DEMO_GRID_STATE_FILE", "data/run_demo_grid_state.json"))
    state_flush_sec = float(os.getenv("RUN_DEMO_GRID_STATE_FLUSH_SEC", "15"))
    s3_log_sync_sec = float(os.getenv("RUN_DEMO_GRID_S3_LOG_SYNC_SEC", "0"))
    s3_uploader = S3BackupUploader.from_env(symbol=symbol)
    state_loaded = False

    if s3_uploader is not None:
        logger.info(
            "S3 backup enabled | endpoint={} bucket={} region={} prefix={} log_sync_sec={}",
            s3_uploader.endpoint,
            s3_uploader.bucket,
            s3_uploader.region,
            s3_uploader.prefix or "-",
            s3_log_sync_sec,
        )

    # Optional: inject live execution into adapter/strategy if it supports it.
    for attr_name in ("set_execution", "attach_execution", "bind_execution"):
        fn = getattr(strategy, attr_name, None)
        if callable(fn):
            try:
                out = fn(execution)
                await _maybe_await(out)
                logger.info("Strategy execution attached via {}()", attr_name)
                break
            except Exception as e:
                logger.warning("Strategy {}() failed: {}", attr_name, e)

    for attr_name in ("execution", "executor"):
        try:
            if hasattr(strategy, attr_name):
                setattr(strategy, attr_name, execution)
                logger.info("Strategy execution injected into .{}", attr_name)
                break
        except Exception:
            pass

    candle_builder = CandleIntervalBuilder(interval=interval)
    paper_adapter = None
    GridPaperAdapterCls, grid_adapter_import_err = _resolve_grid_paper_adapter_class()
    if GridPaperAdapterCls is not None:
        try:
            paper_adapter = GridPaperAdapterCls(symbol=symbol, interval=interval)
            logger.info("Using GridPaperAdapter for synthetic {} candles", interval)
        except TypeError:
            # Older adapter signatures may not accept `interval`.
            if interval == "15m":
                paper_adapter = GridPaperAdapterCls(symbol=symbol)
                logger.info("Using GridPaperAdapter for synthetic {} candles", interval)
            else:
                logger.warning(
                    "GridPaperAdapter doesn't accept interval={} in constructor; using fallback bar builder",
                    interval,
                )
        except Exception as e:
            logger.warning("GridPaperAdapter init failed, fallback to CandleIntervalBuilder: {}", e)
    else:
        if grid_adapter_import_err:
            logger.info(
                "GridPaperAdapter import unavailable, using CandleIntervalBuilder fallback: {}",
                grid_adapter_import_err,
            )
    runtime_stats: dict[str, Any] = {
        "started_at": datetime.now(timezone.utc),
        "strategy_on_quote_calls": 0,
        "strategy_on_bar_close_calls": 0,
        "strategy_on_fill_calls": 0,
        "fills_total": 0,
        "bars_total": 0,
        "bar_builder_fallback_uses": 0,
        "ws_messages": 0,
        "ws_reconnects": 0,
        "ws_timeouts": 0,
        "ws_errors": 0,
        "ws_last_msg_ts": None,
        "hb_prev_ws_messages": 0,
        "processed_fill_keys": set(),
        "processed_update_keys": set(),
        "startup_sync_done": False,
    }
    tick_count_ref: dict[str, int] = {"tick_count": 0}
    state_stop_evt = asyncio.Event()
    state_task: asyncio.Task | None = None

    heartbeat_sec = float(os.getenv("HEARTBEAT_SEC", "10"))
    next_heartbeat_at = (
        datetime.now(timezone.utc) + timedelta(seconds=heartbeat_sec)
        if heartbeat_sec > 0
        else None
    )
    t0 = datetime.now(timezone.utc)
    min_bid: Decimal | None = None
    max_bid: Decimal | None = None
    min_ask: Decimal | None = None
    max_ask: Decimal | None = None
    fills_since_heartbeat = 0
    bars_since_heartbeat = 0
    last_hb_tick_count = 0
    last_hb_ts = datetime.now(timezone.utc)

    # --- State restore before preflight log
    if state_path.exists():
        try:
            raw_state = json.loads(state_path.read_text(encoding="utf-8"))
            if isinstance(raw_state, dict):
                _apply_runtime_state(state=raw_state, strategy=strategy, runtime_stats=runtime_stats)
                state_loaded = True
                logger.info("STATE restore ok | path={} saved_at={} prev_tick_count={}", state_path, raw_state.get("saved_at"), raw_state.get("tick_count"))
            else:
                logger.warning("STATE restore skipped: invalid payload type in {}", state_path)
        except Exception as e:
            logger.warning("STATE restore failed [{}]: {}", state_path, e)

    _emit_preflight_log(symbol=symbol, strategy=strategy, execution=execution, state_file=state_path, state_loaded=state_loaded)
    jsonl.metric(
        kind="preflight",
        ts=datetime.now(timezone.utc),
        symbol=symbol,
        strategy=type(strategy).__name__ if strategy is not None else "None",
        quote_balance=_get_quote_balance_total(execution),
        base_balance=_get_base_balance_total(execution),
        open_orders=(len(getattr(execution, "open_orders", [])) if isinstance(getattr(execution, "open_orders", None), list) else getattr(execution, "open_orders", None)),
        demo_rest=os.environ.get("BINANCE_DEMO_REST_BASE_URL", "-"),
        demo_ws=os.environ.get("BINANCE_DEMO_WS_BASE_URL", "-"),
        state_file=str(state_path),
        state_loaded=state_loaded,
    )

    tick_count = 0
    logger.info(
        "START demo grid runner | symbol={} interval={} max_ticks={}",
        symbol,
        interval,
        max_ticks or "∞",
    )

    if state_flush_sec > 0:
        state_task = asyncio.create_task(
            _periodic_state_flush_task(
                symbol=symbol,
                strategy=strategy,
                execution=execution,
                runtime_stats=runtime_stats,
                tick_count_ref=tick_count_ref,
                path=state_path,
                interval_sec=state_flush_sec,
                stop_evt=state_stop_evt,
                jsonl=jsonl,
                s3_uploader=s3_uploader,
                s3_log_sync_sec=s3_log_sync_sec,
            )
        )

    skipped_same_quote = 0
    last_hb_skipped_same_quote = 0
    last_seen_bid: Decimal | None = None
    last_seen_ask: Decimal | None = None

    known_open_order_ids: set[str] = _extract_open_order_ids(getattr(execution, "open_orders", None))
    # Keep a short grace before treating "disappeared from open_orders" as cancel.
    # Exchange snapshots can be temporarily stale/incomplete between refresh cycles.
    open_order_missing_since_mono: dict[str, float] = {}
    open_order_diff_grace_sec = float(os.getenv("RUN_DEMO_GRID_OPEN_ORDERS_DIFF_GRACE_SEC", "6.0"))
    # Conservative default: open_orders diff is telemetry-only unless explicitly enabled.
    # Real cancel propagation should primarily come from explicit order updates.
    open_order_diff_strategy_cancel = _env_flag(
        "RUN_DEMO_GRID_OPEN_ORDERS_DIFF_STRATEGY_CANCEL",
        default=False,
    )
    order_filled_qty: dict[str, Decimal] = {}
    order_last_status: dict[str, str] = {}
    canceled_total = 0
    partial_fills_total = 0
    new_orders_total = 0
    # Log-noise controls for startup/local cancel storms.
    log_local_cancel_events = _env_flag("RUN_DEMO_GRID_LOG_LOCAL_CANCELS", default=False)
    log_startup_cancel_events = _env_flag("RUN_DEMO_GRID_LOG_STARTUP_CANCELS", default=False)
    suppressed_local_cancel_logs = 0
    suppressed_startup_cancel_logs = 0
    startup_sync_polls_left = max(0, int(os.getenv("RUN_DEMO_GRID_STARTUP_SYNC_POLLS", "2")))
    # --- REST polling throttle / backoff ---
    poll_fills_every_sec = float(os.getenv("RUN_DEMO_GRID_POLL_FILLS_EVERY_SEC", "5.0"))
    poll_updates_every_sec = float(os.getenv("RUN_DEMO_GRID_POLL_UPDATES_EVERY_SEC", "10.0"))
    # Keep execution.open_orders fresh even without fills, so LOCAL-* placeholders expire
    # and adapter sync does not work on stale snapshots.
    refresh_open_orders_every_sec = float(os.getenv("RUN_DEMO_GRID_REFRESH_OPEN_ORDERS_EVERY_SEC", "2.0"))

    poll_backoff_max_sec = float(os.getenv("RUN_DEMO_GRID_POLL_BACKOFF_MAX_SEC", "120.0"))
    loop_mono = asyncio.get_running_loop().time
    next_poll_fills = loop_mono()
    next_poll_updates = loop_mono()
    next_refresh_open_orders = loop_mono()

    poll_backoff_sec = 0.0
    stream = stream_book_ticker(symbol)
    try:
        async for t in stream:
            tick_count += 1
            tick_count_ref["tick_count"] = tick_count
            tick_ts = getattr(t, "ts", None) or datetime.now(timezone.utc)
            now_utc = tick_ts if isinstance(tick_ts, datetime) else datetime.now(timezone.utc)
            now_mono = asyncio.get_running_loop().time()
            bid = Decimal(t.bid)
            ask = Decimal(t.ask)
            tick_is_startup_sync = startup_sync_polls_left > 0
            runtime_stats["ws_messages"] = int(runtime_stats.get("ws_messages", 0)) + 1
            runtime_stats["ws_last_msg_ts"] = now_utc
            same_quote = (last_seen_bid == bid and last_seen_ask == ask)
            if same_quote:
                skipped_same_quote += 1
            else:
                last_seen_bid = bid
                last_seen_ask = ask
            jsonl.quote(
                kind="quote",
                ts=now_utc,
                symbol=symbol,
                bid=bid,
                ask=ask,
                mid=(bid + ask) / Decimal("2"),
                spread=(ask - bid),
                tick=tick_count,
            )

            # Keep open-orders snapshot in sync on a timer (independent from fills/updates).
            refresh_open_orders = getattr(execution, "refresh_open_orders", None)
            if (
                callable(refresh_open_orders)
                and refresh_open_orders_every_sec > 0
                and now_mono >= next_refresh_open_orders
            ):
                next_refresh_open_orders = now_mono + refresh_open_orders_every_sec + poll_backoff_sec
                try:
                    await _maybe_await(refresh_open_orders())
                    if poll_backoff_sec > 0:
                        poll_backoff_sec = max(0.0, poll_backoff_sec - 0.5)
                except Exception as e:
                    msg = str(e)
                    if "-1003" in msg or "Too much request weight" in msg:
                        poll_backoff_sec = min(max(poll_backoff_sec * 2.0, 5.0), poll_backoff_max_sec)
                        next_refresh_open_orders = now_mono + refresh_open_orders_every_sec + poll_backoff_sec
                    logger.warning("refresh_open_orders failed: {}", e)

            # min/max tracking
            min_bid = bid if (min_bid is None or bid < min_bid) else min_bid
            max_bid = bid if (max_bid is None or bid > max_bid) else max_bid
            min_ask = ask if (min_ask is None or ask < min_ask) else min_ask
            max_ask = ask if (max_ask is None or ask > max_ask) else max_ask

            # 1) feed quote to strategy
            # 1) feed quote to strategy (only when quote changed)
            if not same_quote:
                _call_strategy_hook(strategy, "on_quote", broker=execution, ts=now_utc, bid=bid, ask=ask)
                runtime_stats["strategy_on_quote_calls"] = int(runtime_stats.get("strategy_on_quote_calls", 0)) + 1

            # 2a) poll order status updates (NEW / PARTIALLY_FILLED / FILLED / CANCELED)
            poll_updates = getattr(execution, "poll_order_updates", None)
            if callable(poll_updates) and now_mono >= next_poll_updates:
                next_poll_updates = now_mono + poll_updates_every_sec + poll_backoff_sec
                try:
                    updates = await _maybe_await(poll_updates())
                    # если успешно — слегка “отпускаем” backoff
                    if poll_backoff_sec > 0:
                        poll_backoff_sec = max(0.0, poll_backoff_sec - 1.0)
                except Exception as e:
                    msg = str(e)
                    if "-1003" in msg or "Too much request weight" in msg:
                        poll_backoff_sec = min(max(poll_backoff_sec * 2.0, 5.0), poll_backoff_max_sec)
                        next_poll_updates = now_mono + poll_updates_every_sec + poll_backoff_sec
                    logger.warning("poll_order_updates failed: {}", e)
                    updates = []
                for upd in updates or []:
                    upd_key = _make_event_key(upd, kind="upd")
                    seen_update_keys = runtime_stats.setdefault("processed_update_keys", set())
                    if upd_key in seen_update_keys:
                        continue
                    seen_update_keys.add(upd_key)

                    upd_order_id = _extract_order_id(upd)
                    upd_status_raw = _extract_fill_status(upd)
                    upd_status = str(upd_status_raw).upper() if upd_status_raw is not None else "?"
                    upd_qty_dec = _extract_fill_qty(upd)
                    upd_symbol = getattr(upd, "symbol", symbol) if not isinstance(upd, dict) else upd.get("symbol", symbol)
                    upd_side = getattr(upd, "side", "?") if not isinstance(upd, dict) else upd.get("side", "?")
                    upd_price = getattr(upd, "price", "?") if not isinstance(upd, dict) else upd.get("price", "?")
                    upd_ts = getattr(upd, "ts", datetime.now(timezone.utc)) if not isinstance(upd, dict) else upd.get("ts", datetime.now(timezone.utc))

                    if upd_order_id:
                        open_order_missing_since_mono.pop(upd_order_id, None)

                    if upd_order_id and upd_status != "?":
                        order_last_status[upd_order_id] = upd_status

                    if upd_status == "NEW":
                        if not tick_is_startup_sync:
                            new_orders_total += 1
                        logger.info(
                            "ORDER_NEW {} {} qty={} price={} order_id={} ts={}",
                            upd_symbol,
                            upd_side,
                            _fmt_log_val(upd_qty_dec),
                            _fmt_log_val(upd_price),
                            (upd_order_id or "?"),
                            (upd_ts.isoformat() if isinstance(upd_ts, datetime) else upd_ts),
                        )
                        jsonl.trade(
                            kind="order_new",
                            ts=datetime.now(timezone.utc),
                            symbol=symbol,
                            tick=tick_count,
                            order_update=upd,
                            order_id=upd_order_id,
                            status=upd_status,
                        )

                    elif upd_status == "PARTIALLY_FILLED":
                        if not tick_is_startup_sync:
                            partial_fills_total += 1
                        logger.info(
                            "PARTIAL_FILL {} {} qty={} price={} order_id={} status={} ts={}",
                            upd_symbol,
                            upd_side,
                            _fmt_log_val(upd_qty_dec),
                            _fmt_log_val(upd_price),
                            (upd_order_id or "?"),
                            upd_status,
                            (upd_ts.isoformat() if isinstance(upd_ts, datetime) else upd_ts),
                        )
                        jsonl.trade(
                            kind="order_partial",
                            ts=datetime.now(timezone.utc),
                            symbol=symbol,
                            tick=tick_count,
                            order_update=upd,
                            order_id=upd_order_id,
                            status=upd_status,
                        )

                    elif upd_status == "CANCELED":
                        is_local_cancel = _is_local_order_id(upd_order_id)

                        if not tick_is_startup_sync and not is_local_cancel:
                            canceled_total += 1

                        # LOCAL-* — это локальные placeholder-ордера. Их нельзя отдавать в on_cancel(),
                        # иначе адаптер может повторно освободить капитал.
                        if not tick_is_startup_sync and not is_local_cancel:
                            _call_strategy_hook(strategy, "on_cancel", broker=execution, order_id=(upd_order_id or "?"))

                        emit_cancel_log = True
                        if is_local_cancel and not log_local_cancel_events:
                            emit_cancel_log = False
                            suppressed_local_cancel_logs += 1
                        if tick_is_startup_sync and not log_startup_cancel_events:
                            emit_cancel_log = False
                            suppressed_startup_cancel_logs += 1

                        if emit_cancel_log:
                            logger.info(
                                "CANCEL order_id={} status={} source=order_updates local={} startup_sync={} ts={}",
                                (upd_order_id or "?"),
                                upd_status,
                                is_local_cancel,
                                tick_is_startup_sync,
                                (upd_ts.isoformat() if isinstance(upd_ts, datetime) else upd_ts),
                            )
                            jsonl.trade(
                                kind="cancel",
                                ts=datetime.now(timezone.utc),
                                symbol=symbol,
                                tick=tick_count,
                                order_update=upd,
                                order_id=upd_order_id,
                                source="order_updates",
                                status=upd_status,
                                local_placeholder=is_local_cancel,
                                startup_sync=tick_is_startup_sync,
                            )


            # 2) poll newly filled orders from execution and forward to strategy
            poll_fills = getattr(execution, "poll_new_fills", None)
            if callable(poll_fills) and now_mono >= next_poll_fills:
                next_poll_fills = now_mono + poll_fills_every_sec + poll_backoff_sec
                try:
                    fills = await _maybe_await(poll_fills())
                    if poll_backoff_sec > 0 and not fills:
                        poll_backoff_sec = max(0.0, poll_backoff_sec - 1.0)
                except Exception as e:
                    msg = str(e)
                    if "-1003" in msg or "Too much request weight" in msg:
                        poll_backoff_sec = min(max(poll_backoff_sec * 2.0, 5.0), poll_backoff_max_sec)
                        next_poll_fills = now_mono + poll_fills_every_sec + poll_backoff_sec
                    logger.warning("poll_new_fills failed: {}", e)
                    fills = []

                fills_by_order: dict[str, int] = {}
                for _f in fills or []:
                    _oid = _extract_order_id(_f)
                    if _oid:
                        fills_by_order[_oid] = fills_by_order.get(_oid, 0) + 1
                for fill in fills or []:
                    fill_key = _make_event_key(fill, kind="fill")
                    seen_fill_keys = runtime_stats.setdefault("processed_fill_keys", set())
                    if fill_key in seen_fill_keys:
                        continue
                    seen_fill_keys.add(fill_key)
                    fill_order_id = _extract_order_id(fill)
                    fill_status = _extract_fill_status(fill)    
                    if fill_order_id:
                        open_order_missing_since_mono.pop(fill_order_id, None)
                    fill_qty_dec = _extract_fill_qty(fill)
                    if fill_qty_dec is not None and fill_qty_dec <= Decimal("0"):
                        logger.info(
                            "SKIP_ZERO_FILL order_id={} qty={} source=live_poll",
                            (fill_order_id or "?"),
                            _fmt_qty(fill_qty_dec),
                        )
                        continue
                    
                    # determine partial
                    is_partial = False
                    if fill_order_id and fills_by_order.get(fill_order_id, 0) > 1:
                        is_partial = True
                    if fill_order_id and fill_qty_dec is not None:
                        prev_qty = order_filled_qty.get(fill_order_id, Decimal("0"))
                        order_filled_qty[fill_order_id] = prev_qty + fill_qty_dec

                        status_upper = str(fill_status or "").upper()
                        last_known_status = str(order_last_status.get(fill_order_id, "")).upper()

                        if "PART" in status_upper or "PART" in last_known_status:
                            is_partial = True

                    # propagate to strategy
                    fill_for_strategy = fill
                    if isinstance(fill, dict):
                        fill_for_strategy = dict(fill)
                        fill_for_strategy["partial"] = is_partial
                    else:
                        try:
                            setattr(fill_for_strategy, "partial", is_partial)
                        except Exception:
                            pass

                    # IMPORTANT: if we saw a trade for this order, its disappearance from open_orders is NOT a cancel.
                    if fill_order_id:
                        order_last_status[fill_order_id] = "PARTIALLY_FILLED" if is_partial else "FILLED"    

                    if not tick_is_startup_sync:
                        _call_strategy_hook(strategy, "on_fill", broker=execution, fill=fill_for_strategy)
                        runtime_stats["strategy_on_fill_calls"] = int(runtime_stats.get("strategy_on_fill_calls", 0)) + 1
                        runtime_stats["fills_total"] = int(runtime_stats.get("fills_total", 0)) + 1
                        fills_since_heartbeat += 1

                    fill_symbol = getattr(fill, "symbol", symbol) if not isinstance(fill, dict) else fill.get("symbol", symbol)
                    fill_side = getattr(fill, "side", "?") if not isinstance(fill, dict) else fill.get("side", "?")
                    fill_price = getattr(fill, "price", "?") if not isinstance(fill, dict) else fill.get("price", "?")
                    fill_fee = (
                        getattr(fill, "fee_quote", getattr(fill, "fee", "?"))
                        if not isinstance(fill, dict)
                        else fill.get("fee_quote", fill.get("fee", "?"))
                    )
                    fill_maker = bool(getattr(fill, "maker", False)) if not isinstance(fill, dict) else bool(fill.get("maker", False))
                    fill_ts = getattr(fill, "ts", datetime.now(timezone.utc)) if not isinstance(fill, dict) else fill.get("ts", datetime.now(timezone.utc))

                    fill_qty_disp = fill_qty_dec if fill_qty_dec is not None else (getattr(fill, "qty", "?") if not isinstance(fill, dict) else fill.get("qty", "?"))

                    partial_marker = " partial=True" if is_partial else ""

                    logger.info(
                        "FILL {} {} qty={} price={} fee_quote={} maker={} order_id={} status={}{} source={} ts={}",
                        fill_symbol,
                        fill_side,
                        _fmt_qty(fill_qty_disp),
                        _fmt_log_val(fill_price),
                        _fmt_log_val(fill_fee),
                        fill_maker,
                        (fill_order_id or "?"),
                        (fill_status or "?"),
                        partial_marker,
                        ("startup_sync" if tick_is_startup_sync else "live_poll"),
                        (fill_ts.isoformat() if isinstance(fill_ts, datetime) else fill_ts),
                    )
                    jsonl.trade(
                        kind="fill",
                        ts=datetime.now(timezone.utc),
                        symbol=symbol,
                        tick=tick_count,
                        side=fill_side,
                        qty=fill_qty_disp,
                        price=fill_price,
                        fee_quote=fill_fee,
                        maker=fill_maker,
                        fill_ts=fill_ts,
                        fill=fill,
                        order_id=fill_order_id,
                        fill_status=fill_status,
                        partial=bool(is_partial),
                    )
            
            # Mark startup sync as completed after first N polling cycles.
            if tick_is_startup_sync:
                startup_sync_polls_left -= 1
                if startup_sync_polls_left == 0:
                    runtime_stats["startup_sync_done"] = True
                    logger.info(
                        "STARTUP_SYNC complete | suppressed_cancel_logs[startup={} local={}] | log_startup_cancels={} log_local_cancels={}",
                        suppressed_startup_cancel_logs,
                        suppressed_local_cancel_logs,
                        log_startup_cancel_events,
                        log_local_cancel_events,
                    )

            # 3) build synthetic bars from mid and emit on close for selected interval
            closed_bar = None
            if paper_adapter is not None:
                try:
                    closed = paper_adapter.on_quote(ts=now_utc, bid=bid, ask=ask)
                    if closed is not None:
                        closed_bar = {
                            "ts": closed.open_time,
                            "open": closed.open,
                            "high": closed.high,
                            "low": closed.low,
                            "close": closed.close,
                            "volume": Decimal("0"),
                        }
                except Exception as e:
                    runtime_stats["bar_builder_fallback_uses"] = int(runtime_stats.get("bar_builder_fallback_uses", 0)) + 1
                    logger.warning("GridPaperAdapter on_quote failed, fallback builder: {}", e)
                    paper_adapter = None

            # Use fallback builder only when GridPaperAdapter is unavailable.
            # Otherwise it will mirror the same ticks and can emit duplicate bar closes.
            if paper_adapter is None and closed_bar is None:
                closed_bar = candle_builder.on_bbo(BBO(ts=now_utc, bid=bid, ask=ask))

            if closed_bar is not None:
                logger.info(
                    "BAR_CLOSE {} O={} H={} L={} C={}",
                    closed_bar["ts"].isoformat(),
                    closed_bar["open"],
                    closed_bar["high"],
                    closed_bar["low"],
                    closed_bar["close"],
                )
                _call_strategy_hook(strategy, "on_bar_close", broker=execution, bar=closed_bar)                
                runtime_stats["strategy_on_bar_close_calls"] = int(runtime_stats.get("strategy_on_bar_close_calls", 0)) + 1
                runtime_stats["bars_total"] = int(runtime_stats.get("bars_total", 0)) + 1
                bars_since_heartbeat += 1
                jsonl.metric(
                    kind="bar_close",
                    ts=datetime.now(timezone.utc),
                    symbol=symbol,
                    tick=tick_count,
                    bar=closed_bar,
                    bars_total=int(runtime_stats.get("bars_total", 0)),
                )

                # after bar-close: do NOT force extra REST refreshes (avoids weight-limit spikes).
                pass

            # 4) detect order cancellations/removals by diffing open order ids (best effort)
            current_open_order_ids = _extract_open_order_ids(getattr(execution, "open_orders", None))
            for open_oid in current_open_order_ids:
                open_order_missing_since_mono.pop(open_oid, None)
            disappeared_order_ids = sorted(known_open_order_ids - current_open_order_ids)
            if disappeared_order_ids:
                for canceled_order_id in disappeared_order_ids:
                    last_status = str(order_last_status.get(canceled_order_id, "")).upper()

                    # already processed cancel
                    if last_status == "CANCELED":
                        open_order_missing_since_mono.pop(canceled_order_id, None)
                        continue

                    # if we saw fills, disappearance is expected (FILLED orders vanish from open_orders)
                    if last_status in {"FILLED", "PARTIALLY_FILLED"}:
                        open_order_missing_since_mono.pop(canceled_order_id, None)
                        continue
                    if order_filled_qty.get(canceled_order_id, Decimal("0")) > 0:
                        open_order_missing_since_mono.pop(canceled_order_id, None)
                        continue

                    if open_order_diff_grace_sec > 0:
                        miss_since = open_order_missing_since_mono.get(canceled_order_id)
                        if miss_since is None:
                            open_order_missing_since_mono[canceled_order_id] = now_mono
                            continue
                        if (now_mono - miss_since) < open_order_diff_grace_sec:
                            continue
                        open_order_missing_since_mono.pop(canceled_order_id, None)

                    if open_order_diff_grace_sec <= 0:
                        open_order_missing_since_mono.pop(canceled_order_id, None)

                    if not last_status:
                        # Ignore unknown-status disappearances; wait for explicit updates.
                        continue

                    if last_status == "NEW":
                        # Don't force-cancel right after NEW; wait for explicit status updates.
                        continue

                    is_local_cancel = _is_local_order_id(canceled_order_id)

                    # LOCAL-* — это локальные placeholder id (pending submit / dedupe),
                    # их исчезновение НЕ должно вызывать on_cancel() у стратегии.
                    if is_local_cancel:
                        emit_local_cancel_log = True
                        if not log_local_cancel_events:
                            emit_local_cancel_log = False
                            suppressed_local_cancel_logs += 1
                        if tick_is_startup_sync and not log_startup_cancel_events:
                            emit_local_cancel_log = False
                            suppressed_startup_cancel_logs += 1

                        if emit_local_cancel_log:
                            logger.info(
                                "CANCEL order_id={} source=open_orders_diff local=True startup_sync={} ignored_for_strategy",
                                canceled_order_id,
                                tick_is_startup_sync,
                            )
                            jsonl.trade(
                                kind="cancel",
                                ts=datetime.now(timezone.utc),
                                symbol=symbol,
                                tick=tick_count,
                                order_id=canceled_order_id,
                                source="open_orders_diff",
                                local_placeholder=True,
                                startup_sync=tick_is_startup_sync,
                                ignored_for_strategy=True,
                            )
                        continue

                    # Startup sync can contain historical disappear events.
                    # Do not propagate them to strategy/on-cancel counters by default.
                    if tick_is_startup_sync:
                        if log_startup_cancel_events:
                            logger.info(
                                "CANCEL order_id={} source=open_orders_diff startup_sync=True ignored_for_strategy",
                                canceled_order_id,
                            )
                            jsonl.trade(
                                kind="cancel",
                                ts=datetime.now(timezone.utc),
                                symbol=symbol,
                                tick=tick_count,
                                order_id=canceled_order_id,
                                source="open_orders_diff",
                                startup_sync=True,
                                local_placeholder=False,
                                ignored_for_strategy=True,
                            )
                        else:
                            suppressed_startup_cancel_logs += 1
                        continue

                    if not open_order_diff_strategy_cancel:
                        logger.debug(
                            "CANCEL order_id={} source=open_orders_diff ignored_for_strategy reason=diff_strategy_cancel_disabled",
                            canceled_order_id,
                        )
                        jsonl.trade(
                            kind="cancel",
                            ts=datetime.now(timezone.utc),
                            symbol=symbol,
                            tick=tick_count,
                            order_id=canceled_order_id,
                            source="open_orders_diff",
                            local_placeholder=False,
                            ignored_for_strategy=True,
                        )
                        continue

                    canceled_total += 1
                    logger.info("CANCEL order_id={} source=open_orders_diff", canceled_order_id)
                    _call_strategy_hook(strategy, "on_cancel", broker=execution, order_id=canceled_order_id)
                    order_last_status[canceled_order_id] = "CANCELED"
                    jsonl.trade(
                        kind="cancel",
                        ts=datetime.now(timezone.utc),
                        symbol=symbol,
                        tick=tick_count,
                        order_id=canceled_order_id,
                        source="open_orders_diff",
                        local_placeholder=False,
                    )
            known_open_order_ids = current_open_order_ids

            # Heartbeat block (time-based)
            if next_heartbeat_at is not None and datetime.now(timezone.utc) >= next_heartbeat_at:
                q_total = _get_quote_balance_total(execution)
                q_free = getattr(execution, "quote_balance", None)

                b_total = _get_base_balance_total(execution)
                b_free = getattr(execution, "base_balance", None)
                oo = getattr(execution, "open_orders", None)

                mid = (bid + ask) / Decimal("2")
                spread = ask - bid

                base_usd_total = None
                base_usd_free = None
                equity_total = None

                try:
                    if b_total is not None:
                        base_usd_total = Decimal(str(b_total)) * mid
                except Exception:
                    base_usd_total = None

                try:
                    if b_free is not None:
                        base_usd_free = Decimal(str(b_free)) * mid
                except Exception:
                    base_usd_free = None

                try:
                    if q_total is not None:
                        equity_total = Decimal(str(q_total)) + (base_usd_total or Decimal("0"))
                except Exception:
                    equity_total = None
                hb_now = datetime.now(timezone.utc)
                hb_dt = max((hb_now - last_hb_ts).total_seconds(), 1e-9)
                ticks_delta = tick_count - last_hb_tick_count
                ticks_ps = ticks_delta / hb_dt

                q_calls_total = int(runtime_stats.get("strategy_on_quote_calls", 0))
                f_total = int(runtime_stats.get("fills_total", 0))
                bars_total = int(runtime_stats.get("bars_total", 0))
                q_prev = int(runtime_stats.get("hb_prev_strategy_on_quote_calls", 0))
                f_prev = int(runtime_stats.get("hb_prev_fills_total", 0))
                b_prev = int(runtime_stats.get("hb_prev_bars_total", 0))
                q_delta = q_calls_total - q_prev
                f_delta = f_total - f_prev
                bars_delta = bars_total - b_prev

                runtime_stats["hb_prev_strategy_on_quote_calls"] = q_calls_total
                runtime_stats["hb_prev_fills_total"] = f_total
                runtime_stats["hb_prev_bars_total"] = bars_total

                ws_last_msg_ts = runtime_stats.get("ws_last_msg_ts")
                ws_last_msg_age_sec = None
                if isinstance(ws_last_msg_ts, datetime):
                    ws_last_msg_age_sec = max(0.0, (hb_now - ws_last_msg_ts).total_seconds())

                ws_msgs_total = int(runtime_stats.get("ws_messages", 0) or 0)
                ws_prev = int(runtime_stats.get("hb_prev_ws_messages", 0) or 0)
                ws_msgs_delta = ws_msgs_total - ws_prev
                runtime_stats["hb_prev_ws_messages"] = ws_msgs_total
                ws_msgs_rate = float(ws_msgs_delta) / hb_dt if hb_dt > 0 else 0.0

                q_rate = float(q_delta) / hb_dt if hb_dt > 0 else 0.0
                f_rate = float(f_delta) / hb_dt if hb_dt > 0 else 0.0
                bars_rate = float(bars_delta) / hb_dt if hb_dt > 0 else 0.0

                hb_dt = max((now_utc - last_hb_ts).total_seconds(), 1e-9)
                skipped_same_quote_delta = skipped_same_quote - last_hb_skipped_same_quote
                skipped_same_quote_rate = float(skipped_same_quote_delta) / hb_dt if hb_dt > 0 else 0.0
                skipped_same_quote_rate_disp = _fmt_num(skipped_same_quote_rate, 2)

                q_rate_disp = _fmt_num(q_rate, 2)
                f_rate_disp = _fmt_num(f_rate, 2)
                bars_rate_disp = _fmt_num(bars_rate, 2)
                ws_msgs_rate_disp = _fmt_num(ws_msgs_rate, 2)
                ws_last_age_disp = "n/a" if ws_last_msg_age_sec is None else f"{_fmt_num(ws_last_msg_age_sec, 2)}s"

                runtime_health_suffix = (
                    f" | rt=qps={q_rate_disp}({q_delta}) fills_ps={f_rate_disp}({f_delta}) bars_ps={bars_rate_disp}({bars_delta})"
                    f" ws_msgs_ps={ws_msgs_rate_disp}({ws_msgs_delta}) ws_msgs_total={ws_msgs_total}"
                    f" ws_last_age={ws_last_age_disp}"
                    f" ws_reconnects={int(runtime_stats.get('ws_reconnects', 0))}"
                    f" ws_timeouts={int(runtime_stats.get('ws_timeouts', 0))}"
                    f" ws_errors={int(runtime_stats.get('ws_errors', 0))}"
                )

                adapter_fields = _safe_strategy_fields(strategy, execution=execution)
                adapter_suffix = _compact_adapter_suffix(adapter_fields, hb_dt=hb_dt, runtime_stats=runtime_stats)

                logger.info(
                "HEARTBEAT run#{} total#{} {} bid={} ask={} spread={} | USD(total)={} USD(free)={} | coin_qty(total)={} coin_qty(free)={} | base_USD(total)≈{} base_USD(free)≈{} | equity_total≈{} | open_orders={} fills_since_hb={} bars_since_hb={} new_orders_total={} cancels_total={} partial_fills_total={}{} skipped_same_quote={}(+{}, {}/s){}",                    tick_count,
                    tick_count,
                    symbol,
                    _fmt_num(bid),
                    _fmt_num(ask),
                    _fmt_num(spread),
                    _fmt_num(q_total) if q_total is not None else "?",
                    _fmt_num(q_free) if q_free is not None else "?",
                    _fmt_qty(b_total) if b_total is not None else "?",
                    _fmt_qty(b_free) if b_free is not None else "?",
                    _fmt_num(base_usd_total) if base_usd_total is not None else "?",
                    _fmt_num(base_usd_free) if base_usd_free is not None else "?",
                    _fmt_num(equity_total) if equity_total is not None else "?",
                    len(oo) if isinstance(oo, list) else oo,
                    fills_since_heartbeat,
                    bars_since_heartbeat,
                    new_orders_total,
                    canceled_total,
                    partial_fills_total,
                    adapter_suffix,
                    skipped_same_quote,
                    skipped_same_quote_delta,
                    skipped_same_quote_rate_disp,
                    runtime_health_suffix,
                )
                jsonl.metric(
                        kind="heartbeat",
                        ts=hb_now,
                        symbol=symbol,
                        tick=tick_count,
                        bid=_fmt_num(bid),
                        ask=_fmt_num(ask),
                        spread=_fmt_num(spread),
                        quote_balance=(_fmt_num(q_total) if q_total is not None else None),
                        quote_balance_free=(_fmt_num(q_free) if q_free is not None else None),
                        base_balance=(_fmt_qty(b_total) if b_total is not None else None),
                        base_balance_free=(_fmt_qty(b_free) if b_free is not None else None),
                        base_notional=(_fmt_num(base_usd_total) if isinstance(base_usd_total, Decimal) else None),
                        base_notional_free=(_fmt_num(base_usd_free) if isinstance(base_usd_free, Decimal) else None),
                        open_orders=(len(oo) if isinstance(oo, list) else oo),
                        equity=(_fmt_num(equity_total) if isinstance(equity_total, Decimal) else None),
                        ticks_delta=ticks_delta,
                        ticks_ps=ticks_ps,
                        fills_hb=fills_since_heartbeat,
                        bars_hb=bars_since_heartbeat,
                        cancels_total=canceled_total,
                        partial_fills_total=partial_fills_total,
                        strategy_on_quote_calls=q_calls_total,
                        strategy_on_quote_calls_delta=q_delta,
                        fills_total=f_total,
                        fills_total_delta=f_delta,
                        bars_total=bars_total,
                        bars_total_delta=bars_delta,
                        skipped_same_quote=skipped_same_quote,
                        skipped_same_quote_delta=skipped_same_quote_delta,
                        ws_messages_total=ws_msgs_total,
                        ws_messages_delta=ws_msgs_delta,
                        adapter=_compact_adapter_preview(adapter_fields),
                )

                fills_since_heartbeat = 0
                bars_since_heartbeat = 0
                last_hb_tick_count = tick_count
                last_hb_ts = hb_now
                last_hb_skipped_same_quote = skipped_same_quote
                next_heartbeat_at = hb_now + timedelta(seconds=heartbeat_sec)

            last_seen_bid = bid
            last_seen_ask = ask

            if max_ticks > 0 and tick_count >= max_ticks:
                elapsed_s = (datetime.now(timezone.utc) - t0).total_seconds()
                mid = (bid + ask) / Decimal("2")
                q = _get_quote_balance_total(execution)
                b = _get_base_balance_total(execution)
                oo = getattr(execution, "open_orders", None)
                stop_adapter_fields = _safe_strategy_fields(strategy, execution=execution)
                stop_adapter_suffix = _compact_adapter_suffix(stop_adapter_fields)
                logger.info(
                    "STOP summary | ticks={} elapsed_s={} | bid[min..max]={}..{} ask[min..max]={}..{} | quote_balance={} base_balance={} equity≈{} | open_orders={} | fills_total={} bars_total={} cancels_total={} partial_fills_total={} fallback_bars={}{}",
                    tick_count,
                    f"{elapsed_s:.2f}",
                    (_fmt_num(min_bid) if min_bid is not None else "NA"),
                    (_fmt_num(max_bid) if max_bid is not None else "NA"),
                    (_fmt_num(min_ask) if min_ask is not None else "NA"),
                    (_fmt_num(max_ask) if max_ask is not None else "NA"),
                    (_fmt_num(q) if q is not None else "?"),
                    (_fmt_num(b) if b is not None else "?"),
                    (_fmt_num((q + (b * mid))) if isinstance(q, Decimal) and isinstance(b, Decimal) else "?"),
                    len(oo) if isinstance(oo, list) else oo,
                    int(runtime_stats.get("fills_total", 0)),
                    int(runtime_stats.get("bars_total", 0)),
                    canceled_total,
                    partial_fills_total,
                    int(runtime_stats.get("bar_builder_fallback_uses", 0)),
                    stop_adapter_suffix,
                )
                jsonl.metric(
                    kind="stop_summary",
                    ts=datetime.now(timezone.utc),
                    symbol=symbol,
                    tick=tick_count,
                    elapsed_s=elapsed_s,
                    bid_min=min_bid,
                    bid_max=max_bid,
                    ask_min=min_ask,
                    ask_max=max_ask,
                    quote_balance=q,
                    base_balance=b,
                    equity=((q + (b * mid)) if isinstance(q, Decimal) and isinstance(b, Decimal) else None),
                    open_orders=(len(oo) if isinstance(oo, list) else oo),
                    fills_total=int(runtime_stats.get("fills_total", 0)),
                    bars_total=int(runtime_stats.get("bars_total", 0)),
                    cancels_total=canceled_total,
                    partial_fills_total=partial_fills_total,
                    fallback_bars=int(runtime_stats.get("bar_builder_fallback_uses", 0)),
                    adapter=_compact_adapter_preview(stop_adapter_fields),
                    new_orders_total=new_orders_total,
                )
                logger.info("STOP by max_ticks={}", max_ticks)
                break
    finally:
        # Best-effort shutdown to avoid aiohttp/websocket leaks on normal stop or Ctrl+C.
        logger.info(
            "Demo grid runner shutting down | ticks={} fills_total={} bars_total={} cancels_total={} partial_fills_total={}",
            tick_count if 'tick_count' in locals() else 0,
            int(runtime_stats.get("fills_total", 0)) if 'runtime_stats' in locals() else 0,
            int(runtime_stats.get("bars_total", 0)) if 'runtime_stats' in locals() else 0,
            canceled_total if 'canceled_total' in locals() else 0,
            partial_fills_total if 'partial_fills_total' in locals() else 0,
        )

        try:
            state_stop_evt.set()
            if state_task is not None:
                with contextlib.suppress(Exception):
                    await state_task
        except Exception:
            pass

        try:
            final_tick = tick_count if "tick_count" in locals() else 0
            final_state = _capture_runtime_state(
                symbol=symbol,
                strategy=strategy,
                execution=execution,
                runtime_stats=runtime_stats,
                tick_count=final_tick,
            )
            state_path.parent.mkdir(parents=True, exist_ok=True)
            state_path.write_text(
                json.dumps(final_state, ensure_ascii=False, default=_json_default, indent=2),
                encoding="utf-8",
            )
            logger.debug("STATE: final flush completed")

            if s3_uploader is not None:
                await s3_uploader.upload_state(
                    state_path=state_path,
                    reason="final",
                    tick=final_tick,
                )
                await s3_uploader.upload_logs(
                    quotes_path=jsonl.quotes_path,
                    trades_path=jsonl.trades_path,
                    metrics_path=jsonl.metrics_path,
                    reason="final",
                )
        except Exception as e:
            logger.warning("STATE: final flush failed: {}", e)

        try:
            jsonl.metric(
                kind="shutdown",
                ts=datetime.now(timezone.utc),
                symbol=symbol,
                tick=tick_count if 'tick_count' in locals() else 0,
                fills_total=int(runtime_stats.get("fills_total", 0)) if 'runtime_stats' in locals() else 0,
                bars_total=int(runtime_stats.get("bars_total", 0)) if 'runtime_stats' in locals() else 0,
                cancels_total=(canceled_total if 'canceled_total' in locals() else 0),
                partial_fills_total=(partial_fills_total if 'partial_fills_total' in locals() else 0),
                state_file=(str(state_path) if 'state_path' in locals() else None),
            )
        except Exception:
            pass
        close_fn = getattr(execution, "close", None)
        if callable(close_fn):
            try:
                await _maybe_await(close_fn())
            except Exception as e:
                logger.warning("Execution close() failed: {}", e)

        try:
            await stream.aclose()
        except RuntimeError as e:
            logger.debug("stream.aclose() skipped: {}", e)
        except Exception as e:
            logger.warning("stream.aclose() failed: {}", e)


class NoopStrategy:
    def on_quote(self, **kwargs: Any) -> None:
        return None

    def on_fill(self, **kwargs: Any) -> None:
        fill = kwargs.get("fill")
        logger.info("FILL -> strategy stub received: {}", fill)

    def on_bar_close(self, **kwargs: Any) -> None:
        bar = kwargs.get("bar")
        logger.info("BAR -> strategy stub received close={} ts={}", bar.get("close"), bar.get("ts"))


def build_strategy(symbol: str, interval: str = "15m") -> Any:
    symbol = (symbol or "ETHUSDT").strip().upper() or "ETHUSDT"
    interval = _normalize_interval(interval)

    candidates: list[tuple[str, str]] = [
        ("src.paper.execution_bridge", "BacktestAdapter"),
        ("src.paper.execution_bridge", "ExecutionBridge"),
        ("src.paper.execution_bridge", "PaperExecutionBridge"),
        ("src.strategy.grid_backtest_adapter", "GridBacktestAdapter"),
    ]

    ctor_variants: tuple[dict[str, Any], ...] = (
        {"symbol": symbol, "interval": interval},
        {"symbol": symbol},
        {"execution": None, "symbol": symbol, "interval": interval},
        {"execution": None},
        {"executor": None, "symbol": symbol, "interval": interval},
        {"executor": None},
        {},
    )

    errors: list[str] = []

    for mod_name, cls_name in candidates:
        try:
            mod = __import__(mod_name, fromlist=[cls_name])
        except Exception as e:
            errors.append(f"{mod_name}: {e}")
            continue

        cls = getattr(mod, cls_name, None)
        if cls is None:
            errors.append(f"{mod_name}.{cls_name}: not found")
            continue

        for kwargs in ctor_variants:
            try:
                strategy = cls(**kwargs)
                logger.info("Using strategy adapter: {}.{}", mod_name, cls_name)
                return strategy
            except TypeError:
                continue
            except Exception as e:
                errors.append(f"{mod_name}.{cls_name} init failed: {e}")
                break

        errors.append(f"{mod_name}.{cls_name}: no supported constructor signature")

    logger.warning(
        "Backtest adapter not wired. Falling back to NoopStrategy. Tried: {}",
        " | ".join(errors),
    )
    return NoopStrategy()


def _configure_terminal_logger(level: str) -> None:
    """Configure colored, readable terminal logs similar to run_paper output."""
    logger.remove()
    logger.add(
        sys.stderr,
        level=level.upper(),
        colorize=True,
        enqueue=False,
        backtrace=False,
        diagnose=False,
        format=(
            "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
            "<level>{level:<8}</level> | "
            "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
            "<level>{message}</level>"
        ),
    )

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run live demo grid loop on Binance testnet bookTicker")
    p.add_argument("--symbol", default="ETHUSDT")
    p.add_argument(
        "--interval",
        default=os.getenv("RUN_DEMO_GRID_INTERVAL", "15m"),
        help="Synthetic candle interval (e.g. 1m, 5m, 15m, 1h, 4h, 1d, 1w)",
    )
    p.add_argument("--max-ticks", type=int, default=0, help="0 = infinite")
    p.add_argument("--log-level", default="INFO")
    return p.parse_args()


async def amain() -> int:
    args = parse_args()
    _configure_terminal_logger(args.log_level)

    symbol = (args.symbol or "ETHUSDT").strip().upper()
    try:
        interval = _normalize_interval(args.interval)
    except ValueError as e:
        logger.error("Invalid --interval value: {}", e)
        return 2

    strategy = build_strategy(symbol=symbol, interval=interval)
    try:
        await run_loop(symbol=symbol, strategy=strategy, interval=interval, max_ticks=args.max_ticks)
        return 0
    finally:
        close_fn = getattr(strategy, "close", None)
        if callable(close_fn):
            try:
                await _maybe_await(close_fn())
            except Exception as e:
                logger.warning("Strategy close() failed: {}", e)


def main() -> int:
    try:
        return asyncio.run(amain())
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
