from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import re
from typing import Any, Callable, Optional


UTC = timezone.utc
_INTERVAL_RE = re.compile(r"^(\d+)([smhdwM])$")


@dataclass(frozen=True)
class BookTickerTick:
    """Normalized top-of-book tick used by paper mode.

    We keep only the fields the adapter needs so it can be wired into any WS client.
    """

    symbol: str
    ts: datetime
    bid: Decimal
    ask: Decimal

    @property
    def mid(self) -> Decimal:
        return (self.bid + self.ask) / Decimal("2")


@dataclass
class Candle15m:
    """Synthetic candle built from bookTicker mid-price."""

    symbol: str
    open_time: datetime
    close_time: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    ticks: int = 0

    def as_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "open_time": self.open_time,
            "close_time": self.close_time,
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "ticks": self.ticks,
        }


@dataclass
class AdapterSnapshot:
    """Lightweight state snapshot for logging/debugging in run_paper."""

    current_bucket_open: Optional[datetime]
    current_mid: Optional[Decimal]
    current_ticks: int
    closed_candles: int


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


def normalize_interval(interval: str) -> str:
    qty, unit = _parse_interval(interval)
    return f"{qty}{unit}"


def floor_to_interval(ts: datetime, interval: str) -> datetime:
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=UTC)
    ts = ts.astimezone(UTC)
    qty, unit = _parse_interval(interval)

    if unit == "M":
        bucket_month = ((ts.month - 1) // qty) * qty + 1
        return ts.replace(month=bucket_month, day=1, hour=0, minute=0, second=0, microsecond=0)
    if unit == "w":
        anchor = datetime(1970, 1, 5, tzinfo=UTC)  # Monday 00:00 UTC
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
    return datetime.fromtimestamp(bucket_epoch, tz=UTC)


def _advance_bucket_open(bucket_open: datetime, interval: str) -> datetime:
    qty, unit = _parse_interval(interval)
    if unit == "M":
        total_month = (bucket_open.year * 12 + (bucket_open.month - 1)) + qty
        year = total_month // 12
        month = (total_month % 12) + 1
        return bucket_open.replace(year=year, month=month, day=1, hour=0, minute=0, second=0, microsecond=0)
    if unit == "w":
        return bucket_open + timedelta(weeks=qty)

    unit_seconds = {
        "s": 1,
        "m": 60,
        "h": 3600,
        "d": 86400,
    }.get(unit)
    if unit_seconds is None:
        raise ValueError(f"Unsupported interval unit in {interval!r}")
    return bucket_open + timedelta(seconds=(qty * unit_seconds))


def floor_to_15m(ts: datetime) -> datetime:
    # Backward-compatible helper for callers that still import this name.
    return floor_to_interval(ts, "15m")


class GridPaperAdapter:
    """Bridge between high-frequency bookTicker stream and strategy bar cadence.

    Responsibilities:
    - consume bid/ask ticks from `run_paper`
    - build synthetic OHLC candles from mid-price for selected interval
    - emit callback when a candle closes

    This module intentionally does not place orders or depend on broker/runtime code.
    `run_paper` can subscribe to `on_candle_close` and then call the strategy logic.
    """

    def __init__(
        self,
        symbol: str,
        interval: str = "15m",
        on_candle_close: Optional[Callable[[Candle15m], None]] = None,
    ) -> None:
        self.symbol = symbol.upper()
        self.interval = normalize_interval(interval)
        self.on_candle_close = on_candle_close

        self._cur_bucket_open: Optional[datetime] = None
        self._cur: Optional[Candle15m] = None
        self._closed_count = 0

    def snapshot(self) -> AdapterSnapshot:
        return AdapterSnapshot(
            current_bucket_open=self._cur_bucket_open,
            current_mid=(self._cur.close if self._cur else None),
            current_ticks=(self._cur.ticks if self._cur else 0),
            closed_candles=self._closed_count,
        )

    def export_state(self) -> dict[str, Any]:
        """Serializable adapter state for run_paper resume."""
        cur = self._cur
        return {
            "symbol": self.symbol,
            "interval": self.interval,
            "current_bucket_open": self._cur_bucket_open.isoformat() if self._cur_bucket_open else None,
            "current_candle": (
                {
                    "symbol": cur.symbol,
                    "open_time": cur.open_time.isoformat(),
                    "close_time": cur.close_time.isoformat(),
                    "open": str(cur.open),
                    "high": str(cur.high),
                    "low": str(cur.low),
                    "close": str(cur.close),
                    "ticks": int(cur.ticks),
                }
                if cur is not None
                else None
            ),
            "closed_count": int(self._closed_count),
        }

    def restore_state(self, state: dict[str, Any]) -> None:
        """Restore adapter state from `export_state()` output (best-effort)."""
        if not isinstance(state, dict):
            return

        def _parse_dt(v: Any) -> Optional[datetime]:
            if v is None:
                return None
            try:
                if isinstance(v, datetime):
                    return v if v.tzinfo is not None else v.replace(tzinfo=UTC)
                s = str(v).strip()
                dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
                return dt if dt.tzinfo is not None else dt.replace(tzinfo=UTC)
            except Exception:
                return None

        def _d(v: Any) -> Decimal:
            return Decimal(str(v))

        state_interval = state.get("interval")
        if state_interval is not None:
            try:
                self.interval = normalize_interval(str(state_interval))
            except Exception:
                pass

        self._cur_bucket_open = _parse_dt(state.get("current_bucket_open"))
        self._closed_count = int(state.get("closed_count", self._closed_count))

        cur = state.get("current_candle")
        if isinstance(cur, dict):
            try:
                self._cur = Candle15m(
                    symbol=str(cur.get("symbol", self.symbol)).upper(),
                    open_time=_parse_dt(cur.get("open_time")) or self._cur_bucket_open or datetime.now(tz=UTC),
                    close_time=_parse_dt(cur.get("close_time")) or _advance_bucket_open(
                        self._cur_bucket_open or datetime.now(tz=UTC), self.interval
                    ),
                    open=_d(cur.get("open")),
                    high=_d(cur.get("high")),
                    low=_d(cur.get("low")),
                    close=_d(cur.get("close")),
                    ticks=int(cur.get("ticks", 0)),
                )
            except Exception:
                self._cur = None
        else:
            self._cur = None

        # If bucket timestamp was missing but candle exists, derive it from candle.open_time.
        if self._cur_bucket_open is None and self._cur is not None:
            self._cur_bucket_open = self._cur.open_time

    def on_quote(self, *, ts: datetime, bid: Decimal, ask: Decimal) -> Optional[Candle15m]:
        """Consume one quote tick.

        Returns a closed candle when a bucket rollover happens, otherwise None.
        If the stream skips multiple buckets, the adapter simply starts the next candle
        from the first tick seen (it does not synthesize empty candles).
        """
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=UTC)
        ts = ts.astimezone(UTC)

        mid = (bid + ask) / Decimal("2")
        bucket_open = floor_to_interval(ts, self.interval)
        bucket_close = _advance_bucket_open(bucket_open, self.interval)

        # First tick: initialize current candle
        if self._cur is None:
            self._cur_bucket_open = bucket_open
            self._cur = Candle15m(
                symbol=self.symbol,
                open_time=bucket_open,
                close_time=bucket_close,
                open=mid,
                high=mid,
                low=mid,
                close=mid,
                ticks=1,
            )
            return None

        assert self._cur_bucket_open is not None

        # Same bucket -> update OHLC
        if bucket_open == self._cur_bucket_open:
            if mid > self._cur.high:
                self._cur.high = mid
            if mid < self._cur.low:
                self._cur.low = mid
            self._cur.close = mid
            self._cur.ticks += 1
            return None

        # Bucket rollover -> close previous candle, start new one
        closed = self._cur
        self._closed_count += 1

        self._cur_bucket_open = bucket_open
        self._cur = Candle15m(
            symbol=self.symbol,
            open_time=bucket_open,
            close_time=bucket_close,
            open=mid,
            high=mid,
            low=mid,
            close=mid,
            ticks=1,
        )

        if self.on_candle_close is not None:
            self.on_candle_close(closed)

        return closed

    def flush_current(self) -> Optional[Candle15m]:
        """Return current unfinished candle (without advancing state).

        Useful for shutdown diagnostics only. The candle is *not* considered closed.
        """
        return self._cur
