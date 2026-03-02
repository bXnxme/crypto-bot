from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Callable, Optional


UTC = timezone.utc


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
    """Synthetic 15m candle built from bookTicker mid-price."""

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


@dataclass(frozen=True)
class YearBoundaryEvent:
    """Signal that a calendar year has rolled over on 15m candle close.

    Emitted when the adapter closes the last 15m candle of the previous year and
    sees the first tick of the new year bucket.
    """

    symbol: str
    closed_candle: Candle15m
    next_bucket_open: datetime
    year_closed: int
    year_opened: int

    @property
    def ts(self) -> datetime:
        """Compatibility alias for runtime callbacks: timestamp of the first bucket in the new year."""
        return self.next_bucket_open

    @property
    def year(self) -> int:
        """Compatibility alias for runtime callbacks: the newly opened calendar year."""
        return self.year_opened


def floor_to_15m(ts: datetime) -> datetime:
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=UTC)
    ts = ts.astimezone(UTC)
    minute = (ts.minute // 15) * 15
    return ts.replace(minute=minute, second=0, microsecond=0)


class GridPaperAdapter:
    """Bridge between high-frequency bookTicker stream and 15m strategy cadence.

    Responsibilities:
    - consume bid/ask ticks from `run_paper`
    - build synthetic 15m OHLC candles from mid-price
    - emit callback when a 15m candle closes

    This module intentionally does not place orders or depend on broker/runtime code.
    `run_paper` can subscribe to `on_candle_close` and then call the strategy logic.
    """

    def __init__(
        self,
        symbol: str,
        on_candle_close: Optional[Callable[[Candle15m], None]] = None,
        on_year_boundary: Optional[Callable[[YearBoundaryEvent], None]] = None,
    ) -> None:
        self.symbol = symbol.upper()
        self.on_candle_close = on_candle_close
        self.on_year_boundary = on_year_boundary

        self._cur_bucket_open: Optional[datetime] = None
        self._cur: Optional[Candle15m] = None
        self._closed_count = 0
        self._pending_year_boundary_event: Optional[YearBoundaryEvent] = None

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

        self._cur_bucket_open = _parse_dt(state.get("current_bucket_open"))
        self._closed_count = int(state.get("closed_count", self._closed_count))

        cur = state.get("current_candle")
        if isinstance(cur, dict):
            try:
                self._cur = Candle15m(
                    symbol=str(cur.get("symbol", self.symbol)).upper(),
                    open_time=_parse_dt(cur.get("open_time")) or self._cur_bucket_open or datetime.now(tz=UTC),
                    close_time=_parse_dt(cur.get("close_time")) or ((self._cur_bucket_open or datetime.now(tz=UTC)) + timedelta(minutes=15)),
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

        Returns a closed 15m candle when a bucket rollover happens, otherwise None.
        If the stream skips multiple 15m buckets, the adapter simply starts the next candle
        from the first tick seen (it does not synthesize empty candles).
        """
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=UTC)
        ts = ts.astimezone(UTC)

        mid = (bid + ask) / Decimal("2")
        bucket_open = floor_to_15m(ts)
        bucket_close = bucket_open + timedelta(minutes=15)

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

        # Same 15m bucket -> update OHLC
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

        # Detect calendar year rollover on candle boundary:
        # closed candle is the last candle of previous year, current bucket is first bucket of new year.
        # Example: closed.close_time == 2027-01-01T00:00:00Z while closed.open_time is in 2026.
        if closed.close_time.year != bucket_open.year:
            ev = YearBoundaryEvent(
                symbol=self.symbol,
                closed_candle=closed,
                next_bucket_open=bucket_open,
                year_closed=closed.open_time.year,
                year_opened=bucket_open.year,
            )
            self._pending_year_boundary_event = ev
            if self.on_year_boundary is not None:
                self.on_year_boundary(ev)

        return closed

    def pop_year_boundary_event(self) -> Optional[YearBoundaryEvent]:
        """Return and clear the latest detected year-boundary event (if any).

        This is useful when `run_paper` prefers polling instead of callbacks.
        """
        ev = self._pending_year_boundary_event
        self._pending_year_boundary_event = None
        return ev

    def flush_current(self) -> Optional[Candle15m]:
        """Return current unfinished candle (without advancing state).

        Useful for shutdown diagnostics only. The candle is *not* considered closed.
        """
        return self._cur
