from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Iterable

from binance.client import Client
import time
from datetime import timezone

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)


@dataclass(frozen=True)
class Candle:
    open_time_ms: int
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    close_time_ms: int


def _d(x) -> Decimal:
    return Decimal(str(x))

_INTERVAL_MS: dict[str, int] = {
    "1m": 60_000,
    "3m": 180_000,
    "5m": 300_000,
    "15m": 900_000,
    "30m": 1_800_000,
    "1h": 3_600_000,
    "2h": 7_200_000,
    "4h": 14_400_000,
    "6h": 21_600_000,
    "8h": 28_800_000,
    "12h": 43_200_000,
    "1d": 86_400_000,
}

def _parse_ymd_to_ms(s: str) -> int:
    # "YYYY-MM-DD" -> ms UTC at 00:00:00
    dt = datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)

def _end_ymd_to_ms_exclusive(s: str) -> int:
    # end is exclusive: end_date + 1 day at 00:00
    dt = datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    dt = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    dt_plus = dt.timestamp() + 86400  # +1 day
    return int(dt_plus * 1000)

def fetch_klines_to_csv(
    client: Client,
    symbol: str,
    interval: str,
    start_str: str,
    end_str: str | None = None,
) -> Path:
    """
    interval: '1m', '5m', '15m', '1h', '4h', '1d', ...
    start_str/end_str: 'YYYY-MM-DD'
    IMPORTANT: end_str treated as inclusive date; internally uses end exclusive (end+1day 00:00 UTC)
    """
    if interval not in _INTERVAL_MS:
        raise ValueError(f"Unsupported interval for pagination mapping: {interval}")

    out = DATA_DIR / f"{symbol}_{interval}_{start_str}_{end_str or 'NOW'}.csv"

    start_ms = _parse_ymd_to_ms(start_str)
    end_ms = _end_ymd_to_ms_exclusive(end_str) if end_str else None
    step_ms = _INTERVAL_MS[interval]

    all_klines: list[list] = []
    cur = start_ms
    limit = 1000

    # paginate
    while True:
        params = {
            "symbol": symbol,
            "interval": interval,
            "startTime": cur,
            "limit": limit,
        }
        if end_ms is not None:
            params["endTime"] = end_ms

        chunk = client.get_klines(**params)
        if not chunk:
            break

        all_klines.extend(chunk)

        last_open = int(chunk[-1][0])
        next_cur = last_open + step_ms

        # safety against infinite loop
        if next_cur <= cur:
            break
        cur = next_cur

        if end_ms is not None and cur >= end_ms:
            break

        # be nice to API
        time.sleep(0.05)

        # stop if returned less than limit (usually means end reached)
        if len(chunk) < limit:
            break

    # write
    with out.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["open_time_ms", "open", "high", "low", "close", "volume", "close_time_ms"])
        for k in all_klines:
            w.writerow([k[0], k[1], k[2], k[3], k[4], k[5], k[6]])

    return out


def read_candles_csv(path: Path) -> Iterable[Candle]:
    import csv
    with path.open("r", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            yield Candle(
                open_time_ms=int(row["open_time_ms"]),
                open=_d(row["open"]),
                high=_d(row["high"]),
                low=_d(row["low"]),
                close=_d(row["close"]),
                volume=_d(row["volume"]),
                close_time_ms=int(row["close_time_ms"]),
            )