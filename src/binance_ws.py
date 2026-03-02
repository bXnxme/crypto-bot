from __future__ import annotations

import asyncio
import json
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from typing import AsyncIterator, Callable

import websockets
from loguru import logger


@dataclass(frozen=True)
class BookTicker:
    symbol: str
    bid: Decimal
    ask: Decimal
    bid_qty: Decimal
    ask_qty: Decimal
    ts: datetime


@dataclass
class WSRuntimeStats:
    symbol: str
    reconnects: int = 0
    errors: int = 0
    timeouts: int = 0
    messages_total: int = 0
    last_message_ts: datetime | None = None
    connected_since_ts: datetime | None = None
    disconnected_since_ts: datetime | None = None
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock, repr=False)

    async def mark_connected(self) -> None:
        async with self._lock:
            now = datetime.now(timezone.utc)
            self.connected_since_ts = now
            self.disconnected_since_ts = None

    async def mark_disconnected(self) -> None:
        async with self._lock:
            self.disconnected_since_ts = datetime.now(timezone.utc)
            self.connected_since_ts = None

    async def mark_message(self) -> None:
        async with self._lock:
            self.messages_total += 1
            self.last_message_ts = datetime.now(timezone.utc)

    async def mark_error(self) -> None:
        async with self._lock:
            self.errors += 1

    async def mark_timeout(self) -> None:
        async with self._lock:
            self.timeouts += 1

    async def mark_reconnect(self) -> None:
        async with self._lock:
            self.reconnects += 1



def _normalize_ws_base_url(raw: str) -> str:
    v = (raw or "").strip()
    if not v:
        return ""

    # Accept REST-style values from .env and convert to WS host/base.
    # Examples:
    #   https://testnet.binance.vision -> wss://stream.testnet.binance.vision
    #   https://demo-api.binance.com/api -> wss://demo-stream.binance.com
    v = v.rstrip("/")

    if v.startswith("https://"):
        v = "wss://" + v[len("https://"):]
    elif v.startswith("http://"):
        v = "ws://" + v[len("http://"):]

    # Map common REST hosts to WS hosts
    v = v.replace("demo-api.binance.com/api", "demo-stream.binance.com")
    v = v.replace("demo-api.binance.com", "demo-stream.binance.com")
    v = v.replace("testnet.binance.vision", "stream.testnet.binance.vision")

    # If user passed a full path like /api, strip it for WS base
    for suffix in ("/api", "/api/"):
        if v.endswith(suffix):
            v = v[: -len(suffix)]

    return v.rstrip("/")


def _resolve_book_ticker_ws_url(symbol: str) -> str:
    sym = symbol.lower()

    # Optional explicit override (highest priority)
    explicit_stream = os.environ.get("BINANCE_WS_STREAM_URL", "").strip()
    if explicit_stream:
        base = _normalize_ws_base_url(explicit_stream)
        return f"{base}/ws/{sym}@bookTicker"

    # Demo mode switch (accepts several common flags)
    demo_flag = str(os.environ.get("BINANCE_DEMO", os.environ.get("BINANCE_DEMO_MODE", "0"))).strip().lower()
    use_demo = demo_flag in {"1", "true", "yes", "y", "on"}

    if use_demo:
        # User may provide a REST demo URL; we normalize it to demo WS host.
        demo_base = _normalize_ws_base_url(
            os.environ.get("BINANCE_DEMO_WS_BASE_URL", "")
            or os.environ.get("BINANCE_DEMO_BASE_URL", "")
            or "wss://demo-stream.binance.com"
        )
        return f"{demo_base}/ws/{sym}@bookTicker"

    # Default/prod path (also supports testnet if user sets BINANCE_WS_BASE_URL)
    base = _normalize_ws_base_url(
        os.environ.get("BINANCE_WS_BASE_URL", "")
        or "wss://stream.binance.com:9443"
    )
    return f"{base}/ws/{sym}@bookTicker"


def _d(x) -> Decimal:
    return Decimal(str(x))


async def stream_book_ticker_with_stats(
    symbol: str,
    *,
    stats: WSRuntimeStats | None = None,
    stale_timeout_sec: float | None = None,
    reconnect_delay_sec: float = 2.0,
    on_message: Callable[[WSRuntimeStats], None] | None = None,
) -> AsyncIterator[BookTicker]:
    """
    Binance Spot public WS: <symbol>@bookTicker
    Adds runtime stats and optional stale-stream watchdog.
    """
    url = _resolve_book_ticker_ws_url(symbol)
    runtime_stats = stats or WSRuntimeStats(symbol=symbol)

    while True:
        try:
            logger.info("Connecting WS: {}", url)
            async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                await runtime_stats.mark_connected()
                logger.info("WS connected: {}", symbol)

                while True:
                    try:
                        if stale_timeout_sec and stale_timeout_sec > 0:
                            msg = await asyncio.wait_for(ws.recv(), timeout=stale_timeout_sec)
                        else:
                            msg = await ws.recv()
                    except asyncio.TimeoutError:
                        await runtime_stats.mark_timeout()
                        logger.warning(
                            "WS stale timeout for {} ({}s without messages). Reconnecting...",
                            symbol,
                            stale_timeout_sec,
                        )
                        break

                    data = json.loads(msg)
                    await runtime_stats.mark_message()
                    if on_message is not None:
                        on_message(runtime_stats)

                    yield BookTicker(
                        symbol=data["s"],
                        bid=_d(data["b"]),
                        ask=_d(data["a"]),
                        bid_qty=_d(data["B"]),
                        ask_qty=_d(data["A"]),
                        ts=datetime.now(timezone.utc),
                    )

        except asyncio.CancelledError:
            await runtime_stats.mark_disconnected()
            raise
        except Exception as e:
            await runtime_stats.mark_error()
            logger.warning("WS error: {}. Reconnecting in {}s...", e, reconnect_delay_sec)
        finally:
            await runtime_stats.mark_disconnected()
            if reconnect_delay_sec > 0:
                await runtime_stats.mark_reconnect()
                await asyncio.sleep(reconnect_delay_sec)
            else:
                await runtime_stats.mark_reconnect()


async def stream_book_ticker(symbol: str) -> AsyncIterator[BookTicker]:
    """
    Backward-compatible wrapper without explicit stats.
    """
    async for tick in stream_book_ticker_with_stats(symbol):
        yield tick