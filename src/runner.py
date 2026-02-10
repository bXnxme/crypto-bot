from __future__ import annotations

import argparse
import time
from dataclasses import dataclass
from decimal import Decimal, ROUND_DOWN
from pathlib import Path

from binance.client import Client
from loguru import logger
from requests.exceptions import ConnectionError as RequestsConnectionError, ReadTimeout
from urllib3.exceptions import ProtocolError

from src.config import settings
from src.retry import retry_call
from src.state_io import load_state, save_state
from src.storage import append_trade_row
from src.strategy import StrategyConfig, BotState, decide_action

LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

TRADES_CSV = LOG_DIR / "trades.csv"


@dataclass(frozen=True)
class SymbolFilters:
    min_qty: Decimal
    step_size: Decimal
    min_notional: Decimal


def _d(x) -> Decimal:
    return Decimal(str(x))


def _round_step(value: Decimal, step: Decimal) -> Decimal:
    if step == 0:
        return value
    steps = (value / step).to_integral_value(rounding=ROUND_DOWN)
    return steps * step


def _fmt_dec(x: Decimal, ndigits: int = 8) -> str:
    q = Decimal("1").scaleb(-ndigits)  # 10^-ndigits
    return str(x.quantize(q))


def _pct(price: Decimal, entry: Decimal) -> Decimal:
    """Percent change from entry to price."""
    if entry == 0:
        return Decimal("0")
    return (price / entry - Decimal("1")) * Decimal("100")


def make_data_client() -> Client:
    """
    Mainnet client ONLY for historical klines / public market data.
    No testnet base_url override.
    """
    def _make() -> Client:
        client = Client(
            api_key="", api_secret="",
            requests_params={"timeout": 10},
        )
        client.ping()
        return client

    return retry_call(
        _make,
        retries=5,
        base_delay=0.5,
        max_delay=6.0,
        retry_exceptions=(RequestsConnectionError, ReadTimeout, ProtocolError, ConnectionResetError),
        on_retry=lambda n, e, d: logger.warning(
            f"make_data_client retry #{n}: {type(e).__name__}: {e} (sleep {d:.2f}s)"
        ),
    )

def make_client() -> Client:
    def _make() -> Client:
        client = Client(
            settings.api_key,
            settings.api_secret,
            requests_params={"timeout": 10},
        )
        client.API_URL = settings.base_url.rstrip("/") + "/api"  # Spot Testnet (binance.vision)
        client.ping()
        return client

    return retry_call(
        _make,
        retries=5,
        base_delay=0.5,
        max_delay=6.0,
        retry_exceptions=(RequestsConnectionError, ReadTimeout, ProtocolError, ConnectionResetError),
        on_retry=lambda n, e, d: logger.warning(
            f"make_client retry #{n}: {type(e).__name__}: {e} (sleep {d:.2f}s)"
        ),
    )


def get_symbol_filters(client: Client, symbol: str) -> SymbolFilters:
    def _call() -> dict:
        return client.get_symbol_info(symbol)

    info = retry_call(
        _call,
        retries=5,
        base_delay=0.3,
        max_delay=4.0,
        retry_exceptions=(RequestsConnectionError, ReadTimeout, ProtocolError, ConnectionResetError),
        on_retry=lambda n, e, d: logger.warning(
            f"get_symbol_info retry #{n}: {type(e).__name__}: {e} (sleep {d:.2f}s)"
        ),
    )

    if not info:
        raise RuntimeError(f"Symbol info not found for {symbol}")

    lot = next(f for f in info["filters"] if f["filterType"] == "LOT_SIZE")
    notional = next(f for f in info["filters"] if f["filterType"] in {"MIN_NOTIONAL", "NOTIONAL"})

    min_qty = _d(lot["minQty"])
    step_size = _d(lot["stepSize"])
    min_notional_val = notional.get("minNotional") or notional.get("minNotionalValue")
    min_notional = _d(min_notional_val) if min_notional_val is not None else Decimal("0")

    return SymbolFilters(min_qty=min_qty, step_size=step_size, min_notional=min_notional)


def get_price(client: Client, symbol: str) -> Decimal:
    def _call() -> dict:
        return client.get_symbol_ticker(symbol=symbol)

    t = retry_call(
        _call,
        retries=5,
        base_delay=0.2,
        max_delay=3.0,
        retry_exceptions=(RequestsConnectionError, ReadTimeout, ProtocolError, ConnectionResetError),
        on_retry=lambda n, e, d: logger.warning(
            f"get_price retry #{n}: {type(e).__name__}: {e} (sleep {d:.2f}s)"
        ),
    )
    return _d(t["price"])


def market_buy_quote(client: Client, symbol: str, spend_quote: Decimal) -> dict:
    return client.create_order(
        symbol=symbol,
        side=Client.SIDE_BUY,
        type=Client.ORDER_TYPE_MARKET,
        quoteOrderQty=str(spend_quote),
    )


def market_sell_qty(client: Client, symbol: str, qty: Decimal) -> dict:
    return client.create_order(
        symbol=symbol,
        side=Client.SIDE_SELL,
        type=Client.ORDER_TYPE_MARKET,
        quantity=str(qty),
    )


def normalize_state(symbol: str, state: BotState) -> None:
    # If state says "in position", entry_price and qty must exist
    if state.in_position and (state.entry_price is None or state.qty is None):
        logger.warning(
            f"[{symbol}] Corrupted state: in_position=True but entry_price/qty is None. Resetting position."
        )
        state.in_position = False
        state.entry_price = None
        state.qty = None
        state.buy_order_id = None
        state.buy_quote = None

    # If not in position, clean tails
    if not state.in_position:
        state.entry_price = None
        state.qty = None
        # buy_* можно хранить “для истории”, но лучше чистить чтобы не путаться
        # state.buy_order_id = None
        # state.buy_quote = None


def _levels(cfg: StrategyConfig, state: BotState) -> tuple[Decimal | None, Decimal | None]:
    """Return (tp_price, sl_price) if entry exists."""
    if state.entry_price is None:
        return None, None
    tp_price = state.entry_price * (Decimal("1") + cfg.tp_pct)
    sl_price = state.entry_price * (Decimal("1") - cfg.sl_pct)
    return tp_price, sl_price


def _sell_reason(cfg: StrategyConfig, state: BotState, price: Decimal) -> str:
    """Best-effort reason for SELL for logging/stats."""
    tp_price, sl_price = _levels(cfg, state)
    if tp_price is not None and price >= tp_price:
        return "tp"
    if sl_price is not None and price <= sl_price:
        return "sl"
    return "signal"


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--symbol", default="ETHUSDT")
    p.add_argument("--spend", type=str, default="10")          # USDT per entry
    p.add_argument("--interval", type=int, default=2)          # seconds
    p.add_argument("--iterations", type=int, default=300)      # loops
    p.add_argument("--cooldown", type=int, default=120)        # seconds between trades

    p.add_argument("--ema_fast", type=int, default=20)
    p.add_argument("--ema_slow", type=int, default=50)
    p.add_argument("--tp", type=str, default="0.02")           # +2%
    p.add_argument("--sl", type=str, default="0.01")           # -1%
    p.add_argument("--ema_long", type=int, default=200)
    p.add_argument("--pullback_tol", type=str, default="0.0005")
    p.add_argument("--breakout_lookback", type=int, default=20)
    p.add_argument("--breakout_min_delta", type=str, default="0")

    p.add_argument("--log_every", type=int, default=10)        # log tick line every N iterations
    p.add_argument("--reset", action="store_true")
    p.add_argument("--close_on_exit", action="store_true", help="Close open position at the end of run")
    p.add_argument("--bounce_min_pct", type=str, default="0.0")
    args = p.parse_args()

    from loguru import logger as _logger
    _logger.remove()

    log_file = LOG_DIR / f"bot_{args.symbol}.log"
    _logger.add(log_file, rotation="1 MB", retention="7 days")

    _logger.info(f"Logging to {log_file}")

    symbol = args.symbol
    spend_quote = _d(args.spend)

    cfg = StrategyConfig(
        ema_fast=args.ema_fast,
        ema_slow=args.ema_slow,
        ema_long=args.ema_long,
        tp_pct=_d(args.tp),
        sl_pct=_d(args.sl),
        pullback_tolerance_pct=_d(args.pullback_tol),
        breakout_lookback=args.breakout_lookback,
        breakout_min_delta_pct=_d(args.breakout_min_delta),
    )

    _logger.info("Runner started")
    _logger.info(
        f"DRY_RUN={settings.dry_run} symbol={symbol} spend={spend_quote} "
        f"interval={args.interval}s iterations={args.iterations}"
    )
    logger.info(
        f"Strategy: EMA{cfg.ema_fast}/{cfg.ema_slow}/{cfg.ema_long} "
        f"tol={cfg.pullback_tolerance_pct} brk_lb={cfg.breakout_lookback} brk_min={cfg.breakout_min_delta_pct} "
        f"tp={cfg.tp_pct} sl={cfg.sl_pct} cooldown={args.cooldown}s"
    )

    client = make_client()
    filters = get_symbol_filters(client, symbol)
    _logger.info(
        f"{symbol} filters: minQty={filters.min_qty}, stepSize={filters.step_size}, minNotional={filters.min_notional}"
    )

    # load state (symbol-scoped)
    price0 = get_price(client, symbol)
    state = load_state(symbol, price0)
    normalize_state(symbol, state)

    if args.reset:
        _logger.warning("RESET STATE requested")
        state.in_position = False
        state.entry_price = None
        state.qty = None
        state.buy_order_id = None
        state.buy_quote = None
        state.last_trade_ts_ms = 0

    _logger.info(
        f"Loaded state: in_position={state.in_position} entry={state.entry_price} qty={state.qty} "
        f"ema_fast={getattr(state, 'ema_fast', None)} ema_slow={getattr(state, 'ema_slow', None)}"
    )

    stats = {
        "ticks": 0,
        "signals_buy": 0,
        "signals_sell": 0,
        "fills_buy": 0,
        "fills_sell": 0,
        "tp": 0,
        "sl": 0,
        "pnl_usdt": Decimal("0"),
        "pnl_pct_sum": Decimal("0"),  # суммарный % по закрытым сделкам (для справки)
        "forced_closes": 0,
    }

    last_price = price0
    interrupted = False

    try:
        for i in range(args.iterations):
            price = get_price(client, symbol)
            last_price = price

            # decide_action updates EMA fields inside state
            action = decide_action(cfg, state, price)

            now_ms = int(time.time() * 1000)
            since_trade_s = (now_ms - state.last_trade_ts_ms) / 1000 if state.last_trade_ts_ms else 1e9

            # EMA diagnostics (safe if attrs missing)
            ema_fast_val = getattr(state, "ema_fast", None)
            ema_slow_val = getattr(state, "ema_slow", None)
            prev_ema_fast_val = getattr(state, "prev_ema_fast", None)
            prev_ema_slow_val = getattr(state, "prev_ema_slow", None)

            trend_up = (ema_fast_val is not None and ema_slow_val is not None and ema_fast_val > ema_slow_val)
            cross_up = (
                prev_ema_fast_val is not None
                and prev_ema_slow_val is not None
                and ema_fast_val is not None
                and ema_slow_val is not None
                and prev_ema_fast_val <= prev_ema_slow_val
                and ema_fast_val > ema_slow_val
            )

            tp_price, sl_price = _levels(cfg, state)

            # distances (in %)
            dist_to_tp = None
            dist_to_sl = None
            if tp_price is not None:
                dist_to_tp = (tp_price - price) / price * Decimal("100")
            if sl_price is not None:
                dist_to_sl = (price - sl_price) / price * Decimal("100")

            stats["ticks"] += 1

            should_log_tick = ((i + 1) % args.log_every == 0) or (action in {"BUY", "SELL"})
            if should_log_tick:
                base = (
                    f"[{i+1}/{args.iterations}] price={price} "
                    f"ema{cfg.ema_fast}={ema_fast_val} ema{cfg.ema_slow}={ema_slow_val} "
                    f"trend_up={trend_up} cross_up={cross_up} "
                    f"in_position={state.in_position} action={action}"
                )

                if state.in_position and state.entry_price is not None and tp_price is not None and sl_price is not None:
                    base += (
                        f" | entry={state.entry_price} tp={tp_price} sl={sl_price}"
                    )
                    if dist_to_tp is not None and dist_to_sl is not None:
                        base += f" | to_tp={dist_to_tp:.3f}% to_sl={dist_to_sl:.3f}%"

                _logger.info(base)

            normalize_state(symbol, state)

            if since_trade_s < args.cooldown:
                _logger.info(f"Cooldown active: {since_trade_s:.1f}s < {args.cooldown}s")
                save_state(symbol, state)
                time.sleep(args.interval)
                continue

            if action == "BUY":
                stats["signals_buy"] += 1

                if spend_quote < filters.min_notional:
                    _logger.warning(f"Spend {spend_quote} < minNotional {filters.min_notional}. Skipping BUY.")
                    save_state(symbol, state)
                    time.sleep(args.interval)
                    continue

                if settings.dry_run:
                    est_qty = _round_step(spend_quote / price, filters.step_size)
                    if est_qty < filters.min_qty:
                        _logger.warning(f"DRY BUY skipped: est_qty={est_qty} < minQty={filters.min_qty}")
                        save_state(symbol, state)
                        time.sleep(args.interval)
                        continue

                    _logger.info(f"DRY BUY: would spend {spend_quote} -> qty={est_qty} {symbol[:-4]}")
                    stats["fills_buy"] += 1
                    _logger.info(
                        "TRADE | BUY  | price={} spend={} qty={} ema_fast={} ema_slow={}",
                        price, spend_quote, est_qty, state.ema_fast, state.ema_slow
                    )

                    state.in_position = True
                    state.entry_price = price
                    state.qty = est_qty
                    state.buy_order_id = None
                    state.buy_quote = spend_quote  # simulate quote spend for PnL
                    state.last_trade_ts_ms = now_ms
                    save_state(symbol, state)
                    time.sleep(args.interval)
                    continue

                buy = market_buy_quote(client, symbol, spend_quote)
                bought_qty = _d(buy["executedQty"])
                buy_quote = _d(buy["cummulativeQuoteQty"])
                state.buy_order_id = str(buy.get("orderId"))
                state.buy_quote = buy_quote

                _logger.info(f"BUY filled: qty={bought_qty} quote={buy_quote} orderId={buy.get('orderId')}")
                stats["fills_buy"] += 1
                _logger.info(
                    "TRADE | BUY  | price={} quote={} qty={} orderId={}",
                    price, buy_quote, bought_qty, buy.get("orderId")
                )

                state.in_position = True
                state.entry_price = price
                state.qty = bought_qty
                state.last_trade_ts_ms = now_ms
                save_state(symbol, state)

            elif action == "SELL":
                stats["signals_sell"] += 1
                # compute reason + levels for logging/stats
                reason = "sell"
                tp_price = None
                sl_price = None
                if state.entry_price is not None:
                    tp_price = state.entry_price * (Decimal("1") + cfg.tp_pct)
                    sl_price = state.entry_price * (Decimal("1") - cfg.sl_pct)
                    if price >= tp_price:
                        reason = "tp"
                    elif price <= sl_price:
                        reason = "sl"

                if not state.in_position or state.qty is None:
                    _logger.warning("SELL signal but no position. Resetting.")
                    state.in_position = False
                    state.entry_price = None
                    state.qty = None
                    save_state(symbol, state)
                    time.sleep(args.interval)
                    continue

                reason = _sell_reason(cfg, state, price)

                sell_qty = _round_step(state.qty, filters.step_size)
                if sell_qty < filters.min_qty:
                    _logger.warning(f"Sell qty {sell_qty} < minQty {filters.min_qty}. Skipping SELL.")
                    save_state(symbol, state)
                    time.sleep(args.interval)
                    continue

                if settings.dry_run:
                    # estimated pnl in quote currency (USDT)
                    est_pnl = Decimal("0")
                    est_pnl_pct = Decimal("0")
                    if state.entry_price is not None and state.qty is not None:
                        est_pnl = (price - state.entry_price) * state.qty
                        est_pnl_pct = _pct(price, state.entry_price)

                    stats["pnl_usdt"] += est_pnl
                    stats["pnl_pct_sum"] += est_pnl_pct
                    stats["fills_sell"] += 1
                    if reason == "tp":
                        stats["tp"] += 1
                    elif reason == "sl":
                        stats["sl"] += 1

                    _logger.info(
                        "TRADE | SELL | price={} qty={} reason={} pnl={} pnl_pct={} entry={} tp={} sl={}",
                        price,
                        sell_qty,
                        reason,
                        _fmt_dec(est_pnl, 8),
                        _fmt_dec(est_pnl_pct, 4),
                        state.entry_price,
                        tp_price,
                        sl_price,
                    )

                    # clear position
                    state.in_position = False
                    state.entry_price = None
                    state.qty = None
                    state.last_trade_ts_ms = now_ms
                    save_state(symbol, state)
                    time.sleep(args.interval)
                    continue

                sell = market_sell_qty(client, symbol, sell_qty)
                sell_qty_exe = _d(sell["executedQty"])
                sell_quote = _d(sell["cummulativeQuoteQty"])
                _logger.info(f"SELL filled: qty={sell_qty_exe} quote={sell_quote} orderId={sell.get('orderId')}")

                # PnL in quote currency (USDT)
                if state.buy_quote is None:
                    _logger.warning("No buy_quote in state, using fallback PnL")
                    pnl = sell_quote - spend_quote
                    buy_quote_str = ""
                else:
                    pnl = sell_quote - state.buy_quote
                    buy_quote_str = str(state.buy_quote)

                pnl_pct = Decimal("0")
                if state.entry_price is not None:
                    pnl_pct = _pct(price, state.entry_price)

                    stats["pnl_usdt"] += pnl
                    stats["pnl_pct_sum"] += pnl_pct
                    stats["fills_sell"] += 1
                    if reason == "tp":
                        stats["tp"] += 1
                    elif reason == "sl":
                        stats["sl"] += 1

                    _logger.info(
                        "TRADE | SELL | price={} qty={} reason={} pnl={} pnl_pct={} entry={} tp={} sl={} orderId={}",
                        price,
                        sell_qty_exe,
                        reason,
                        _fmt_dec(pnl, 8),
                        _fmt_dec(pnl_pct, 4),
                        state.entry_price,
                        tp_price,
                        sl_price,
                        sell.get("orderId"),
                    )

                append_trade_row(
                    TRADES_CSV,
                    {
                        "ts_ms": now_ms,
                        "symbol": symbol,
                        "buy_order_id": state.buy_order_id or "",
                        "sell_order_id": str(sell.get("orderId") or ""),
                        "buy_qty_btc": str(state.qty),  # keep column name for compatibility
                        "buy_quote_usdt": buy_quote_str,
                        "sell_qty_btc": str(sell_qty_exe),
                        "sell_quote_usdt": str(sell_quote),
                        "pnl_usdt": str(pnl),
                        "reason": reason,
                    },
                )
                _logger.info(f"Saved trade to {TRADES_CSV} PnL={pnl} reason={reason}")

                # clear position
                state.in_position = False
                state.entry_price = None
                state.qty = None
                state.buy_order_id = None
                state.buy_quote = None
                state.last_trade_ts_ms = now_ms
                save_state(symbol, state)

            else:
                save_state(symbol, state)

            time.sleep(args.interval)

    except KeyboardInterrupt:
        interrupted = True
        _logger.warning("KeyboardInterrupt received. Stopping bot...")

    finally:
        if interrupted:
            _logger.warning("Graceful shutdown in progress...")

    # ---- CLOSE ON EXIT ----
    if args.close_on_exit and state.in_position and state.qty is not None and state.entry_price is not None:
        logger.warning("CLOSE_ON_EXIT enabled: closing open position at end of run")

        # get fresh price before closing
        try:
            last_price = get_price(client, symbol)
            logger.info(f"CLOSE_ON_EXIT fresh price fetched: {last_price}")
        except Exception as e:
            logger.warning(f"Failed to fetch fresh price on exit, using cached price: {e}")

        now_ms = int(time.time() * 1000)
        sell_qty = _round_step(state.qty, filters.step_size)

        if sell_qty < filters.min_qty:
            logger.warning(f"CLOSE_ON_EXIT skipped: sell_qty {sell_qty} < minQty {filters.min_qty}")
        else:
            reason = "close_on_exit"
            stats["forced_closes"] += 1

            if settings.dry_run:
                est_pnl = (last_price - state.entry_price) * state.qty
                est_pnl_pct = _pct(last_price, state.entry_price)

                stats["pnl_usdt"] += est_pnl
                stats["pnl_pct_sum"] += est_pnl_pct
                stats["fills_sell"] += 1

                logger.info(
                    "TRADE | SELL | price={} qty={} reason={} pnl={} pnl_pct={} entry={}",
                    last_price,
                    sell_qty,
                    reason,
                    _fmt_dec(est_pnl, 8),
                    _fmt_dec(est_pnl_pct, 4),
                    state.entry_price,
                )

            else:
                sell = market_sell_qty(client, symbol, sell_qty)
                sell_qty_exe = _d(sell["executedQty"])
                sell_quote = _d(sell["cummulativeQuoteQty"])

                if state.buy_quote is not None:
                    pnl = sell_quote - state.buy_quote
                else:
                    pnl = sell_quote - spend_quote

                pnl_pct = _pct(last_price, state.entry_price)

                stats["pnl_usdt"] += pnl
                stats["pnl_pct_sum"] += pnl_pct
                stats["fills_sell"] += 1

                logger.info(
                    "TRADE | SELL | price={} qty={} reason={} pnl={} pnl_pct={} entry={} orderId={}",
                    last_price,
                    sell_qty_exe,
                    reason,
                    _fmt_dec(pnl, 8),
                    _fmt_dec(pnl_pct, 4),
                    state.entry_price,
                    sell.get("orderId"),
                )

            # clear state
            state.in_position = False
            state.entry_price = None
            state.qty = None
            state.buy_order_id = None
            state.buy_quote = None
            state.last_trade_ts_ms = now_ms
            save_state(symbol, state)

        # ---- SUMMARY ----
        _logger.info(
            "SUMMARY | "
            f"ticks={stats['ticks']} "
            f"signals_buy={stats['signals_buy']} fills_buy={stats['fills_buy']} "
            f"signals_sell={stats['signals_sell']} fills_sell={stats['fills_sell']} "
            f"tp={stats['tp']} sl={stats['sl']} "
            f"forced_closes={stats['forced_closes']} "
            f"pnl_usdt={stats['pnl_usdt']}"
        )

        if state.in_position and state.entry_price is not None and state.qty is not None:
            unreal = (last_price - state.entry_price) * state.qty
            unreal_pct = _pct(last_price, state.entry_price)
            _logger.info(
                "SUMMARY | open_position entry={} qty={} last_price={} unreal_pnl={} unreal_pct={}",
                state.entry_price,
                state.qty,
                last_price,
                _fmt_dec(unreal, 8),
                _fmt_dec(unreal_pct, 4),
            )
        else:
            _logger.info("SUMMARY | no_open_position")

        _logger.info("Runner finished ✅")


if __name__ == "__main__":
    main()