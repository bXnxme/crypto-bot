from __future__ import annotations

import argparse
from dataclasses import dataclass
from decimal import Decimal, ROUND_DOWN
from pathlib import Path

from loguru import logger

from src.runner import make_client, make_data_client, get_symbol_filters
from src.strategy import StrategyConfig, BotState, decide_action
from src.data_feed import fetch_klines_to_csv, read_candles_csv


def _d(x) -> Decimal:
    return Decimal(str(x))


def _round_step(value: Decimal, step: Decimal) -> Decimal:
    if step == 0:
        return value
    steps = (value / step).to_integral_value(rounding=ROUND_DOWN)
    return steps * step


@dataclass
class Trade:
    entry_price: Decimal
    exit_price: Decimal
    qty: Decimal
    pnl: Decimal
    reason: str  # tp/sl/signal/close_on_end/regime_flip
    bars_held: int


def backtest_csv(
    csv_path: Path,
    symbol: str,
    cfg: StrategyConfig,
    spend_quote: Decimal,
    fee_rate: Decimal,
    close_on_end: bool,
    maker_fee_rate: Decimal,
    taker_fee_rate: Decimal,
    tp_as_maker: bool,
) -> dict:
    client = make_client()
    filters = get_symbol_filters(client, symbol)

    # Backtest must be isolated from disk state
    state = BotState.default()

    state.in_position = False
    state.entry_price = None
    state.qty = None
    state.buy_order_id = None
    state.buy_quote = None
    state.last_trade_ts_ms = 0
    
    # --- trade mgmt flags (backtest-only, but stored in state) ---
    state.be_active = False        # BE-stop for remainder is active
    state.partial_done = False     # partial was executed once
    state.stop_price = None        # Decimal | None, BE-stop for remainder
 
    # сброс всех индикаторов/предыдущих значений
    state.ema_fast = None
    state.ema_slow = None
    state.ema_long = None
    state.prev_ema_fast = None
    state.prev_ema_slow = None
    state.prev_ema_long = None
    state.prev_price = None
    state.prev_low = None
    state.adx = None
    state.prev_plus_dm = _d(0)
    state.prev_minus_dm = _d(0)
    state.prev_tr14 = None
    state.prev_high = None
    state.prev_low_adx = None
    # очистка окна цен (и maxlen под cfg)
    if cfg.breakout_lookback > 0:
        from collections import deque
        state.prices = deque(maxlen=cfg.breakout_lookback)
 

    trades: list[Trade] = []
    equity = Decimal("0")
    peak = Decimal("0")
    max_dd = Decimal("0")

    # diagnostics
    counts = {
        "bars": 0,
        "trend_ok": 0,
        "bounce_up": 0,
        "breakout_ok": 0,
        "atr_ok": 0,
        "adx_ok": 0,
        "entry_ok": 0,
        "in_pos_bars": 0,
        "price_above_slow": 0,
        "slow_rising": 0,
        "trend_and_above_slow": 0,
        "trend_and_slow_rising": 0,
    }

    sig_buy = 0
    sig_sell = 0
    buy_skips = {"min_notional": 0, "qty_lt_min": 0}
    first_buy_logs = 0

    # --- exit reason stats ---
    reason_counts: dict[str, int] = {}
    reason_pnl_sum: dict[str, Decimal] = {}
    reason_bars_sum: dict[str, int] = {}

    entry_bar_idx: int | None = None
    total_trade_bars = 0
    last_price: Decimal | None = None

    # for durations
    cur_trade_bars = 0
    trade_bars: list[int] = []

    last_price: Decimal | None = None
    bars_in_pos = 0
    bar_i = 0

    for candle in read_candles_csv(csv_path):
        close = candle.close
        high = candle.high
        low = candle.low
        last_price = close
        bar_i += 1
        ts = getattr(candle, "ts_ms", None) or getattr(candle, "open_time", None) or getattr(candle, "ts", None)

        # --- snapshots BEFORE decide_action updates indicators on this candle ---
        was_in_position = state.in_position
        forced_exit_price: Decimal | None = None
        forced_reason: str | None = None

        # 1) INTRABAR (CONSERVATIVE):
        # - worst-case SL_base first
        # - if BE was activated earlier, allow BE-stop to be hit intrabar
        if was_in_position and state.entry_price is not None and state.qty is not None:
            if getattr(cfg, "use_atr_exits", False) and state.atr is not None:

                entry = state.entry_price
                atr = state.atr

                sl_base = entry - atr * _d(cfg.sl_atr_mult)
                tp_price = entry + atr * _d(cfg.tp_atr_mult)

                # A) worst-case first: SL_base hit intrabar -> exit
                if low <= sl_base:
                    forced_exit_price = sl_base
                    forced_reason = "sl"
                    action = "SELL"
                else:
                    # B0) TP intrabar (conservative fill at tp_price)
                    if high >= tp_price:
                        forced_exit_price = tp_price
                        forced_reason = "tp"
                        action = "SELL"
                # B) if BE already active from PREVIOUS candles -> check BE-stop intrabar
                if getattr(state, "be_active", False):
                    be_price = entry * (Decimal("1") + taker_fee_rate) / (Decimal("1") - taker_fee_rate)
                    lock_mult = _d(getattr(cfg, "lock_atr_mult", 0.0))

                    # lock_atr_mult:
                    # - 0.0  => plain fee-aware breakeven
                    # - >0.0 => trailing stop above BE by (lock_atr_mult * ATR)
                    desired_stop = be_price if lock_mult <= 0 else (be_price + atr * lock_mult)

                    if state.stop_price is None:
                        state.stop_price = desired_stop
                    else:
                        # trailing: only tighten
                        if desired_stop > state.stop_price:
                            state.stop_price = desired_stop

                    if low <= state.stop_price:
                        forced_exit_price = state.stop_price
                        forced_reason = "be"
                        action = "SELL"

                if forced_exit_price is not None:
                    ts = getattr(candle, "ts_ms", None) \
                        or getattr(candle, "open_time", None) \
                        or getattr(candle, "ts", None)
                    
        # 2) If intrabar exit happened -> SELL now, else normal decision by close
        if forced_exit_price is not None:
            action = "SELL"
        else:
            action = decide_action(cfg, state, candle)

        # 2.1) normalize actions (no pyramiding, no phantom sells)
        if action == "SELL" and not state.in_position:
            action = "HOLD"
        if action == "BUY" and state.in_position:
            action = "HOLD"

        # 2.2) DIAG2: count finalized actions
        if action == "BUY":
            sig_buy += 1
        elif action == "SELL":
            sig_sell += 1

        
        # --- END-OF-BAR (CONSERVATIVE) partial on CLOSE + activate BE for remainder ---
        # We do this AFTER intrabar SL handling and AFTER decide_action updates indicators.
        if (
            forced_exit_price is None
            and state.in_position
            and was_in_position
            and state.entry_price is not None
            and state.qty is not None
            and getattr(cfg, "use_atr_exits", False)
            and state.atr is not None
            and action != "SELL"
        ):
            entry = state.entry_price
            atr = state.atr

            # params (later move to cfg)
            partial_frac = Decimal(str(cfg.partial_frac))    # 33% фиксируем раньше/чаще
            be_trigger_mult = Decimal(str(cfg.be_trigger_mult))       # триггер на +1 ATR (раньше)
            lock_atr_mult = Decimal(str(cfg.lock_atr_mult))             # запас над fee-BE, чтобы не выбивало шумом             # stop above BE by 0.10 ATR

            sl_base = entry - atr * _d(cfg.sl_atr_mult)
            be_trigger = entry + atr * be_trigger_mult

            # Intrabar SL_base would have exited already. Here we assume:
            # "trigger happened within candle" AND partial executes conservatively at CLOSE.
            if (not getattr(state, "partial_done", False)) and (close >= be_trigger):

                old_qty = state.qty
                qty_part = _round_step(old_qty * cfg.partial_frac, filters.step_size)

                # execute partial only if meaningful and leaves remainder
                if qty_part >= filters.min_qty and qty_part < old_qty:
                    part_price = close  # conservative: close price, not high/trigger

                    # apply taker fee on the partial market exit
                    gross = qty_part * part_price
                    sell_fee = gross * taker_fee_rate
                    # realized proceeds are not added to equity here; we adjust cost basis instead

                    # shrink remaining cost basis proportionally (critical!)
                    if state.buy_quote is not None:
                        remaining_qty = old_qty - qty_part
                        if remaining_qty > 0:
                            state.buy_quote = state.buy_quote * (remaining_qty / old_qty)

                    # reduce position size
                    state.qty = old_qty - qty_part

                    # activate BE for remainder (from NEXT candle intrabar)
                    state.partial_done = True
                    state.be_active = True

                    # fee-aware BE stop for remainder (taker in + taker out).
                    # If lock_atr_mult == 0 -> plain BE; if > 0 -> BE + lock_atr_mult*ATR (trailing will tighten later).
                    be_price = entry * (Decimal("1") + taker_fee_rate) / (Decimal("1") - taker_fee_rate)
                    state.stop_price = be_price if lock_atr_mult <= 0 else (be_price + atr * lock_atr_mult)

                    logger.info(
                        "PARTIAL@CLOSE | bar={} ts={} close={} entry={} atr={} be_trigger={} qty_part={} qty_left={} be_stop={}",
                        bar_i, ts, close, entry, atr, be_trigger,
                        qty_part, state.qty, state.stop_price
                    )    

        # OPTIONAL: close-based TP for remainder (conservative)
        # If BE active and close >= tp -> exit remainder at close.
        if (
            state.in_position
            and getattr(state, "be_active", False)
            and state.entry_price is not None
            and state.atr is not None
            and was_in_position
            and action != "SELL"
        ):
            tp_price = state.entry_price + state.atr * _d(cfg.tp_atr_mult)
            if close >= tp_price:
                forced_exit_price = close
                forced_reason = "tp"
                action = "SELL"

        # conservative time-stop (close-based)
        if (
            state.in_position
            and state.entry_price is not None
            and forced_exit_price is None
            and cfg.max_hold_bars is not None
            and bars_in_pos >= cfg.max_hold_bars
        ):
            forced_exit_price = close
            forced_reason = "time_stop"
            action = "SELL"


        if action == "BUY":
            if state.in_position:
                logger.error("BUG: BUY while already in position | close={} entry={} qty={}", close, state.entry_price, state.qty)
                action = "HOLD"


        # 4) BUY execution (only if no position yet; intrabar never runs after BUY on same candle)
        if action == "BUY" and not state.in_position:
            if spend_quote < filters.min_notional:
                buy_skips["min_notional"] += 1
                action = "HOLD"
            else:
                buy_fee_budget = spend_quote * taker_fee_rate
                qty = _round_step((spend_quote - buy_fee_budget) / close, filters.step_size)
                if qty < filters.min_qty:
                    buy_skips["qty_lt_min"] += 1
                    action = "HOLD"
                else:
                    buy_notional = qty * close
                    buy_fee = buy_notional * taker_fee_rate
                    buy_cost = buy_notional + buy_fee

                    state.in_position = True

                    state.entry_price = close
                    state.qty = qty
                    state.buy_quote = buy_cost
                    state.be_active = False
                    state.partial_done = False
                    state.stop_price = None

                    # counters
                    bars_in_pos = 0
                    cur_trade_bars = 0
                    entry_bar_idx = counts["bars"] + 1  # entry happens on this bar

        # 5) bars_in_pos / длительности: инкремент после того, как стало понятно, что позиция реально была в этом баре
        if state.in_position:
            bars_in_pos += 1
            counts["in_pos_bars"] += 1
            cur_trade_bars += 1

        # 6) DIAG flags (после decide_action, уже обновлены state.last_*)
        counts["bars"] += 1
        if getattr(state, "last_trend_ok", False):
            counts["trend_ok"] += 1
        if getattr(state, "last_bounce_up", False):
            counts["bounce_up"] += 1
        if getattr(state, "last_breakout_ok", False):
            counts["breakout_ok"] += 1
        if getattr(state, "last_atr_ok", False):
            counts["atr_ok"] += 1
        if getattr(state, "last_adx_ok", False):
            counts["adx_ok"] += 1
        if getattr(state, "last_price_above_slow", False):
            counts["price_above_slow"] += 1
        if getattr(state, "last_slow_rising", False):
            counts["slow_rising"] += 1
        if getattr(state, "last_trend_ok", False) and getattr(state, "last_slow_rising", False):
            counts["trend_and_slow_rising"] += 1
        if getattr(state, "last_trend_ok", False) and getattr(state, "last_price_above_slow", False):
            counts["trend_and_above_slow"] += 1

        # ВАЖНО: entry_ok в DIAG — только по state.last_entry_ok
        if getattr(state, "last_entry_ok", False):
            counts["entry_ok"] += 1

        # 7) SELL execution
        if action == "SELL" and state.in_position and state.entry_price is not None and state.qty is not None:
            exit_price = forced_exit_price if forced_exit_price is not None else close

            qty = _round_step(state.qty, filters.step_size)
            if qty >= filters.min_qty:
                gross = qty * exit_price

                reason = forced_reason if forced_reason is not None else "signal"

                exit_fee_rate = taker_fee_rate
                if reason == "tp" and tp_as_maker:
                    exit_fee_rate = maker_fee_rate

                sell_fee = gross * exit_fee_rate
                net_proceeds = gross - sell_fee

                buy_cost = state.buy_quote if state.buy_quote is not None else spend_quote
                pnl = net_proceeds - buy_cost

                reason_counts[reason] = reason_counts.get(reason, 0) + 1
                reason_pnl_sum[reason] = reason_pnl_sum.get(reason, Decimal("0")) + pnl
                reason_bars_sum[reason] = reason_bars_sum.get(reason, 0) + int(bars_in_pos)

                equity += pnl
                peak = max(peak, equity)
                max_dd = max(max_dd, peak - equity)

                bars_held = 0
                if entry_bar_idx is not None:
                    bars_held = counts["bars"] - entry_bar_idx + 1
                    total_trade_bars += bars_held
                    entry_bar_idx = None

                trades.append(Trade(
                    entry_price=state.entry_price,
                    exit_price=exit_price,
                    qty=qty,
                    pnl=pnl,
                    reason=reason,
                    bars_held=bars_held
                ))

                trade_bars.append(cur_trade_bars)
                cur_trade_bars = 0


            state.in_position = False
            state.entry_price = None
            state.qty = None
            state.buy_quote = None
            bars_in_pos = 0


    # optional: close open position at end of dataset
    if close_on_end and state.in_position and state.entry_price is not None and state.qty is not None and last_price is not None:
        qty = _round_step(state.qty, filters.step_size)
        if qty >= filters.min_qty:
            gross = qty * last_price
            sell_fee = gross * taker_fee_rate
            buy_cost = state.buy_quote if state.buy_quote is not None else (spend_quote + spend_quote * taker_fee_rate)
            pnl = (gross - sell_fee) - buy_cost

            equity += pnl
            if equity > peak:
                peak = equity
            dd = peak - equity
            if dd > max_dd:
                max_dd = dd

            bars_held = 0
            if entry_bar_idx is not None:
                bars_held = counts["bars"] - entry_bar_idx + 1
                total_trade_bars += bars_held
                entry_bar_idx = None

            trades.append(
                Trade(
                    entry_price=state.entry_price,
                    exit_price=last_price,
                    qty=qty,
                    pnl=pnl,
                    reason="close",
                    bars_held=bars_held,
                )
            )

        state.in_position = False
        state.entry_price = None
        state.qty = None        

    wins = [t for t in trades if t.pnl > 0]
    losses = [t for t in trades if t.pnl <= 0]

    count = len(trades)
    winrate = (Decimal(len(wins)) / Decimal(count) * Decimal("100")) if count else Decimal("0")

    gross_profit = sum((t.pnl for t in wins), Decimal("0"))
    gross_loss_abs = sum((-t.pnl for t in losses), Decimal("0"))
    if gross_loss_abs == 0:
        profit_factor = Decimal("Infinity") if gross_profit > 0 else Decimal("0")
    else:
        profit_factor = gross_profit / gross_loss_abs

    avg_win = (gross_profit / Decimal(len(wins))) if wins else Decimal("0")
    avg_loss = (gross_loss_abs / Decimal(len(losses))) if losses else Decimal("0")

    expectancy = (equity / Decimal(count)) if count else Decimal("0")

    avg_trade_bars = (Decimal(total_trade_bars) / Decimal(count)) if count else Decimal("0")

    logger.info(
        "DIAG | bars={bars} trend_ok={trend_ok} bounce_up={bounce_up} breakout_ok={breakout_ok} atr_ok={atr_ok} adx_ok={adx_ok} "
        "price_above_slow={price_above_slow} slow_rising={slow_rising} "
        "trend&slow_rising={trend_and_slow_rising} trend&above_slow={trend_and_above_slow} "
        "entry_ok={entry_ok} in_pos_bars={in_pos_bars} avg_trade_bars={avg_trade_bars:.2f}",
        **counts,
        avg_trade_bars=float(avg_trade_bars),
    )

    logger.info(
        "DIAG2 | sig_buy={} sig_sell={} buy_skips={}",
        sig_buy, sig_sell, buy_skips
    )

    # --- DIAG3: exit reasons ---
    parts = []
    for r in sorted(reason_counts.keys()):
        c = reason_counts[r]
        ps = reason_pnl_sum.get(r, Decimal("0"))
        bs = reason_bars_sum.get(r, 0)
        avg_pnl = (ps / Decimal(c)) if c else Decimal("0")
        avg_bars = (Decimal(bs) / Decimal(c)) if c else Decimal("0")
        parts.append(f"{r}: n={c} pnl={ps} avg_pnl={avg_pnl} avg_bars={avg_bars:.2f}")
    logger.info("DIAG3 | " + " | ".join(parts) if parts else "DIAG3 | (no exits)")

    return {
        "trades": trades,
        "count": count,
        "wins": len(wins),
        "losses": len(losses),
        "winrate_pct": winrate,
        "pnl_total": equity,
        "max_drawdown": max_dd,
        "open_position": bool(state.in_position),
        "profit_factor": profit_factor,
        "avg_win": avg_win,
        "avg_loss": avg_loss,
        "expectancy": expectancy,
        "avg_trade_bars": avg_trade_bars,
        "reason_counts": reason_counts,
        "reason_pnl_sum": reason_pnl_sum,
        "reason_bars_sum": reason_bars_sum,
    }


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--symbol", required=True)
    p.add_argument("--interval", default="1m")
    p.add_argument("--start", required=True)  # YYYY-MM-DD
    p.add_argument("--end", default=None)     # YYYY-MM-DD or None

    # Strategy params
    p.add_argument("--ema_fast", type=int, default=20)
    p.add_argument("--ema_slow", type=int, default=50)
    p.add_argument("--ema_long", type=int, default=200)
    p.add_argument("--pullback_tol", type=str, default="0.0005")
    p.add_argument("--bounce_min_pct", type=str, default="0.0")
    p.add_argument("--atr_min_pct", type=str, default="0.0015")
    p.add_argument("--breakout_lookback", type=int, default=20)
    p.add_argument("--breakout_min_delta", type=str, default="0")
    p.add_argument("--tp", type=str, default="0.002")
    p.add_argument("--sl", type=str, default="0.002")

    p.add_argument("--spend", type=str, default="10")
    p.add_argument("--fee", type=str, default="0.001")

    # backtest behavior
    p.add_argument("--no_close_on_end", action="store_true", help="Do not close open position at the end")
    p.add_argument("--close_on_end", action="store_true")
    p.add_argument("--atr_period", type=int, default=14)

    # time stops
    p.add_argument("--max_hold_bars", type=int, default=10)
    p.add_argument("--time_stop_loss_only", action="store_true")
    p.add_argument("--time_stop_any", action="store_true")
    p.add_argument("--mode", type=str, default="pullback", choices=["pullback", "structure"])
    p.add_argument("--taker_fee", type=str, default=None, help="Override --fee for taker (market) fills")
    p.add_argument("--maker_fee", type=str, default="0.0006", help="Maker fee for limit TP fills")
    p.add_argument("--tp_as_maker", action="store_true", help="Use maker fee for TP exits")
    p.add_argument("--adx_min", type=float, default=20)
    p.add_argument("--adx_period", type=int, default=14)
    p.add_argument("--adx_rising", action="store_true")
    p.add_argument("--min_trend_strength", type=float, default=0.0)
    # --- ATR exits ---
    p.add_argument("--use_atr_exits", action="store_true")
    p.add_argument("--sl_atr_mult", type=float, default=1.2)
    p.add_argument("--tp_atr_mult", type=float, default=2.0)
    p.add_argument("--be_trigger_mult", type=float, default=1.0)
    p.add_argument("--partial_frac", type=float, default=0.33)
    p.add_argument("--lock_atr_mult", type=float, default=0.0)
    args = p.parse_args()

    taker_fee = _d(args.taker_fee) if args.taker_fee is not None else _d(args.fee)
    maker_fee = _d(args.maker_fee)

    close_on_end = True
    if args.close_on_end:
        close_on_end = True
    if args.no_close_on_end:
        close_on_end = False

    data_client = make_data_client()
    csv_path = fetch_klines_to_csv(client=data_client,
        symbol=args.symbol,
        interval=args.interval,
        start_str=args.start,
        end_str=args.end,
    )
    logger.info(f"Saved candles to {csv_path}")

    cfg = StrategyConfig(
        exit_on_regime_flip=False,
        ema_fast=args.ema_fast,
        ema_slow=args.ema_slow,
        ema_long=args.ema_long,
        tp_pct=_d(args.tp),
        sl_pct=_d(args.sl),
        pullback_tolerance_pct=_d(args.pullback_tol),
        bounce_min_pct=_d(args.bounce_min_pct),
        breakout_lookback=int(args.breakout_lookback),
        breakout_min_delta_pct=_d(args.breakout_min_delta),
        atr_period=int(args.atr_period),
        atr_min_pct=_d(args.atr_min_pct),
        max_hold_bars=int(args.max_hold_bars),
        adx_period=args.adx_period,
        adx_min=args.adx_min,
        min_trend_strength_pct=args.min_trend_strength,
        use_atr_exits=bool(args.use_atr_exits),
        sl_atr_mult=float(args.sl_atr_mult),
        tp_atr_mult=float(args.tp_atr_mult),
        adx_rising=bool(args.adx_rising),
        be_trigger_mult=args.be_trigger_mult,
        partial_frac=Decimal(str(args.partial_frac)),
        lock_atr_mult=args.lock_atr_mult,
    )

    logger.info("CFG CHECK | breakout_lookback={} breakout_min_delta_pct={}", cfg.breakout_lookback, cfg.breakout_min_delta_pct)
    logger.info(
        "CFG CHECK | use_atr_exits={} sl_atr_mult={} tp_atr_mult={} tp_pct={} sl_pct={}",
        cfg.use_atr_exits, cfg.sl_atr_mult, cfg.tp_atr_mult, cfg.tp_pct, cfg.sl_pct
    )
    logger.info(
        "CFG CHECK | adx_period={} adx_min={} adx_rising={}",
        cfg.adx_period, cfg.adx_min, cfg.adx_rising
    )

    res = backtest_csv(
        csv_path=csv_path,
        symbol=args.symbol,
        cfg=cfg,
        spend_quote=_d(args.spend),
        fee_rate=_d(args.fee),          # можно оставить, но дальше не использовать
        close_on_end=close_on_end,
        maker_fee_rate=maker_fee,
        taker_fee_rate=taker_fee,
        tp_as_maker=bool(args.tp_as_maker),
    )

    logger.info(
        "BACKTEST | symbol={} interval={} start={} end={} | EMA{}/{}/{} tol={} bounce_min={} brk_lb={} brk_min={} tp={} sl={} atr_p={} atr_min={} spend={} taker_fee={} maker_fee={} tp_as_maker={} close_on_end={}",
        args.symbol, args.interval, args.start, args.end,
        args.ema_fast, args.ema_slow, args.ema_long,
        args.pullback_tol,
        args.bounce_min_pct,
        args.breakout_lookback, args.breakout_min_delta,
        args.tp, args.sl,
        args.atr_period, args.atr_min_pct,
        args.spend, args.taker_fee, args.maker_fee, args.tp_as_maker,
        close_on_end,
    )

    logger.info(
        "RESULT | trades={} wins={} losses={} winrate={:.2f}% pnl_total={} max_dd={} pf={} avg_win={} avg_loss={} expectancy={} avg_trade_bars={:.2f} open_position={}",
        res["count"],
        res["wins"],
        res["losses"],
        float(res["winrate_pct"]),
        res["pnl_total"],
        res["max_drawdown"],
        res["profit_factor"],
        res["avg_win"],
        res["avg_loss"],
        res["expectancy"],
        float(res["avg_trade_bars"]),
        res["open_position"],
    )

    if res.get("reason_counts"):
        logger.info("REASONS | {}", res["reason_counts"])


if __name__ == "__main__":
    main()