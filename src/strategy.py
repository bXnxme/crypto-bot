from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Deque, Dict, Literal, Optional

from src.data_feed import Candle
from loguru import logger

Action = Literal["HOLD", "BUY", "SELL"]


def _d(x) -> Decimal:
    return Decimal(str(x))


def _ema_next(prev: Optional[Decimal], price: Decimal, period: int) -> Decimal:
    alpha = _d(2) / _d(period + 1)
    if prev is None:
        return price
    return alpha * price + (_d(1) - alpha) * prev


def _tr(high: Decimal, low: Decimal, prev_close: Optional[Decimal]) -> Decimal:
    if prev_close is None:
        return high - low
    a = high - low
    b = abs(high - prev_close)
    c = abs(low - prev_close)
    return max(a, b, c)


def _atr_next(prev_atr: Optional[Decimal], tr: Decimal, period: int) -> Decimal:
    # Wilder smoothing
    if prev_atr is None:
        return tr
    n = _d(period)
    return (prev_atr * (n - _d(1)) + tr) / n


from dataclasses import dataclass
from decimal import Decimal
from typing import Literal

Mode = Literal["pullback"]  # пока только этот режим, чтобы не ломалось

@dataclass
class StrategyConfig:

    pb_lookback: int = 6          # сколько баров ищем факт отката
    pb_tol_pct: Decimal = Decimal("0.0015")  # допуск к EMA_fast (0.15%)
    require_reclaim_cross: bool = True       # обязательно ли пересечение EMA_fast вверх
    require_microbreak: bool = True          # close >= prev_high
    cooldown_bars_after_exit: int = 2        # чтобы не лупить сразу же обратно

    use_atr_exits: bool = False
    sl_atr_mult: float = 1.2
    tp_atr_mult: float = 2.0
    min_trend_strength_pct: float = 0.0  # например 0.002 = 0.2%

    # breakeven / partial
    be_trigger_mult: float = 1.0
    partial_frac: Decimal = _d("0.33")
    lock_atr_mult: float = 0.0   # 0 = просто BE, >0 = trailing

    # switches
    require_price_above_slow: bool = True
    require_slow_rising: bool = True
    mode: Mode = "pullback"

    # ATR
    atr_period: int = 14
    atr_min_pct: Decimal = _d("0.0015")

    adx_period: int = 14
    adx_min: float = 0.0
    adx_rising: bool = False

    # RSI
    rsi_period: int = 14
    rsi_min: Decimal = _d("55")
    rsi_max: Decimal = _d("75")

    # bounce strength
    bounce_min_pct: Decimal = _d("0.0")

    # time stop
    max_hold_bars: int = 10
    time_stop_loss_only: bool = True

    # trend
    ema_fast: int = 20
    ema_slow: int = 50
    ema_long: int = 200
    exit_on_regime_flip: bool = True

    # exits
    tp_pct: Decimal = _d("0.006")
    sl_pct: Decimal = _d("0.003")

    # pullback / breakout
    pullback_tolerance_pct: Decimal = _d("0.0005")
    breakout_lookback: int = 20
    breakout_min_delta_pct: Decimal = _d("0")


def _adx_next(state: BotState, high: Decimal, low: Decimal, close: Decimal, period: int) -> Optional[Decimal]:
    # init
    if state.prev_high_adx is None or state.prev_low_adx is None or state.prev_close is None:
        state.prev_high_adx = high
        state.prev_low_adx = low
        # prev_close обновляется у тебя выше по коду, но на первом баре пусть будет close
        if state.prev_close is None:
            state.prev_close = close
        return None

    up_move = high - state.prev_high_adx
    down_move = state.prev_low_adx - low

    plus_dm = _d(0)
    minus_dm = _d(0)

    if up_move > down_move and up_move > 0:
        plus_dm = up_move
    if down_move > up_move and down_move > 0:
        minus_dm = down_move

    tr = _tr(high, low, state.prev_close)

    p = Decimal(str(period))

    # Wilder smoothing init
    if state.prev_tr14 is None:
        state.prev_tr14 = tr
        state.prev_plus_dm = plus_dm
        state.prev_minus_dm = minus_dm

        # update prevs for next bar
        state.prev_high_adx = high
        state.prev_low_adx = low
        return None

    tr14 = state.prev_tr14 - (state.prev_tr14 / p) + tr
    plus14 = state.prev_plus_dm - (state.prev_plus_dm / p) + plus_dm
    minus14 = state.prev_minus_dm - (state.prev_minus_dm / p) + minus_dm

    state.prev_tr14 = tr14
    state.prev_plus_dm = plus14
    state.prev_minus_dm = minus14

    # update prevs for next bar
    state.prev_high_adx = high
    state.prev_low_adx = low

    if tr14 == 0:
        return None

    plus_di = _d(100) * (plus14 / tr14)
    minus_di = _d(100) * (minus14 / tr14)

    denom = plus_di + minus_di
    if denom == 0:
        dx = _d(0)
    else:
        dx = _d(100) * (abs(plus_di - minus_di) / denom)

    if state.adx is None:
        return dx

    return (state.adx * (p - _d(1)) + dx) / p

@dataclass
class BotState:

    pb_touched: bool = False
    pb_touch_bars_ago: int = 10**9
    bar_index: int = 0

    # --- prev for ADX/DM ---
    prev_high_adx: Optional[Decimal] = None
    prev_low_adx: Optional[Decimal] = None
    prev_tr14: Optional[Decimal] = None
    prev_plus_dm: Decimal = _d(0)
    prev_minus_dm: Decimal = _d(0)
    prev_high_price: Optional[Decimal] = None

    adx: Optional[Decimal] = None
    prev_adx: Optional[Decimal] = None

    # --- Position ---
    in_position: bool = False
    entry_price: Optional[Decimal] = None
    qty: Optional[Decimal] = None
    buy_order_id: Optional[str] = None
    buy_quote: Optional[Decimal] = None
    last_trade_ts_ms: int = 0
    pos_bars: int = 0

    # --- EMAs ---
    ema_fast: Optional[Decimal] = None
    ema_slow: Optional[Decimal] = None
    ema_long: Optional[Decimal] = None
    prev_ema_fast: Optional[Decimal] = None
    prev_ema_slow: Optional[Decimal] = None
    prev_ema_long: Optional[Decimal] = None

    # --- ATR ---
    atr: Optional[Decimal] = None
    prev_atr: Optional[Decimal] = None
    prev_close: Optional[Decimal] = None

    # --- prev for bounce/RSI ---
    prev_price: Optional[Decimal] = None
    prev_low: Optional[Decimal] = None

    # --- RSI state ---
    rsi: Optional[Decimal] = None
    avg_gain: Optional[Decimal] = None
    avg_loss: Optional[Decimal] = None

    # --- rolling prices for breakout ---
    prices: Deque[Decimal] = field(default_factory=lambda: deque(maxlen=20))

    # --- Diagnostics flags ---
    last_trend_ok: bool = False
    last_bounce_up: bool = False
    last_breakout_ok: bool = False
    last_entry_ok: bool = False
    last_atr_ok: bool = False
    last_adx_ok: bool = False
    last_price_above_slow: bool = False
    last_slow_rising: bool = False
    last_rsi_ok: bool = False
    last_strong_bounce: bool = False
    
    @staticmethod
    def default(initial_price: Optional[Decimal] = None) -> "BotState":
        return BotState()


def _rsi_next(state: BotState, price: Decimal, period: int) -> Optional[Decimal]:
    if state.prev_price is None:
        return None

    change = price - state.prev_price
    gain = max(change, _d(0))
    loss = max(-change, _d(0))

    if state.avg_gain is None or state.avg_loss is None:
        state.avg_gain = gain
        state.avg_loss = loss
        return None

    k = _d(1) / _d(period)
    state.avg_gain = state.avg_gain * (_d(1) - k) + gain * k
    state.avg_loss = state.avg_loss * (_d(1) - k) + loss * k

    if state.avg_loss == 0:
        return _d(100)

    rs = state.avg_gain / state.avg_loss
    return _d(100) - (_d(100) / (_d(1) + rs))


def decide_action(cfg: StrategyConfig, state: BotState, candle: Candle) -> Action:
    price = candle.close
    high = candle.high
    low = candle.low
    state.bar_index += 1
    state.pb_touch_bars_ago += 1

    # ---------- ATR ----------
    state.prev_atr = state.atr
    tr = _tr(high, low, state.prev_close)
    state.atr = _atr_next(state.atr, tr, cfg.atr_period)

    atr_ok = True
    if cfg.atr_min_pct > _d(0):
        atr_ok = (state.atr is not None and price != 0 and (state.atr / price) >= cfg.atr_min_pct)
    state.last_atr_ok = bool(atr_ok)

    # ---------- ADX ----------
    prev_adx = getattr(state, "adx", None)  # старый
    new_adx = _adx_next(state, high, low, price, cfg.adx_period)

    adx_ok = True
    if cfg.adx_min and cfg.adx_min > 0:
        adx_ok = (new_adx is not None and new_adx >= _d(cfg.adx_min))

    if cfg.adx_rising:
        adx_ok = adx_ok and (prev_adx is None or (new_adx is not None and new_adx >= prev_adx))

    state.prev_adx = prev_adx
    state.adx = new_adx
    state.last_adx_ok = bool(adx_ok)

    state.prev_close = price  # для ATR и ADX, после расчёта

    # ---------- RSI ----------
    state.rsi = _rsi_next(state, price, cfg.rsi_period)
    rsi_ok = bool(state.rsi is not None and cfg.rsi_min <= state.rsi <= cfg.rsi_max)
    state.last_rsi_ok = bool(rsi_ok)

    # stable prev snapshots
    prev_low = state.prev_low if state.prev_low is not None else low

    # ---------- EMA updates ----------
    state.prev_ema_fast = state.ema_fast
    state.prev_ema_slow = state.ema_slow
    state.prev_ema_long = state.ema_long

    state.ema_fast = _ema_next(state.ema_fast, price, cfg.ema_fast)
    state.ema_slow = _ema_next(state.ema_slow, price, cfg.ema_slow)
    state.ema_long = _ema_next(state.ema_long, price, cfg.ema_long)

    # ---------- Trend / regime ----------
    trend_ok = bool(
        state.ema_slow is not None
        and state.ema_long is not None
        and state.ema_slow > state.ema_long
    )

    if trend_ok and cfg.min_trend_strength_pct > 0:
        if price == 0:
            trend_ok = False
        else:
            strength = (state.ema_slow - state.ema_long) / price
            trend_ok = bool(strength >= cfg.min_trend_strength_pct)

    price_above_slow = bool(state.ema_slow is not None and price >= state.ema_slow)
    slow_rising = bool(
        state.prev_ema_slow is not None
        and state.ema_slow is not None
        and state.ema_slow >= state.prev_ema_slow
    )

    if not cfg.require_price_above_slow:
        price_above_slow = True
    if not cfg.require_slow_rising:
        slow_rising = True

    state.last_trend_ok = bool(trend_ok)
    state.last_price_above_slow = bool(price_above_slow)
    state.last_slow_rising = bool(slow_rising)

    # ---------- Bounce-up + strength ----------
    was_below = False
    now_above = False
    strong_bounce = True

    if state.prev_ema_fast is not None and state.ema_fast is not None:
        tol = cfg.pullback_tolerance_pct
        was_below = prev_low <= state.prev_ema_fast * (_d(1) + tol)
        now_above = price >= state.ema_fast * (_d(1) - tol)

        if cfg.bounce_min_pct > _d(0):
            strong_bounce = ((price - low) / price) >= cfg.bounce_min_pct if price != 0 else False

    bounce_up = bool(was_below and now_above and strong_bounce)

    state.last_bounce_up = bool(bounce_up)
    state.last_strong_bounce = bool(strong_bounce)

    # --- Breakout ---
    breakout_ok = True  # по умолчанию не блокируем

    if cfg.breakout_lookback > 0:
        if state.prices.maxlen != cfg.breakout_lookback:
            state.prices = deque(state.prices, maxlen=cfg.breakout_lookback)

        if len(state.prices) == state.prices.maxlen:
            prev_max_close = max(state.prices)
            breakout_ok = candle.high >= prev_max_close * (_d(1) + cfg.breakout_min_delta_pct)
        else:
            breakout_ok = True  # пока окно не прогрето

    state.prices.append(price)  # price = close

    state.last_breakout_ok = bool(breakout_ok)   # <-- ВАЖНО: всегда

    # --- Pullback continuation flags ---
    pb_lookback = getattr(cfg, "pb_lookback", 6)
    pb_tol = getattr(cfg, "pb_tol_pct", _d("0.0015"))  # 0.15% допуск

    # 1) Фиксируем факт отката к EMA_fast (close <= EMA_fast*(1+tol))
    pb_touch_now = False
    if state.prev_ema_fast is not None:
        pb_touch_now = (low <= state.prev_ema_fast * (Decimal("1") + pb_tol))

    # 1) фиксируем факт касания в тренде
    if trend_ok and pb_touch_now:
        state.pb_touched = True
        state.pb_touch_bars_ago = 0

    pb_ok = state.pb_touched and (state.pb_touch_bars_ago <= pb_lookback)

    # 2) Реклейм EMA_fast вверх (пересечение)

    reclaim_ok = (state.ema_fast is not None and price > state.ema_fast)
    
    # 3) Микро-break (опционально, но полезно)
    micro_ok = True
    if cfg.require_microbreak:
        ph = getattr(state, "prev_high_price", None)
        if ph is not None:
            micro_ok = (candle.high >= ph)   # вместо close >= prev_high

    # ---------- Entry ----------
    entry_ok = bool(
        trend_ok and pb_ok and reclaim_ok and micro_ok and
        breakout_ok and atr_ok and adx_ok and
        price_above_slow and slow_rising
    )

    state.last_entry_ok = bool(entry_ok)   # <-- ВОТ ЭТО ДОЛЖНО БЫТЬ ВСЕГДА


    # ---------- Position / exits ----------
    if not state.in_position:
        action: Action = "BUY" if entry_ok else "HOLD"
        if action == "BUY":
            state.pos_bars = 0
            state.pb_touched = False
            state.pb_touch_bars_ago = 10**9    
    else:
        state.pos_bars += 1

        if state.entry_price is None:
            state.in_position = False
            state.qty = None
            action = "HOLD"
        else:
            # time stop
            if state.pos_bars >= cfg.max_hold_bars:
                if (not cfg.time_stop_loss_only) or (price < state.entry_price):
                    state.prev_price = price
                    state.prev_low = low
                    return "SELL"

            # regime flip
            if cfg.exit_on_regime_flip and not trend_ok:
                action = "SELL"
            else:
                if cfg.use_atr_exits and state.atr is not None:
                    be_level = state.entry_price + state.atr * _d(cfg.be_trigger_mult)
                    # breakeven: если цена ушла >= +1 ATR — стоп в ноль
                    if price >= be_level:
                        sl = state.entry_price  # безубыток
                    else:
                        sl = state.entry_price - (state.atr * _d(cfg.sl_atr_mult))

                    tp = state.entry_price + (state.atr * _d(cfg.tp_atr_mult))
                    action = "SELL" if (price >= tp or price <= sl) else "HOLD"
                else:
                    action = "HOLD"

    # ---------- update prev ----------
    state.prev_price = price
    state.prev_low = low
    state.prev_high_price = candle.high

    return action


def state_to_dict(state: BotState) -> Dict[str, Any]:
    return {
        "in_position": state.in_position,
        "entry_price": str(state.entry_price) if state.entry_price is not None else None,
        "qty": str(state.qty) if state.qty is not None else None,
        "buy_order_id": state.buy_order_id,
        "buy_quote": str(state.buy_quote) if state.buy_quote is not None else None,
        "last_trade_ts_ms": state.last_trade_ts_ms,
        "pos_bars": state.pos_bars,

        "ema_fast": str(state.ema_fast) if state.ema_fast is not None else None,
        "ema_slow": str(state.ema_slow) if state.ema_slow is not None else None,
        "ema_long": str(state.ema_long) if state.ema_long is not None else None,
        "prev_ema_fast": str(state.prev_ema_fast) if state.prev_ema_fast is not None else None,
        "prev_ema_slow": str(state.prev_ema_slow) if state.prev_ema_slow is not None else None,
        "prev_ema_long": str(state.prev_ema_long) if state.prev_ema_long is not None else None,

        "atr": str(state.atr) if state.atr is not None else None,
        "prev_atr": str(state.prev_atr) if state.prev_atr is not None else None,
        "prev_close": str(state.prev_close) if state.prev_close is not None else None,

        "rsi": str(state.rsi) if state.rsi is not None else None,
        "avg_gain": str(state.avg_gain) if state.avg_gain is not None else None,
        "avg_loss": str(state.avg_loss) if state.avg_loss is not None else None,

        "prev_price": str(state.prev_price) if state.prev_price is not None else None,
        "prev_low": str(state.prev_low) if state.prev_low is not None else None,

        "prices": [str(x) for x in state.prices],
        "prices_maxlen": state.prices.maxlen,
        "last_entry_ok": state.last_entry_ok,
        "last_adx_ok": getattr(state, "last_adx_ok", False),
    }


def state_from_dict(d: Dict[str, Any]) -> BotState:
    def dec(x: Any) -> Optional[Decimal]:
        if x is None:
            return None
        return Decimal(str(x))

    st = BotState(
        in_position=bool(d.get("in_position", False)),
        entry_price=dec(d.get("entry_price")),
        qty=dec(d.get("qty")),
        buy_order_id=d.get("buy_order_id"),
        buy_quote=dec(d.get("buy_quote")),
        last_trade_ts_ms=int(d.get("last_trade_ts_ms", 0)),
        pos_bars=int(d.get("pos_bars", 0)),

        ema_fast=dec(d.get("ema_fast")),
        ema_slow=dec(d.get("ema_slow")),
        ema_long=dec(d.get("ema_long")),
        prev_ema_fast=dec(d.get("prev_ema_fast")),
        prev_ema_slow=dec(d.get("prev_ema_slow")),
        prev_ema_long=dec(d.get("prev_ema_long")),

        atr=dec(d.get("atr")),
        prev_atr=dec(d.get("prev_atr")),
        prev_close=dec(d.get("prev_close")),

        rsi=dec(d.get("rsi")),
        avg_gain=dec(d.get("avg_gain")),
        avg_loss=dec(d.get("avg_loss")),

        prev_price=dec(d.get("prev_price")),
        prev_low=dec(d.get("prev_low")),
    )
    st.last_entry_ok = bool(d.get("last_entry_ok", False))
    st.last_adx_ok = bool(d.get("last_adx_ok", False))
    prices_list = d.get("prices") or []
    maxlen = int(d.get("prices_maxlen") or 20)
    st.prices = deque((Decimal(str(x)) for x in prices_list), maxlen=maxlen)
    return st