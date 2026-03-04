from __future__ import annotations

from decimal import Decimal, ROUND_DOWN
from typing import Optional

from .grid_types import FillResult, GridConfig, GridLot, GridState, PlaceIntent, Side


# ---------- decimal helpers ----------

def d(x) -> Decimal:
    return Decimal(str(x))


def round_step(value: Decimal, step: Decimal) -> Decimal:
    if step <= 0:
        return value
    steps = (value / step).to_integral_value(rounding=ROUND_DOWN)
    return steps * step


class GridCore:
    """Pure infinity-grid mechanics (no exchange/broker coupling).

    This core now supports backtest-parity features used by paper/live adapters:
    - spend sizing mode: absolute quote or percent of contributed capital
    - exposure caps (base qty / base value)
    - fee-aware sell guard (sell only above cost)
    - optional minimal sell markup
    """

    def __init__(self, cfg: GridConfig, state: GridState):
        self.cfg = cfg
        self.state = state
        self.cfg.validate()

    # ---------- price levels ----------
    def _buy_level(self, anchor: Decimal, k: int) -> Decimal:
        return anchor * (Decimal("1") - self.cfg.step_pct * Decimal(k))

    def _sell_level_from_buy(self, buy_level: Decimal) -> Decimal:
        return buy_level * (Decimal("1") + self.cfg.step_pct)

    def rebuild_buy_levels(self, anchor: Decimal) -> None:
        self.state.anchor = anchor
        self.state.buy_levels.clear()
        for k in range(1, int(self.cfg.n_buy_levels) + 1):
            p = self._buy_level(anchor, k)
            if p > 0:
                self.state.buy_levels.add(p)

    # ---------- sizing ----------
    def calc_qty_for_quote(self, spend_quote: Decimal, price: Decimal) -> Decimal:
        qty = round_step(spend_quote / price, self.cfg.qty_step)
        return qty

    def _passes_exchange_like_filters(self, qty: Decimal, price: Decimal) -> bool:
        if qty <= 0:
            return False
        if self.cfg.min_qty > 0 and qty < self.cfg.min_qty:
            return False
        if self.cfg.min_notional > 0 and (qty * price) < self.cfg.min_notional:
            return False
        return True

    def _contributed_total(self) -> Decimal:
        return self.state.contributed_total()

    def _spend_quote_now(self) -> Decimal:
        """Backtest-parity spend interpretation.

        - spend_mode='abs'             -> fixed quote amount per order
        - spend_mode='pct_contributed' -> fraction of contributed capital (initial + deposits)
        """
        mode = str(getattr(self.cfg, "spend_mode", "abs") or "abs")
        if mode == "pct_contributed":
            spend = self._contributed_total() * self.cfg.spend_quote
        else:
            spend = self.cfg.spend_quote
        return spend if spend > 0 else Decimal("0")

    def _base_value(self, mark_price: Decimal) -> Decimal:
        return self.state.base * mark_price

    def _equity(self, mark_price: Decimal) -> Decimal:
        return self.state.equity(mark_price)

    def _max_base_value(self, mark_price: Decimal) -> Optional[Decimal]:
        frac = d(getattr(self.cfg, "max_base_value_frac", Decimal("0")))
        if frac <= 0:
            return None
        return self._equity(mark_price) * frac

    def _max_base_qty(self, mark_price: Decimal) -> Optional[Decimal]:
        frac = d(getattr(self.cfg, "max_base_frac", Decimal("0")))
        if frac <= 0:
            return None
        if mark_price <= 0:
            return None
        return (self._equity(mark_price) * frac) / mark_price

    def can_add_base(self, mark_price: Decimal) -> bool:
        """Exposure cap check only (cash sufficiency is checked separately)."""
        mbv = self._max_base_value(mark_price)
        if mbv is not None and self._base_value(mark_price) >= mbv:
            return False

        mbq = self._max_base_qty(mark_price)
        if mbq is not None and self.state.base >= mbq:
            return False

        return True

    def can_place_buy_at(self, level_price: Decimal) -> bool:
        """Combined gate for adapters: exposure + filters + enough quote (incl maker fee).

        Uses the same sizing basis as backtest (`_spend_quote_now`).
        """
        if level_price <= 0:
            return False
        if not self.can_add_base(level_price):
            return False

        spend_now = self._spend_quote_now()
        if spend_now <= 0:
            return False

        qty = self.calc_qty_for_quote(spend_now, level_price)
        if not self._passes_exchange_like_filters(qty, level_price):
            return False

        cost_quote = (qty * level_price) * (Decimal("1") + self.cfg.maker_fee_rate)
        return cost_quote <= self.state.quote

    def can_place_sell_for_lot(self, lot: GridLot) -> bool:
        """Backtest-parity sell guards.

        - sell_only_above_cost: net maker proceeds must exceed lot cost
        - min_sell_markup_pct:  lot.sell_price >= lot.buy_price * (1 + markup)
        """
        if lot.qty <= 0:
            return False

        if bool(getattr(self.cfg, "sell_only_above_cost", False)):
            proceeds_net = (lot.qty * lot.sell_price) * (Decimal("1") - self.cfg.maker_fee_rate)
            if proceeds_net <= lot.cost_quote:
                return False

        min_markup = d(getattr(self.cfg, "min_sell_markup_pct", Decimal("0")))
        if min_markup > 0:
            if lot.sell_price < lot.buy_price * (Decimal("1") + min_markup):
                return False

        return True

    # ---------- bootstrap ----------
    def bootstrap(self, mark_price: Decimal) -> list[PlaceIntent]:
        """Initialize anchor + buy ladder and optionally initial base position.

        Returns a list because bootstrap may emit:
        - initial BUY (market-like via adapter using aggressive limit), and/or
        - the first passive BUY ladder order (if adapter places orders one-by-one)

        Most adapters will call `next_buy_to_place()` right after bootstrap.
        """
        if self.state.anchor is None:
            self.rebuild_buy_levels(mark_price)
        self.state.update_drawdown(mark_price)

        out: list[PlaceIntent] = []
        if self.cfg.init_base_frac > 0 and self.state.base <= 0 and self.state.quote > 0:
            spend = self.state.quote * self.cfg.init_base_frac
            qty = self.calc_qty_for_quote(spend, mark_price)
            if self._passes_exchange_like_filters(qty, mark_price):
                out.append(
                    PlaceIntent(
                        side=Side.BUY,
                        qty=qty,
                        limit_price=mark_price,
                        reason="init_base",
                    )
                )
        return out

    def next_buy_to_place(self) -> Optional[PlaceIntent]:
        """Return nearest buy level to current anchor (highest buy), if any.

        Uses backtest-parity spend sizing (`abs` or `% of contributed`).
        """
        if not self.state.buy_levels:
            return None
        bp = max(self.state.buy_levels)

        if not self.can_add_base(bp):
            return None

        spend_now = self._spend_quote_now()
        if spend_now <= 0:
            return None

        qty = self.calc_qty_for_quote(spend_now, bp)
        if not self._passes_exchange_like_filters(qty, bp):
            return None

        cost_quote = (qty * bp) * (Decimal("1") + self.cfg.maker_fee_rate)
        if cost_quote > self.state.quote:
            return None

        return PlaceIntent(side=Side.BUY, qty=qty, limit_price=bp, reason="grid_buy")

    # ---------- fill processing ----------
    def on_buy_fill(
        self,
        *,
        qty: Decimal,
        price: Decimal,
        fee_quote: Decimal,
        level_price: Optional[Decimal] = None,
    ) -> FillResult:
        """Process BUY fill and return the paired SELL intent."""
        self.state.seq += 1

        cost_quote = (qty * price) + fee_quote
        self.state.quote -= cost_quote
        self.state.base += qty
        self.state.fills_buy += 1

        # Remove matched buy level if adapter knows which passive level was filled.
        if level_price is not None and level_price in self.state.buy_levels:
            self.state.buy_levels.discard(level_price)
        else:
            # Best-effort: remove nearest level to fill price to avoid duplicate ladder levels.
            if self.state.buy_levels:
                nearest = min(self.state.buy_levels, key=lambda x: abs(x - price))
                self.state.buy_levels.discard(nearest)

        sell_anchor = level_price if level_price is not None else price
        sell_price = self._sell_level_from_buy(sell_anchor)

        lot = GridLot(
            buy_price=price,
            sell_price=sell_price,
            qty=qty,
            cost_quote=cost_quote,
            open_seq=self.state.seq,
        )
        self.state.open_lots.append(lot)

        self.state.update_drawdown(price)

        next_order: Optional[PlaceIntent] = None
        if self.can_place_sell_for_lot(lot):
            next_order = PlaceIntent(
                side=Side.SELL,
                qty=qty,
                limit_price=sell_price,
                reason="pair_after_buy",
            )

        return FillResult(
            next_order=next_order,
            realized_pnl_quote=Decimal("0"),
            matched=True,
            note="buy_opened_lot",
        )

    def on_sell_fill(
        self,
        *,
        qty: Decimal,
        price: Decimal,
        fee_quote: Decimal,
        level_price: Optional[Decimal] = None,
    ) -> FillResult:
        """Process SELL fill and close one or more lots (supports aggregated fills)."""
        if not self.state.open_lots:
            return FillResult(matched=False, note="no_open_lots")
        if qty <= 0:
            return FillResult(matched=False, note="non_positive_sell_qty")

        eps = self.cfg.qty_step if self.cfg.qty_step > 0 else Decimal("0")
        remaining = qty
        requested = qty
        total_pnl = Decimal("0")
        matched_any = False
        last_closed_buy_level = Decimal("0")

        while remaining > eps and self.state.open_lots:
            lot_idx: Optional[int] = None

            if level_price is not None:
                # For consolidated SELL orders, prefer lots closest to the target SELL level.
                lot_idx = min(
                    range(len(self.state.open_lots)),
                    key=lambda i: abs(self.state.open_lots[i].sell_price - level_price),
                )
            else:
                for i, lot in enumerate(self.state.open_lots):
                    if lot.qty == remaining:
                        lot_idx = i
                        break
                if lot_idx is None:
                    lot_idx = min(
                        range(len(self.state.open_lots)),
                        key=lambda i: abs(self.state.open_lots[i].qty - remaining),
                    )

            lot = self.state.open_lots[lot_idx]
            if lot.qty <= 0:
                self.state.open_lots.pop(lot_idx)
                continue

            sold_qty = remaining if remaining <= lot.qty else lot.qty
            if sold_qty <= 0:
                break

            fee_part = Decimal("0")
            if fee_quote > 0 and requested > 0:
                fee_part = fee_quote * (sold_qty / requested)

            lot_cost_sold = lot.cost_quote * (sold_qty / lot.qty)
            proceeds = (sold_qty * price) - fee_part
            pnl = proceeds - lot_cost_sold

            self.state.quote += proceeds
            self.state.base -= sold_qty
            self.state.fills_sell += 1
            self.state.realized_pnl_quote += pnl

            total_pnl += pnl
            matched_any = True

            lot_remaining = lot.qty - sold_qty
            lot_fully_closed = lot_remaining <= eps
            if lot_fully_closed:
                self.state.open_lots.pop(lot_idx)
                next_buy_level = lot.sell_price * (Decimal("1") - self.cfg.step_pct)
                if next_buy_level > 0:
                    self.state.buy_levels.add(next_buy_level)
                    last_closed_buy_level = next_buy_level
            else:
                lot.qty = lot_remaining
                lot.cost_quote = lot.cost_quote - lot_cost_sold

            remaining -= sold_qty

        if not matched_any:
            return FillResult(matched=False, note="no_matching_lot")

        if self.state.base < 0 and abs(self.state.base) <= eps:
            self.state.base = Decimal("0")

        self.state.update_drawdown(price)

        next_order = None
        if last_closed_buy_level > 0 and self.can_add_base(last_closed_buy_level):
            spend_now = self._spend_quote_now()
            if spend_now > 0:
                next_qty = self.calc_qty_for_quote(spend_now, last_closed_buy_level)
                if self._passes_exchange_like_filters(next_qty, last_closed_buy_level):
                    cost_quote = (next_qty * last_closed_buy_level) * (Decimal("1") + self.cfg.maker_fee_rate)
                    if cost_quote <= self.state.quote:
                        next_order = PlaceIntent(
                            side=Side.BUY,
                            qty=next_qty,
                            limit_price=last_closed_buy_level,
                            reason="pair_after_sell",
                        )

        note = "sell_closed_lot" if remaining <= eps else "sell_partially_closed_lot"
        return FillResult(
            next_order=next_order,
            realized_pnl_quote=total_pnl,
            matched=True,
            note=note,
        )

    # ---------- diagnostics ----------
    def snapshot(self, mark_price: Decimal) -> dict:
        self.state.update_drawdown(mark_price)
        return {
            "quote": self.state.quote,
            "base": self.state.base,
            "equity": self.state.equity(mark_price),
            "contributed_total": self.state.contributed_total(),
            "profit_total": self.state.profit_total(mark_price),
            "anchor": self.state.anchor,
            "buy_levels": len(self.state.buy_levels),
            "open_lots": len(self.state.open_lots),
            "fills_buy": self.state.fills_buy,
            "fills_sell": self.state.fills_sell,
            "realized_pnl_quote": self.state.realized_pnl_quote,
            "max_dd_abs": self.state.max_dd_abs,
            "max_dd_pct": self.state.max_dd_pct,
        }
