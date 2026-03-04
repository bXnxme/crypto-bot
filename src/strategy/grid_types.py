from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Literal, Optional



class Side(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


@dataclass(slots=True)
class GridConfig:
    """Pure strategy config used by paper/live adapters."""

    step_pct: Decimal
    n_buy_levels: int
    spend_quote: Decimal
    maker_fee_rate: Decimal
    taker_fee_rate: Decimal = Decimal("0")
    qty_step: Decimal = Decimal("0.000001")
    min_qty: Decimal = Decimal("0")
    min_notional: Decimal = Decimal("0")
    init_base_frac: Decimal = Decimal("0")

    # spend sizing mode parity with backtest:
    # - "abs": spend_quote is a fixed quote amount per order
    # - "pct_contributed": spend_quote is a fraction (or percent converted upstream) of contributed capital
    spend_mode: Literal["abs", "pct_contributed"] = "abs"

    # Sell guards / fee-aware behavior
    sell_only_above_cost: bool = False
    min_sell_markup_pct: Decimal = Decimal("0")

    # Reanchor behavior (bar-close driven in adapter, config lives here)
    reanchor_up: bool = False
    reanchor_down: bool = False
    reanchor_trigger_steps: int = 1
    reanchor_min_pct: Decimal = Decimal("0.002")

    # Exposure caps (0 = disabled)
    max_base_frac: Decimal = Decimal("0")
    max_base_value_frac: Decimal = Decimal("0")

    # Cashflows
    monthly_deposit_quote: Decimal = Decimal("0")

    def validate(self) -> None:
        if self.step_pct <= 0:
            raise ValueError(f"step_pct must be > 0, got {self.step_pct}")
        if self.n_buy_levels <= 0:
            raise ValueError(f"n_buy_levels must be > 0, got {self.n_buy_levels}")
        if self.spend_quote <= 0:
            raise ValueError(f"spend_quote must be > 0, got {self.spend_quote}")
        if self.qty_step <= 0:
            raise ValueError(f"qty_step must be > 0, got {self.qty_step}")
        if self.maker_fee_rate < 0:
            raise ValueError(f"maker_fee_rate must be >= 0, got {self.maker_fee_rate}")
        if self.taker_fee_rate < 0:
            raise ValueError(f"taker_fee_rate must be >= 0, got {self.taker_fee_rate}")

        if self.spend_mode not in ("abs", "pct_contributed"):
            raise ValueError(f"spend_mode must be 'abs' or 'pct_contributed', got {self.spend_mode!r}")
        if self.spend_mode == "pct_contributed" and self.spend_quote > 1:
            raise ValueError(
                f"spend_quote must be a fraction in (0,1] when spend_mode='pct_contributed', got {self.spend_quote}"
            )

        if self.min_sell_markup_pct < 0:
            raise ValueError(f"min_sell_markup_pct must be >= 0, got {self.min_sell_markup_pct}")

        if self.reanchor_trigger_steps <= 0:
            raise ValueError(f"reanchor_trigger_steps must be > 0, got {self.reanchor_trigger_steps}")
        if self.reanchor_min_pct < 0:
            raise ValueError(f"reanchor_min_pct must be >= 0, got {self.reanchor_min_pct}")

        if self.max_base_frac < 0 or self.max_base_frac > 1:
            raise ValueError(f"max_base_frac must be in [0,1], got {self.max_base_frac}")
        if self.max_base_value_frac < 0 or self.max_base_value_frac > 1:
            raise ValueError(f"max_base_value_frac must be in [0,1], got {self.max_base_value_frac}")

        if self.monthly_deposit_quote < 0:
            raise ValueError(f"monthly_deposit_quote must be >= 0, got {self.monthly_deposit_quote}")


@dataclass(slots=True)
class GridLot:
    buy_price: Decimal
    sell_price: Decimal
    qty: Decimal
    cost_quote: Decimal  # buy cost incl. fee
    open_seq: int


@dataclass(slots=True)
class PlaceIntent:
    side: Side
    qty: Decimal
    limit_price: Decimal
    reason: str


@dataclass(slots=True)
class FillResult:
    """Strategy reaction to a broker fill."""

    next_order: Optional[PlaceIntent] = None
    realized_pnl_quote: Decimal = Decimal("0")
    matched: bool = False
    note: str = ""

@dataclass(slots=True)
class GridState:
    quote: Decimal
    base: Decimal
    anchor: Optional[Decimal] = None
    buy_levels: set[Decimal] = field(default_factory=set)
    open_lots: list[GridLot] = field(default_factory=list)
    seq: int = 0

    fills_buy: int = 0
    fills_sell: int = 0
    realized_pnl_quote: Decimal = Decimal("0")

    # cashflow accounting for parity with backtest reporting / sizing
    initial_quote: Decimal = Decimal("0")
    deposits_total: Decimal = Decimal("0")

    # drawdown stats (mark-to-market)
    peak_equity: Optional[Decimal] = None
    max_dd_abs: Decimal = Decimal("0")
    max_dd_pct: Decimal = Decimal("0")

    def equity(self, mark_price: Decimal) -> Decimal:
        return self.quote + (self.base * mark_price)

    def contributed_total(self) -> Decimal:
        """Total principal contributed to the strategy (initial + deposits)."""
        return self.initial_quote + self.deposits_total

    def open_lots_cost_quote(self) -> Decimal:
        """Quote principal tied up in currently open lots (includes buy fees)."""
        return sum((lot.cost_quote for lot in self.open_lots), Decimal("0"))

    def profit_total(self, mark_price: Decimal) -> Decimal:
        """Total profit over contributed capital."""
        return self.equity(mark_price) - self.contributed_total()

    def __post_init__(self) -> None:
        # If caller doesn't pass initial_quote explicitly, treat current quote as the initial principal.
        if self.initial_quote == Decimal("0") and self.quote > 0:
            self.initial_quote = self.quote

    def update_drawdown(self, mark_price: Decimal) -> None:
        eq = self.equity(mark_price)
        if self.peak_equity is None or eq > self.peak_equity:
            self.peak_equity = eq
        if self.peak_equity is None or self.peak_equity <= 0:
            return

        dd_abs = self.peak_equity - eq
        if dd_abs > self.max_dd_abs:
            self.max_dd_abs = dd_abs

        dd_pct = dd_abs / self.peak_equity
        if dd_pct > self.max_dd_pct:
            self.max_dd_pct = dd_pct
