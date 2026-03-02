from __future__ import annotations

from dataclasses import dataclass, field, asdict
from decimal import Decimal
from enum import Enum
from typing import Literal, Optional

from datetime import datetime, timedelta, timezone


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

    # Reanchor behavior (15m bar-close driven in adapter, config lives here)
    reanchor_up: bool = False
    reanchor_down: bool = False
    reanchor_trigger_steps: int = 1
    reanchor_min_pct: Decimal = Decimal("0.002")

    # Exposure caps (0 = disabled)
    max_base_frac: Decimal = Decimal("0")
    max_base_value_frac: Decimal = Decimal("0")

    # Cashflows / withdrawals (stateful logic may be handled by adapter/runtime, but config belongs to strategy)
    monthly_deposit_quote: Decimal = Decimal("0")
    profit_withdraw_mode: Literal["off", "monthly", "end", "yearly", "yearly_close90"] = "off"

    # Manual confirmation window for yearly_close90 withdrawals (0 = disabled)
    yearly_withdraw_confirm_days: int = 14
    yearly_withdraw_take_frac: Decimal = Decimal("0.90")

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
        if self.profit_withdraw_mode not in ("off", "monthly", "end", "yearly", "yearly_close90"):
            raise ValueError(
                "profit_withdraw_mode must be one of: off, monthly, end, yearly, yearly_close90; "
                f"got {self.profit_withdraw_mode!r}"
            )

        if self.yearly_withdraw_confirm_days < 0:
            raise ValueError(
                f"yearly_withdraw_confirm_days must be >= 0, got {self.yearly_withdraw_confirm_days}"
            )
        if self.yearly_withdraw_take_frac < 0 or self.yearly_withdraw_take_frac > 1:
            raise ValueError(
                f"yearly_withdraw_take_frac must be in [0,1], got {self.yearly_withdraw_take_frac}"
            )


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



UTC = timezone.utc


@dataclass(slots=True)
class PendingWithdrawal:
    """Pending withdrawal request that waits for user confirmation."""

    created_at: datetime
    expires_at: datetime
    amount_quote: Decimal
    reason: str = "yearly_close90"
    year: Optional[int] = None
    status: Literal["pending", "confirmed", "expired", "cancelled"] = "pending"
    confirmed_at: Optional[datetime] = None
    expired_at: Optional[datetime] = None
    cancelled_at: Optional[datetime] = None

    def is_active(self, now: datetime) -> bool:
        if now.tzinfo is None:
            now = now.replace(tzinfo=UTC)
        return self.status == "pending" and now <= self.expires_at

    def as_dict(self) -> dict:
        data = asdict(self)
        for k in ("created_at", "expires_at", "confirmed_at", "expired_at", "cancelled_at"):
            v = data.get(k)
            if isinstance(v, datetime):
                data[k] = v.isoformat()
        return data

    @classmethod
    def from_dict(cls, row: dict) -> "PendingWithdrawal":
        def _dt(v):
            if v is None:
                return None
            if isinstance(v, datetime):
                return v if v.tzinfo is not None else v.replace(tzinfo=UTC)
            s = str(v)
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            return dt if dt.tzinfo is not None else dt.replace(tzinfo=UTC)

        return cls(
            created_at=_dt(row["created_at"]),
            expires_at=_dt(row["expires_at"]),
            amount_quote=Decimal(str(row["amount_quote"])),
            reason=str(row.get("reason", "yearly_close90")),
            year=(None if row.get("year") is None else int(row.get("year"))),
            status=str(row.get("status", "pending")),
            confirmed_at=_dt(row.get("confirmed_at")),
            expired_at=_dt(row.get("expired_at")),
            cancelled_at=_dt(row.get("cancelled_at")),
        )

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
    withdrawals_total: Decimal = Decimal("0")

    pending_withdrawal: Optional[PendingWithdrawal] = None

    # drawdown stats (mark-to-market, no withdrawals logic here)
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

    def equity_gross(self, mark_price: Decimal) -> Decimal:
        """Equity including already withdrawn profit (for total profit accounting)."""
        return self.equity(mark_price) + self.withdrawals_total

    def profit_total(self, mark_price: Decimal) -> Decimal:
        """Total profit over contributed capital, including withdrawn profit."""
        return self.equity_gross(mark_price) - self.contributed_total()

    def profit_unwithdrawn(self, mark_price: Decimal) -> Decimal:
        """Profit still inside the bot (can be negative)."""
        return self.equity(mark_price) - self.contributed_total()

    def withdrawable_profit_quote(self) -> Decimal:
        """Cash-only profit available for withdrawal without selling base."""
        contributed = self.contributed_total()
        in_pos = self.open_lots_cost_quote()
        principal_required_in_cash = contributed - in_pos
        if principal_required_in_cash < 0:
            principal_required_in_cash = Decimal("0")
        p = self.quote - principal_required_in_cash
        return p if p > 0 else Decimal("0")

    def request_withdrawal(self, amount_quote: Decimal, *, now: datetime, ttl_days: int, reason: str = "manual", year: Optional[int] = None) -> PendingWithdrawal:
        """Create or replace a pending withdrawal request (no funds are moved yet)."""
        if now.tzinfo is None:
            now = now.replace(tzinfo=UTC)
        amount = Decimal(str(amount_quote))
        if amount < 0:
            raise ValueError(f"amount_quote must be >= 0, got {amount}")
        req = PendingWithdrawal(
            created_at=now,
            expires_at=now + timedelta(days=int(ttl_days)),
            amount_quote=amount,
            reason=reason,
            year=year,
        )
        self.pending_withdrawal = req
        return req

    def confirm_pending_withdrawal(self, *, now: datetime) -> Decimal:
        """Confirm pending withdrawal and deduct quote balance if still active."""
        if now.tzinfo is None:
            now = now.replace(tzinfo=UTC)
        req = self.pending_withdrawal
        if req is None:
            return Decimal("0")
        if not req.is_active(now):
            if req.status == "pending":
                req.status = "expired"
                req.expired_at = now
            return Decimal("0")

        amount = req.amount_quote
        if amount <= 0:
            req.status = "cancelled"
            req.cancelled_at = now
            return Decimal("0")

        available = self.withdrawable_profit_quote()
        if amount > available:
            amount = available
        if amount <= 0:
            req.status = "cancelled"
            req.cancelled_at = now
            return Decimal("0")

        self.quote -= amount
        self.withdrawals_total += amount

        req.amount_quote = amount
        req.status = "confirmed"
        req.confirmed_at = now
        return amount

    def cancel_pending_withdrawal(self, *, now: datetime) -> None:
        if now.tzinfo is None:
            now = now.replace(tzinfo=UTC)
        req = self.pending_withdrawal
        if req is None:
            return
        if req.status == "pending":
            req.status = "cancelled"
            req.cancelled_at = now

    def expire_pending_withdrawal_if_needed(self, *, now: datetime) -> bool:
        if now.tzinfo is None:
            now = now.replace(tzinfo=UTC)
        req = self.pending_withdrawal
        if req is None:
            return False
        if req.status == "pending" and now > req.expires_at:
            req.status = "expired"
            req.expired_at = now
            return True
        return False

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