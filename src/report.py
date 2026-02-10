from __future__ import annotations

import csv
from decimal import Decimal
from pathlib import Path

TRADES_CSV = Path("logs/trades.csv")

def d(x: str) -> Decimal:
    return Decimal(x)

def main() -> None:
    if not TRADES_CSV.exists():
        print("No trades yet: logs/trades.csv not found")
        return

    rows = []
    with TRADES_CSV.open("r", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            row["pnl_usdt"] = d(row["pnl_usdt"])
            row["buy_quote_usdt"] = d(row["buy_quote_usdt"])
            row["sell_quote_usdt"] = d(row["sell_quote_usdt"])
            rows.append(row)

    n = len(rows)
    total_pnl = sum((x["pnl_usdt"] for x in rows), Decimal("0"))
    avg_pnl = (total_pnl / n) if n else Decimal("0")

    wins = sum(1 for x in rows if x["pnl_usdt"] > 0)
    winrate = (wins / n * 100) if n else 0.0

    gross_profit = sum((x["pnl_usdt"] for x in rows if x["pnl_usdt"] > 0), Decimal("0"))
    gross_loss = -sum((x["pnl_usdt"] for x in rows if x["pnl_usdt"] < 0), Decimal("0"))  # positive number
    profit_factor = (gross_profit / gross_loss) if gross_loss != 0 else Decimal("0")

    turnover = sum((x["buy_quote_usdt"] + x["sell_quote_usdt"] for x in rows), Decimal("0"))

    best_pnl = max((x["pnl_usdt"] for x in rows), default=Decimal("0"))
    worst_pnl = min((x["pnl_usdt"] for x in rows), default=Decimal("0"))
    best_rows = [x for x in rows if x["pnl_usdt"] == best_pnl]
    worst_rows = [x for x in rows if x["pnl_usdt"] == worst_pnl]

    print(f"Trades: {n}")
    print(f"Total PnL (USDT): {total_pnl}")
    print(f"Avg PnL (USDT):   {avg_pnl}")
    print(f"Winrate:          {winrate:.2f}%")
    print(f"Turnover (USDT):  {turnover}")
    print(f"Profit factor:    {profit_factor}")

    def fmt_row(label: str, row: dict) -> str:
        return (
            f"{label}: pnl={row['pnl_usdt']} | "
            f"buy={row['buy_quote_usdt']} sell={row['sell_quote_usdt']} | "
            f"ids={row['buy_order_id']}/{row['sell_order_id']}"
        )

    # Show up to 2 examples if many ties
    if best_rows:
        print(fmt_row("Best", best_rows[0]))
        if len(best_rows) > 1:
            print(fmt_row("Best (tie)", best_rows[1]))

    if worst_rows:
        print(fmt_row("Worst", worst_rows[0]))
        if len(worst_rows) > 1:
            print(fmt_row("Worst (tie)", worst_rows[1]))

    print("\nLast 5 trades:")
    for row in rows[-5:]:
        print(fmt_row("-", row))

if __name__ == "__main__":
    main()
