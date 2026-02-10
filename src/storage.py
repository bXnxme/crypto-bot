from __future__ import annotations

import csv
from pathlib import Path
from typing import Dict, Any

def append_trade_row(csv_path: Path, row: Dict[str, Any]) -> None:
    csv_path.parent.mkdir(exist_ok=True)
    file_exists = csv_path.exists()

    fieldnames = [
        "ts_ms",
        "symbol",
        "buy_order_id",
        "sell_order_id",
        "buy_qty_btc",
        "buy_quote_usdt",
        "sell_qty_btc",
        "sell_quote_usdt",
        "pnl_usdt",
    ]

    with csv_path.open("a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            w.writeheader()
        w.writerow(row)
