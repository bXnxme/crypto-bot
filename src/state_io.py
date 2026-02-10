from __future__ import annotations

import json
from pathlib import Path
from decimal import Decimal

from src.strategy import BotState, state_from_dict, state_to_dict

def _state_path(symbol: str) -> Path:
    safe = symbol.replace("/", "_")
    return Path("logs") / f"state_{safe}.json"

def load_state(symbol: str, initial_price: Decimal) -> BotState:
    path = _state_path(symbol)
    if path.exists():
        data = json.loads(path.read_text())
        return state_from_dict(data)
    state = BotState.default(initial_price)
    save_state(symbol, state)
    return state

def save_state(symbol: str, state: BotState) -> None:
    path = _state_path(symbol)
    path.parent.mkdir(exist_ok=True)
    path.write_text(json.dumps(state_to_dict(state), ensure_ascii=False, indent=2))
