from __future__ import annotations

from collections import deque
from decimal import Decimal

class RollingMax:
    def __init__(self, window: int):
        self.window = window
        self.q = deque(maxlen=window)

    def update(self, price: Decimal) -> Decimal:
        self.q.append(price)
        return max(self.q) if self.q else price
