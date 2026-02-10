from __future__ import annotations

import random
import time
from typing import Callable, TypeVar, Tuple

T = TypeVar("T")


def retry_call(
    fn: Callable[[], T],
    *,
    retries: int = 5,
    base_delay: float = 0.5,
    max_delay: float = 8.0,
    retry_exceptions: Tuple[type[BaseException], ...] = (Exception,),
    on_retry: Callable[[int, BaseException, float], None] | None = None,
) -> T:
    attempt = 0
    while True:
        try:
            return fn()
        except retry_exceptions as e:
            attempt += 1
            if attempt > retries:
                raise

            delay = min(max_delay, base_delay * (2 ** (attempt - 1)))
            delay = delay * (0.7 + random.random() * 0.6)  # jitter ~ [0.7..1.3]

            if on_retry:
                on_retry(attempt, e, delay)

            time.sleep(delay)