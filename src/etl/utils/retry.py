
from __future__ import annotations
import time, random
from typing import Callable, Type, Tuple, Any

def retry_call(
    func: Callable[..., Any],
    *args,
    retries: int = 3,
    backoff_base: float = 0.8,
    backoff_factor: float = 2.0,
    jitter: float = 0.2,
    exceptions: Tuple[Type[BaseException], ...] = (Exception,),
    **kwargs,
):
    attempt = 0
    while True:
        try:
            return func(*args, **kwargs)
        except exceptions:
            attempt += 1
            if attempt > retries:
                raise
            sleep_s = backoff_base * (backoff_factor ** (attempt - 1))
            sleep_s *= (1.0 + random.uniform(-jitter, jitter))
            time.sleep(max(0.1, sleep_s))
