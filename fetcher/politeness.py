import time
import asyncio
from typing import Dict
from urllib.parse import urlparse

class PolitenessLimiter:
    """Per-host token bucket rate limiter to prevent hammering news servers."""
    def __init__(self, min_delay_seconds: float = 1.5):
        self.min_delay = min_delay_seconds
        self.last_fetch: Dict[str, float] = {}
        self._lock = asyncio.Lock()

    async def wait_for_host(self, url: str):
        host = urlparse(url).netloc
        async with self._lock:
            now = time.time()
            last = self.last_fetch.get(host, 0.0)
            elapsed = now - last
            if elapsed < self.min_delay:
                wait_time = self.min_delay - elapsed
                await asyncio.sleep(wait_time)
            self.last_fetch[host] = time.time()

politeness = PolitenessLimiter()
