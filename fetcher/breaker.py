import time
from typing import Dict, Optional

class CircuitBreaker:
    """Per-source circuit breaker to isolate failing or dead feeds."""
    def __init__(self, failure_threshold: int = 5, base_cooldown_s: int = 60, max_cooldown_s: int = 3600):
        self.failure_threshold = failure_threshold
        self.base_cooldown = base_cooldown_s
        self.max_cooldown = max_cooldown_s
        self.state: Dict[str, str] = {}               # 'closed', 'open', 'half-open'
        self.failures: Dict[str, int] = {}
        self.open_until: Dict[str, float] = {}

    def is_available(self, source_id: str) -> bool:
        current_state = self.state.get(source_id, "closed")
        if current_state == "closed":
            return True
        now = time.time()
        if current_state == "open":
            if now >= self.open_until.get(source_id, 0.0):
                self.state[source_id] = "half-open"
                return True
            return False
        # half-open allows one probe request
        return True

    def record_success(self, source_id: str):
        self.failures[source_id] = 0
        self.state[source_id] = "closed"

    def record_failure(self, source_id: str):
        count = self.failures.get(source_id, 0) + 1
        self.failures[source_id] = count
        if count >= self.failure_threshold:
            self.state[source_id] = "open"
            cooldown = min(self.max_cooldown, self.base_cooldown * (2 ** (count - self.failure_threshold)))
            self.open_until[source_id] = time.time() + cooldown

circuit_breaker = CircuitBreaker()
