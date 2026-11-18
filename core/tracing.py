import time
import uuid
import logging
from typing import Dict, Any, List
from collections import defaultdict, deque

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s"
)
logger = logging.getLogger("kairos")

class StageMetrics:
    """Tracks latency, throughput, and lag across pipeline stages."""
    def __init__(self):
        self._latencies: Dict[str, deque] = defaultdict(lambda: deque(maxlen=200))
        self._counts: Dict[str, int] = defaultdict(int)
        self._errors: Dict[str, int] = defaultdict(int)
        self._last_processed: Dict[str, float] = {}

    def record(self, stage: str, duration_ms: float, error: bool = False):
        self._latencies[stage].append(duration_ms)
        self._counts[stage] += 1
        self._last_processed[stage] = time.time()
        if error:
            self._errors[stage] += 1

    def get_summary(self) -> Dict[str, Any]:
        result = {}
        now = time.time()
        for stage, durations in self._latencies.items():
            if durations:
                sorted_d = sorted(durations)
                p50 = sorted_d[len(sorted_d) // 2]
                p95 = sorted_d[int(len(sorted_d) * 0.95)]
                avg = sum(durations) / len(durations)
            else:
                p50, p95, avg = 0.0, 0.0, 0.0

            result[stage] = {
                "total_processed": self._counts[stage],
                "errors": self._errors[stage],
                "p50_ms": round(p50, 2),
                "p95_ms": round(p95, 2),
                "avg_ms": round(avg, 2),
                "last_active_seconds_ago": round(now - self._last_processed.get(stage, now), 1)
            }
        return result

metrics = StageMetrics()

def generate_trace_id() -> str:
    """Generates a unique per-article trace ID."""
    return f"krs_{uuid.uuid4().hex[:12]}"

class TraceContext:
    """Context manager for tracing stage execution time."""
    def __init__(self, stage: str, trace_id: str):
        self.stage = stage
        self.trace_id = trace_id
        self.start_time = 0.0

    def __enter__(self):
        self.start_time = time.perf_counter()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        duration_ms = (time.perf_counter() - self.start_time) * 1000.0
        is_error = exc_type is not None
        metrics.record(self.stage, duration_ms, error=is_error)
        logger.debug(
            f"trace={self.trace_id} stage={self.stage} duration={duration_ms:.2f}ms error={is_error}"
        )
