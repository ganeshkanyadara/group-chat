import time
from collections import deque
import threading
import psutil


class MetricsTracker:
    """
    Thread-safe, lightweight performance and load metrics tracker.
    
    Tracks active concurrent requests, monotonic request latencies,
    request counts, and non-blocking system CPU & memory utilization
    for dynamic load balancing.
    """

    def __init__(self, max_recent_samples: int = 100):
        self._lock = threading.Lock()
        self._active_requests: int = 0
        self._total_requests: int = 0
        self._total_latency_ms: float = 0.0
        self._recent_latencies: deque[float] = deque(maxlen=max_recent_samples)
        self._start_monotonic: float = time.monotonic()
        self._start_timestamp: float = time.time()
        # Prime psutil non-blocking cpu calculation
        psutil.cpu_percent(interval=None)

    def increment_active(self) -> None:
        """Increment active requests safely."""
        with self._lock:
            self._active_requests += 1

    def decrement_active(self) -> None:
        """Decrement active requests safely, never dropping below zero."""
        with self._lock:
            if self._active_requests > 0:
                self._active_requests -= 1

    def record_request(self, duration_ms: float) -> None:
        """Record a completed request duration in milliseconds."""
        with self._lock:
            self._total_requests += 1
            self._total_latency_ms += duration_ms
            self._recent_latencies.append(duration_ms)

    @property
    def active_requests(self) -> int:
        with self._lock:
            return self._active_requests

    @property
    def request_count(self) -> int:
        with self._lock:
            return self._total_requests

    @property
    def avg_latency_ms(self) -> float:
        with self._lock:
            if self._total_requests == 0:
                return 0.0
            return round(self._total_latency_ms / self._total_requests, 2)

    @property
    def recent_latency_ms(self) -> float:
        with self._lock:
            if not self._recent_latencies:
                return 0.0
            return round(sum(self._recent_latencies) / len(self._recent_latencies), 2)

    @property
    def uptime_seconds(self) -> int:
        return int(time.time() - self._start_timestamp)

    def get_metrics(self, backend_id: str, is_healthy: bool = True) -> dict:
        """
        Produce structured JSON performance metric payload for the Load Balancer.
        Uses non-blocking system calls to avoid delaying request processing.
        """
        cpu = psutil.cpu_percent(interval=None)
        memory = psutil.virtual_memory().percent

        return {
            "backend_id": backend_id,
            "healthy": is_healthy,
            "cpu_percent": round(cpu, 1),
            "memory_percent": round(memory, 1),
            "active_requests": self.active_requests,
            "avg_latency_ms": self.avg_latency_ms,
            "recent_latency_ms": self.recent_latency_ms,
            "request_count": self.request_count,
            "uptime_seconds": self.uptime_seconds,
        }


# Global metrics tracker instance
metrics_tracker = MetricsTracker()
