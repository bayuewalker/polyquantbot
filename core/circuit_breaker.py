"""
core/circuit_breaker.py  —  PolyBot v7.1 FINAL
Three independent trip conditions:
  1. N consecutive failures without a success
  2. Failure rate over rolling window exceeds threshold
  3. Cumulative session loss exceeds limit
Manual reset only.
"""
from __future__ import annotations

import logging
import os
import time

log = logging.getLogger("polybot.circuit_breaker")


class CircuitBreaker:

    def __init__(
        self,
        consecutive_fail_max: int = 5,
        failure_rate_window: int = 20,
        failure_rate_thresh: float = 0.50,
        session_loss_max_usd: float = 200.0,
    ) -> None:
        self.consecutive_fail_max = int(
            os.getenv("CB_CONSEC_MAX", str(consecutive_fail_max))
        )
        self.failure_rate_window = int(
            os.getenv("CB_RATE_WINDOW", str(failure_rate_window))
        )
        self.failure_rate_thresh = float(
            os.getenv("CB_RATE_THRESH", str(failure_rate_thresh))
        )
        self.session_loss_max_usd = float(
            os.getenv("CB_LOSS_MAX", str(session_loss_max_usd))
        )

        self._tripped: bool = False
        self._trip_reason: str = ""
        self._trip_time: float = 0.0
        self._consecutive_failures: int = 0
        self._session_loss: float = 0.0
        self._recent_outcomes: list[bool] = []

        log.info(
            "CircuitBreaker ready | consec_max=%d rate_window=%d "
            "rate_thresh=%.0f%% loss_max=$%.0f",
            self.consecutive_fail_max,
            self.failure_rate_window,
            self.failure_rate_thresh * 100,
            self.session_loss_max_usd,
        )

    @property
    def is_tripped(self) -> bool:
        return self._tripped

    @property
    def trip_reason(self) -> str:
        return self._trip_reason

    def status(self) -> dict:
        c = self._recent_outcomes
        fail_n = c.count(False)
        rate = fail_n / len(c) if c else 0.0
        return {
            "tripped": self._tripped,
            "trip_reason": self._trip_reason,
            "trip_time": self._trip_time,
            "consecutive_failures": self._consecutive_failures,
            "session_loss_usd": round(self._session_loss, 2),
            "recent_failure_rate": round(rate, 3),
            "recent_window": len(c),
        }

    def record_success(self) -> None:
        self._consecutive_failures = 0
        self._push_outcome(True)

    def record_failure(self, loss_usd: float = 0.0) -> None:
        self._consecutive_failures += 1
        if loss_usd > 0.0:
            self._session_loss += loss_usd
        self._push_outcome(False)
        self._evaluate()

    def record_loss(self, loss_usd: float) -> None:
        if loss_usd <= 0.0:
            return
        self._session_loss += loss_usd
        self._evaluate()

    def reset(self) -> None:
        log.warning(
            "CircuitBreaker RESET | was_tripped=%s reason=%r "
            "consec=%d loss=$%.2f",
            self._tripped,
            self._trip_reason,
            self._consecutive_failures,
            self._session_loss,
        )
        self._tripped = False
        self._trip_reason = ""
        self._trip_time = 0.0
        self._consecutive_failures = 0
        self._session_loss = 0.0
        self._recent_outcomes.clear()

    def _push_outcome(self, success: bool) -> None:
        self._recent_outcomes.append(success)
        if len(self._recent_outcomes) > self.failure_rate_window:
            self._recent_outcomes.pop(0)

    def _evaluate(self) -> None:
        if self._tripped:
            return

        if self._consecutive_failures >= self.consecutive_fail_max:
            self._trip(
                f"consecutive_failures={self._consecutive_failures}"
                f">={self.consecutive_fail_max}"
            )
            return

        if len(self._recent_outcomes) >= self.failure_rate_window:
            fail_n = self._recent_outcomes.count(False)
            rate = fail_n / len(self._recent_outcomes)
            if rate > self.failure_rate_thresh:
                self._trip(
                    f"failure_rate={rate:.0%}>{self.failure_rate_thresh:.0%} "
                    f"over last {len(self._recent_outcomes)} trades"
                )
                return

        if self._session_loss >= self.session_loss_max_usd:
            self._trip(
                f"session_loss=${self._session_loss:.2f}"
                f">=${self.session_loss_max_usd:.2f}"
            )

    def _trip(self, reason: str) -> None:
        if self._tripped:
            return
        self._tripped = True
        self._trip_reason = reason
        self._trip_time = time.time()
        log.critical(
            "CIRCUIT BREAKER TRIPPED: %s — ALL TRADING HALTED", reason
        )
