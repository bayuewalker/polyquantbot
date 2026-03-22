"""
core/execution_guard.py  —  PolyBot v7.1 FINAL

Central safety layer: FSM, idempotency, locks, position lock,
order-timeout index, global halt, failure-rate gate, persistence.

FSM transitions:
  INIT -> REGISTERED -> PLACED -> PARTIAL_FILL -> FILLED -> CLOSED
                     -> FAILED
                                -> FAILED
                                -> CANCELLED
                     -> CANCELLED
       -> FAILED
"""
from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import time
from typing import Optional

from core.circuit_breaker import CircuitBreaker
from core.persistence import PersistenceManager

log = logging.getLogger("polybot.execution_guard")


class TradeState:
    INIT         = "INIT"
    REGISTERED   = "REGISTERED"
    PLACED       = "PLACED"
    PARTIAL_FILL = "PARTIAL_FILL"
    FILLED       = "FILLED"
    CLOSED       = "CLOSED"
    CANCELLED    = "CANCELLED"
    FAILED       = "FAILED"

    _TRANSITIONS: set[tuple[str, str]] = {
        ("INIT",         "REGISTERED"),
        ("REGISTERED",   "PLACED"),
        ("REGISTERED",   "FAILED"),
        ("PLACED",       "FILLED"),
        ("PLACED",       "PARTIAL_FILL"),
        ("PLACED",       "FAILED"),
        ("PLACED",       "CANCELLED"),
        ("PLACED",       "REGISTERED"),
        ("PARTIAL_FILL", "FILLED"),
        ("PARTIAL_FILL", "CANCELLED"),
        ("PARTIAL_FILL", "FAILED"),
        ("FILLED",       "CLOSED"),
    }

    @classmethod
    def transition(cls, current: str, target: str, trade_id: str) -> str:
        if (current, target) not in cls._TRANSITIONS:
            raise RuntimeError(
                f"FSM illegal transition {current}->{target} for trade_id={trade_id}"
            )
        log.debug("FSM %s  %s -> %s", trade_id, current, target)
        return target


class ExecutionGuard:

    TRADE_TTL_S: float = 300.0
    WINDOW_S:    int   = 60

    def __init__(
        self,
        max_concurrent:  int   = 3,
        order_timeout_s: float = 60.0,
        persistence:     Optional[PersistenceManager]  = None,
        circuit_breaker: Optional[CircuitBreaker]      = None,
    ) -> None:
        self.active_trades:  dict[str, dict]           = {}
        self._market_locks:  dict[tuple, asyncio.Lock] = {}
        self._reg_lock:      asyncio.Lock              = asyncio.Lock()
        self._exec_sem:      asyncio.Semaphore         = asyncio.Semaphore(max_concurrent)

        self.last_trade_time:  dict[str, float] = {}
        self.trade_cooldown_s: float = float(os.getenv("TRADE_COOLDOWN_S", "300"))
        self.active_positions: dict[str, bool]  = {}
        self.order_timeout_s:  float            = order_timeout_s

        self.GLOBAL_TRADING_ENABLED: bool  = True
        self._halt_reason:            str   = ""

        self._recent_outcomes:        list[bool] = []
        self._failure_rate_window:    int   = int(os.getenv("CB_RATE_WINDOW",   "20"))
        self._failure_rate_threshold: float = float(os.getenv("CB_RATE_THRESH", "0.50"))

        self._persistence    = persistence     or PersistenceManager()
        self.circuit_breaker = circuit_breaker or CircuitBreaker()

        self._load_state()

        log.info(
            "ExecutionGuard ready | max_concurrent=%d timeout=%.0fs cooldown=%.0fs",
            max_concurrent, order_timeout_s, self.trade_cooldown_s,
        )

    # ── Global halt ────────────────────────────────────────────────────────────

    def halt(self, reason: str) -> None:
        if self.GLOBAL_TRADING_ENABLED:
            self.GLOBAL_TRADING_ENABLED = False
            self._halt_reason = reason
            log.critical("GLOBAL HALT: %s", reason)

    def resume(self) -> None:
        self.GLOBAL_TRADING_ENABLED = True
        self._halt_reason = ""
        self.circuit_breaker.reset()
        self._recent_outcomes.clear()
        log.warning("GLOBAL TRADING RESUMED by operator")

    @property
    def is_halted(self) -> bool:
        return (not self.GLOBAL_TRADING_ENABLED) or self.circuit_breaker.is_tripped

    # ── Outcomes ──────────────────────────────────────────────────────────────

    def record_success(self, market_id: str) -> None:
        self.last_trade_time[market_id] = time.time()
        self.circuit_breaker.record_success()
        self._push_outcome(True)

    def record_failure(self, loss_usd: float = 0.0) -> None:
        self.circuit_breaker.record_failure(loss_usd)
        self._push_outcome(False)

    def _push_outcome(self, success: bool) -> None:
        self._recent_outcomes.append(success)
        if len(self._recent_outcomes) > self._failure_rate_window:
            self._recent_outcomes.pop(0)
        if len(self._recent_outcomes) >= self._failure_rate_window:
            fails = self._recent_outcomes.count(False)
            rate  = fails / len(self._recent_outcomes)
            if rate > self._failure_rate_threshold:
                self.halt(
                    f"failure_rate={rate:.0%}>{self._failure_rate_threshold:.0%} "
                    f"over last {len(self._recent_outcomes)} trades"
                )

    # ── Trade ID ──────────────────────────────────────────────────────────────

    def trade_id(self, market_id: str, side: str) -> str:
        bucket = int(time.time() // self.WINDOW_S)
        raw    = f"{market_id}:{side}:{bucket}"
        return hashlib.sha256(raw.encode()).hexdigest()[:16]

    # ── Duplicate check ───────────────────────────────────────────────────────

    def is_duplicate(self, trade_id: str) -> bool:
        self._evict_stale()
        entry = self.active_trades.get(trade_id)
        return entry is not None and entry.get("state") not in (
            TradeState.FAILED, TradeState.CANCELLED
        )

    # ── Cooldown ──────────────────────────────────────────────────────────────

    def is_cooled_down(self, market_id: str) -> bool:
        last = self.last_trade_time.get(market_id, 0.0)
        return (time.time() - last) >= self.trade_cooldown_s

    # ── Position lock ─────────────────────────────────────────────────────────

    def has_open_position(self, market_id: str) -> bool:
        return self.active_positions.get(market_id, False)

    def set_position_open(self, market_id: str) -> None:
        self.active_positions[market_id] = True

    def set_position_closed(self, market_id: str) -> None:
        self.active_positions.pop(market_id, None)

    # ── Locks ─────────────────────────────────────────────────────────────────

    def get_market_lock(self, market_id: str, side: str) -> asyncio.Lock:
        key = (market_id, side)
        if key not in self._market_locks:
            self._market_locks[key] = asyncio.Lock()
        return self._market_locks[key]

    # ── FSM ───────────────────────────────────────────────────────────────────

    def _fsm_get(self, trade_id: str) -> str:
        entry = self.active_trades.get(trade_id)
        return entry["state"] if entry else TradeState.INIT

    def _fsm_set(self, trade_id: str, target: str) -> None:
        current   = self._fsm_get(trade_id)
        new_state = TradeState.transition(current, target, trade_id)
        if trade_id in self.active_trades:
            self.active_trades[trade_id]["state"] = new_state
        elif target == TradeState.REGISTERED:
            self.active_trades[trade_id] = {
                "state":         TradeState.REGISTERED,
                "timestamp":     time.time(),
                "placed_at":     None,
                "order_id":      "",
                "market_id":     "",
                "side":          "",
                "original_size": 0.0,
                "filled_size":   0.0,
            }
        log.info("FSM trade_id=%s  %s -> %s", trade_id, current, new_state)
        self._save_state()

    def set_order_id(self, trade_id: str, order_id: str) -> None:
        if trade_id in self.active_trades:
            self.active_trades[trade_id]["order_id"]  = order_id
            self.active_trades[trade_id]["placed_at"] = time.time()
            self._save_state()

    def set_trade_meta(self, trade_id: str, market_id: str, side: str, size: float) -> None:
        if trade_id in self.active_trades:
            self.active_trades[trade_id].update({
                "market_id":     market_id,
                "side":          side,
                "original_size": size,
            })

    def fail(self, trade_id: str) -> None:
        try:
            self._fsm_set(trade_id, TradeState.FAILED)
        except RuntimeError:
            pass
        self.active_trades.pop(trade_id, None)
        self._save_state()
        log.info("FSM trade_id=%s -> FAILED (removed)", trade_id)

    # ── Timeout index ─────────────────────────────────────────────────────────

    def timed_out_orders(self) -> list[tuple[str, str]]:
        cutoff = time.time() - self.order_timeout_s
        return [
            (tid, e["order_id"])
            for tid, e in list(self.active_trades.items())
            if e.get("state") == TradeState.PLACED
            and (e.get("placed_at") or e.get("timestamp", 0)) < cutoff
            and e.get("order_id")
        ]

    # ── Persistence ───────────────────────────────────────────────────────────

    def _save_state(self) -> None:
        self._persistence.save(self.active_trades)

    def _load_state(self) -> None:
        loaded = self._persistence.load()
        self.active_trades.update(loaded)
        log.info("ExecutionGuard: restored %d trade(s) from disk", len(loaded))

    # ── Eviction ──────────────────────────────────────────────────────────────

    def _evict_stale(self) -> None:
        cutoff = time.time() - self.TRADE_TTL_S
        stale  = [
            tid for tid, e in self.active_trades.items()
            if e.get("timestamp", 0) < cutoff
        ]
        for tid in stale:
            del self.active_trades[tid]
        if stale:
            log.debug("ExecutionGuard: evicted %d stale trade(s)", len(stale))
