"""
core/edge_filter.py  —  PolyBot v7.1 FINAL
Statistical edge filter v1.0.1
Pure synchronous, <1ms, zero I/O, zero async.

Five gates (fail-fast, cheapest first):
  1. PROB_SANITY    — p_model outside [prob_min, prob_max]
  2. LOW_EV         — ev < ev_threshold  (EPS-tolerant)
  3. WEAK_SIGNAL    — z_score < z_threshold  (EPS-tolerant)
  4. WIDE_SPREAD    — spread > spread_max
  5. LOW_LIQUIDITY  — liquidity < liquidity_min
"""
from __future__ import annotations

import os
import logging
from dataclasses import dataclass

log = logging.getLogger("polybot.edge_filter")

# Float tolerance: prevents spurious rejections at exact threshold boundary
# caused by IEEE-754 representation error (e.g. 0.585-0.555 = 0.02999...).
EPS: float = 1e-6

_D_EV_THRESHOLD: float = 0.03
_D_Z_THRESHOLD: float = 1.5
_D_SPREAD_MAX: float = 0.05
_D_LIQUIDITY_MIN: float = 10_000.0
_D_PROB_MIN: float = 0.05
_D_PROB_MAX: float = 0.95


def _ef(key: str, default: float) -> float:
    try:
        return float(os.environ[key])
    except (KeyError, ValueError):
        return default


@dataclass(frozen=True)
class EdgeContext:
    p_model: float       # Bayesian posterior probability
    p_market: float      # CLOB market price
    best_bid: float      # orderbook best bid
    best_ask: float      # orderbook best ask
    liquidity: float     # market depth USD
    volatility: float = 0.05   # rolling price std-dev (sigma)
    market_id: str = ""
    question: str = ""
    side: str = ""
    strategy: str = ""


@dataclass
class FilterResult:
    allowed: bool
    reason: str          # "PASS" | "LOW_EV" | "WEAK_SIGNAL" | "WIDE_SPREAD" | "LOW_LIQUIDITY" | "PROB_SANITY"
    ev: float = 0.0
    z_score: float = 0.0
    spread: float = 0.0
    mid: float = 0.0


class EdgeFilter:

    def __init__(
        self,
        ev_threshold: float | None = None,
        z_threshold: float | None = None,
        spread_max: float | None = None,
        liquidity_min: float | None = None,
        prob_min: float | None = None,
        prob_max: float | None = None,
    ) -> None:
        self.ev_threshold  = ev_threshold  if ev_threshold  is not None else _ef("EF_EV_THRESHOLD",  _D_EV_THRESHOLD)
        self.z_threshold   = z_threshold   if z_threshold   is not None else _ef("EF_Z_THRESHOLD",   _D_Z_THRESHOLD)
        self.spread_max    = spread_max    if spread_max    is not None else _ef("EF_SPREAD_MAX",     _D_SPREAD_MAX)
        self.liquidity_min = liquidity_min if liquidity_min is not None else _ef("EF_LIQUIDITY_MIN",  _D_LIQUIDITY_MIN)
        self.prob_min      = prob_min      if prob_min      is not None else _ef("EF_PROB_MIN",       _D_PROB_MIN)
        self.prob_max      = prob_max      if prob_max      is not None else _ef("EF_PROB_MAX",       _D_PROB_MAX)

        self._n_pass: int = 0
        self._n_prob_sanity: int = 0
        self._n_low_ev: int = 0
        self._n_weak_signal: int = 0
        self._n_wide_spread: int = 0
        self._n_low_liquidity: int = 0

        log.info(
            "EdgeFilter v1.0.1 | EV>=%.3f Z>=%.1f spread<=%.3f liq>=$%.0f p=[%.2f,%.2f]",
            self.ev_threshold, self.z_threshold, self.spread_max,
            self.liquidity_min, self.prob_min, self.prob_max,
        )

    def evaluate(self, ctx: EdgeContext) -> FilterResult:
        sigma   = max(ctx.volatility, 0.01)
        ev      = ctx.p_model - ctx.p_market
        z_score = ev / sigma
        spread  = ctx.best_ask - ctx.best_bid
        mid     = (ctx.best_bid + ctx.best_ask) * 0.5

        def _reject(reason: str) -> FilterResult:
            self._inc_counter(reason)
            log.info(
                "EdgeFilter REJECT [%s] %s | %s ev=%.4f z=%.2f spread=%.4f liq=%.0f",
                ctx.market_id[:12], ctx.side, reason,
                ev, z_score, spread, ctx.liquidity,
            )
            return FilterResult(
                allowed=False, reason=reason,
                ev=round(ev, 4), z_score=round(z_score, 3),
                spread=round(spread, 4), mid=round(mid, 4),
            )

        if not (self.prob_min <= ctx.p_model <= self.prob_max):
            return _reject("PROB_SANITY")

        if ev < (self.ev_threshold - EPS):
            return _reject("LOW_EV")

        if z_score < (self.z_threshold - EPS):
            return _reject("WEAK_SIGNAL")

        if spread > self.spread_max:
            return _reject("WIDE_SPREAD")

        if ctx.liquidity < self.liquidity_min:
            return _reject("LOW_LIQUIDITY")

        self._n_pass += 1
        log.info(
            "EdgeFilter PASS [%s] %s | ev=%.4f z=%.2f spread=%.4f liq=%.0f",
            ctx.market_id[:12], ctx.side, ev, z_score, spread, ctx.liquidity,
        )
        return FilterResult(
            allowed=True, reason="PASS",
            ev=round(ev, 4), z_score=round(z_score, 3),
            spread=round(spread, 4), mid=round(mid, 4),
        )

    def counters(self) -> dict[str, int]:
        return {
            "PASS":          self._n_pass,
            "PROB_SANITY":   self._n_prob_sanity,
            "LOW_EV":        self._n_low_ev,
            "WEAK_SIGNAL":   self._n_weak_signal,
            "WIDE_SPREAD":   self._n_wide_spread,
            "LOW_LIQUIDITY": self._n_low_liquidity,
        }

    def stats_summary(self) -> str:
        c = self.counters()
        total  = sum(c.values())
        passed = c["PASS"]
        rate   = f"{passed / total * 100:.1f}%" if total else "n/a"
        parts  = [f"EdgeFilter: {passed}/{total} passed ({rate})"]
        for reason in ("LOW_EV", "WEAK_SIGNAL", "WIDE_SPREAD", "LOW_LIQUIDITY", "PROB_SANITY"):
            if c.get(reason, 0):
                parts.append(f"{reason}={c[reason]}")
        return " | ".join(parts)

    def _inc_counter(self, reason: str) -> None:
        if   reason == "PROB_SANITY":   self._n_prob_sanity   += 1
        elif reason == "LOW_EV":        self._n_low_ev         += 1
        elif reason == "WEAK_SIGNAL":   self._n_weak_signal    += 1
        elif reason == "WIDE_SPREAD":   self._n_wide_spread    += 1
        elif reason == "LOW_LIQUIDITY": self._n_low_liquidity  += 1
