"""
core/edge_filter.py  —  PolyBot v7.1  v2.0.0
==============================================
LATEST UPDATE — v2.0.0:
NEW: NO_EDGE gate — rejects when ev < 0 (directional, not just below threshold)
NEW: reset_counters() — clear all stats (useful after daily reset)
NEW: hot_summary() — concise one-liner for Telegram status alerts
IMPROVED: FilterResult now includes a `gate_index` (0-5) for fast sorting
IMPROVED: evaluate() logs side in PASS line for easier debug filtering
KEPT: 5 fail-fast gates in cheapest-first order, EPS=1e-6 tolerance
"""
from __future__ import annotations
import os, logging
from dataclasses import dataclass, field

log = logging.getLogger("polybot.edge_filter")

EPS: float = 1e-6

_D_EV_THRESHOLD:  float = 0.03
_D_Z_THRESHOLD:   float = 1.5
_D_SPREAD_MAX:    float = 0.05
_D_LIQUIDITY_MIN: float = 10_000.0
_D_PROB_MIN:      float = 0.05
_D_PROB_MAX:      float = 0.95


def _ef(key: str, default: float) -> float:
    try: return float(os.environ[key])
    except (KeyError, ValueError): return default


@dataclass(frozen=True)
class EdgeContext:
    p_model:    float          # Bayesian posterior
    p_market:   float          # CLOB mid price
    best_bid:   float
    best_ask:   float
    liquidity:  float          # market depth USD
    volatility: float  = 0.05  # rolling sigma
    market_id:  str    = ""
    question:   str    = ""
    side:       str    = ""
    strategy:   str    = ""


@dataclass
class FilterResult:
    allowed:    bool
    reason:     str   # PASS | NO_EDGE | LOW_EV | WEAK_SIGNAL | WIDE_SPREAD | LOW_LIQUIDITY | PROB_SANITY
    ev:         float = 0.0
    z_score:    float = 0.0
    spread:     float = 0.0
    mid:        float = 0.0
    gate_index: int   = 0     # 0=PASS, 1=NO_EDGE, 2=LOW_EV … lower=cheaper gate


class EdgeFilter:

    # Gate order (cheapest computation first)
    _GATE_PROB_SANITY:  int = 6
    _GATE_NO_EDGE:      int = 5
    _GATE_LOW_EV:       int = 4
    _GATE_WEAK_SIGNAL:  int = 3
    _GATE_WIDE_SPREAD:  int = 2
    _GATE_LOW_LIQUIDITY: int = 1

    def __init__(
        self,
        ev_threshold:  float | None = None,
        z_threshold:   float | None = None,
        spread_max:    float | None = None,
        liquidity_min: float | None = None,
        prob_min:      float | None = None,
        prob_max:      float | None = None,
    ) -> None:
        self.ev_threshold  = ev_threshold  if ev_threshold  is not None else _ef("EF_EV_THRESHOLD",  _D_EV_THRESHOLD)
        self.z_threshold   = z_threshold   if z_threshold   is not None else _ef("EF_Z_THRESHOLD",   _D_Z_THRESHOLD)
        self.spread_max    = spread_max    if spread_max    is not None else _ef("EF_SPREAD_MAX",     _D_SPREAD_MAX)
        self.liquidity_min = liquidity_min if liquidity_min is not None else _ef("EF_LIQUIDITY_MIN",  _D_LIQUIDITY_MIN)
        self.prob_min      = prob_min      if prob_min      is not None else _ef("EF_PROB_MIN",       _D_PROB_MIN)
        self.prob_max      = prob_max      if prob_max      is not None else _ef("EF_PROB_MAX",       _D_PROB_MAX)

        self._n_pass:         int = 0
        self._n_no_edge:      int = 0
        self._n_prob_sanity:  int = 0
        self._n_low_ev:       int = 0
        self._n_weak_signal:  int = 0
        self._n_wide_spread:  int = 0
        self._n_low_liquidity:int = 0

        log.info(
            "EdgeFilter v2.0.0 | EV>=%.3f Z>=%.1f spread<=%.3f liq>=$%.0f p=[%.2f,%.2f]",
            self.ev_threshold, self.z_threshold, self.spread_max,
            self.liquidity_min, self.prob_min, self.prob_max,
        )

    def evaluate(self, ctx: EdgeContext) -> FilterResult:
        sigma   = max(ctx.volatility, 0.01)
        ev      = ctx.p_model - ctx.p_market
        z_score = ev / sigma
        spread  = ctx.best_ask - ctx.best_bid
        mid     = (ctx.best_bid + ctx.best_ask) * 0.5

        def _reject(reason: str, gate: int) -> FilterResult:
            self._inc_counter(reason)
            log.info(
                "EdgeFilter REJECT [%s] %s | %s ev=%.4f z=%.2f spread=%.4f liq=%.0f",
                ctx.market_id[:12], ctx.side, reason, ev, z_score, spread, ctx.liquidity,
            )
            return FilterResult(
                allowed=False, reason=reason,
                ev=round(ev, 4), z_score=round(z_score, 3),
                spread=round(spread, 4), mid=round(mid, 4),
                gate_index=gate,
            )

        # Gate 0 — prob sanity (invalid model output)
        if not (self.prob_min <= ctx.p_model <= self.prob_max):
            return _reject("PROB_SANITY", self._GATE_PROB_SANITY)

        # Gate 1 — directional: model says wrong direction entirely
        if ev < 0:
            return _reject("NO_EDGE", self._GATE_NO_EDGE)

        # Gate 2 — EV magnitude too small
        if ev < (self.ev_threshold - EPS):
            return _reject("LOW_EV", self._GATE_LOW_EV)

        # Gate 3 — statistical confidence too weak
        if z_score < (self.z_threshold - EPS):
            return _reject("WEAK_SIGNAL", self._GATE_WEAK_SIGNAL)

        # Gate 4 — spread eats the edge
        if spread > self.spread_max:
            return _reject("WIDE_SPREAD", self._GATE_WIDE_SPREAD)

        # Gate 5 — insufficient liquidity to fill
        if ctx.liquidity < self.liquidity_min:
            return _reject("LOW_LIQUIDITY", self._GATE_LOW_LIQUIDITY)

        self._n_pass += 1
        log.info(
            "EdgeFilter PASS [%s] %s | ev=%.4f z=%.2f spread=%.4f liq=%.0f",
            ctx.market_id[:12], ctx.side, ev, z_score, spread, ctx.liquidity,
        )
        return FilterResult(
            allowed=True, reason="PASS",
            ev=round(ev, 4), z_score=round(z_score, 3),
            spread=round(spread, 4), mid=round(mid, 4),
            gate_index=0,
        )

    # ── Stats ─────────────────────────────────────────────────────────────────

    def counters(self) -> dict[str, int]:
        return {
            "PASS":          self._n_pass,
            "NO_EDGE":       self._n_no_edge,
            "PROB_SANITY":   self._n_prob_sanity,
            "LOW_EV":        self._n_low_ev,
            "WEAK_SIGNAL":   self._n_weak_signal,
            "WIDE_SPREAD":   self._n_wide_spread,
            "LOW_LIQUIDITY": self._n_low_liquidity,
        }

    def stats_summary(self) -> str:
        """Full summary for /status command."""
        c      = self.counters()
        total  = sum(c.values())
        passed = c["PASS"]
        rate   = f"{passed/total*100:.1f}%" if total else "n/a"
        parts  = [f"EdgeFilter: {passed}/{total} passed ({rate})"]
        for reason in ("NO_EDGE", "LOW_EV", "WEAK_SIGNAL", "WIDE_SPREAD", "LOW_LIQUIDITY", "PROB_SANITY"):
            if c.get(reason, 0):
                parts.append(f"{reason}={c[reason]}")
        return " | ".join(parts)

    def hot_summary(self) -> str:
        """Concise one-liner for Telegram alerts (e.g. daily summary)."""
        c      = self.counters()
        total  = sum(c.values())
        passed = c["PASS"]
        rate   = f"{passed/total*100:.0f}%" if total else "n/a"
        top_reject = max(
            (r for r in ("NO_EDGE","LOW_EV","WEAK_SIGNAL","WIDE_SPREAD","LOW_LIQUIDITY","PROB_SANITY")),
            key=lambda r: c.get(r, 0),
            default="n/a",
        )
        return f"EF: {passed}/{total} pass ({rate})  top reject: {top_reject}={c.get(top_reject,0)}"

    def reset_counters(self) -> None:
        """Clear all rejection counters (call on daily reset)."""
        self._n_pass = self._n_no_edge = self._n_prob_sanity = 0
        self._n_low_ev = self._n_weak_signal = 0
        self._n_wide_spread = self._n_low_liquidity = 0
        log.info("EdgeFilter: counters reset")

    # ── Internal ──────────────────────────────────────────────────────────────

    def _inc_counter(self, reason: str) -> None:
        if   reason == "PROB_SANITY":   self._n_prob_sanity   += 1
        elif reason == "NO_EDGE":       self._n_no_edge        += 1
        elif reason == "LOW_EV":        self._n_low_ev         += 1
        elif reason == "WEAK_SIGNAL":   self._n_weak_signal    += 1
        elif reason == "WIDE_SPREAD":   self._n_wide_spread    += 1
        elif reason == "LOW_LIQUIDITY": self._n_low_liquidity  += 1
