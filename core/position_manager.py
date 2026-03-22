"""
core/position_manager.py  —  PolyBot v7.1 FINAL
Portfolio state, position lifecycle, Kelly sizing, dynamic stops,
exit checks, and CLOB reconciliation on startup.
"""
from __future__ import annotations

import logging
import math
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Optional

log = logging.getLogger("polybot.position_manager")


@dataclass
class Position:
    id:              str
    market_id:       str
    question:        str
    side:            str
    entry_price:     float
    current_price:   float
    size_usd:        float
    shares:          float
    stop_loss:       float = 0.0
    take_profit:     float = 0.0
    pnl:             float = 0.0
    peak_price:      float = 0.0
    trailing_active: bool  = False
    opened_at:       float = field(default_factory=time.time)
    token_id:        str   = ""

    def __post_init__(self) -> None:
        if self.peak_price == 0.0:
            self.peak_price = self.entry_price


class Portfolio:

    def __init__(self, initial_cash: float = 0.0) -> None:
        self.cash:          float = initial_cash
        self.total_value:   float = initial_cash
        self.peak_value:    float = initial_cash
        self.daily_pnl:     float = 0.0
        self.all_time_pnl:  float = 0.0
        self.trades_total:  int   = 0
        self.trades_won:    int   = 0
        self.gross_profit:  float = 0.0
        self.gross_loss:    float = 0.0
        self.signals_today: int   = 0
        self.positions:     dict[str, Position] = {}
        self._returns:      deque[float]        = deque(maxlen=200)

    def win_rate(self) -> float:
        return self.trades_won / self.trades_total if self.trades_total else 0.0

    def profit_factor(self) -> float:
        return self.gross_profit / max(self.gross_loss, 1e-9)

    def max_drawdown(self) -> float:
        if self.peak_value <= 0:
            return 0.0
        return max(0.0, (self.peak_value - self.total_value) / self.peak_value)

    def sharpe_ratio(self) -> float:
        if len(self._returns) < 5:
            return 0.0
        returns = list(self._returns)
        n    = len(returns)
        mean = sum(returns) / n
        var  = sum((r - mean) ** 2 for r in returns) / max(n - 1, 1)
        std  = math.sqrt(var) if var > 0 else 1e-9
        return mean / std * math.sqrt(252)

    def record_return(self, pnl: float) -> None:
        size = sum(p.size_usd for p in self.positions.values()) or 1.0
        self._returns.append(pnl / size)


class PositionManager:

    def __init__(self, portfolio: Portfolio, cfg: dict) -> None:
        self.portfolio       = portfolio
        self.cfg             = cfg
        self.stop_loss_pct   = cfg.get("stop_loss_pct",    0.20)
        self.tp_pct          = cfg.get("take_profit_pct",  None)
        self.trail_dist      = cfg.get("trail_dist_pct",   0.02)
        self.trail_start     = cfg.get("trail_start_pct",  0.03)
        self.break_even      = cfg.get("break_even_pct",   0.05)
        self.time_exit_sec   = cfg.get("time_exit_minutes", 90.0) * 60

    # ── Open ──────────────────────────────────────────────────────────────────

    def open_position(
        self,
        order_id:  str,
        market_id: str,
        question:  str,
        side:      str,
        price:     float,
        size_usd:  float,
        token_id:  str = "",
    ) -> Position:
        shares = size_usd / price if price > 0 else 0.0
        sl     = self._stop_price(price, side)
        tp     = self._tp_price(price, side)
        pos    = Position(
            id=order_id, market_id=market_id, question=question,
            side=side, entry_price=price, current_price=price,
            size_usd=size_usd, shares=shares,
            stop_loss=sl, take_profit=tp, token_id=token_id,
        )
        self.portfolio.positions[order_id] = pos
        self.portfolio.cash         = max(0.0, self.portfolio.cash - size_usd)
        self.portfolio.trades_total += 1
        if self.portfolio.total_value > self.portfolio.peak_value:
            self.portfolio.peak_value = self.portfolio.total_value
        log.info(
            "Position OPENED %s %s %s $%.2f @ %.4f SL=%.4f",
            order_id[:12], side, question[:40], size_usd, price, sl,
        )
        return pos

    # ── Close ─────────────────────────────────────────────────────────────────

    def close_position(
        self, order_id: str, exit_price: float, reason: str
    ) -> Optional[Position]:
        pos = self.portfolio.positions.pop(order_id, None)
        if not pos:
            return None

        pos.current_price = exit_price
        pnl = (exit_price - pos.entry_price) * pos.shares
        if pos.side == "NO":
            pnl = -pnl
        pos.pnl = round(pnl, 4)

        self.portfolio.cash         += pos.size_usd + pnl
        self.portfolio.total_value   = self.portfolio.cash + sum(
            p.size_usd for p in self.portfolio.positions.values()
        )
        if self.portfolio.total_value > self.portfolio.peak_value:
            self.portfolio.peak_value = self.portfolio.total_value

        self.portfolio.daily_pnl    += pnl
        self.portfolio.all_time_pnl += pnl
        self.portfolio.record_return(pnl)

        if pnl >= 0:
            self.portfolio.trades_won   += 1
            self.portfolio.gross_profit += pnl
        else:
            self.portfolio.gross_loss   += abs(pnl)

        log.info(
            "Position CLOSED %s %s reason=%s pnl=$%+.2f",
            order_id[:12], pos.side, reason, pnl,
        )
        return pos

    # ── Price update + dynamic stops ─────────────────────────────────────────

    def update_price(self, order_id: str, new_price: float) -> None:
        pos = self.portfolio.positions.get(order_id)
        if not pos:
            return
        pos.current_price = new_price
        pnl = (new_price - pos.entry_price) * pos.shares
        if pos.side == "NO":
            pnl = -pnl
        pos.pnl = round(pnl, 4)

        gain_pct = (
            (new_price - pos.entry_price) / pos.entry_price
            if pos.entry_price else 0
        )
        if gain_pct >= self.break_even and not pos.trailing_active:
            pos.stop_loss      = pos.entry_price
            pos.trailing_active = True

        if pos.trailing_active:
            if new_price > pos.peak_price:
                pos.peak_price = new_price
            trail_stop = pos.peak_price * (1 - self.trail_dist)
            if trail_stop > pos.stop_loss:
                pos.stop_loss = trail_stop

    # ── Exit checks ───────────────────────────────────────────────────────────

    def check_exits(self) -> list[tuple[str, str]]:
        now      = time.time()
        to_exit  = []
        for oid, pos in list(self.portfolio.positions.items()):
            curr = pos.current_price
            if pos.stop_loss > 0 and curr <= pos.stop_loss:
                to_exit.append((oid, "stop_loss")); continue
            if pos.take_profit > 0 and curr >= pos.take_profit:
                to_exit.append((oid, "take_profit")); continue
            if self.time_exit_sec > 0 and (now - pos.opened_at) > self.time_exit_sec:
                to_exit.append((oid, "time_exit"))
        return to_exit

    # ── CLOB reconciliation ───────────────────────────────────────────────────

    def reconcile_from_clob(self, clob_positions: list[dict]) -> list[str]:
        added    = []
        clob_ids = {
            item.get("condition_id") or item.get("market_id") or ""
            for item in clob_positions
        }
        tracked = {pos.market_id for pos in self.portfolio.positions.values()}

        for item in clob_positions:
            size  = float(item.get("size",  0) or 0)
            price = float(item.get("price", 0) or item.get("avg_price", 0) or 0)
            if size * price < 1.0:
                continue
            cid    = item.get("condition_id") or item.get("market_id") or ""
            side   = (item.get("outcome") or item.get("side") or "YES").upper()
            tok_id = item.get("token_id", "")

            if cid and cid not in tracked:
                size_usd = round(size * price, 2)
                recon_id = f"recon_{cid[:12]}_{int(time.time())}"
                pos      = Position(
                    id=recon_id, market_id=cid,
                    question=f"[Reconciled] {cid[:20]}",
                    side=side, entry_price=price, current_price=price,
                    size_usd=size_usd, shares=size, token_id=tok_id,
                )
                pos.stop_loss = self._stop_price(price, side)
                self.portfolio.positions[recon_id] = pos
                self.portfolio.cash = max(0.0, self.portfolio.cash - size_usd)
                added.append(cid)
                log.info("Reconciled position: %s %s $%.2f", side, cid[:12], size_usd)

        for oid, pos in list(self.portfolio.positions.items()):
            if (
                pos.market_id
                and pos.market_id not in clob_ids
                and not oid.startswith("paper_")
                and not oid.startswith("recon_")
            ):
                log.warning(
                    "Orphan position not in CLOB: %s %s", oid[:12], pos.market_id[:12]
                )
        return added

    # ── Risk sizing ───────────────────────────────────────────────────────────

    def kelly_size(
        self, ev: float, confidence: float, price: float, kelly_fraction: float = 0.25
    ) -> float:
        if price <= 0 or price >= 1:
            return 0.0
        b = (1.0 - price) / price
        p = min(0.95, max(0.05, confidence))
        q = 1.0 - p
        f = (p * b - q) / b if b > 0 else 0.0
        f = max(0.0, min(f, 1.0)) * kelly_fraction
        total   = max(self.portfolio.total_value, 1.0)
        raw_usd = f * total
        max_pos = self.cfg.get("max_position_usd", 500.0)
        return round(min(raw_usd, max_pos, self.portfolio.cash), 2)

    def can_trade(self) -> tuple[bool, str]:
        max_pos = self.cfg.get("max_positions", 10)
        if len(self.portfolio.positions) >= max_pos:
            return False, f"max_positions={max_pos}"

        max_exp  = self.cfg.get("max_exposure_pct", 0.50)
        deployed = sum(p.size_usd for p in self.portfolio.positions.values())
        total    = max(self.portfolio.total_value, 1.0)
        if deployed / total >= max_exp:
            return False, f"max_exposure={max_exp:.0%} deployed={deployed/total:.0%}"

        daily_limit = self.cfg.get("daily_loss_limit_usd", 200.0)
        if self.portfolio.daily_pnl <= -daily_limit:
            return False, f"daily_loss_limit=${daily_limit}"

        return True, "ok"

    # ── Internal ──────────────────────────────────────────────────────────────

    def _stop_price(self, price: float, side: str) -> float:
        sl = price * (1 - self.stop_loss_pct) if side == "YES" else price * (1 + self.stop_loss_pct)
        return round(max(0.01, min(0.99, sl)), 4)

    def _tp_price(self, price: float, side: str) -> float:
        if not self.tp_pct:
            return 0.0
        tp = price * (1 + self.tp_pct) if side == "YES" else price * (1 - self.tp_pct)
        return round(max(0.01, min(0.99, tp)), 4)
