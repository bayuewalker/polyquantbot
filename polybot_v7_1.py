"""
polybot_v7_1.py  —  PolyBot v7.1 FINAL  (cloud-hardened)

Changes vs previous version:
  - All ENV vars read with safe fallbacks — system starts even with empty ENV
  - PAPER_MODE = True by default; only live if PAPER_TRADING=false in env/config
  - startup wrapped in try/except with 5s back-off before retry
  - Main scan loop: every exception caught, 5s sleep, loop continues
  - Health log every cycle: "BOT ALIVE | positions=N | balance=X"
  - SIGTERM handler for graceful cloud shutdown
  - State recovery on startup: load persisted trades + CLOB reconciliation
  - balance_sync_loop and maintenance_loop both restart on error
  - daily_check resets per-day counters safely

CLI:
  python polybot_v7_1.py
  python polybot_v7_1.py --paper
  python polybot_v7_1.py --once
  python polybot_v7_1.py --config /path/to/config.yaml
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import math
import os
import signal
import sys
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import yaml
from dotenv import load_dotenv

from core.circuit_breaker  import CircuitBreaker
from core.edge_filter       import EdgeFilter, EdgeContext
from core.execution_guard   import ExecutionGuard
from core.persistence       import PersistenceManager
from core.position_manager  import Portfolio, PositionManager
from execution.order_manager import OrderManager
from integrations.polymarket_client import PolymarketClient
from integrations.telegram_bot      import TelegramBot


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def setup_logging(level: str = "INFO", fmt: str = "") -> None:
    fmt = fmt or "%(asctime)s %(levelname)-8s %(name)s | %(message)s"
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format=fmt,
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    for noisy in ("httpx", "aiohttp.access", "telegram.ext", "telegram._bot"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


log = logging.getLogger("polybot.main")


# ---------------------------------------------------------------------------
# Safe ENV helpers
# ---------------------------------------------------------------------------

def _env(key: str, default: str = "") -> str:
    return os.getenv(key, default) or default


def _env_bool(key: str, default: bool = True) -> bool:
    v = os.getenv(key, "").strip().lower()
    if v in ("false", "0", "no", "off"):
        return False
    if v in ("true", "1", "yes", "on"):
        return True
    return default


def _env_list(key: str) -> list[str]:
    raw = os.getenv(key, "")
    return [c.strip() for c in raw.split(",") if c.strip()]


# ---------------------------------------------------------------------------
# Signal model  (pure Python, no ML deps)
# ---------------------------------------------------------------------------

class SignalModel:

    CATEGORY_PRIORS: dict[str, float] = {
        "crypto":    0.40,
        "politics":  0.45,
        "sports":    0.50,
        "economics": 0.50,
        "default":   0.50,
    }
    KEYWORD_SCORE: dict[str, float] = {
        "bitcoin": 0.38, "btc": 0.38, "ethereum": 0.40,
        "federal reserve": 0.70, "rate cut": 0.68,
        "recession": 0.35, "inflation": 0.35,
        "election": 0.72, "trump": 0.72,
        "elon": 0.72, "ai": 0.72,
    }

    def __init__(self, weights: Optional[dict] = None) -> None:
        self._w  = weights or {"prior": 0.35, "market": 0.20, "sentiment": 0.45}
        self._ph: dict[str, deque] = {}

    def update(
        self,
        market_id: str,
        question:  str,
        category:  str,
        yes_price: float,
    ) -> dict:
        if market_id not in self._ph:
            self._ph[market_id] = deque(maxlen=168)
        self._ph[market_id].append(yes_price)

        p_prior   = self.CATEGORY_PRIORS.get(category.lower(), 0.50)
        q_lower   = question.lower()
        sent      = next((v for k, v in self.KEYWORD_SCORE.items() if k in q_lower), None)

        ll_prior  = math.log(max(0.01, p_prior)  / max(0.01, 1 - p_prior))
        ll_market = math.log(max(0.01, yes_price) / max(0.01, 1 - yes_price))
        ll_sent   = (
            math.log(max(0.01, sent) / max(0.01, 1 - sent)) if sent else 0.0
        )
        w        = self._w
        log_post = w["prior"]*ll_prior + w["market"]*ll_market + w["sentiment"]*ll_sent
        p_model  = max(0.01, min(0.99, 1 / (1 + math.exp(-log_post))))
        ev       = p_model - yes_price

        hist = self._ph[market_id]
        if len(hist) >= 5:
            pts   = list(hist)
            mean  = sum(pts) / len(pts)
            sigma = max(math.sqrt(sum((x - mean)**2 for x in pts) / max(len(pts)-1, 1)), 0.02)
        else:
            sigma = 0.05

        z_score    = ev / sigma
        confidence = round(0.80 * min(1.0, abs(z_score)/3.0) + 0.20*0.5, 3)

        return {
            "model_prob": round(p_model, 4),
            "ev":         round(ev, 4),
            "z_score":    round(z_score, 2),
            "sigma":      round(sigma, 4),
            "confidence": confidence,
        }


# ---------------------------------------------------------------------------
# Orderbook evaluator
# ---------------------------------------------------------------------------

class OrderbookEvaluator:

    def __init__(self, cfg: dict) -> None:
        self.max_spread = cfg.get("max_spread_pct",      0.04)
        self.min_vol    = cfg.get("min_volume_per_side", 50.0)
        self.min_fp     = cfg.get("min_fill_prob",       0.30)

    def evaluate(self, ob: Optional[dict], side: str) -> dict:
        _empty = {
            "allow": False, "reason": "no_data",
            "best_bid": 0.0, "best_ask": 1.0,
            "spread": 1.0, "spread_pct": 1.0,
            "bid_volume": 0.0, "ask_volume": 0.0,
            "liquidity_score": 0.0, "fill_prob": 0.0,
            "adjusted_price": 0.0,
        }
        if not ob:
            return _empty

        def parse(raw: list) -> list[dict]:
            out = []
            for item in raw:
                try:
                    out.append({"price": float(item["price"]), "size": float(item["size"])})
                except Exception:
                    continue
            return out

        try:
            bids = sorted(parse(ob.get("bids", [])), key=lambda x: x["price"], reverse=True)
            asks = sorted(parse(ob.get("asks", [])), key=lambda x: x["price"])
        except Exception:
            return _empty

        if not bids or not asks:
            return {**_empty, "allow": True, "reason": "no_ob_fallback",
                    "liquidity_score": 0.15, "fill_prob": 0.50}

        bb   = bids[0]["price"]
        ba   = asks[0]["price"]
        sp   = ba - bb
        sp_p = sp / ba if ba > 0 else 1.0
        bvol = sum(l["size"] for l in bids)
        avol = sum(l["size"] for l in asks)
        fp   = bvol / (bvol + avol + 1e-9)
        liq  = min(1.0, (bvol + avol) / 2000.0) ** 1.5

        if sp_p > self.max_spread:
            return {**_empty, "allow": False, "reason": f"spread:{sp_p:.3%}",
                    "best_bid": bb, "best_ask": ba}
        if bvol < self.min_vol or avol < self.min_vol:
            return {**_empty, "allow": False, "reason": "low_ob_volume",
                    "best_bid": bb, "best_ask": ba}
        if fp < self.min_fp:
            return {**_empty, "allow": False, "reason": f"fill_prob:{fp:.2f}",
                    "best_bid": bb, "best_ask": ba}

        slippage = sp_p * 0.5
        adjusted = (
            round(ba * (1 + slippage), 4) if side == "YES"
            else round(bb * (1 - slippage), 4)
        )
        adjusted = max(0.01, min(0.99, adjusted))

        return {
            "allow": True, "reason": "ok",
            "best_bid": bb, "best_ask": ba,
            "spread": sp, "spread_pct": sp_p,
            "bid_volume": bvol, "ask_volume": avol,
            "liquidity_score": liq, "fill_prob": fp,
            "adjusted_price": adjusted,
        }


# ---------------------------------------------------------------------------
# TradingBot
# ---------------------------------------------------------------------------

class TradingBot:

    def __init__(self, cfg: dict, args: argparse.Namespace) -> None:
        self.cfg     = cfg
        self.args    = args
        self.running = False

        t_cfg = cfg.get("trading",         {})
        e_cfg = cfg.get("edge_filter",     {})
        x_cfg = cfg.get("execution",       {})
        c_cfg = cfg.get("circuit_breaker", {})
        p_cfg = cfg.get("persistence",     {})
        m_cfg = cfg.get("polymarket",      {})
        o_cfg = cfg.get("orderbook",       {})
        n_cfg = cfg.get("notifications",   {})

        # PAPER_MODE: default True; disabled only by explicit env or config
        paper = (
            args.paper
            or _env_bool("PAPER_TRADING", default=True)
            or bool(cfg.get("paper_trading", True))
        )
        private_key = "" if paper else _env("PRIVATE_KEY")
        maker_addr  = _env("MAKER_ADDRESS")
        tg_token    = _env("TELEGRAM_TOKEN")
        tg_chats    = _env_list("TELEGRAM_CHAT_IDS")

        # Task 1: Paper balance seed — read from env, default $1,000
        # Only applied in paper mode; live mode uses real wallet balance.
        self._paper_balance: float = (
            float(os.getenv("PAPER_BALANCE", "1000"))
            if paper else 0.0
        )

        # ── Client ────────────────────────────────────────────────────────────
        self.client = PolymarketClient(
            cfg=m_cfg, private_key=private_key, maker_address=maker_addr,
        )

        # ── Portfolio + position manager ──────────────────────────────────────
        # Task 2: Seed cash/total_value/peak_value from PAPER_BALANCE on startup
        self.portfolio        = Portfolio(initial_cash=self._paper_balance)
        self.position_manager = PositionManager(
            portfolio=self.portfolio, cfg=t_cfg
        )

        # ── Persistence + CB + guard ──────────────────────────────────────────
        self._pers = PersistenceManager(
            state_file=p_cfg.get("state_file", "data/active_trades.json"),
            stale_ttl =float(p_cfg.get("stale_trade_ttl_sec", 300)),
        )
        self._cb = CircuitBreaker(
            consecutive_fail_max =int(c_cfg.get("consecutive_fail_max",  5)),
            failure_rate_window  =int(c_cfg.get("failure_rate_window",   20)),
            failure_rate_thresh  =float(c_cfg.get("failure_rate_thresh", 0.50)),
            session_loss_max_usd =float(c_cfg.get("session_loss_max_usd", 200.0)),
        )
        self.guard = ExecutionGuard(
            max_concurrent  =int(x_cfg.get("queue_concurrency",   3)),
            order_timeout_s =float(x_cfg.get("order_timeout_sec", 60.0)),
            persistence     =self._pers,
            circuit_breaker =self._cb,
        )

        # ── Edge filter ───────────────────────────────────────────────────────
        self.edge_filter = EdgeFilter(
            ev_threshold  =float(e_cfg.get("ev_threshold",  0.03)),
            z_threshold   =float(e_cfg.get("z_threshold",   1.5)),
            spread_max    =float(e_cfg.get("spread_max",    0.05)),
            liquidity_min =float(e_cfg.get("liquidity_min", 10000.0)),
            prob_min      =float(e_cfg.get("prob_min",      0.05)),
            prob_max      =float(e_cfg.get("prob_max",      0.95)),
        )

        # ── Signal model + OB evaluator ───────────────────────────────────────
        self.model        = SignalModel()
        self.ob_evaluator = OrderbookEvaluator(o_cfg)

        # ── Telegram ──────────────────────────────────────────────────────────
        self.telegram = TelegramBot(token=tg_token, chat_ids=tg_chats)
        self.telegram.attach(self)

        # ── Order manager ─────────────────────────────────────────────────────
        self.order_manager = OrderManager(
            client           =self.client,
            guard            =self.guard,
            position_manager =self.position_manager,
            portfolio        =self.portfolio,
            telegram         =self.telegram,
            cfg              =x_cfg,
        )

        # ── Scan config ───────────────────────────────────────────────────────
        self._notify_filtered = bool(n_cfg.get("notify_filtered",  False))
        self._notify_daily    = bool(n_cfg.get("notify_daily",     True))
        self._scan_interval   = int(t_cfg.get("scan_interval_sec",  60))
        self._cooldown_sec    = int(t_cfg.get("signal_cooldown_sec", 3600))
        self._market_limit    = int(m_cfg.get("market_limit",        300))
        self._kelly           = float(t_cfg.get("kelly_fraction",    0.25))
        self._price_min       = float(t_cfg.get("price_min",         0.05))
        self._price_max       = float(t_cfg.get("price_max",         0.95))
        self._min_liq         = float(t_cfg.get("min_liquidity_usd", 10000.0))

        self._sent_cooldown: dict[str, float] = {}
        self._last_daily    = datetime.now(timezone.utc).date()
        self._scan_count    = 0
        self._shutdown_flag = False
        self._last_heartbeat: float = time.time()   # Task 1+6: watchdog timestamp

    # ── Lifecycle ──────────────────────────────────────────────────────────────

    async def start(self) -> None:
        await self.client.start()
        log.info(
            "PolyBot v7.1 started | paper=%s wallet=%s",
            self.client.is_paper_trading(),
            (self.client.wallet_address[:10] + "...") if self.client.wallet_address else "none",
        )

        # Sync balance (exchange is source of truth)
        await self._sync_balance()

        # State recovery: reconcile CLOB positions
        if not self.client.is_paper_trading():
            await self._reconcile_positions()

        # Start Telegram (non-fatal if it fails)
        await self.telegram.start()

        # Background tasks
        asyncio.create_task(self.order_manager.maintenance_loop(), name="order_maintenance")
        asyncio.create_task(self._balance_sync_loop(),              name="balance_sync")
        asyncio.create_task(self._watchdog_loop(),                  name="loop_watchdog")

        try:
            if self.client.is_paper_trading():
                # Task 4: Paper mode confirmation with seeded balance
                await self.telegram.send(
                    f"PAPER MODE ACTIVE | Balance: ${self.portfolio.total_value:.2f}\n"
                    f"PolyBot v7.1 started\n"
                    f"Send /run to start trading"
                )
            else:
                await self.telegram.send(
                    f"PolyBot v7.1 started\n"
                    f"Mode: LIVE\n"
                    f"Balance: ${self.portfolio.total_value:.2f} USDC\n"
                    f"Send /run to start trading"
                )
        except Exception:
            pass

        # Main scan loop — never exits while running
        while not self._shutdown_flag:
            try:
                if self.running:
                    await self._scan()
                    await self._check_exits()
                    await self._daily_check()

                # Task 1: Heartbeat — label updated, timestamp recorded for watchdog
                self._last_heartbeat = time.time()
                log.info(
                    "BOT ALIVE | active_trades=%d | running=%s | cash=%.2f | balance=%.2f",
                    len(self.portfolio.positions),
                    self.running,
                    self.portfolio.cash,
                    self.portfolio.total_value,
                )

                # Task 5: Memory safety — prune _sent_cooldown unconditionally
                # (daily_check only runs when trading; this fires every cycle)
                if len(self._sent_cooldown) > 1_000:
                    _now = time.time()
                    self._sent_cooldown = {
                        k: v for k, v in self._sent_cooldown.items()
                        if _now - v < self._cooldown_sec
                    }

            except asyncio.CancelledError:
                break
            except Exception as exc:
                log.error("Main loop error: %s", exc, exc_info=True)
                # Task 2 & 4: Alert on error — never crash silently
                try:
                    await self.telegram.send(f"CRITICAL ERROR: {exc}")
                except Exception:
                    pass
                await asyncio.sleep(5)
                continue

            try:
                await asyncio.sleep(self._scan_interval)
            except asyncio.CancelledError:
                break

    async def stop(self) -> None:
        """Graceful shutdown: save state, stop telegram, close session."""
        self._shutdown_flag = True
        self.running = False
        log.info("PolyBot: initiating graceful shutdown")
        try:
            # Persist current state before closing
            self.guard._save_state()
        except Exception as exc:
            log.warning("State save on shutdown failed: %s", exc)
        try:
            await self.telegram.stop()
        except Exception as exc:
            log.warning("Telegram stop error: %s", exc)
        try:
            await self.client.stop()
        except Exception as exc:
            log.warning("Client stop error: %s", exc)
        log.info("PolyBot stopped cleanly")

    # ── State recovery ────────────────────────────────────────────────────────

    async def _reconcile_positions(self) -> None:
        """
        On startup: fetch CLOB positions and sync them into local portfolio.
        Exchange is the source of truth.
        """
        try:
            clob_pos = await self.client.get_positions()
        except Exception as exc:
            log.error("_reconcile_positions: get_positions failed: %s", exc)
            return

        try:
            added = self.position_manager.reconcile_from_clob(clob_pos)
        except Exception as exc:
            log.error("_reconcile_positions: reconcile failed: %s", exc)
            return

        if added:
            log.info("Reconciled %d CLOB position(s)", len(added))
            try:
                await self.telegram.send(
                    f"State Recovery\n"
                    f"Reconciled {len(added)} open position(s) from CLOB:\n"
                    + "\n".join(f"  {c[:16]}" for c in added[:10])
                )
            except Exception:
                pass
        else:
            log.info("Reconciliation: portfolio in sync with CLOB")

        # Mark recovered positions in the guard
        for pos in self.portfolio.positions.values():
            if pos.market_id:
                self.guard.set_position_open(pos.market_id)

    # ── Scan ──────────────────────────────────────────────────────────────────

    async def _scan(self) -> None:
        self._scan_count += 1
        log.info(
            "Scan #%d | positions=%d cash=$%.2f halted=%s",
            self._scan_count,
            len(self.portfolio.positions),
            self.portfolio.cash,
            self.guard.is_halted,
        )

        if self.guard.is_halted:
            log.warning(
                "Scan skipped: halted | %s",
                self.guard._halt_reason or self.guard.circuit_breaker.trip_reason,
            )
            return

        try:
            markets_raw = await self.client.get_markets(self._market_limit)
        except Exception as exc:
            log.error("get_markets failed: %s", exc)
            return

        if not markets_raw:
            log.warning("No markets returned from API")
            return

        candidates: list[dict] = []
        seen: set[str] = set()

        for m in markets_raw:
            try:
                tokens = m.get("tokens", [])
                yt = next((t for t in tokens if t.get("outcome", "").upper() == "YES"), {})
                nt = next((t for t in tokens if t.get("outcome", "").upper() == "NO"),  {})
                cid    = m.get("conditionId", "")
                yes_px = float(yt.get("price", 0.5))
                liq    = float(m.get("liquidity", 0))

                if not cid or cid in seen:                       continue
                if cid in self.portfolio.positions:              continue
                if self.guard.has_open_position(cid):           continue
                if liq < self._min_liq:                          continue
                if not (self._price_min <= yes_px <= self._price_max): continue

                seen.add(cid)
                candidates.append({
                    "condition_id": cid,
                    "question":     m.get("question", ""),
                    "yes_price":    yes_px,
                    "no_price":     float(nt.get("price", 0.5)),
                    "liquidity":    liq,
                    "volume_24h":   float(m.get("volume24hr", 0)),
                    "category":     m.get("category", "default"),
                    "token_id_yes": yt.get("tokenId", ""),
                    "token_id_no":  nt.get("tokenId", ""),
                })
            except Exception:
                continue

        candidates.sort(key=lambda x: x["liquidity"], reverse=True)
        log.info("Candidates: %d / %d pass pre-filter", len(candidates), len(markets_raw))

        processed = 0
        for mkt in candidates[:10]:
            try:
                await self._process_market(mkt)
            except Exception as exc:
                log.error("_process_market error %s: %s", mkt.get("condition_id", "?")[:12], exc)
            processed += 1
            if processed >= 5:
                break

    async def _process_market(self, mkt: dict) -> None:
        cid      = mkt["condition_id"]
        question = mkt["question"]
        yes_px   = mkt["yes_price"]
        liq      = mkt["liquidity"]
        cat      = mkt["category"]
        tok_id   = mkt["token_id_yes"]

        # Signal cooldown
        key = f"{cid}:YES"
        if time.time() - self._sent_cooldown.get(key, 0.0) < self._cooldown_sec:
            return

        # Score signal
        r = self.model.update(cid, question, cat, yes_px)
        if r["ev"] <= 0:
            return

        # Orderbook
        try:
            ob_raw = await self.client.get_orderbook(tok_id)
        except Exception as exc:
            log.warning("get_orderbook failed %s: %s", cid[:12], exc)
            return
        ob = self.ob_evaluator.evaluate(ob_raw, "YES")
        if not ob["allow"]:
            return

        price    = ob["adjusted_price"] or yes_px
        best_bid = ob["best_bid"]
        best_ask = ob["best_ask"]

        # Edge filter
        ef_ctx = EdgeContext(
            p_model    = r["model_prob"],
            p_market   = yes_px,
            best_bid   = best_bid,
            best_ask   = best_ask,
            liquidity  = liq,
            volatility = r["sigma"],
            market_id  = cid,
            question   = question,
            side       = "YES",
            strategy   = "Bayesian",
        )
        ef = self.edge_filter.evaluate(ef_ctx)

        if not ef.allowed:
            if self._notify_filtered:
                await self.telegram.send(
                    f"[FILTERED] {ef.reason}\n"
                    f"market: {question[:50]}\n"
                    f"EV: {ef.ev:+.4f}  Z: {ef.z_score:.2f}"
                )
            return

        # Kelly size
        size_usd = self.position_manager.kelly_size(
            ev=r["ev"], confidence=r["confidence"],
            price=price, kelly_fraction=self._kelly,
        )
        if size_usd < 1.0:
            return

        # Mark cooldown before execution (prevents re-entry while async executes)
        self._sent_cooldown[key] = time.time()

        log.info(
            "Signal PASS [YES %s]: ev=%.4f z=%.2f conf=%.2f size=$%.2f @ %.4f",
            question[:40], r["ev"], r["z_score"], r["confidence"], size_usd, price,
        )

        try:
            await self.order_manager.execute(
                market_id=cid, question=question, token_id=tok_id,
                side="YES", size_usd=size_usd, price=price, strategy="Bayesian",
            )
        except Exception as exc:
            log.error("order_manager.execute error %s: %s", cid[:12], exc)

    # ── Exit checks ───────────────────────────────────────────────────────────

    async def _check_exits(self) -> None:
        try:
            exits = self.position_manager.check_exits()
        except Exception as exc:
            log.error("check_exits error: %s", exc)
            return

        for oid, reason in exits:
            try:
                pos = self.portfolio.positions.get(oid)
                if not pos:
                    continue
                closed = self.position_manager.close_position(oid, pos.current_price, reason)
                if closed:
                    self.guard.set_position_closed(pos.market_id)
                    self._cb.record_loss(max(0.0, -closed.pnl))
                    await self.telegram.send(
                        f"<b>Position Closed</b>\n"
                        f"Market: {closed.question[:50]}\n"
                        f"Side: {closed.side}  Reason: {reason}\n"
                        f"Entry: {closed.entry_price:.4f}  "
                        f"Exit: {closed.current_price:.4f}\n"
                        f"P&L: ${closed.pnl:+.2f}"
                    )
            except Exception as exc:
                log.error("close_position error %s: %s", oid[:12], exc)

    # ── Balance sync ─────────────────────────────────────────────────────────

    async def _sync_balance(self) -> None:
        if self.client.is_paper_trading():
            return
        try:
            balance = await self.client.get_balance()
            if balance > 0:
                deployed = sum(p.size_usd for p in self.portfolio.positions.values())
                self.portfolio.cash        = max(0.0, balance - deployed)
                self.portfolio.total_value = balance
                if balance > self.portfolio.peak_value:
                    self.portfolio.peak_value = balance
                log.info(
                    "Balance synced: total=$%.2f cash=$%.2f deployed=$%.2f",
                    balance, self.portfolio.cash, deployed,
                )
        except Exception as exc:
            log.warning("Balance sync failed: %s", exc)

    async def _balance_sync_loop(self) -> None:
        while not self._shutdown_flag:
            try:
                await asyncio.sleep(120)
                await self._sync_balance()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                log.error("balance_sync_loop error (continuing): %s", exc)
                await asyncio.sleep(10)

    # ── Loop watchdog (Task 6) ────────────────────────────────────────────────

    async def _watchdog_loop(self) -> None:
        """
        Task 6: Detect a stuck main loop.
        If no heartbeat has been recorded in STALL_THRESHOLD_S seconds, send a
        Telegram alert. Checks every 30 s to minimise overhead.
        """
        STALL_THRESHOLD_S = 120.0
        CHECK_INTERVAL_S  = 30.0
        # Give the bot time to complete its first cycle before arming the watchdog.
        await asyncio.sleep(CHECK_INTERVAL_S * 2)
        while not self._shutdown_flag:
            try:
                await asyncio.sleep(CHECK_INTERVAL_S)
                elapsed = time.time() - self._last_heartbeat
                if elapsed > STALL_THRESHOLD_S:
                    log.error(
                        "WATCHDOG: main loop stalled — no heartbeat for %.0fs "
                        "(threshold=%.0fs)",
                        elapsed, STALL_THRESHOLD_S,
                    )
                    try:
                        await self.telegram.send(
                            f"WATCHDOG ALERT: Main loop stalled\n"
                            f"No iteration for {elapsed:.0f}s "
                            f"(threshold: {STALL_THRESHOLD_S:.0f}s)\n"
                            f"Bot may be stuck — check immediately."
                        )
                    except Exception:
                        pass
            except asyncio.CancelledError:
                break
            except Exception as exc:
                log.warning("_watchdog_loop error (non-fatal): %s", exc)

    # ── Daily check ───────────────────────────────────────────────────────────

    async def _daily_check(self) -> None:
        try:
            today = datetime.now(timezone.utc).date()
            if today == self._last_daily:
                return
            self._last_daily = today
            p = self.portfolio
            if self._notify_daily:
                await self.telegram.send(
                    f"<b>Daily Summary</b>  {today}\n"
                    f"P&L today:  ${p.daily_pnl:+.2f}\n"
                    f"All-time:   ${p.all_time_pnl:+.2f}\n"
                    f"Balance:    ${p.total_value:.2f}\n"
                    f"Win rate:   {p.win_rate()*100:.1f}%\n"
                    f"{self.edge_filter.stats_summary()}"
                )
            p.daily_pnl     = 0.0
            p.signals_today = 0
            now = time.time()
            self._sent_cooldown = {
                k: v for k, v in self._sent_cooldown.items()
                if now - v < self._cooldown_sec * 2
            }
        except Exception as exc:
            log.error("daily_check error: %s", exc)


# ---------------------------------------------------------------------------
# Config loader
# ---------------------------------------------------------------------------

def load_config(path: str = "config.yaml") -> dict:
    p = Path(path)
    if not p.exists():
        log.warning("config.yaml not found at %s — using empty defaults", path)
        return {}
    try:
        with open(p, encoding="utf-8") as fh:
            return yaml.safe_load(fh) or {}
    except Exception as exc:
        log.error("config load error %s: %s — using defaults", path, exc)
        return {}


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

async def main() -> None:
    load_dotenv()

    ap = argparse.ArgumentParser(description="PolyBot v7.1 FINAL")
    ap.add_argument("--paper",  action="store_true",   help="Force paper trading mode")
    ap.add_argument("--once",   action="store_true",   help="Single scan then exit")
    ap.add_argument("--config", default="config.yaml", help="Path to config YAML")
    args = ap.parse_args()

    cfg     = load_config(args.config)
    log_cfg = cfg.get("logging", {})
    setup_logging(
        level=log_cfg.get("level",  "INFO"),
        fmt  =log_cfg.get("format", ""),
    )

    log.info("PolyBot v7.1 FINAL booting | PID=%d", os.getpid())

    if args.once:
        # Single-scan mode for testing / CI
        bot = TradingBot(cfg=cfg, args=args)
        try:
            await bot.client.start()
            await bot._sync_balance()
            bot.running = True
            await bot._scan()
        except Exception as exc:
            log.error("--once mode error: %s", exc, exc_info=True)
        finally:
            try:
                await bot.client.stop()
            except Exception:
                pass
        return

    # Full persistent mode with crash-restart protection
    bot: Optional[TradingBot] = None
    while True:
        try:
            bot = TradingBot(cfg=cfg, args=args)

            # Register SIGTERM handler for graceful cloud shutdown
            loop = asyncio.get_running_loop()
            def _sigterm_handler():
                log.warning("SIGTERM received — initiating graceful shutdown")
                asyncio.create_task(bot.stop())
            try:
                loop.add_signal_handler(signal.SIGTERM, _sigterm_handler)
            except (NotImplementedError, RuntimeError):
                # Windows / some container configs don't support SIGTERM handlers
                pass

            await bot.start()
            break   # clean exit (SIGTERM / shutdown flag)

        except KeyboardInterrupt:
            log.info("KeyboardInterrupt — shutting down")
            if bot:
                try:
                    await bot.stop()
                except Exception:
                    pass
            break

        except Exception as exc:
            log.error("Startup/runtime failed: %s — restarting in 5s", exc, exc_info=True)
            if bot:
                # Task 2 & 4: Alert before restart — never crash silently
                try:
                    await bot.telegram.send(
                        f"CRITICAL ERROR: {exc}\n"
                        f"Bot restarting in 5s — state will be restored."
                    )
                except Exception:
                    pass
                try:
                    await bot.stop()
                except Exception:
                    pass
                bot = None
            await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(main())
