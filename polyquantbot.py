"""
polyquantbot.py  —  PolyBot v7.1 FINAL  (cloud-hardened)

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
  python polyquantbot.py
  python polyquantbot.py --paper
  python polyquantbot.py --once
  python polyquantbot.py --config /path/to/config.yaml
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
from integrations.intelligence_client import (
    IntelligenceClient,
    IntelligenceAPIError,
    compute_momentum,
    extract_keywords,
    unix_seconds_ago,
)
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
        "election": 0.50, "trump": 0.55,
        "elon": 0.60, "ai": 0.60,
    }

    def __init__(self, weights: Optional[dict] = None) -> None:
        self._w  = weights or {"prior": 0.25, "market": 0.50, "sentiment": 0.25}
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

        # ── Client ────────────────────────────────────────────────────────────
        self.client = PolymarketClient(
            cfg=m_cfg, private_key=private_key, maker_address=maker_addr,
        )

        # ── Portfolio + position manager ──────────────────────────────────────
        self.portfolio        = Portfolio(initial_cash=0.0)
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

        # ── Intelligence API ──────────────────────────────────────────────────
        self._i_cfg = cfg.get("intelligence_api", {})
        self.intelligence = IntelligenceClient(
            base_url=self._i_cfg.get("base_url", "https://narrative.agent.heisenberg.so"),
            api_key=_env("INTELLIGENCE_API_KEY"),
        )
        # Market Insights cycle cache: {condition_id -> insight_data}
        self._mi_cache: dict[str, dict] = {}
        # Copy-trade alert dedup: {wallet:condition_id -> sent_timestamp}
        self._copy_trade_sent: dict[str, float] = {}

        self._sent_cooldown: dict[str, float] = {}
        self._last_daily    = datetime.now(timezone.utc).date()
        self._scan_count    = 0
        self._shutdown_flag = False

    # ── Lifecycle ──────────────────────────────────────────────────────────────

    async def start(self) -> None:
        await self.client.start()
        await self.intelligence.start()
        log.info(
            "PolyBot v7.1 started | paper=%s wallet=%s intel=%s",
            self.client.is_paper_trading(),
            (self.client.wallet_address[:10] + "...") if self.client.wallet_address else "none",
            "ONLINE" if self.intelligence.available else "OFFLINE",
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

        # Copy-trading pipeline (runs only when enabled and intelligence API is available)
        if self._i_cfg.get("copy_trading_enabled", False):
            asyncio.create_task(self._copy_trading_loop(), name="copy_trading")

        try:
            mode         = "PAPER" if self.client.is_paper_trading() else "LIVE"
            intel_status = "ONLINE" if self.intelligence.available else "OFFLINE"
            await self.telegram.send(
                f"PolyBot v7.1 started\n"
                f"Mode: {mode}\n"
                f"Intelligence: {intel_status}\n"
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

                # Health log every cycle
                log.info(
                    "BOT ALIVE | running=%s | positions=%d | cash=%.2f | balance=%.2f",
                    self.running,
                    len(self.portfolio.positions),
                    self.portfolio.cash,
                    self.portfolio.total_value,
                )

            except asyncio.CancelledError:
                break
            except Exception as exc:
                log.error("Main loop error: %s", exc, exc_info=True)
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
            await self.intelligence.stop()
        except Exception as exc:
            log.warning("Intelligence stop error: %s", exc)
        try:
            await self.client.stop()
        except Exception as exc:
            log.warning("Client stop error: %s", exc)
        log.info("PolyBot stopped cleanly")

    # ── Safe intelligence wrapper ──────────────────────────────────────────────

    async def safe_intelligence_call(self, fn, *args, **kwargs):
        """Wrapper for all Intelligence API calls — always fail-open (never blocks trading)."""
        try:
            return await fn(*args, **kwargs)
        except Exception as exc:
            log.warning("INTEL_FAIL: %s", exc)
            return None

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

        # ── Market Insights enrichment (agent 575) ────────────────────────────
        # Pre-fetch once per scan cycle and cache by condition_id.
        # Fail-open: empty or failed response never blocks trading.
        self._mi_cache = {}
        if self._i_cfg.get("market_insights_enabled", False) and self.intelligence.available:
            insights = await self.safe_intelligence_call(
                self.intelligence.get_market_insights,
                volume_trend=self._i_cfg.get("market_insights_volume_trend", "UP"),
                min_liquidity_percentile=self._i_cfg.get(
                    "market_insights_min_liquidity_percentile", "60"
                ),
                limit=300,
            )
            if insights:
                for item in insights:
                    cid_val = (
                        item.get("condition_id")
                        or item.get("conditionId")
                        or item.get("market_id")
                        or item.get("id")
                        or ""
                    )
                    if cid_val:
                        self._mi_cache[cid_val] = item
                log.info("Market Insights: %d markets cached for signal boost", len(self._mi_cache))

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

        # ── Candlestick momentum gate (agent 568) ────────────────────────────
        # Bearish momentum dampens effective model probability (fail-open).
        ev_momentum_adj = 0.0
        if self._i_cfg.get("candlestick_enabled", False) and self.intelligence.available and tok_id:
            try:
                candles = await self.intelligence.get_candlesticks(
                    token_id=tok_id,
                    interval=self._i_cfg.get("candlestick_interval", "1h"),
                    start_time=unix_seconds_ago(
                        int(self._i_cfg.get("candlestick_limit", 24)) * 3600
                    ),
                )
                if candles:
                    momentum = compute_momentum(
                        candles,
                        ema_period=int(self._i_cfg.get("candlestick_ema_period", 6)),
                    )
                    if momentum is not None:
                        bearish_thresh = float(self._i_cfg.get("candlestick_bearish_threshold", -0.05))
                        dampen_factor  = float(self._i_cfg.get("candlestick_ev_dampen_factor", 0.1))
                        log.info("Candlestick momentum [%s]: %.4f", cid[:12], momentum)
                        if momentum < bearish_thresh:
                            ev_momentum_adj = momentum * dampen_factor
                            log.info(
                                "Candlestick bearish [%s]: momentum=%.4f → ev_adj=%.4f",
                                cid[:12], momentum, ev_momentum_adj,
                            )
            except Exception as exc:
                log.warning("Candlestick fetch failed %s (non-fatal): %s", cid[:12], exc)

        # ── Social Pulse boost/penalty (agent 585) ───────────────────────────
        # Adjusts model probability; never blocks trading on its own (fail-open).
        social_boost = 0.0
        if self._i_cfg.get("social_pulse_enabled", False) and self.intelligence.available:
            keywords = extract_keywords(question)
            if keywords:
                pulse = await self.safe_intelligence_call(
                    self.intelligence.get_social_pulse, keywords
                )
                if pulse:
                    accel      = float(pulse.get("acceleration", 1.0) or 1.0)
                    diversity  = float(pulse.get("author_diversity_pct", 50.0) or 50.0)
                    accel_thr  = float(self._i_cfg.get("social_pulse_accel_boost", 1.5))
                    div_min    = float(self._i_cfg.get("social_pulse_diversity_min", 40.0))
                    ev_boost   = float(self._i_cfg.get("social_pulse_ev_boost", 0.02))
                    ev_penalty = float(self._i_cfg.get("social_pulse_ev_penalty", 0.015))
                    if accel > accel_thr and diversity >= div_min:
                        social_boost = min(0.10, (accel - 1) * 0.05)
                        log.info(
                            "Social Pulse BOOST [%s]: accel=%.2f diversity=%.1f%% +%.3f",
                            cid[:12], accel, diversity, social_boost,
                        )
                    elif diversity < div_min:
                        social_boost = -ev_penalty
                        log.info(
                            "Social Pulse REDUCE [%s]: diversity=%.1f%% %.3f",
                            cid[:12], diversity, social_boost,
                        )

        # ── Market Insights liquidity boost (cycle-cached, agent 575) ────────
        liquidity_boost = 0.0
        if self._i_cfg.get("market_insights_enabled", False):
            mi_data = self._mi_cache.get(cid, {})
            if mi_data:
                mi_liq = float(mi_data.get("liquidity_score", 0) or 0)
                mi_vol = float(mi_data.get("volume_24h", 0) or mi_data.get("volume24h", 0) or 0)
                if mi_liq > 0.7 and mi_vol > 10000:
                    liquidity_boost = 0.05
                    log.info(
                        "Market Insights BOOST [%s]: liq_score=%.2f vol_24h=%.0f +%.3f",
                        cid[:12], mi_liq, mi_vol, liquidity_boost,
                    )

        # Apply combined EV adjustments before orderbook + edge filter
        total_ev_adj = ev_momentum_adj + social_boost + liquidity_boost
        adjusted_model_prob = max(0.01, min(0.99, r["model_prob"] + total_ev_adj))

        # ── CLOB orderbook ───────────────────────────────────────────────────
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

        # ── Intelligence orderbook crosscheck (agent 572) ────────────────────
        # Cross-validates spread and depth from Intelligence API (fail-open).
        if self._i_cfg.get("orderbook_crosscheck_enabled", False) and self.intelligence.available and tok_id:
            try:
                intel_ob_list = await self.intelligence.get_orderbook_snapshot(
                    token_id=tok_id,
                    start_time=unix_seconds_ago(300),
                )
                if intel_ob_list:
                    intel_ob = intel_ob_list[-1] if isinstance(intel_ob_list, list) else intel_ob_list
                    # Staleness check
                    max_age  = float(self._i_cfg.get("orderbook_crosscheck_max_age_sec", 300))
                    snap_ts  = intel_ob.get("timestamp") or intel_ob.get("ts") or intel_ob.get("created_at")
                    if snap_ts:
                        try:
                            age = time.time() - float(snap_ts)
                            if age > max_age:
                                log.info(
                                    "Intel OB stale [%s]: age=%.0fs — crosscheck skipped",
                                    cid[:12], age,
                                )
                                intel_ob = None
                        except (TypeError, ValueError):
                            pass
                    if intel_ob is not None:
                        ob_spread_max = float(self._i_cfg.get(
                            "orderbook_crosscheck_max_spread",
                            self.ob_evaluator.max_spread * 1.5,
                        ))
                        i_spread = intel_ob.get("spread") or intel_ob.get("spread_pct")
                        if i_spread is not None and float(i_spread) > ob_spread_max:
                            log.info(
                                "Intel OB REJECT [%s]: spread=%.4f > max=%.4f",
                                cid[:12], float(i_spread), ob_spread_max,
                            )
                            return
                        i_bid  = float(intel_ob.get("bid_size", 0) or intel_ob.get("total_bid", 0) or 0)
                        i_ask  = float(intel_ob.get("ask_size", 0) or intel_ob.get("total_ask", 0) or 0)
                        i_depth = i_bid + i_ask
                        min_depth = float(self._i_cfg.get(
                            "orderbook_crosscheck_min_depth",
                            self.ob_evaluator.min_vol * 2,
                        ))
                        if i_depth > 0 and i_depth < min_depth:
                            log.info(
                                "Intel OB REJECT [%s]: depth=%.1f < min=%.1f",
                                cid[:12], i_depth, min_depth,
                            )
                            return
            except Exception as exc:
                log.warning("Intel OB crosscheck %s (non-fatal): %s", cid[:12], exc)

        # ── Edge filter ───────────────────────────────────────────────────────
        ef_ctx = EdgeContext(
            p_model    = adjusted_model_prob,
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

        # Kelly size (uses intelligence-adjusted probability)
        size_usd = self.position_manager.kelly_size(
            ev=ef.ev, confidence=adjusted_model_prob,
            price=price, kelly_fraction=self._kelly,
        )
        if size_usd < 1.0:
            return

        # Mark cooldown before execution (prevents re-entry while async executes)
        self._sent_cooldown[key] = time.time()

        log.info(
            "Signal PASS [YES %s]: ev=%.4f z=%.2f conf=%.2f size=$%.2f @ %.4f",
            question[:40], ef.ev, r["z_score"], adjusted_model_prob, size_usd, price,
        )

        try:
            await self.order_manager.execute(
                market_id=cid, question=question, token_id=tok_id,
                side="YES", size_usd=size_usd, price=price, strategy="Bayesian",
            )
        except Exception as exc:
            log.error("order_manager.execute error %s: %s", cid[:12], exc)

    # ── Copy trading pipeline ─────────────────────────────────────────────────

    async def _copy_trading_loop(self) -> None:
        """
        Background pipeline: runs every N minutes.
        H-Score leaderboard → Wallet 360 → recent trades → market quality
        → Social Pulse confirmation → Telegram alert.
        """
        interval_min = int(self._i_cfg.get("copy_trading_interval_min", 15))
        await asyncio.sleep(60)  # Brief startup delay

        while not self._shutdown_flag:
            try:
                await self._run_copy_trading_cycle()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                log.error("copy_trading_loop error (continuing): %s", exc)

            try:
                await asyncio.sleep(interval_min * 60)
            except asyncio.CancelledError:
                break

    async def _run_copy_trading_cycle(self) -> None:
        """
        Copy-trading pipeline (6 stages):
        1. H-Score leaderboard (agent 584) — skilled traders
        2. Wallet 360 profiling (agent 581) — filter bots/low-diversity
        3. Polymarket Trades (agent 556) — detect recent entries
        4. Market Insights quality check (agent 575) — liquidity/volume validation
        5. Social Pulse confirmation (agent 585) — narrative validation
        6. Telegram alert — no auto-execution
        """
        if not self.intelligence.available:
            return

        min_wr  = float(self._i_cfg.get("copy_trading_min_win_rate", 0.45))
        max_wr  = float(self._i_cfg.get("copy_trading_max_win_rate", 0.92))
        min_tr  = int(self._i_cfg.get("copy_trading_min_trades",    30))
        bot_max = float(self._i_cfg.get("copy_trading_max_bot_score", 0.4))

        # Stage 1: H-Score leaderboard
        try:
            leaders = await self.intelligence.get_hscore_leaderboard(
                min_win_rate_15d=str(min_wr),
                max_win_rate_15d=str(max_wr),
                min_roi_15d="0",
                min_total_trades_15d=str(min_tr),
                max_total_trades_15d="5000",
                sort_by="roi",
                limit=20,
            )
        except Exception as exc:
            log.warning("copy_trading: H-Score fetch failed: %s", exc)
            return

        if not leaders:
            log.info("copy_trading: no qualified traders from H-Score leaderboard")
            return

        # Stage 2: Wallet 360 profiling — filter bots and low-diversity wallets
        qualified_wallets: list[dict] = []
        for trader in leaders[:10]:
            wallet = (
                trader.get("proxy_wallet")
                or trader.get("wallet")
                or trader.get("maker_address")
                or trader.get("address")
                or ""
            )
            roi = float(trader.get("roi_15d", 0) or trader.get("roi", 0) or 0)
            if not wallet or roi <= 0:
                continue

            try:
                profile_list = await self.intelligence.get_wallet_360(
                    proxy_wallet=wallet,
                    window_days="7",
                )
            except Exception as exc:
                log.warning("copy_trading: wallet360 failed %s: %s", wallet[:10], exc)
                continue

            profile = profile_list[0] if profile_list else None
            if not profile:
                continue

            bot_score = float(profile.get("bot_score", 0) or 0)
            diversity  = float(
                profile.get("market_diversity", 0)
                or profile.get("unique_markets", 0)
                or 0
            )
            if bot_score > bot_max or diversity < 2:
                log.info(
                    "copy_trading: skip %s bot_score=%.2f diversity=%.1f",
                    wallet[:10], bot_score, diversity,
                )
                continue

            qualified_wallets.append({
                "wallet":   wallet,
                "rank":     trader.get("rank", "?"),
                "win_rate": float(trader.get("win_rate_15d", 0) or trader.get("win_rate", 0) or 0),
                "roi":      roi,
                "h_score":  float(trader.get("h_score", 0) or 0),
            })

        if not qualified_wallets:
            log.info("copy_trading: no qualified non-bot wallets found")
            return

        # Stage 4: Market Insights quality gate — pre-fetch once per cycle
        cycle_mi_approved: Optional[set[str]] = None
        try:
            mi_cycle = await self.intelligence.get_market_insights(
                volume_trend=self._i_cfg.get("market_insights_volume_trend", "UP"),
                min_liquidity_percentile=self._i_cfg.get(
                    "market_insights_min_liquidity_percentile", "60"
                ),
                limit=200,
            )
            cycle_mi_approved = {
                item.get("condition_id")
                or item.get("conditionId")
                or item.get("id")
                or ""
                for item in mi_cycle
            }
            log.info(
                "copy_trading: Market Insights cycle cache: %d approved markets",
                len(cycle_mi_approved),
            )
        except IntelligenceAPIError as exc:
            log.warning("copy_trading: Market Insights pre-fetch failed (fail-open): %s", exc)
        except Exception as exc:
            log.warning("copy_trading: Market Insights pre-fetch error (fail-open): %s", exc)

        # Stage 3+5+6: Trades → Social Pulse → alert per wallet
        for wdata in qualified_wallets[:5]:
            wallet = wdata["wallet"]
            try:
                trades = await self.intelligence.get_trades(
                    wallet_proxy=wallet,
                    start_time=unix_seconds_ago(48 * 3600),
                    side="BUY",
                    limit=20,
                )
            except Exception as exc:
                log.warning("copy_trading: trades fetch failed %s: %s", wallet[:10], exc)
                continue

            for trade in trades[:5]:
                condition_id = trade.get("condition_id") or trade.get("market_id") or ""
                market_slug  = trade.get("market_slug") or trade.get("slug") or condition_id[:20]
                trade_side   = (trade.get("side") or "BUY").upper()
                trade_price  = float(trade.get("price", 0) or 0)

                # Normalise timestamp (unix-seconds, unix-ms, or ISO-8601)
                raw_ts = (
                    trade.get("timestamp")
                    or trade.get("created_at")
                    or trade.get("transacted_at")
                    or 0
                )
                trade_time = 0.0
                if raw_ts:
                    try:
                        ts_float = float(raw_ts)
                        trade_time = ts_float / 1000.0 if ts_float > 1e12 else ts_float
                    except (TypeError, ValueError):
                        try:
                            from datetime import datetime as _dt
                            trade_time = _dt.fromisoformat(
                                str(raw_ts).replace("Z", "+00:00")
                            ).timestamp()
                        except Exception:
                            trade_time = 0.0

                if not condition_id or trade_price <= 0:
                    continue
                if trade_time and (time.time() - trade_time) > 14400:
                    continue

                # Market Insights quality gate
                if cycle_mi_approved is not None and condition_id not in cycle_mi_approved:
                    log.info(
                        "copy_trading: market %s rejected by MI quality check",
                        condition_id[:16],
                    )
                    continue

                # Deduplication: skip if alerted this wallet/market in last 4h
                dedupe_key = f"{wallet}:{condition_id}"
                if time.time() - self._copy_trade_sent.get(dedupe_key, 0.0) < 14400:
                    log.info(
                        "copy_trading: skip duplicate %s wallet=%s",
                        condition_id[:16], wallet[:10],
                    )
                    continue

                # Stage 5: Social Pulse confirmation
                sp_summary = ""
                sp_flag    = ""
                if self._i_cfg.get("social_pulse_enabled", True):
                    try:
                        q_text   = trade.get("question") or trade.get("market_question") or market_slug
                        keywords = extract_keywords(q_text)
                        if keywords:
                            pulse = await self.intelligence.get_social_pulse(keywords)
                            if pulse:
                                accel    = float(pulse.get("acceleration", 1.0) or 1.0)
                                div_pct  = float(pulse.get("author_diversity_pct", 50.0) or 50.0)
                                sp_summary = f"accel={accel:.2f} diversity={div_pct:.0f}%"
                                if accel > float(self._i_cfg.get("social_pulse_accel_boost", 1.5)) \
                                        and div_pct >= float(self._i_cfg.get("social_pulse_diversity_min", 40.0)):
                                    sp_flag = " [confirmed]"
                                elif div_pct < 20:
                                    sp_flag = " [low diversity — possible noise]"
                    except Exception:
                        pass

                confidence_notes = []
                if wdata["win_rate"] >= 0.65:
                    confidence_notes.append("high win-rate")
                if wdata["roi"] >= 0.5:
                    confidence_notes.append("strong ROI")
                confidence_str = ", ".join(confidence_notes) or "qualified trader"

                alert = (
                    f"<b>Copy-Trade Signal</b>\n"
                    f"Market: <code>{market_slug[:40]}</code>\n"
                    f"Side:   {trade_side} @ {trade_price:.4f}\n"
                    f"Trader: rank={wdata['rank']} h_score={wdata['h_score']:.1f}\n"
                    f"        win_rate={wdata['win_rate']*100:.1f}% ROI={wdata['roi']*100:.1f}%\n"
                    f"Signal: {confidence_str}{sp_flag}\n"
                )
                if sp_summary:
                    alert += f"Social: {sp_summary}\n"
                alert += "<i>Alert only — no auto-execution</i>"

                try:
                    await self.telegram.send(alert)
                    self._copy_trade_sent[dedupe_key] = time.time()
                    log.info(
                        "copy_trading: alert sent for %s wallet=%s",
                        market_slug[:20], wallet[:10],
                    )
                    # Prune stale dedupe entries (older than 24h)
                    _now = time.time()
                    self._copy_trade_sent = {
                        k: v for k, v in self._copy_trade_sent.items()
                        if _now - v < 86400
                    }
                except Exception as exc:
                    log.warning("copy_trading: telegram send failed: %s", exc)

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
                try:
                    await bot.stop()
                except Exception:
                    pass
                bot = None
            await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(main())
