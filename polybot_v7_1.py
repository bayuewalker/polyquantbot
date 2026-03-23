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
from integrations.intelligence_client import (
    IntelligenceAPIError,
    IntelligenceClient,
    compute_momentum,
    extract_keywords,
    unix_seconds_ago,
)
from integrations.polymarket_client import PolymarketClient
from integrations.telegram_bot      import TelegramBot
from integrations.telegram_formatter import (
    format_trade_close,
    format_heartbeat,
    format_error,
)


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
        # Crypto
        "bitcoin": 0.38, "btc": 0.38, "ethereum": 0.40,
        # Macro
        "federal reserve": 0.70, "rate cut": 0.68,
        "recession": 0.35, "inflation": 0.35,
        # Politics — high-signal proper nouns
        "election": 0.72, "trump": 0.72, "elon": 0.72,
        # Politics — structural terms covering nomination/primary markets
        "president": 0.68, "nomination": 0.62,
        "republican": 0.65, "democrat": 0.65,
        "primary": 0.62, "senate": 0.60, "congress": 0.58,
        # Sports — covers NBA MVP, World Cup, etc.
        "mvp": 0.58, "nba": 0.50, "nfl": 0.48,
        "world cup": 0.58, "fifa": 0.55,
        "championship": 0.50, "finals": 0.50,
        # Tech / AI
        "ai": 0.72, "openai": 0.65, "tesla": 0.60,
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
            # CLOB unavailable (AMM-only market or no active orders) — treat as no_ob_fallback
            return {**_empty, "allow": True, "reason": "no_ob_fallback",
                    "best_bid": 0.49, "best_ask": 0.51,
                    "spread": 0.02, "spread_pct": 0.02,
                    "liquidity_score": 0.15, "fill_prob": 0.50}

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
                    "best_bid": 0.49, "best_ask": 0.51,
                    "spread": 0.02, "spread_pct": 0.02,
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
        self.running = os.environ.get("AUTOSTART", "").lower() == "true"

        t_cfg = cfg.get("trading",         {})
        e_cfg = cfg.get("edge_filter",     {})
        x_cfg = cfg.get("execution",       {})
        c_cfg = cfg.get("circuit_breaker", {})
        p_cfg = cfg.get("persistence",     {})
        m_cfg = cfg.get("polymarket",      {})
        o_cfg = cfg.get("orderbook",       {})
        n_cfg = cfg.get("notifications",   {})
        i_cfg = cfg.get("intelligence_api", {})

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

        # ── Intelligence API client ───────────────────────────────────────────
        self.intelligence = IntelligenceClient(
            base_url=i_cfg.get("base_url", ""),
            api_key =_env("INTELLIGENCE_API_KEY"),
        )
        self._i_cfg = i_cfg

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
        self._last_heartbeat: float = time.time()   # watchdog timestamp
        self._last_tg_heartbeat: float = 0.0        # last time heartbeat was sent to Telegram
        # Copy-trade alert deduplication: {wallet:market_id -> sent_timestamp}
        # Prevents re-alerting the same wallet/market combo within 4 hours
        self._copy_trade_sent: dict[str, float] = {}
        self.max_trades_per_cycle: int = 3
        self._top_signals: list[dict] = []
        self._mi_cache: dict[str, dict] = {}

        # ── Anti-spam / dedup controls ────────────────────────────────────
        # Part 1: per-market execution lock — prevents concurrent duplicate fills
        self._active_markets: set[str] = set()
        # Part 2: signal hash cache with 5-min TTL
        self._signal_cache: dict[str, float] = {}

        # ── Per-cycle scan stats (reset each cycle, used for heartbeat) ───────
        self._cycle_markets_scanned:   int = 0
        self._cycle_candidates_eval:   int = 0
        self._cycle_signals_passed:    int = 0
        self._cycle_trades_executed:   int = 0

    @property
    def _last_alpha_signals(self) -> list:
        return self._top_signals

    # ── Lifecycle ──────────────────────────────────────────────────────────────

    async def start(self) -> None:
        await self.client.start()
        await self.intelligence.start()
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

        # Copy-trading pipeline (runs only when intelligence API is available)
        if self._i_cfg.get("copy_trading_enabled", True):
            asyncio.create_task(self._copy_trading_loop(), name="copy_trading")

        try:
            mode = "PAPER" if self.client.is_paper_trading() else "LIVE"
            intel_status = "ONLINE" if self.intelligence.available else "OFFLINE"
            t_cfg = self.cfg.get("trading", {})
            await self.telegram.send_startup_report(
                mode=mode,
                balance=self.portfolio.total_value,
                cash=self.portfolio.cash,
                position_count=len(self.portfolio.positions),
                max_positions=int(t_cfg.get("max_positions", 10)),
                market_count=self._market_limit,
                scan_interval=self._scan_interval,
                intelligence_status=intel_status,
                autostart=self.running,
            )
        except Exception:
            pass

        # Main scan loop — never exits while running
        while not self._shutdown_flag:
            try:
                if self.running:
                    # Reset per-cycle scan stats
                    self._cycle_markets_scanned  = 0
                    self._cycle_candidates_eval  = 0
                    self._cycle_signals_passed   = 0
                    self._cycle_trades_executed  = 0

                    await self._scan()
                    await self._check_exits()
                    await self._daily_check()

                    # Part 4: Heartbeat — send at most once every 60 seconds
                    _hb_now = time.time()
                    if _hb_now - self._last_tg_heartbeat >= 60:
                        self._last_tg_heartbeat = _hb_now
                        try:
                            await self.telegram.send_keyed(
                                "heartbeat",
                                format_heartbeat(
                                    running=self.running,
                                    cycle=self._scan_count,
                                    markets_scanned=self._cycle_markets_scanned,
                                    candidates_evaluated=self._cycle_candidates_eval,
                                    signals_passed=self._cycle_signals_passed,
                                    trades_executed=self._cycle_trades_executed,
                                    balance=self.portfolio.total_value,
                                    cash=self.portfolio.cash,
                                    active_trades=len(self.portfolio.positions),
                                    drawdown_pct=self.portfolio.max_drawdown() * 100,
                                    daily_pnl=self.portfolio.daily_pnl,
                                ),
                                cooldown=60.0,
                            )
                        except Exception:
                            pass

                # Watchdog timestamp — updated every cycle whether running or not
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
                # Alert on error — never crash silently
                try:
                    await self.telegram.send(format_error(
                        exc_type=type(exc).__name__,
                        detail=str(exc),
                        module="main_loop",
                        action="AUTO-RECOVERY",
                    ))
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
        try:
            await self.intelligence.stop()
        except Exception as exc:
            log.warning("Intelligence client stop error: %s", exc)
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

    # ── Safe intelligence wrapper ──────────────────────────────────────────────

    async def safe_intelligence_call(self, fn, *args, **kwargs):
        """Wrapper for all Intelligence API calls — always fail-open (never blocks trading)."""
        try:
            return await fn(*args, **kwargs)
        except Exception as exc:
            log.warning("INTEL_FAIL: %s", exc)
            return None

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
        self._cycle_markets_scanned = 0
        self._cycle_candidates_eval = 0
        self._cycle_signals_passed  = 0
        self._cycle_trades_executed = 0

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
        self._cycle_markets_scanned = len(markets_raw)

        # ── Market Insights enrichment (agent 575) ──────────────────────────
        # Pre-fetch market insights and cache by condition_id for signal boosts.
        # This is NOT a hard filter — an empty or failed response never blocks trading.
        self._mi_cache = {}
        if self._i_cfg.get("market_insights_enabled", False) and self.intelligence.available:
            insights = await self.safe_intelligence_call(
                self.intelligence.get_market_insights,
                volume_trend=self._i_cfg.get("market_insights_volume_trend", "UP"),
                min_liquidity_percentile=self._i_cfg.get(
                    "market_insights_min_liquidity_percentile", "60"
                ),
                limit=300,
                all_pages=True,
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
                log.info(
                    "Market Insights: %d markets cached for signal boost",
                    len(self._mi_cache),
                )

        # Clear top signals for this scan cycle
        self._top_signals = []

        candidates: list[dict] = []
        seen: set[str] = set()

        for m in markets_raw:
            try:
                tokens    = m.get("tokens", [])
                yt = next((t for t in tokens if t.get("outcome", "").upper() == "YES"), {})
                nt = next((t for t in tokens if t.get("outcome", "").upper() == "NO"),  {})
                cid    = m.get("conditionId", "")
                yes_px = float(yt.get("price", 0.5))
                liq    = float(m.get("liquidity", 0))

                if not cid or cid in seen:                            continue
                if cid in self.portfolio.positions:                   continue
                if self.guard.has_open_position(cid):                 continue
                if liq < self._min_liq:                               continue
                if not (self._price_min <= yes_px <= self._price_max): continue

                # Bug fix: group markets store token IDs in clobTokenIds, not tokens[].tokenId
                clob_ids = m.get("clobTokenIds") or []
                tok_yes  = yt.get("tokenId", "") or (clob_ids[0] if len(clob_ids) > 0 else "")
                tok_no   = nt.get("tokenId", "") or (clob_ids[1] if len(clob_ids) > 1 else "")

                seen.add(cid)
                candidates.append({
                    "condition_id": cid,
                    "question":     m.get("question", ""),
                    "yes_price":    yes_px,
                    "no_price":     float(nt.get("price", 0.5)),
                    "liquidity":    liq,
                    "volume_24h":   float(m.get("volume24hr", 0) or 0),
                    "category":     m.get("category", "default"),
                    "token_id_yes": tok_yes,
                    "token_id_no":  tok_no,
                })
            except Exception:
                continue

        # Sort by 24h volume — prioritises markets with active orderbooks
        candidates.sort(key=lambda x: x["volume_24h"], reverse=True)
        log.info(
            "Candidates: %d / %d pass pre-filter",
            len(candidates),
            len(markets_raw),
        )

        trades_this_cycle = 0
        processed = 0
        self._cycle_candidates_eval = len(candidates)
        for mkt in candidates[:60]:
            if trades_this_cycle >= self.max_trades_per_cycle:
                log.info("Cycle trade cap reached (%d/%d) — stopping scan early",
                         trades_this_cycle, self.max_trades_per_cycle)
                break
            try:
                placed = await self._process_market(mkt)
                if placed:
                    trades_this_cycle += 1
                    self._cycle_trades_executed += 1
            except Exception as exc:
                log.error("_process_market error %s: %s", mkt.get("condition_id", "?")[:12], exc)
            processed += 1
            if processed >= 30:
                break

    async def _process_market(self, mkt: dict) -> bool:
        cid      = mkt["condition_id"]
        question = mkt["question"]
        yes_px   = mkt["yes_price"]
        liq      = mkt["liquidity"]
        cat      = mkt["category"]
        tok_id   = mkt["token_id_yes"]

        # Signal cooldown
        key = f"{cid}:YES"
        if time.time() - self._sent_cooldown.get(key, 0.0) < self._cooldown_sec:
            return False

        # Score signal
        r = self.model.update(cid, question, cat, yes_px)
        if r["ev"] <= 0:
            return False

        # ── Candlestick momentum gate (agent 568) ────────────────────────────
        # Negative momentum (price falling below recent EMA) reduces EV confidence.
        momentum: Optional[float] = None
        if self._i_cfg.get("candlestick_enabled", True) and self.intelligence.available and tok_id:
            try:
                # Use start_time to fetch last ~24 hours of 1h candles
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
                        log.info(
                            "Candlestick momentum [%s]: %.4f", cid[:12], momentum
                        )
            except Exception as exc:
                log.warning("Candlestick fetch failed %s (non-fatal): %s", cid[:12], exc)

        # Momentum gate: strongly negative momentum (< -0.05) is a bearish signal.
        # Reduce model confidence by dampening the effective model probability toward market.
        ev_momentum_adj = 0.0
        if momentum is not None and momentum < -0.05:
            # Bearish candle momentum — dampen EV to reduce confidence
            ev_momentum_adj = momentum * 0.1  # e.g. -0.05 → -0.005 EV reduction
            log.info(
                "Candlestick bearish momentum [%s]: %.4f → EV adj=%.4f",
                cid[:12], momentum, ev_momentum_adj,
            )

        # ── CLOB orderbook ───────────────────────────────────────────────────
        try:
            ob_raw = await self.client.get_orderbook(tok_id)
        except Exception as exc:
            log.warning("get_orderbook failed %s: %s", cid[:12], exc)
            return False
        ob = self.ob_evaluator.evaluate(ob_raw, "YES")
        if not ob["allow"]:
            return False

        price    = ob["adjusted_price"] or yes_px
        best_bid = ob["best_bid"]
        best_ask = ob["best_ask"]

        # ── Intelligence orderbook cross-validation (agent 572) ──────────────
        # Supplements CLOB data: cross-validate spread AND depth from intelligence API.
        # Requires token_id (not market_id) per the API spec.
        if self._i_cfg.get("orderbook_crosscheck_enabled", True) and self.intelligence.available and tok_id:
            try:
                intel_ob_list = await self.intelligence.get_orderbook_snapshot(
                    token_id=tok_id,
                    start_time=unix_seconds_ago(300),  # last 5 min snapshot
                )
                if intel_ob_list:
                    # Use most recent snapshot
                    intel_ob = intel_ob_list[-1] if isinstance(intel_ob_list, list) else intel_ob_list
                    ob_spread_max = float(self._i_cfg.get(
                        "orderbook_crosscheck_max_spread",
                        self.ob_evaluator.max_spread * 1.5,
                    ))
                    # Cross-validate spread
                    i_spread = intel_ob.get("spread") or intel_ob.get("spread_pct")
                    if i_spread is not None and float(i_spread) > ob_spread_max:
                        log.info(
                            "Intelligence OB REJECT [%s]: intel_spread=%.4f > max=%.4f",
                            cid[:12], float(i_spread), ob_spread_max,
                        )
                        return False
                    # Cross-validate depth (bid + ask total size)
                    i_bid_size = float(intel_ob.get("bid_size", 0) or intel_ob.get("total_bid", 0) or 0)
                    i_ask_size = float(intel_ob.get("ask_size", 0) or intel_ob.get("total_ask", 0) or 0)
                    i_depth    = i_bid_size + i_ask_size
                    min_depth  = float(self._i_cfg.get(
                        "orderbook_crosscheck_min_depth", self.ob_evaluator.min_vol * 2
                    ))
                    if i_depth > 0 and i_depth < min_depth:
                        log.info(
                            "Intelligence OB REJECT [%s]: intel_depth=%.1f < min=%.1f",
                            cid[:12], i_depth, min_depth,
                        )
                        return False
            except Exception as exc:
                log.warning("Intelligence OB crosscheck %s (non-fatal): %s", cid[:12], exc)

        # ── Social Pulse signal boost (agent 585) ────────────────────────────
        # BOOST ONLY — never blocks trading on low signal, always fail-open.
        social_boost = 0.0
        if self._i_cfg.get("social_pulse_enabled", False) and self.intelligence.available:
            keywords = extract_keywords(question)
            if keywords:
                pulse = await self.safe_intelligence_call(
                    self.intelligence.get_social_pulse, keywords
                )
                if pulse:
                    accel    = float(pulse.get("acceleration", 1.0) or 1.0)
                    diversity = float(pulse.get("author_diversity_pct", 50.0) or 50.0)
                    accel_boost_thresh = float(self._i_cfg.get("social_pulse_accel_boost", 1.5))
                    div_min            = float(self._i_cfg.get("social_pulse_diversity_min", 40.0))
                    ev_boost           = float(self._i_cfg.get("social_pulse_ev_boost", 0.02))
                    ev_penalty         = float(self._i_cfg.get("social_pulse_ev_penalty", 0.015))

                    if accel > accel_boost_thresh and diversity >= div_min:
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

        # ── Market Insights liquidity boost ──────────────────────────────────
        # Uses the pre-fetched cycle cache — zero API calls at this point.
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

        # Apply combined EV adjustments before edge filter
        total_ev_adj = ev_momentum_adj + social_boost + liquidity_boost
        adjusted_model_prob = max(0.01, min(0.99, r["model_prob"] + total_ev_adj))

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
            return False

        # ── Kelly size ─────────────────────────────────────────────────────────
        # Use the intelligence-adjusted probability so momentum/social
        # penalties/boosts affect not just edge-filter pass/fail but also size.
        adjusted_ev = ef.ev   # EdgeFilter already computed this from adjusted_model_prob
        adjusted_conf = adjusted_model_prob  # use adjusted posterior directly
        size_usd = self.position_manager.kelly_size(
            ev=adjusted_ev, confidence=adjusted_conf,
            price=price, kelly_fraction=self._kelly,
        )
        if size_usd < 1.0:
            return False

        # ── Store in top signals for /alpha ───────────────────────────────────
        self._top_signals.append({
            "title":    question,
            "ev":       round(adjusted_ev, 4),
            "z":        round(ef.z_score, 2),
            "prob":     round(adjusted_conf, 3),
            "size":     round(size_usd, 2),
            "price":    round(price, 4),
            "social":   round(social_boost, 4),
            "liq":      round(liquidity_boost, 4),
        })
        # Keep top 10 by EV
        self._top_signals.sort(key=lambda x: x["ev"], reverse=True)
        self._top_signals = self._top_signals[:10]
        self._cycle_signals_passed += 1

        # Mark cooldown before execution (prevents re-entry while async executes)
        self._sent_cooldown[key] = time.time()

        momentum_str = f"  momentum={momentum:+.4f}" if momentum is not None else ""
        log.info(
            "Signal PASS [YES %s]: raw_ev=%.4f adj_ev=%.4f adj_conf=%.2f"
            " z=%.2f size=$%.2f @ %.4f%s",
            question[:40], r["ev"], adjusted_ev, adjusted_conf,
            ef.z_score, size_usd, price, momentum_str,
        )

        # ── Part 1: Per-market execution lock ─────────────────────────────
        if cid in self._active_markets:
            log.debug("Execution lock: %s already active — skipping", cid[:12])
            return False
        self._active_markets.add(cid)

        # ── Part 2: Signal dedup cache (5-min TTL) ─────────────────────────
        _now = time.time()
        signal_key = f"{cid}:{round(adjusted_ev, 3)}:{round(adjusted_conf, 3)}"
        if signal_key in self._signal_cache:
            log.debug("Signal cache hit: %s — skipping duplicate", cid[:12])
            self._active_markets.discard(cid)
            return False
        # Prune expired cache entries
        self._signal_cache = {k: v for k, v in self._signal_cache.items()
                              if _now - v < 300}
        self._signal_cache[signal_key] = _now

        signal_meta = {
            "ev":           adjusted_ev,
            "z_score":      ef.z_score,
            "confidence":   adjusted_conf,
            "social_boost": social_boost,
            "liq_boost":    liquidity_boost,
        }

        try:
            placed = await self.order_manager.execute(
                market_id=cid, question=question, token_id=tok_id,
                side="YES", size_usd=size_usd, price=price, strategy="Bayesian",
                signal_meta=signal_meta,
            )
        except Exception as exc:
            log.error("order_manager.execute error %s: %s", cid[:12], exc)
            self._active_markets.discard(cid)
            return False

        if not placed:
            self._active_markets.discard(cid)

        return bool(placed)

    # ── Copy-trading pipeline ─────────────────────────────────────────────────

    async def _copy_trading_loop(self) -> None:
        """
        Background pipeline: runs every N minutes.
        H-Score leaderboard → Wallet 360 profiling → recent trades detection
        → market quality validation → Social Pulse confirmation → Telegram alert.
        No auto-execution — alerts only.
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
        Copy-trading pipeline (spec steps 1-6):
        1. H-Score leaderboard (agent 584) — skilled traders
        2. Wallet 360 profiling (agent 581) — filter bots/low-diversity
        3. Polymarket Trades (agent 556) — detect recent entries
        4. Market Insights quality check (agent 575) — liquidity/volume validation
        5. Social Pulse confirmation (agent 585) — narrative validation
        Emits Telegram alerts only — no auto-execution.
        """
        if not self.intelligence.available:
            return

        min_wr  = float(self._i_cfg.get("copy_trading_min_win_rate", 0.45))
        max_wr  = float(self._i_cfg.get("copy_trading_max_win_rate", 0.92))
        min_tr  = int(self._i_cfg.get("copy_trading_min_trades",    30))
        bot_max = float(self._i_cfg.get("copy_trading_max_bot_score", 0.4))

        # Step 1: H-Score leaderboard — skilled traders with positive ROI
        # Param names per spec: min_win_rate_15d, max_win_rate_15d, etc. (all strings)
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

        # Step 2: Profile each wallet via Wallet 360 — filter bot-like or low-diversity
        # Spec: required param is proxy_wallet (not wallet)
        qualified_wallets: list[dict] = []
        for trader in leaders[:10]:  # Limit API calls
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
                log.warning("copy_trading: wallet360 fetch failed %s: %s", wallet[:10], exc)
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
                    "copy_trading: skipping wallet %s bot_score=%.2f diversity=%.1f",
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

        # Pre-fetch Market Insights once per cycle (not per trade) to avoid
        # repeated API calls and reduce latency/rate-limit risk.
        cycle_mi_approved: Optional[set[str]] = None  # None = fetch failed (fail-open)
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
            log.warning(
                "copy_trading: Market Insights cycle pre-fetch failed (fail-open): %s", exc
            )
        except Exception as exc:
            log.warning(
                "copy_trading: Market Insights cycle pre-fetch error (fail-open): %s", exc
            )

        # Step 3 + 4 + 5: Trades → Market Insights quality → Social Pulse per wallet
        for wdata in qualified_wallets[:5]:
            wallet = wdata["wallet"]
            try:
                # Trades in last 48h (spec: wallet_proxy param, start_time as unix seconds)
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
                raw_ts = (
                    trade.get("timestamp")
                    or trade.get("created_at")
                    or trade.get("transacted_at")
                    or 0
                )
                # Normalize: handle unix-seconds, unix-milliseconds, and ISO-8601 strings
                trade_time = 0.0
                if raw_ts:
                    try:
                        ts_float = float(raw_ts)
                        # Heuristic: timestamps > 1e12 are milliseconds
                        trade_time = ts_float / 1000.0 if ts_float > 1e12 else ts_float
                    except (TypeError, ValueError):
                        # ISO-8601 string (e.g. "2025-01-15T12:34:56Z")
                        try:
                            from datetime import datetime as _dt
                            trade_time = _dt.fromisoformat(
                                str(raw_ts).replace("Z", "+00:00")
                            ).timestamp()
                        except Exception:
                            trade_time = 0.0

                if not condition_id or trade_price <= 0:
                    continue

                # Only consider entries in the last 4 hours
                if trade_time and (time.time() - trade_time) > 14400:
                    continue

                # Step 4: Market Insights quality check — use cycle-cached approved set
                # cycle_mi_approved=None means pre-fetch failed (fail-open, allow all).
                # cycle_mi_approved=set() means API returned zero matching markets (reject all).
                market_quality_ok = True
                if cycle_mi_approved is not None:
                    if condition_id not in cycle_mi_approved:
                        log.info(
                            "copy_trading: market %s rejected by Market Insights quality check"
                            " (approved=%d)",
                            condition_id[:16], len(cycle_mi_approved),
                        )
                        market_quality_ok = False

                if not market_quality_ok:
                    continue

                # Deduplication: skip if we already alerted this wallet/market in last 4h
                dedupe_key = f"{wallet}:{condition_id}"
                if time.time() - self._copy_trade_sent.get(dedupe_key, 0.0) < 14400:
                    log.info(
                        "copy_trading: skipping duplicate alert %s wallet=%s",
                        condition_id[:16], wallet[:10],
                    )
                    continue

                # Step 5: Social Pulse confirmation (agent 585)
                sp_summary = ""
                sp_confidence_flag = ""
                if self._i_cfg.get("social_pulse_enabled", True):
                    try:
                        question_text = (
                            trade.get("question")
                            or trade.get("market_question")
                            or market_slug
                        )
                        keywords = extract_keywords(question_text)
                        if keywords:
                            pulse = await self.intelligence.get_social_pulse(keywords)
                            if pulse:
                                acceleration = float(pulse.get("acceleration", 1.0) or 1.0)
                                diversity    = float(pulse.get("author_diversity_pct", 50.0) or 50.0)
                                sp_summary   = f"accel={acceleration:.2f} diversity={diversity:.0f}%"
                                accel_boost  = float(self._i_cfg.get("social_pulse_accel_boost", 1.5))
                                div_min      = float(self._i_cfg.get("social_pulse_diversity_min", 40.0))
                                if acceleration > accel_boost and diversity >= div_min:
                                    sp_confidence_flag = " [confirmed]"
                                elif diversity < 20:
                                    sp_confidence_flag = " [low diversity — possible noise]"
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
                    f"Signal: {confidence_str}{sp_confidence_flag}\n"
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
                    _closed_market_id = pos.market_id
                    self.guard.set_position_closed(_closed_market_id)
                    self._active_markets.discard(_closed_market_id)
                    self._cb.record_loss(max(0.0, -closed.pnl))
                    try:
                        hold_min = round(
                            (time.time() - closed.opened_at) / 60
                        ) if hasattr(closed, "opened_at") else 0
                    except Exception:
                        hold_min = 0
                    pnl_pct = (closed.pnl / max(closed.size_usd, 0.01)) * 100
                    await self.telegram.send_keyed(
                        f"trade_close_{_closed_market_id}",
                        format_trade_close(
                            question=closed.question,
                            side=closed.side,
                            exit_price=closed.current_price,
                            pnl=closed.pnl,
                            pnl_pct=pnl_pct,
                            reason=reason,
                            hold_minutes=hold_min,
                            balance=self.portfolio.total_value,
                            cash=self.portfolio.cash,
                            active_trades=len(self.portfolio.positions),
                        ),
                        cooldown=30.0,
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
