"""
integrations/telegram_bot.py  —  PolyBot v7.1 FINAL  (final-critical-fix)

Changes vs previous version:
  - Polling uses Application.run_polling() via background asyncio.Task
    (no updater.start_polling; works correctly with PTB v21)
  - All startup / polling wrapped in try/except — NEVER crashes main loop
  - send() never raises; logs warning on each failed chat send
  - Token and chat_ids read defensively from env with safe splits
  - /reset command added to clear circuit breaker via Telegram
  - Alert helpers for: placed, partial, cancelled, failure, circuit breaker
"""
from __future__ import annotations

import asyncio
import html
import logging
from typing import TYPE_CHECKING, Optional

log = logging.getLogger("polybot.telegram")

try:
    from telegram import Update, ReplyKeyboardMarkup
    from telegram.ext import (
        Application,
        CommandHandler,
        MessageHandler,
        ContextTypes,
        filters,
    )
    _TG_OK = True
except ImportError:
    _TG_OK = False
    log.warning("python-telegram-bot not installed — Telegram disabled")

if TYPE_CHECKING:
    pass


class TelegramBot:

    KEYBOARD = [
        ["\u25b6\ufe0f Run",    "\u23f9\ufe0f Stop"],
        ["\U0001f4ca Status",  "\U0001f4b0 Wallet"],
        ["\U0001f525 Alpha",   "\U0001f504 Reset CB"],
    ]

    def __init__(self, token: str, chat_ids: list[str]) -> None:
        self._token    = (token or "").strip()
        self._chat_ids = [c.strip() for c in chat_ids if c.strip()]
        self._app: Optional[object]  = None
        self._task: Optional[asyncio.Task] = None
        self._bot  = None   # TradingBot reference, set via attach()

        # ── Execution safety (Task 1) ─────────────────────────────────────
        # Prevents /run spam from creating multiple execution loops.
        self.is_running:     bool                    = False
        self.current_task:   Optional[asyncio.Task]  = None
        self.execution_lock: asyncio.Lock            = asyncio.Lock()

    def attach(self, trading_bot) -> None:
        self._bot = trading_bot
        # Sync auto-start state so /run and /stop stay consistent
        if trading_bot.running:
            self.is_running = True

    # kept for backward compatibility with code that calls set_trading_bot()
    def set_trading_bot(self, trading_bot) -> None:
        self._bot = trading_bot

    # ── Lifecycle ──────────────────────────────────────────────────────────────

    async def start(self) -> None:
        """
        Initialise and start polling in a background task.
        NEVER raises — Telegram failure is non-fatal.
        """
        if not _TG_OK or not self._token:
            log.info("Telegram: disabled (no token or library missing)")
            return
        try:
            # Build Application WITHOUT an Updater so PTB never manages polling
            app = (
                Application.builder()
                .token(self._token)
                .updater(None)
                .build()
            )

            for cmd, fn in [
                ("start",        self._cmd_start),
                ("run",          self._cmd_run),
                ("stop",         self._cmd_stop),
                ("status",       self._cmd_status),
                ("wallet",       self._cmd_wallet),
                ("reset",        self._cmd_reset),
                ("help",         self._cmd_start),
                ("trades",       self._cmd_trades),
                ("pnl",          self._cmd_pnl),
                ("positions",    self._cmd_positions),
                ("leaderboard",  self._cmd_leaderboard),
                ("traderpnl",    self._cmd_trader_pnl),
                ("alpha",        self._cmd_alpha),
            ]:
                app.add_handler(CommandHandler(cmd, fn))

            app.add_handler(
                MessageHandler(filters.TEXT & ~filters.COMMAND, self._handle_text)
            )

            self._app = app

            # Initialize the Application (bot session, handler registry)
            await app.initialize()
            # Start the Application dispatcher (update_queue → handlers)
            await app.start()

            # Spawn the polling loop as a background task
            self._task = asyncio.create_task(
                self._polling_task(), name="telegram_polling"
            )
            log.info("Telegram: polling task spawned")

        except Exception as exc:
            log.error("Telegram: start failed (non-fatal): %s", exc)
            self._app  = None
            self._task = None

    async def _polling_task(self) -> None:
        """
        Single-instance manual polling loop.

        Uses bot.get_updates() directly — bypasses the PTB Updater entirely
        so there is exactly ONE getUpdates connection at all times.

        On startup:
          - Calls getUpdates(offset=-1, timeout=0) to take over any stale
            session left by a previous instance. This terminates the old
            connection on Telegram's side before we enter the main loop.

        Each update is put into app.update_queue, which the Application's
        internal _update_fetcher_task dispatches to registered handlers.
        """
        if not self._app:
            return

        bot = self._app.bot
        offset = 0

        # ── Take over any existing session ────────────────────────────────
        # Calling getUpdates(offset=-1) terminates any stale long-poll held
        # by a previous bot instance and fast-forwards the offset so we do
        # not replay old updates.
        try:
            stale = await bot.get_updates(offset=-1, timeout=0)
            if stale:
                offset = stale[-1].update_id + 1
            log.info("Telegram: polling started (single instance)")
        except Exception as exc:
            log.warning("Telegram: session takeover warning (non-fatal): %s", exc)
            log.info("Telegram: polling started (single instance)")

        # ── Main polling loop ─────────────────────────────────────────────
        while True:
            try:
                updates = await bot.get_updates(
                    offset=offset,
                    timeout=10,
                    allowed_updates=Update.ALL_TYPES,
                )
                for update in updates:
                    offset = update.update_id + 1
                    await self._app.update_queue.put(update)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                log.warning("Telegram: polling error (retrying in 5s): %s", exc)
                await asyncio.sleep(5)

    async def stop(self) -> None:
        """Graceful shutdown — does not raise."""
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass

        if not self._app:
            return
        try:
            await self._app.stop()
            await self._app.shutdown()
        except Exception as exc:
            log.warning("Telegram: stop error (non-fatal): %s", exc)

    # ── Alert dispatch ────────────────────────────────────────────────────────

    async def send(self, text: str) -> None:
        """Send a plain-text message. NEVER raises."""
        if not self._app or not self._chat_ids:
            log.info("TG [no-send]: %s", text[:100])
            return
        for cid in self._chat_ids:
            try:
                await self._app.bot.send_message(
                    chat_id=cid, text=text, parse_mode="HTML"
                )
            except Exception as exc:
                log.warning("TG send to %s failed: %s", cid, exc)

    # ── Typed alert helpers ────────────────────────────────────────────────────

    async def alert_order_placed(self, trade_id: str, order_id: str, side: str,
                                  question: str, size: float, price: float) -> None:
        await self.send(
            f"<b>Order Placed</b>\n"
            f"trade_id: <code>{trade_id}</code>\n"
            f"orderID:  <code>{order_id}</code>\n"
            f"Market: {question[:50]}\n"
            f"Side: {side}  Size: ${size:.2f}  @ {price:.4f}"
        )

    async def alert_partial_fill(self, trade_id: str, filled_usd: float,
                                  total_usd: float, question: str) -> None:
        await self.send(
            f"<b>Partial Fill</b>\n"
            f"trade_id: <code>{trade_id}</code>\n"
            f"Market: {question[:50]}\n"
            f"Filled: ${filled_usd:.2f} / ${total_usd:.2f}"
        )

    async def alert_cancelled(self, trade_id: str, order_id: str, reason: str) -> None:
        await self.send(
            f"<b>Order Cancelled</b>\n"
            f"trade_id: <code>{trade_id}</code>\n"
            f"orderID:  <code>{order_id}</code>\n"
            f"Reason: {reason}"
        )

    async def alert_failure(self, trade_id: str, reason: str) -> None:
        await self.send(
            f"<b>Trade Failed</b>\n"
            f"trade_id: <code>{trade_id}</code>\n"
            f"Reason: {reason}"
        )

    async def alert_circuit_breaker(self, reason: str) -> None:
        await self.send(
            f"<b>CIRCUIT BREAKER TRIPPED</b>\n"
            f"Reason: {reason}\n"
            f"All trading halted. Send /reset to clear."
        )

    async def alert_trade_opened(
        self,
        question: str,
        side: str,
        price: float,
        size_usd: float,
        ev: float,
        z_score: float,
        confidence: float,
        social_boost: float,
        liq_boost: float,
        cash: float,
        active_trades: int,
    ) -> None:
        conf_pct = round(confidence * 100, 1)
        boost_line = ""
        if social_boost != 0.0 or liq_boost != 0.0:
            boost_line = (
                f"\n<b>Boost:</b>\n"
                f"Social: {social_boost:+.3f}\n"
                f"Liquidity: {liq_boost:+.3f}"
            )
        await self.send(
            f"<b>TRADE OPENED</b>\n\n"
            f"Market: {html.escape(question[:60])}\n"
            f"Side: <b>{side}</b>\n"
            f"Price: {price:.4f}\n"
            f"Size: <b>${size_usd:.2f}</b>\n\n"
            f"<b>Signal:</b>\n"
            f"EV: {ev:.3f}\n"
            f"Z-Score: {z_score:.2f}\n"
            f"Confidence: {conf_pct}%"
            f"{boost_line}\n\n"
            f"<b>Portfolio:</b>\n"
            f"Cash: ${cash:.2f}\n"
            f"Positions: {active_trades}"
        )

    async def alert_trade_closed(
        self,
        question: str,
        side: str,
        exit_price: float,
        pnl: float,
        pnl_pct: float,
        reason: str,
        hold_minutes: int,
        balance: float,
    ) -> None:
        pnl_sign = "+" if pnl >= 0 else ""
        await self.send(
            f"<b>POSITION CLOSED</b>\n\n"
            f"Market: {html.escape(question[:60])}\n"
            f"Side: {side}\n"
            f"Exit Price: {exit_price:.4f}\n"
            f"PnL: <b>{pnl_sign}${pnl:.2f} ({pnl_sign}{pnl_pct:.1f}%)</b>\n"
            f"Reason: {reason}\n"
            f"Duration: {hold_minutes} min\n\n"
            f"<b>Portfolio:</b>\n"
            f"Balance: ${balance:.2f}"
        )

    # ── Commands ──────────────────────────────────────────────────────────────

    async def _cmd_start(self, update: "Update", context) -> None:
        if not self._auth(update):
            return
        markup = ReplyKeyboardMarkup(self.KEYBOARD, resize_keyboard=True)
        mode   = "PAPER" if (self._bot and self._bot.client.is_paper_trading()) else "LIVE"
        await update.message.reply_text(
            f"<b>PolyBot v7.1</b>  |  Mode: <b>{mode}</b>\n\n"
            "/run                         \u2014 start trading\n"
            "/stop                        \u2014 pause trading\n"
            "/status                      \u2014 portfolio snapshot\n"
            "/wallet                      \u2014 balance\n"
            "/reset                       \u2014 reset circuit breaker\n"
            "/trades                      \u2014 open trades list\n"
            "/pnl                         \u2014 local portfolio PnL\n"
            "/pnl &lt;wallet&gt; [days]   \u2014 trader PnL + Wallet 360\n"
            "/positions                   \u2014 detailed positions\n"
            "/alpha                       \u2014 top opportunities this scan\n"
            "/leaderboard [1d|7d|30d]     \u2014 top traders leaderboard\n"
            "/traderpnl &lt;wallet&gt; [days] \u2014 trader PnL + profile",
            parse_mode="HTML",
            reply_markup=markup,
        )

    async def _cmd_run(self, update: "Update", context) -> None:
        if not self._auth(update):
            return
        if not self._bot:
            await update.message.reply_text("Bot not initialised."); return
        if self._bot.guard.is_halted:
            r = self._bot.guard._halt_reason or self._bot.guard.circuit_breaker.trip_reason
            await update.message.reply_text(
                f"Circuit breaker active.\nReason: {r}\nUse /reset first."
            )
            return

        async with self.execution_lock:          # ── Task 2: single-entry guard ──
            if self.is_running:
                await update.message.reply_text("Bot already running.")
                return

            self.is_running   = True
            self._bot.running = True
            # current_task tracks the loop guard (Task 4); the trading loop is
            # embedded in the main scan cycle, so we store None here to signal
            # that no separate coroutine was spawned from this command.
            self.current_task = None

        # Exactly ONE reply per valid start (Task 5)
        await update.message.reply_text("<b>Trading STARTED.</b>", parse_mode="HTML")
        await self.send("Bot started by operator.")
        log.info("Execution guard: trading started by operator")

    async def _cmd_stop(self, update: "Update", context) -> None:
        if not self._auth(update):
            return

        async with self.execution_lock:          # ── Task 3: single-entry guard ──
            if not self.is_running:
                await update.message.reply_text("Bot already stopped.")
                return

            self.is_running = False
            if self._bot:
                self._bot.running = False

            if self.current_task and not self.current_task.done():
                self.current_task.cancel()
                try:
                    await self.current_task
                except asyncio.CancelledError:
                    pass

            self.current_task = None

        await update.message.reply_text("<b>Trading STOPPED.</b>", parse_mode="HTML")
        await self.send("Bot stopped by operator.")
        log.info("Execution guard: trading stopped by operator")

    async def _cmd_status(self, update: "Update", context) -> None:
        if not self._auth(update):
            return
        if not self._bot:
            await update.message.reply_text("Bot not initialised."); return

        b  = self._bot
        p  = b.portfolio
        cb = b.guard.circuit_breaker
        ef = b.edge_filter

        halt_txt = ("YES \u2014 " + (b.guard._halt_reason or cb.trip_reason)) \
                    if b.guard.is_halted else "NO"
        pos_lines = [
            f"  {pos.side} {pos.question[:26]} ${pos.size_usd:.0f} {pos.pnl:+.2f}"
            for pos in list(p.positions.values())[:5]
        ] or ["  (none)"]

        try:
            await update.message.reply_text(
                f"<b>PolyBot Status</b>\n"
                f"Running: <b>{'YES' if b.running else 'NO'}</b>\n"
                f"Halted:  <b>{halt_txt}</b>\n\n"
                f"<b>Portfolio</b>\n"
                f"  Cash:      ${p.cash:.2f}\n"
                f"  Total:     ${p.total_value:.2f}\n"
                f"  Daily P&L: {p.daily_pnl:+.2f}\n"
                f"  Positions: {len(p.positions)}\n"
                f"  Win rate:  {p.win_rate()*100:.1f}%\n\n"
                f"<b>Positions</b>\n" + "\n".join(pos_lines) + "\n\n"
                f"<b>Circuit Breaker</b>\n"
                f"  Tripped: {cb.is_tripped}  "
                f"Consec: {cb._consecutive_failures}  "
                f"Loss: ${cb._session_loss:.2f}\n\n"
                f"<b>Edge Filter</b>\n  {ef.stats_summary()}",
                parse_mode="HTML",
            )
        except Exception as exc:
            log.warning("_cmd_status send error: %s", exc)

    async def _cmd_wallet(self, update: "Update", context) -> None:
        if not self._auth(update):
            return
        if not self._bot:
            await update.message.reply_text("Bot not initialised."); return
        try:
            addr    = self._bot.client.wallet_address
            balance = await self._bot.client.get_balance()
            paper   = self._bot.client.is_paper_trading()
            await update.message.reply_text(
                f"<b>Wallet</b>\n"
                f"Mode:    <b>{'PAPER' if paper else 'LIVE'}</b>\n"
                f"Address: <code>{addr or 'N/A'}</code>\n"
                f"Balance: <b>${balance:.2f} USDC</b>",
                parse_mode="HTML",
            )
        except Exception as exc:
            await update.message.reply_text(f"Wallet fetch error: {exc}")

    async def _cmd_trades(self, update: "Update", context) -> None:
        if not self._auth(update):
            return
        if not self._bot:
            await update.message.reply_text("Bot not initialised."); return
        positions = list(self._bot.portfolio.positions.values())
        if not positions:
            await update.message.reply_text("No active trades.")
            return
        lines = []
        for pos in positions:
            lines.append(
                f"<b>{pos.side}</b> {html.escape(pos.question[:40])}\n"
                f"  Size: ${pos.size_usd:.2f}  Entry: {pos.entry_price:.4f}"
            )
        try:
            await update.message.reply_text(
                f"<b>Open Trades ({len(positions)})</b>\n\n" + "\n\n".join(lines),
                parse_mode="HTML",
            )
        except Exception as exc:
            log.warning("_cmd_trades send error: %s", exc)

    async def _cmd_pnl(self, update: "Update", context) -> None:
        """
        /pnl              — show local portfolio PnL summary
        /pnl <wallet>     — show trader PnL via Intelligence API (agent 569 + 581)
        /pnl <wallet> <days> — same, for last N days
        """
        if not self._auth(update):
            return
        if not self._bot:
            await update.message.reply_text("Bot not initialised."); return

        # If a wallet argument is provided, use the intelligence API (agents 569 + 581)
        if context.args:
            wallet = context.args[0].strip()
            days   = 30
            if len(context.args) >= 2:
                try:
                    days = int(context.args[1])
                except ValueError:
                    pass

            intel = getattr(self._bot, "intelligence", None)
            if intel is None or not intel.available:
                await update.message.reply_text(
                    "Intelligence API unavailable — INTELLIGENCE_API_KEY not configured.\n"
                    "Use /pnl without arguments to see the local portfolio PnL."
                )
                return

            try:
                from integrations.intelligence_client import days_ago_date, today_date
                pnl_rows, wallet_360 = await asyncio.gather(
                    intel.get_pnl(
                        wallet=wallet,
                        granularity="1d",
                        start_time=days_ago_date(days),
                        end_time=today_date(),
                    ),
                    intel.get_wallet_360(proxy_wallet=wallet, window_days="7"),
                    return_exceptions=True,
                )
            except Exception as exc:
                await update.message.reply_text(f"Trader PnL fetch error: {exc}")
                return

            lines = [f"<b>Trader PnL</b> ({days}d)\n<code>{wallet[:20]}</code>\n"]

            if isinstance(pnl_rows, list) and pnl_rows:
                total_pnl = sum(
                    float(row.get("pnl", 0) or row.get("realized_pnl", 0) or 0)
                    for row in pnl_rows
                )
                lines.append(
                    f"Realized PnL ({days}d): <b>${total_pnl:+.2f}</b>\n"
                    f"Data points: {len(pnl_rows)}\n"
                )
            else:
                lines.append("PnL data unavailable.\n")

            if isinstance(wallet_360, list) and wallet_360:
                w = wallet_360[0]
                wr       = float(w.get("win_rate", 0) or 0)
                roi      = float(w.get("roi", 0) or 0)
                bot_sc   = float(w.get("bot_score", 0) or 0)
                div      = w.get("market_diversity") or w.get("unique_markets") or "?"
                h_score  = float(w.get("h_score", 0) or 0)
                lines.append(
                    f"<b>Wallet 360 (7d)</b>\n"
                    f"  Win rate:  {wr*100:.1f}%\n"
                    f"  ROI:       {roi*100:.1f}%\n"
                    f"  H-Score:   {h_score:.1f}\n"
                    f"  Bot score: {bot_sc:.2f}\n"
                    f"  Diversity: {div} markets\n"
                )
            else:
                lines.append("Wallet 360 data unavailable.\n")

            try:
                await update.message.reply_text("\n".join(lines), parse_mode="HTML")
            except Exception as exc:
                log.warning("_cmd_pnl (wallet) send error: %s", exc)
            return

        # No wallet argument — show local portfolio PnL
        p = self._bot.portfolio
        try:
            await update.message.reply_text(
                f"<b>PnL Summary</b>\n"
                f"Cash:          ${p.cash:.2f}\n"
                f"Total Value:   ${p.total_value:.2f}\n"
                f"Daily PnL:     {p.daily_pnl:+.2f}\n"
                f"All-Time PnL:  {p.all_time_pnl:+.2f}\n\n"
                f"<i>Tip: /pnl &lt;wallet&gt; [days] to look up any trader</i>",
                parse_mode="HTML",
            )
        except Exception as exc:
            log.warning("_cmd_pnl send error: %s", exc)

    async def _cmd_positions(self, update: "Update", context) -> None:
        if not self._auth(update):
            return
        if not self._bot:
            await update.message.reply_text("Bot not initialised."); return
        positions = list(self._bot.portfolio.positions.values())
        if not positions:
            await update.message.reply_text("No active positions.")
            return
        lines = []
        for pos in positions:
            lines.append(
                f"<b>{pos.side}</b> {html.escape(pos.question[:40])}\n"
                f"  Entry: {pos.entry_price:.4f}  Size: ${pos.size_usd:.2f}  PnL: {pos.pnl:+.2f}"
            )
        try:
            await update.message.reply_text(
                f"<b>Positions ({len(positions)})</b>\n\n" + "\n\n".join(lines),
                parse_mode="HTML",
            )
        except Exception as exc:
            log.warning("_cmd_positions send error: %s", exc)

    async def _cmd_leaderboard(self, update: "Update", context) -> None:
        if not self._auth(update):
            return
        if not self._bot:
            await update.message.reply_text("Bot not initialised."); return

        period = "7d"
        if context.args:
            raw = context.args[0].lower().strip()
            if raw in ("1d", "7d", "30d"):
                period = raw

        intel = getattr(self._bot, "intelligence", None)
        if intel is None or not intel.available:
            await update.message.reply_text(
                "Intelligence API unavailable — INTELLIGENCE_API_KEY not configured."
            )
            return

        try:
            entries = await intel.get_leaderboard(leaderboard_period=period, limit=10)
        except Exception as exc:
            await update.message.reply_text(f"Leaderboard fetch error: {exc}")
            return

        if not entries:
            await update.message.reply_text(f"No leaderboard data for period={period}.")
            return

        lines = [f"<b>Top Traders ({period})</b>\n"]
        for i, e in enumerate(entries[:10], 1):
            wallet  = e.get("proxy_wallet") or e.get("wallet") or e.get("address") or "?"
            pnl     = e.get("pnl") or e.get("profit") or 0
            roi     = e.get("roi") or 0
            wr      = e.get("win_rate") or 0
            lines.append(
                f"{i}. <code>{str(wallet)[:12]}</code>  "
                f"PnL=${float(pnl):.0f}  ROI={float(roi)*100:.1f}%  "
                f"WR={float(wr)*100:.1f}%"
            )
        try:
            await update.message.reply_text("\n".join(lines), parse_mode="HTML")
        except Exception as exc:
            log.warning("_cmd_leaderboard send error: %s", exc)

    async def _cmd_trader_pnl(self, update: "Update", context) -> None:
        if not self._auth(update):
            return
        if not self._bot:
            await update.message.reply_text("Bot not initialised."); return

        if not context.args:
            await update.message.reply_text(
                "Usage: /traderpnl &lt;wallet&gt; [days]\nExample: /traderpnl 0xabc123 30",
                parse_mode="HTML",
            )
            return

        wallet = context.args[0].strip()
        days   = 30
        if len(context.args) >= 2:
            try:
                days = int(context.args[1])
            except ValueError:
                pass

        intel = getattr(self._bot, "intelligence", None)
        if intel is None or not intel.available:
            await update.message.reply_text(
                "Intelligence API unavailable — INTELLIGENCE_API_KEY not configured."
            )
            return

        try:
            from integrations.intelligence_client import days_ago_date, today_date
            pnl_rows, wallet_360_list = await asyncio.gather(
                intel.get_pnl(
                    wallet=wallet,
                    granularity="1d",
                    start_time=days_ago_date(days),
                    end_time=today_date(),
                ),
                intel.get_wallet_360(proxy_wallet=wallet, window_days="7"),
                return_exceptions=True,
            )
        except Exception as exc:
            await update.message.reply_text(f"Trader PnL fetch error: {exc}")
            return

        lines = [f"<b>Trader Profile</b>\n<code>{wallet[:20]}</code>\n"]

        if isinstance(pnl_rows, list) and pnl_rows:
            total_pnl = sum(
                float(row.get("pnl", 0) or row.get("realized_pnl", 0) or 0)
                for row in pnl_rows
            )
            lines.append(
                f"<b>PnL ({days}d)</b>\n"
                f"  Realized: ${total_pnl:+.2f}\n"
                f"  Points:   {len(pnl_rows)}\n"
            )
        else:
            lines.append("PnL data unavailable.\n")

        wallet_360 = wallet_360_list[0] if isinstance(wallet_360_list, list) and wallet_360_list else None
        if wallet_360:
            wr        = float(wallet_360.get("win_rate", 0) or 0)
            bot_score = float(wallet_360.get("bot_score", 0) or 0)
            diversity = wallet_360.get("market_diversity") or wallet_360.get("unique_markets") or "?"
            h_score   = float(wallet_360.get("h_score", 0) or 0)
            roi       = float(wallet_360.get("roi", 0) or 0)
            lines.append(
                f"<b>Wallet 360 (7d)</b>\n"
                f"  Win rate:  {wr*100:.1f}%\n"
                f"  ROI:       {roi*100:.1f}%\n"
                f"  H-Score:   {h_score:.1f}\n"
                f"  Bot score: {bot_score:.2f}\n"
                f"  Diversity: {diversity} markets\n"
            )
        else:
            lines.append("Wallet 360 data unavailable.\n")

        try:
            await update.message.reply_text("\n".join(lines), parse_mode="HTML")
        except Exception as exc:
            log.warning("_cmd_trader_pnl send error: %s", exc)

    async def _cmd_reset(self, update: "Update", context) -> None:
        if not self._auth(update):
            return
        if self._bot:
            self._bot.guard.resume()
        await update.message.reply_text(
            "<b>Circuit breaker RESET.</b>\nUse /run to resume trading.",
            parse_mode="HTML",
        )

    async def _cmd_alpha(self, update: "Update", context) -> None:
        if not self._auth(update):
            return
        if not self._bot:
            await update.message.reply_text("Bot not initialised."); return
        signals = getattr(self._bot, "_top_signals", [])
        if not signals:
            await update.message.reply_text(
                "No signals yet — waiting for next scan cycle."
            )
            return
        lines = ["<b>TOP ALPHA SIGNALS</b>\n"]
        for i, s in enumerate(signals[:5], 1):
            boost_txt = ""
            if s.get("social", 0) != 0 or s.get("liq", 0) != 0:
                boost_txt = f"  Social: {s['social']:+.3f}  Liq: {s['liq']:+.3f}\n"
            lines.append(
                f"{i}. {html.escape(s['title'][:50])}\n"
                f"EV: {s['ev']:.3f}  Z: {s['z']:.2f}  Prob: {s['prob']:.2f}\n"
                f"Size: ${s['size']:.2f} @ {s['price']:.4f}\n"
                f"{boost_txt}"
            )
        try:
            await update.message.reply_text("\n".join(lines), parse_mode="HTML")
        except Exception as exc:
            log.warning("_cmd_alpha send error: %s", exc)

    async def _handle_text(self, update: "Update", context) -> None:
        if not self._auth(update):
            return
        text = (update.message.text or "").strip()
        if "\u25b6" in text:       await self._cmd_run(update, context)
        elif "\u23f9" in text:     await self._cmd_stop(update, context)
        elif "Status" in text:     await self._cmd_status(update, context)
        elif "Wallet" in text:     await self._cmd_wallet(update, context)
        elif "Alpha" in text:      await self._cmd_alpha(update, context)
        elif "Reset" in text:      await self._cmd_reset(update, context)
        else:                      await self._cmd_start(update, context)

    # ── Auth ───────────────────────────────────────────────────────────────────

    def _auth(self, update: "Update") -> bool:
        if not self._chat_ids:
            return True
        uid = str(update.effective_user.id) if update.effective_user else ""
        cid = str(update.effective_chat.id) if update.effective_chat else ""
        if uid in self._chat_ids or cid in self._chat_ids:
            return True
        log.warning("Unauthorised Telegram access uid=%s cid=%s", uid, cid)
        return False
