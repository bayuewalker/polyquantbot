"""
integrations/telegram_bot.py  —  PolyBot v7.1  v3.0.0
=======================================================
LATEST UPDATE — v3.0.0:
NEW: send_keyed(key, text, cooldown)  — deduplication with per-key cooldown.
     Prevents heartbeat / startup spam even if called every cycle.
NEW: send_startup_report(...)  — uses format_startup_report() from telegram_formatter,
     also keyed with 30s cooldown to prevent restart-loop spam.
FIXED: Startup message no longer spams on crash-restart loop.
FIXED: HEARTBEAT no longer spams every cycle — 60s cooldown enforced.
KEPT: All v2.0.0 commands (/positions /trades /filter /pause N)
KEPT: All alert helpers (alert_order_placed, alert_partial_*, etc.)
KEPT: PTB v21 run_polling() — zero updater refs, send() never raises
"""
from __future__ import annotations

import asyncio
import datetime
import logging
import time
from typing import TYPE_CHECKING, Optional

log = logging.getLogger("polybot.telegram")

try:
    from telegram import Update, ReplyKeyboardMarkup
    from telegram.ext import (
        Application,
        CommandHandler,
        MessageHandler,
        filters,
    )
    _TG_OK = True
except ImportError:
    _TG_OK = False
    log.warning("python-telegram-bot not installed — Telegram disabled")

if TYPE_CHECKING:
    pass

# Import formatter — fail gracefully if not present
try:
    from integrations.telegram_formatter import (
        format_startup_report,
        format_status,
        format_alpha,
        format_heartbeat,
    )
    _FMT_OK = True
except ImportError:
    _FMT_OK = False
    log.warning("telegram_formatter not found — using fallback formatting")


# ── Formatting helpers ────────────────────────────────────────────────────────

def _pnl_str(pnl: float) -> str:
    return f"<b>+${pnl:.2f}</b>" if pnl >= 0 else f"<b>-${abs(pnl):.2f}</b>"

def _progress_bar(pct: float, width: int = 10) -> str:
    filled = max(0, min(width, round(pct / 100 * width)))
    return "\u2588" * filled + "\u2591" * (width - filled)

def _fmt_duration(seconds: float) -> str:
    if seconds < 60: return f"{seconds:.0f}s"
    if seconds < 3600: return f"{seconds/60:.0f}m"
    return f"{seconds/3600:.1f}h"

def _eta_str(delay_s: float) -> str:
    t = datetime.datetime.now() + datetime.timedelta(seconds=delay_s)
    return t.strftime("%H:%M")


# ── TelegramBot ───────────────────────────────────────────────────────────────

class TelegramBot:

    KEYBOARD = [
        ["\u25b6\ufe0f Run",     "\u23f9\ufe0f Stop"],
        ["\U0001f4ca Status",   "\U0001f4b0 Wallet"],
        ["\U0001f4cb Positions","\U0001f9ee Filter"],
        ["\U0001f504 Reset CB"],
    ]

    def __init__(self, token: str, chat_ids: list[str]) -> None:
        self._token    = (token or "").strip()
        self._chat_ids = [c.strip() for c in chat_ids if c.strip()]
        self._app: Optional[object]        = None
        self._task: Optional[asyncio.Task] = None
        self._pause_task: Optional[asyncio.Task] = None
        self._bot  = None

        # Keyed message deduplication: {key -> last_sent_timestamp}
        self._keyed_sent: dict[str, float] = {}

    def attach(self, trading_bot) -> None:
        self._bot = trading_bot

    def set_trading_bot(self, trading_bot) -> None:
        self._bot = trading_bot

    # ── Lifecycle ──────────────────────────────────────────────────────────────

    async def start(self) -> None:
        if not _TG_OK or not self._token:
            log.info("Telegram: disabled (no token or library missing)")
            return
        try:
            app = Application.builder().token(self._token).build()
            for cmd, fn in [
                ("start",     self._cmd_start), ("help",      self._cmd_start),
                ("run",       self._cmd_run),   ("stop",      self._cmd_stop),
                ("status",    self._cmd_status),("wallet",    self._cmd_wallet),
                ("reset",     self._cmd_reset), ("positions", self._cmd_positions),
                ("trades",    self._cmd_trades),("filter",    self._cmd_filter),
                ("pause",     self._cmd_pause), ("alpha",     self._cmd_alpha),
            ]:
                app.add_handler(CommandHandler(cmd, fn))
            app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self._handle_text))
            self._app = app
            await app.initialize()
            try:
                await app.bot.delete_webhook(drop_pending_updates=True)
            except Exception as e:
                log.warning("delete_webhook: %s", e)
            self._task = asyncio.create_task(self._polling_task(), name="telegram_polling")
            log.info("Telegram: polling started (PTB v21)")
        except Exception as e:
            log.error("Telegram start failed: %s", e)
            self._app = self._task = None

    async def _polling_task(self) -> None:
        if not self._app: return
        while True:
            try:
                await self._app.run_polling(
                    drop_pending_updates=True,
                    allowed_updates=Update.ALL_TYPES,
                    close_loop=False,
                )
                break
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error("Telegram polling error — restart 10s: %s", e)
                await asyncio.sleep(10)

    async def stop(self) -> None:
        for task in (self._pause_task, self._task):
            if task and not task.done():
                task.cancel()
                try: await task
                except: pass
        if not self._app: return
        try:
            await self._app.stop()
            await self._app.shutdown()
        except Exception as e:
            log.warning("Telegram stop error: %s", e)

    # ── Core send ─────────────────────────────────────────────────────────────

    async def send(self, text: str) -> None:
        """Broadcast HTML message to all authorised chats. NEVER raises."""
        if not self._app or not self._chat_ids:
            log.info("TG [no-send]: %s", text[:100])
            return
        for cid in self._chat_ids:
            try:
                await self._app.bot.send_message(
                    chat_id=cid, text=text, parse_mode="HTML"
                )
            except Exception as e:
                log.warning("TG send to %s failed: %s", cid, e)

    async def send_keyed(self, key: str, text: str, cooldown: float = 60.0) -> None:
        """
        Send message only if cooldown has elapsed since last send for this key.
        Prevents duplicate startup / heartbeat spam even when called every cycle.

        key      — unique identifier (e.g. "heartbeat", "startup")
        text     — HTML message body
        cooldown — minimum seconds between sends for this key (default: 60s)
        """
        now  = time.time()
        last = self._keyed_sent.get(key, 0.0)
        if now - last < cooldown:
            log.debug("send_keyed: suppressed key=%s (%.0fs < %.0fs cooldown)",
                      key, now - last, cooldown)
            return
        self._keyed_sent[key] = now
        await self.send(text)

        # Prune stale keys every ~100 sends to avoid unbounded growth
        if len(self._keyed_sent) > 200:
            cutoff = now - 3600
            self._keyed_sent = {k: v for k, v in self._keyed_sent.items() if v > cutoff}

    async def send_startup_report(
        self,
        mode: str,
        balance: float,
        cash: float,
        position_count: int,
        max_positions: int,
        market_count: int,
        scan_interval: int,
        intelligence_status: str,
        autostart: bool,
    ) -> None:
        """
        Send startup notification exactly once per boot, even in crash-restart loops.
        Uses send_keyed with 30s cooldown — second call within 30s is silently dropped.
        """
        if _FMT_OK:
            text = format_startup_report(
                mode=mode,
                balance=balance,
                cash=cash,
                position_count=position_count,
                max_positions=max_positions,
                market_count=market_count,
                scan_interval=scan_interval,
                intelligence_status=intelligence_status,
                autostart=autostart,
            )
        else:
            state = "ACTIVE" if autostart else "STANDBY — send /run to start"
            text = (
                f"\U0001f916 <b>POLYBOT v7.1 ONLINE</b>\n"
                f"Mode: <b>{mode}</b>  Balance: <b>${balance:.2f}</b>\n"
                f"Intelligence: {intelligence_status}\n"
                f"Trading: <b>{state}</b>"
            )
        await self.send_keyed("startup", text, cooldown=30.0)

    # ── Typed alert helpers ────────────────────────────────────────────────────

    async def alert_startup(self, mode: str, balance: float, positions: int) -> None:
        await self.send_keyed(
            "startup",
            f"\U0001f7e2 <b>PolyBot v7.1 Started</b>\n"
            f"Mode: <b>{mode}</b>  Balance: <b>${balance:.2f}</b>\n"
            f"Positions: {positions} open  |  Send /run to trade.",
            cooldown=30.0,
        )

    async def alert_order_placed(
        self, trade_id: str, order_id: str, side: str, question: str,
        size: float, price: float, sl: float,
        ev: float = 0.0, z_score: float = 0.0, strategy: str = "",
        signal_meta: Optional[dict] = None,
    ) -> None:
        meta    = signal_meta or {}
        ev      = ev or meta.get("ev", 0.0) or 0.0
        z_score = z_score or meta.get("z_score", 0.0) or 0.0
        conf    = meta.get("confidence", 0.0) or 0.0
        social  = meta.get("social_boost", 0.0) or 0.0
        liq     = meta.get("liq_boost", 0.0) or 0.0
        strat   = f"  [{strategy}]" if strategy else ""
        boost_txt = ""
        if social != 0.0 or liq != 0.0:
            boost_txt = f"\nBoosts: Social {social:+.3f}  Liq {liq:+.3f}"
        await self.send(
            f"\u2705 <b>Trade Opened</b>{strat}\n"
            f"Market: {question[:50]}\n"
            f"Side: <b>{side}</b>  Size: <b>${size:.2f}</b>  @ {price:.4f}\n"
            f"SL: {sl:.4f}  EV: <code>{ev:+.4f}</code>  Z: <code>{z_score:.2f}</code>"
            f"{boost_txt}\n"
            f"<code>{trade_id}</code>"
        )

    async def alert_order_gtc(
        self, trade_id: str, order_id: str, side: str, question: str,
        size: float, price: float, timeout_s: float = 60.0
    ) -> None:
        await self.send(
            f"\u23f3 <b>Order Placed — Awaiting Fill (GTC)</b>\n"
            f"Market: {question[:50]}\n"
            f"Side: <b>{side}</b>  Size: <b>${size:.2f}</b>  @ {price:.4f}\n"
            f"Auto-cancel after {timeout_s:.0f}s\n"
            f"<code>{trade_id}</code>"
        )

    async def alert_partial_fill(
        self, trade_id: str, filled_usd: float, total_usd: float,
        question: str, timeout_s: float = 60.0
    ) -> None:
        pct = filled_usd / total_usd * 100 if total_usd else 0
        await self.send(
            f"\u26a0\ufe0f <b>Partial Fill Detected</b>\n"
            f"Market: {question[:50]}\n"
            f"{_progress_bar(pct)}  {pct:.0f}%\n"
            f"Filled: <b>${filled_usd:.2f}</b> / ${total_usd:.2f}\n"
            f"Monitoring every 2s  timeout {timeout_s:.0f}s\n"
            f"<code>{trade_id}</code>"
        )

    async def alert_partial_update(
        self, trade_id: str, filled_usd: float, total_usd: float, elapsed_s: float
    ) -> None:
        pct = filled_usd / total_usd * 100 if total_usd else 0
        await self.send(
            f"\U0001f504 <b>Partial Fill Update</b>  ({elapsed_s:.0f}s elapsed)\n"
            f"{_progress_bar(pct)}  {pct:.0f}%\n"
            f"<code>{trade_id}</code>"
        )

    async def alert_partial_resolved(
        self, trade_id: str, filled_usd: float, total_usd: float,
        question: str, price: float, sl: float
    ) -> None:
        pct = filled_usd / total_usd * 100 if total_usd else 0
        await self.send(
            f"\u2705 <b>Partial Fill Resolved</b>\n"
            f"Market: {question[:50]}\n"
            f"Filled: <b>${filled_usd:.2f}</b> / ${total_usd:.2f}  ({pct:.0f}%)\n"
            f"Entry: {price:.4f}  SL: {sl:.4f}\n"
            f"<code>{trade_id}</code>"
        )

    async def alert_cancelled(
        self, trade_id: str, order_id: str, reason: str, held_sec: float = 0.0
    ) -> None:
        held = f"  (held {_fmt_duration(held_sec)})" if held_sec > 0 else ""
        await self.send(
            f"\u274c <b>Order Cancelled</b>{held}\n"
            f"Reason: {reason}\n"
            f"<code>{trade_id}</code>"
        )

    async def alert_failure(self, trade_id: str, reason: str, attempts: int = 0) -> None:
        att = f"  ({attempts} attempts)" if attempts > 0 else ""
        await self.send(
            f"\U0001f534 <b>Trade Failed</b>{att}\n"
            f"Reason: {reason}\n"
            f"<code>{trade_id}</code>"
        )

    async def alert_circuit_breaker(self, reason: str) -> None:
        snap = ""
        if self._bot:
            cb   = self._bot.guard.circuit_breaker
            snap = f"\nConsec failures: {cb._consecutive_failures}\nSession loss: ${cb._session_loss:.2f}"
        await self.send(
            f"\U0001f6a8 <b>CIRCUIT BREAKER TRIPPED</b>\n"
            f"Reason: {reason}" + snap +
            f"\n\nAll trading halted. Use /reset to clear."
        )

    async def alert_trade_closed(
        self, question: str, side: str, reason: str,
        entry: float, exit_price: float, pnl: float, hold_sec: float = 0.0
    ) -> None:
        ret = (exit_price - entry) / entry * 100 if entry > 0 else 0.0
        if side == "NO": ret = -ret
        arrow = "\u2B06\ufe0f" if pnl >= 0 else "\u2B07\ufe0f"
        await self.send(
            f"{arrow} <b>Position Closed</b>\n"
            f"Market: {question[:50]}\n"
            f"Side: {side}  Reason: <i>{reason}</i>\n"
            f"Entry: {entry:.4f}  \u2192  Exit: {exit_price:.4f}\n"
            f"P&amp;L: {_pnl_str(pnl)}  ({ret:+.1f}%)"
            + (f"  [{_fmt_duration(hold_sec)}]" if hold_sec > 0 else "")
        )

    async def alert_daily_summary(
        self, date_str: str, pnl_today: float, pnl_alltime: float,
        balance: float, win_rate: float, sharpe: float, ef_summary: str
    ) -> None:
        arrow = "\u2B06\ufe0f" if pnl_today >= 0 else "\u2B07\ufe0f"
        await self.send(
            f"\U0001f4ca <b>Daily Summary</b>  {date_str}\n"
            f"P&amp;L today:  {arrow} {_pnl_str(pnl_today)}\n"
            f"All-time:    <b>${pnl_alltime:+.2f}</b>\n"
            f"Balance:     <b>${balance:.2f}</b>\n"
            f"Win rate:    {win_rate*100:.1f}%  Sharpe: {sharpe:.2f}\n\n"
            f"{ef_summary}"
        )

    # ── Commands ──────────────────────────────────────────────────────────────

    async def _cmd_start(self, update: "Update", context) -> None:
        if not self._auth(update): return
        try:
            markup = ReplyKeyboardMarkup(self.KEYBOARD, resize_keyboard=True)
            mode   = "PAPER" if (self._bot and self._bot.client.is_paper_trading()) else "LIVE"
            bal = ""
            if self._bot:
                try:
                    b   = await self._bot.client.get_balance()
                    bal = f"\nBalance: <b>${b:.2f} USDC</b>"
                except Exception: pass
            await update.message.reply_text(
                f"<b>PolyBot v7.1</b>  Mode: <b>{mode}</b>{bal}\n\n"
                "/run        \u2014 start trading\n"
                "/stop       \u2014 pause trading\n"
                "/pause N    \u2014 pause for N minutes\n"
                "/status     \u2014 portfolio snapshot\n"
                "/positions  \u2014 open positions\n"
                "/trades     \u2014 today's stats\n"
                "/filter     \u2014 edge filter breakdown\n"
                "/alpha      \u2014 top signals this cycle\n"
                "/wallet     \u2014 balance &amp; address\n"
                "/reset      \u2014 reset circuit breaker",
                parse_mode="HTML", reply_markup=markup,
            )
        except Exception as e:
            log.warning("_cmd_start: %s", e)

    async def _cmd_run(self, update: "Update", context) -> None:
        if not self._auth(update): return
        if not self._bot: await update.message.reply_text("Bot not initialised."); return
        try:
            if self._bot.guard.is_halted:
                r = self._bot.guard._halt_reason or self._bot.guard.circuit_breaker.trip_reason
                await update.message.reply_text(f"\U0001f6a8 CB active: {r}\nUse /reset first.")
                return
            if self._pause_task and not self._pause_task.done():
                self._pause_task.cancel(); self._pause_task = None
            self._bot.running = True
            await update.message.reply_text("\u25b6\ufe0f <b>Trading STARTED.</b>", parse_mode="HTML")
        except Exception as e:
            log.warning("_cmd_run: %s", e)

    async def _cmd_stop(self, update: "Update", context) -> None:
        if not self._auth(update): return
        try:
            if self._bot: self._bot.running = False
            if self._pause_task and not self._pause_task.done():
                self._pause_task.cancel(); self._pause_task = None
            await update.message.reply_text("\u23f9\ufe0f <b>Trading STOPPED.</b>", parse_mode="HTML")
        except Exception as e:
            log.warning("_cmd_stop: %s", e)

    async def _cmd_pause(self, update: "Update", context) -> None:
        if not self._auth(update): return
        if not self._bot: await update.message.reply_text("Bot not initialised."); return
        try:
            minutes = 10
            if context.args:
                try: minutes = max(1, min(int(context.args[0]), 480))
                except: pass
            self._bot.running = False
            if self._pause_task and not self._pause_task.done(): self._pause_task.cancel()
            self._pause_task = asyncio.create_task(
                self._auto_resume(minutes * 60), name="pause_timer"
            )
            await update.message.reply_text(
                f"\u23f8\ufe0f <b>Paused {minutes}min.</b>  Auto-resume at {_eta_str(minutes*60)}",
                parse_mode="HTML",
            )
        except Exception as e:
            log.warning("_cmd_pause: %s", e)

    async def _auto_resume(self, delay_s: float) -> None:
        try:
            await asyncio.sleep(delay_s)
            if self._bot and not self._bot.guard.is_halted:
                self._bot.running = True
                await self.send(f"\u25b6\ufe0f <b>Auto-resumed</b> after {_fmt_duration(delay_s)} pause.")
        except asyncio.CancelledError: pass
        except Exception as e: log.warning("_auto_resume: %s", e)

    async def _cmd_status(self, update: "Update", context) -> None:
        if not self._auth(update): return
        if not self._bot: await update.message.reply_text("Bot not initialised."); return
        try:
            b = self._bot; p = b.portfolio; cb = b.guard.circuit_breaker; ef = b.edge_filter
            halt = ("YES \u2014 " + (b.guard._halt_reason or cb.trip_reason)) if b.guard.is_halted else "NO"

            if _FMT_OK:
                positions_data = [
                    {
                        "side":          pos.side,
                        "question":      pos.question,
                        "entry_price":   pos.entry_price,
                        "current_price": pos.current_price,
                        "pnl":           pos.pnl,
                    }
                    for pos in list(p.positions.values())[:10]
                ]
                unrealized = sum(pos.pnl for pos in p.positions.values())
                text = format_status(
                    running=b.running,
                    is_halted=b.guard.is_halted,
                    halt_reason=b.guard._halt_reason or cb.trip_reason,
                    balance=p.total_value,
                    cash=p.cash,
                    unrealized_pnl=unrealized,
                    daily_pnl=p.daily_pnl,
                    drawdown_pct=p.max_drawdown() * 100,
                    positions=positions_data,
                    cycle_limit=getattr(b, "max_trades_per_cycle", 3),
                    scan_interval=b._scan_interval,
                )
            else:
                pos_lines = [
                    f"  {pos.side} {pos.question[:24]} <b>${pos.size_usd:.0f}</b> {_pnl_str(pos.pnl)}"
                    for pos in list(p.positions.values())[:5]
                ] or ["  (none)"]
                text = (
                    f"\U0001f916 <b>PolyBot Status</b>\n"
                    f"Running: <b>{'YES' if b.running else 'NO'}</b>  Halted: <b>{halt}</b>\n\n"
                    f"Cash: ${p.cash:.2f}  Total: <b>${p.total_value:.2f}</b>\n"
                    f"Daily P&amp;L: {_pnl_str(p.daily_pnl)}  Win rate: {p.win_rate()*100:.1f}%\n\n"
                    f"<b>Positions</b>\n" + "\n".join(pos_lines) + "\n\n"
                    f"CB: {cb.is_tripped}  Consec: {cb._consecutive_failures}  Loss: ${cb._session_loss:.2f}\n"
                    f"{ef.stats_summary()}"
                )
            await update.message.reply_text(text, parse_mode="HTML")
        except Exception as e:
            log.warning("_cmd_status: %s", e)
            try: await update.message.reply_text(f"Error: {e}")
            except: pass

    async def _cmd_positions(self, update: "Update", context) -> None:
        if not self._auth(update): return
        if not self._bot: await update.message.reply_text("Bot not initialised."); return
        try:
            p = self._bot.portfolio
            if not p.positions: await update.message.reply_text("\U0001f4cb No open positions."); return
            lines = [f"\U0001f4cb <b>Open Positions ({len(p.positions)})</b>\n"]
            for pos in p.positions.values():
                ret = (pos.current_price - pos.entry_price) / pos.entry_price * 100 if pos.entry_price else 0
                if pos.side == "NO": ret = -ret
                lines.append(
                    f"\u2022 <b>{pos.side}</b> {pos.question[:34]}\n"
                    f"   ${pos.size_usd:.0f}  {pos.entry_price:.4f}\u2192{pos.current_price:.4f}  "
                    f"{_pnl_str(pos.pnl)} ({ret:+.1f}%)  SL={pos.stop_loss:.4f}"
                )
            await update.message.reply_text("\n".join(lines), parse_mode="HTML")
        except Exception as e:
            log.warning("_cmd_positions: %s", e)

    async def _cmd_trades(self, update: "Update", context) -> None:
        if not self._auth(update): return
        if not self._bot: await update.message.reply_text("Bot not initialised."); return
        try:
            p = self._bot.portfolio
            await update.message.reply_text(
                f"\U0001f4c8 <b>Today's Trading</b>\n"
                f"Trades: {p.trades_total}  Wins: {p.trades_won}  Win rate: {p.win_rate()*100:.1f}%\n"
                f"Daily P&amp;L: {_pnl_str(p.daily_pnl)}\n"
                f"Gross profit: ${p.gross_profit:.2f}  Loss: ${p.gross_loss:.2f}\n"
                f"Profit factor: <b>{p.profit_factor():.2f}</b>",
                parse_mode="HTML",
            )
        except Exception as e:
            log.warning("_cmd_trades: %s", e)

    async def _cmd_filter(self, update: "Update", context) -> None:
        if not self._auth(update): return
        if not self._bot: await update.message.reply_text("Bot not initialised."); return
        try:
            ef = self._bot.edge_filter; c = ef.counters(); total = sum(c.values())
            passed = c["PASS"]; rate = f"{passed/total*100:.1f}%" if total else "n/a"
            lines = [f"\U0001f9ee <b>Edge Filter</b>\nPassed: <b>{passed}</b>/{total}  ({rate})\n"]
            for reason in ("NO_EDGE","LOW_EV","WEAK_SIGNAL","WIDE_SPREAD","LOW_LIQUIDITY","PROB_SANITY"):
                n = c.get(reason, 0)
                if n:
                    pct = n/total*100 if total else 0
                    lines.append(f"  {reason:<16} {_progress_bar(pct,8)} {n}")
            await update.message.reply_text("\n".join(lines), parse_mode="HTML")
        except Exception as e:
            log.warning("_cmd_filter: %s", e)

    async def _cmd_alpha(self, update: "Update", context) -> None:
        if not self._auth(update): return
        if not self._bot: await update.message.reply_text("Bot not initialised."); return
        try:
            signals = getattr(self._bot, "_top_signals", [])
            if _FMT_OK:
                text = format_alpha(signals)
            else:
                if not signals:
                    text = "\U0001f52d <b>No alpha signals</b> — waiting for next scan."
                else:
                    lines = ["\U0001f525 <b>Top Signals</b>\n"]
                    for i, s in enumerate(signals[:5], 1):
                        lines.append(
                            f"{i}. {s.get('title','?')[:50]}\n"
                            f"   EV: {s.get('ev',0):.3f}  Z: {s.get('z',0):.2f}  "
                            f"Size: ${s.get('size',0):.2f}"
                        )
                    text = "\n".join(lines)
            await update.message.reply_text(text, parse_mode="HTML")
        except Exception as e:
            log.warning("_cmd_alpha: %s", e)

    async def _cmd_wallet(self, update: "Update", context) -> None:
        if not self._auth(update): return
        if not self._bot: await update.message.reply_text("Bot not initialised."); return
        try:
            addr = self._bot.client.wallet_address
            bal  = await self._bot.client.get_balance()
            paper = self._bot.client.is_paper_trading()
            await update.message.reply_text(
                f"\U0001f4b0 <b>Wallet</b>\n"
                f"Mode: <b>{'PAPER' if paper else 'LIVE'}</b>\n"
                f"Address: <code>{addr or 'N/A'}</code>\n"
                f"Balance: <b>${bal:.2f} USDC</b>",
                parse_mode="HTML",
            )
        except Exception as e:
            await update.message.reply_text(f"Wallet error: {e}")

    async def _cmd_reset(self, update: "Update", context) -> None:
        if not self._auth(update): return
        try:
            if self._bot: self._bot.guard.resume()
            await update.message.reply_text(
                "\U0001f504 <b>Circuit breaker RESET.</b>\nUse /run to resume.",
                parse_mode="HTML",
            )
        except Exception as e:
            log.warning("_cmd_reset: %s", e)

    async def _handle_text(self, update: "Update", context) -> None:
        if not self._auth(update): return
        text = (update.message.text or "").strip()
        if "\u25b6" in text:       await self._cmd_run(update, context)
        elif "\u23f9" in text:     await self._cmd_stop(update, context)
        elif "Status" in text:     await self._cmd_status(update, context)
        elif "Wallet" in text:     await self._cmd_wallet(update, context)
        elif "Positions" in text:  await self._cmd_positions(update, context)
        elif "Filter" in text:     await self._cmd_filter(update, context)
        elif "Reset" in text:      await self._cmd_reset(update, context)
        else:                      await self._cmd_start(update, context)

    def _auth(self, update: "Update") -> bool:
        if not self._chat_ids: return True
        uid = str(update.effective_user.id) if update.effective_user else ""
        cid = str(update.effective_chat.id) if update.effective_chat else ""
        if uid in self._chat_ids or cid in self._chat_ids: return True
        log.warning("Unauthorised TG uid=%s cid=%s", uid, cid)
        return False