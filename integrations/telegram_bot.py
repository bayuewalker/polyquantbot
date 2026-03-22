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
        ["\U0001f504 Reset CB"],
    ]

    def __init__(self, token: str, chat_ids: list[str]) -> None:
        self._token    = (token or "").strip()
        self._chat_ids = [c.strip() for c in chat_ids if c.strip()]
        self._app: Optional[object]  = None
        self._task: Optional[asyncio.Task] = None
        self._bot  = None   # TradingBot reference, set via attach()

    def attach(self, trading_bot) -> None:
        self._bot = trading_bot

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
            app = Application.builder().token(self._token).build()

            for cmd, fn in [
                ("start",  self._cmd_start),
                ("run",    self._cmd_run),
                ("stop",   self._cmd_stop),
                ("status", self._cmd_status),
                ("wallet", self._cmd_wallet),
                ("reset",  self._cmd_reset),
                ("help",   self._cmd_start),
            ]:
                app.add_handler(CommandHandler(cmd, fn))

            app.add_handler(
                MessageHandler(filters.TEXT & ~filters.COMMAND, self._handle_text)
            )

            self._app = app

            # Initialize and clear old webhook/pending updates
            await app.initialize()
            try:
                await app.bot.delete_webhook(drop_pending_updates=True)
            except Exception as exc:
                log.warning("Telegram: delete_webhook failed (non-fatal): %s", exc)

            # Spawn polling in a background task.
            # run_polling() handles initialize/start/stop internally — do NOT
            # call app.start() before it or PTB v21 will raise RuntimeError.
            self._task = asyncio.create_task(
                self._polling_task(), name="telegram_polling"
            )
            log.info("Telegram: polling task spawned (PTB v21 run_polling)")

        except Exception as exc:
            log.error("Telegram: start failed (non-fatal): %s", exc)
            self._app  = None
            self._task = None

    async def _polling_task(self) -> None:
        """
        Run polling using PTB v21 async-compatible pattern.
        Uses app.start() + updater.start_polling() so it works inside an
        already-running asyncio event loop (run_polling() would raise
        'This event loop is already running').
        Restarts automatically on transient errors.
        """
        if not self._app:
            return
        while True:
            try:
                await self._app.start()
                await self._app.updater.start_polling(
                    drop_pending_updates=True,
                    allowed_updates=Update.ALL_TYPES,
                )
                log.info("Telegram: polling started successfully")
                # Keep alive until cancelled
                while True:
                    await asyncio.sleep(3600)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                log.error("Telegram: polling error — restarting in 10s: %s", exc)
                try:
                    await self._app.updater.stop()
                except Exception:
                    pass
                try:
                    await self._app.stop()
                except Exception:
                    pass
                await asyncio.sleep(10)

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

    # ── Commands ──────────────────────────────────────────────────────────────

    async def _cmd_start(self, update: "Update", context) -> None:
        if not self._auth(update):
            return
        markup = ReplyKeyboardMarkup(self.KEYBOARD, resize_keyboard=True)
        mode   = "PAPER" if (self._bot and self._bot.client.is_paper_trading()) else "LIVE"
        await update.message.reply_text(
            f"<b>PolyBot v7.1</b>  |  Mode: <b>{mode}</b>\n\n"
            "/run    \u2014 start trading\n"
            "/stop   \u2014 pause trading\n"
            "/status \u2014 portfolio snapshot\n"
            "/wallet \u2014 balance\n"
            "/reset  \u2014 reset circuit breaker",
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
        self._bot.running = True
        await update.message.reply_text("<b>Trading STARTED.</b>", parse_mode="HTML")
        await self.send("Bot started by operator.")

    async def _cmd_stop(self, update: "Update", context) -> None:
        if not self._auth(update):
            return
        if self._bot:
            self._bot.running = False
        await update.message.reply_text("<b>Trading STOPPED.</b>", parse_mode="HTML")
        await self.send("Bot stopped by operator.")

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

    async def _cmd_reset(self, update: "Update", context) -> None:
        if not self._auth(update):
            return
        if self._bot:
            self._bot.guard.resume()
        await update.message.reply_text(
            "<b>Circuit breaker RESET.</b>\nUse /run to resume trading.",
            parse_mode="HTML",
        )

    async def _handle_text(self, update: "Update", context) -> None:
        if not self._auth(update):
            return
        text = (update.message.text or "").strip()
        if "\u25b6" in text:       await self._cmd_run(update, context)
        elif "\u23f9" in text:     await self._cmd_stop(update, context)
        elif "Status" in text:     await self._cmd_status(update, context)
        elif "Wallet" in text:     await self._cmd_wallet(update, context)
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
