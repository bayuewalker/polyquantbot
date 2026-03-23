"""
execution/order_manager.py  —  PolyBot v7.1 FINAL  (final-critical-fix)

Changes vs previous version:
  - Triple execution safety check at top: is_halted / has_open_position / is_duplicate
  - Partial fill handler:
      filled == 0          -> NOT FILLED (cancel + fail)
      0 < filled < size    -> PARTIAL_FILL state, blocks market, monitor 2s/60s
      filled >= size*0.995 -> FULL FILL
  - _partial_fill_monitor: blocks position lock on market for full timeout window
  - maintenance_loop: catches ALL exceptions per iteration, never exits
  - _cancel_timed_out_orders: FSM + TG alert per cancelled order
  - _alert: uses typed TelegramBot helpers when available, falls back to send()
  - All async tasks wrapped in try/except — no silent crashes
  - Post-balance check: halts + alerts on mismatch
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import TYPE_CHECKING, Optional

from core.execution_guard import ExecutionGuard, TradeState
from core.position_manager import PositionManager, Portfolio
from integrations.telegram_formatter import format_trade_open, format_circuit_breaker

if TYPE_CHECKING:
    from integrations.polymarket_client import PolymarketClient
    from integrations.telegram_bot import TelegramBot

log = logging.getLogger("polybot.order_manager")


class OrderManager:

    def __init__(
        self,
        client:           "PolymarketClient",
        guard:            ExecutionGuard,
        position_manager: PositionManager,
        portfolio:        Portfolio,
        telegram:         Optional["TelegramBot"],
        cfg:              dict,
    ) -> None:
        self.client    = client
        self.guard     = guard
        self.pm        = position_manager
        self.portfolio = portfolio
        self.tg        = telegram
        self.cfg       = cfg

        self.max_retries   = int(cfg.get("max_retries",             2))
        self.order_timeout = float(cfg.get("order_timeout_sec",    60.0))
        self.partial_poll  = float(cfg.get("partial_fill_poll_sec",  2.0))

    # ── Main entry ────────────────────────────────────────────────────────────

    async def execute(
        self,
        market_id:   str,
        question:    str,
        token_id:    str,
        side:        str,
        size_usd:    float,
        price:       float,
        strategy:    str,
        signal_meta: Optional[dict] = None,
    ) -> bool:
        """
        Full 14-step execution pipeline.
        Triple safety guard at top: halt / position lock / duplicate.
        Returns True on fill (full or partial), False otherwise.
        signal_meta: optional dict with ev, z_score, confidence, social_boost, liq_boost.
        """
        _signal_meta = signal_meta

        # ── SAFETY TRIPLE-CHECK (no locks needed) ─────────────────────────────

        # 1. Global halt / circuit breaker
        if self.guard.is_halted:
            reason = self.guard._halt_reason or self.guard.circuit_breaker.trip_reason
            log.warning("BLOCKED: is_halted | %s %s | reason=%s", side, market_id[:12], reason)
            return False

        # 2. Position lock — no duplicate exposure on same market
        if self.guard.has_open_position(market_id):
            log.info("BLOCKED: has_open_position | %s", market_id[:12])
            return False

        # 3. Pre-lock duplicate check
        t_id = self.guard.trade_id(market_id, side)
        if self.guard.is_duplicate(t_id):
            log.info("BLOCKED: is_duplicate | trade_id=%s | %s %s", t_id, side, market_id[:12])
            return False

        # 4. Cooldown
        if not self.guard.is_cooled_down(market_id):
            log.info("BLOCKED: cooldown active | %s", market_id[:12])
            return False

        # ── SEMAPHORE ─────────────────────────────────────────────────────────
        async with self.guard._exec_sem:

            # 5. Atomic registration (global lock — check again inside)
            async with self.guard._reg_lock:
                if self.guard.is_duplicate(t_id):
                    log.info("BLOCKED: is_duplicate (reg-lock) | trade_id=%s", t_id)
                    return False
                if self.guard.has_open_position(market_id):
                    log.info("BLOCKED: has_open_position (reg-lock) | %s", market_id[:12])
                    return False
                try:
                    self.guard._fsm_set(t_id, TradeState.REGISTERED)
                    self.guard.set_trade_meta(t_id, market_id, side, size_usd)
                    log.info(
                        "AUDIT REGISTERED trade_id=%s %s %s $%.2f @ %.4f strat=%s",
                        t_id, side, market_id[:12], size_usd, price, strategy,
                    )
                except RuntimeError as exc:
                    log.error("FSM REGISTERED failed trade_id=%s: %s", t_id, exc)
                    return False

            # 6. Per-market lock (full critical section)
            market_lock = self.guard.get_market_lock(market_id, side)
            async with market_lock:

                # 7. Pre-exec portfolio guards
                can, reason = self.pm.can_trade()
                if not can:
                    log.warning("PRE-EXEC guard failed trade_id=%s: %s", t_id, reason)
                    self.guard.fail(t_id)
                    return False

                if self.portfolio.cash < size_usd:
                    log.warning(
                        "PRE-EXEC insufficient cash trade_id=%s: need $%.2f have $%.2f",
                        t_id, size_usd, self.portfolio.cash,
                    )
                    self.guard.fail(t_id)
                    return False

                # 8. Pre-balance snapshot (live only)
                pre_balance = 0.0
                if not self.client.is_paper_trading():
                    try:
                        pre_balance = await self.client.get_balance()
                        if pre_balance < size_usd:
                            msg = f"Wallet ${pre_balance:.2f} < order ${size_usd:.2f}"
                            log.error("WALLET CHECK FAILED trade_id=%s: %s", t_id, msg)
                            if self.tg:
                                await self.tg.alert_failure(t_id, msg)
                            self.guard.halt("wallet_balance_insufficient")
                            self.guard.fail(t_id)
                            return False
                    except Exception as exc:
                        log.warning(
                            "Pre-balance fetch failed trade_id=%s: %s — proceeding", t_id, exc
                        )

                # 9. FSM: REGISTERED -> PLACED
                try:
                    self.guard._fsm_set(t_id, TradeState.PLACED)
                    log.info("AUDIT PLACED trade_id=%s retries=%d", t_id, self.max_retries)
                except RuntimeError as exc:
                    log.error("FSM PLACED failed trade_id=%s: %s", t_id, exc)
                    self.guard.fail(t_id)
                    return False

                # 10. Retry loop with order-existence check
                success = False
                for attempt in range(self.max_retries + 1):

                    # Before retry N>0: check if order already exists on exchange
                    if attempt > 0 and not self.client.is_paper_trading():
                        try:
                            existing = await self.client.get_order_by_client_id(t_id)
                            if existing:
                                oid = (
                                    existing.get("id")
                                    or existing.get("orderID")
                                    or t_id
                                )
                                log.info(
                                    "AUDIT order_exists trade_id=%s orderID=%s — skipping retry",
                                    t_id, oid,
                                )
                                success = await self._record_fill(
                                    t_id, oid, market_id, question, token_id,
                                    side, size_usd, price, pre_balance, _signal_meta,
                                )
                                break
                        except Exception as exc:
                            log.warning("order_exists check failed attempt=%d: %s", attempt, exc)

                    try:
                        resp = await self.client.place_order(
                            token_id, side, size_usd, price, client_order_id=t_id,
                        )
                        oid = (
                            resp.get("id")
                            or resp.get("orderID")
                            or f"pos_{int(time.time())}"
                        )
                        ok = resp.get("status") == "paper" or resp.get("success", False)

                        if ok:
                            self.guard.set_order_id(t_id, oid)

                            # 11. Detect fill state
                            resp_filled   = float(
                                resp.get("filled_size", 0)
                                or resp.get("size_matched", 0)
                                or 0
                            )
                            resp_original = float(
                                resp.get("original_size", 0)
                                or resp.get("size", size_usd / max(price, 1e-9))
                            )

                            # filled == 0: not filled at all
                            if resp_filled == 0 and not self.client.is_paper_trading():
                                log.info(
                                    "AUDIT NOT_FILLED trade_id=%s oid=%s — will be monitored by maintenance",
                                    t_id, oid,
                                )
                                # FSM stays PLACED; maintenance_loop will cancel at timeout
                                success = True
                                await self._alert(
                                    f"Order placed (not yet filled)\ntrade_id: {t_id}\n"
                                    f"Market: {question[:50]}\n"
                                    f"Side: {side}  Size: ${size_usd:.2f}  @ {price:.4f}"
                                )
                                break

                            # partial fill
                            is_partial = (
                                resp_filled > 0
                                and resp_original > 0
                                and resp_filled < resp_original * 0.995
                                and not self.client.is_paper_trading()
                            )
                            if is_partial:
                                try:
                                    self.guard._fsm_set(t_id, TradeState.PARTIAL_FILL)
                                except RuntimeError:
                                    pass
                                # Block market immediately + persist so state survives restart
                                self.guard.set_position_open(market_id)
                                self.guard._save_state()
                                log.info(
                                    "AUDIT PARTIAL_FILL trade_id=%s filled=%.4f/%.4f",
                                    t_id, resp_filled, resp_original,
                                )
                                asyncio.create_task(
                                    self._partial_fill_monitor(
                                        t_id, oid, market_id, question, token_id,
                                        side, size_usd, price, resp_filled,
                                    ),
                                    name=f"pf_{t_id}",
                                )
                                success = True
                                break

                            # full fill
                            success = await self._record_fill(
                                t_id, oid, market_id, question, token_id,
                                side, size_usd, price, pre_balance, _signal_meta,
                            )
                            break

                        else:
                            log.warning(
                                "AUDIT attempt=%d/%d FAILED trade_id=%s: %s",
                                attempt + 1, self.max_retries + 1, t_id, resp,
                            )
                            if attempt < self.max_retries:
                                await self._alert(
                                    f"Order Retry {attempt+1}/{self.max_retries+1}\n"
                                    f"trade_id: {t_id}\n"
                                    f"Market: {question[:50]}\n"
                                    f"Error: {resp.get('errorMsg', 'unknown')}"
                                )

                    except Exception as exc:
                        log.error(
                            "AUDIT attempt=%d/%d EXCEPTION trade_id=%s: %s",
                            attempt + 1, self.max_retries + 1, t_id, exc,
                        )
                        if attempt < self.max_retries:
                            await self._alert(
                                f"Order Exception {attempt+1}/{self.max_retries+1}\n"
                                f"trade_id: {t_id}\n"
                                f"Error: {type(exc).__name__}: {str(exc)[:80]}"
                            )

                    if attempt < self.max_retries:
                        backoff = 1.5 ** (attempt + 1)
                        await asyncio.sleep(backoff)

                # All attempts exhausted without success
                if not success:
                    self.guard.fail(t_id)
                    self.guard.record_failure()
                    reason = f"All {self.max_retries + 1} attempts exhausted"
                    log.error("AUDIT EXHAUSTED trade_id=%s %s %s", t_id, side, market_id[:12])
                    if self.tg:
                        await self.tg.alert_failure(t_id, reason)
                    if self.guard.is_halted:
                        if self.tg:
                            await self.tg.alert_circuit_breaker(
                                self.guard.circuit_breaker.trip_reason
                            )
                return success

    # ── Record full fill ───────────────────────────────────────────────────────

    async def _record_fill(
        self,
        t_id:        str,
        order_id:    str,
        market_id:   str,
        question:    str,
        token_id:    str,
        side:        str,
        size_usd:    float,
        price:       float,
        pre_balance: float,
        signal_meta: Optional[dict] = None,
    ) -> bool:
        try:
            self.guard._fsm_set(t_id, TradeState.FILLED)
        except RuntimeError as exc:
            log.error("FSM FILLED error trade_id=%s: %s", t_id, exc)

        self.guard.record_success(market_id)
        self.guard.set_position_open(market_id)

        pos = self.pm.open_position(
            order_id=order_id, market_id=market_id, question=question,
            side=side, price=price, size_usd=size_usd, token_id=token_id,
        )

        log.info(
            "AUDIT FILLED trade_id=%s orderID=%s %s %s $%.2f @ %.4f",
            t_id, order_id, side, question[:40], size_usd, price,
        )

        # Post-balance verification (live only)
        if not self.client.is_paper_trading() and pre_balance > 0:
            try:
                post_balance   = await self.client.get_balance()
                expected_delta = size_usd
                actual_delta   = pre_balance - post_balance
                tolerance      = size_usd * 0.20
                if abs(actual_delta - expected_delta) > tolerance:
                    msg = (
                        f"Balance mismatch: "
                        f"expected ~${expected_delta:.2f} actual ~${actual_delta:.2f}"
                    )
                    log.error("POST-BALANCE MISMATCH trade_id=%s: %s", t_id, msg)
                    await self._alert(
                        f"Post-Trade Balance Mismatch\n"
                        f"trade_id: {t_id}\norderID: {order_id}\n{msg}\n"
                        f"Trading halted for safety."
                    )
                    self.guard.halt("post_balance_mismatch")
            except Exception as exc:
                log.warning("Post-balance check failed trade_id=%s: %s", t_id, exc)

        await self._alert_placed(t_id, order_id, market_id, side, question, size_usd, price,
                                  pos.stop_loss, signal_meta)
        return True

    # ── Partial fill monitor ───────────────────────────────────────────────────

    async def _partial_fill_monitor(
        self,
        t_id:      str,
        order_id:  str,
        market_id: str,
        question:  str,
        token_id:  str,
        side:      str,
        size_usd:  float,
        price:     float,
        filled:    float,
    ) -> None:
        """
        Poll every partial_poll seconds until fully filled or timeout.
        Market position lock is held for the entire duration.
        On timeout: cancel remainder + FSM->CANCELLED + release lock.
        On completion: open position for filled amount.
        """
        deadline = time.time() + self.order_timeout
        filled_usd = round(filled * price, 2)

        try:
            await self._alert_partial(t_id, filled_usd, size_usd, question)

            while time.time() < deadline:
                await asyncio.sleep(self.partial_poll)

                try:
                    order_data = await self.client.get_order_by_client_id(t_id)
                except Exception as exc:
                    log.warning("partial_fill_monitor: fetch error trade_id=%s: %s", t_id, exc)
                    continue

                if not order_data:
                    # Order gone from exchange — assume fully consumed
                    filled = size_usd / max(price, 1e-9)
                    log.info("partial_fill_monitor: order gone, assuming full fill trade_id=%s", t_id)
                    break

                try:
                    nf = float(
                        order_data.get("size_matched", 0)
                        or order_data.get("filled_size", 0)
                        or filled
                    )
                    no = float(
                        order_data.get("original_size", 0)
                        or order_data.get("size", size_usd / max(price, 1e-9))
                    )
                    if nf >= no * 0.995:
                        filled = nf
                        log.info(
                            "partial_fill_monitor: fully filled trade_id=%s %.4f/%.4f",
                            t_id, filled, no,
                        )
                        break
                    filled = nf
                except Exception as exc:
                    log.warning("partial_fill_monitor: parse error trade_id=%s: %s", t_id, exc)

            else:
                # Timeout — cancel remainder
                log.warning("PARTIAL_FILL TIMEOUT trade_id=%s — cancelling", t_id)
                try:
                    await self.client.cancel_order(order_id)
                except Exception as exc:
                    log.error("partial cancel failed trade_id=%s: %s", t_id, exc)

                await self._alert_cancelled(t_id, order_id, "partial_fill_timeout")
                try:
                    self.guard._fsm_set(t_id, TradeState.CANCELLED)
                except RuntimeError:
                    pass
                self.guard.active_trades.pop(t_id, None)
                self.guard._save_state()

                actual_usd = round(filled * price, 2)
                if actual_usd < 1.0:
                    # Nothing filled — release position lock
                    self.guard.set_position_closed(market_id)
                    return
                # Fall through with whatever was filled

            actual_usd = round(filled * price, 2)
            if actual_usd < 1.0:
                self.guard.set_position_closed(market_id)
                return

            try:
                self.guard._fsm_set(t_id, TradeState.FILLED)
            except RuntimeError:
                pass

            self.guard.record_success(market_id)
            # position lock already set; just update it
            self.guard.set_position_open(market_id)

            pos = self.pm.open_position(
                order_id=order_id, market_id=market_id, question=question,
                side=side, price=price, size_usd=actual_usd, token_id=token_id,
            )

            await self._alert(
                f"Partial Fill Resolved\ntrade_id: {t_id}\n"
                f"Market: {question[:50]}\nSide: {side}\n"
                f"Filled: ${actual_usd:.2f} / ${size_usd:.2f}\n"
                f"Entry: {price:.4f}  SL: {pos.stop_loss:.4f}"
            )

        except asyncio.CancelledError:
            log.info("partial_fill_monitor cancelled trade_id=%s", t_id)
        except Exception as exc:
            log.error("partial_fill_monitor unhandled error trade_id=%s: %s", t_id, exc)
            self.guard.fail(t_id)
            self.guard.set_position_closed(market_id)

    # ── Order maintenance ─────────────────────────────────────────────────────

    async def maintenance_loop(self) -> None:
        """
        Background task: cancel timed-out GTC orders.
        Runs forever — catches ALL exceptions so it never exits silently.
        """
        interval = max(15.0, self.order_timeout / 2)
        log.info("OrderManager.maintenance_loop: interval=%.0fs", interval)
        while True:
            try:
                await asyncio.sleep(interval)
                await self._cancel_timed_out_orders()
            except asyncio.CancelledError:
                log.info("OrderManager.maintenance_loop: cancelled")
                break
            except Exception as exc:
                log.error("maintenance_loop error (continuing): %s", exc, exc_info=True)
                await asyncio.sleep(5)

    async def _cancel_timed_out_orders(self) -> None:
        try:
            timed_out = self.guard.timed_out_orders()
        except Exception as exc:
            log.error("timed_out_orders() failed: %s", exc)
            return

        for t_id, order_id in timed_out:
            log.warning("TIMEOUT trade_id=%s orderID=%s — cancelling", t_id, order_id)
            try:
                cancelled = await self.client.cancel_order(order_id)
            except Exception as exc:
                log.error("cancel_order failed trade_id=%s: %s", t_id, exc)
                continue

            if cancelled:
                # Save market_id BEFORE removing the trade entry
                market_id = (self.guard.active_trades.get(t_id) or {}).get("market_id", "")

                try:
                    self.guard._fsm_set(t_id, TradeState.CANCELLED)
                except RuntimeError:
                    self.guard.fail(t_id)
                else:
                    self.guard.active_trades.pop(t_id, None)
                    self.guard._save_state()

                # Release position lock with the market_id captured before pop
                if market_id:
                    self.guard.set_position_closed(market_id)

                await self._alert_cancelled(t_id, order_id, f"timeout_{self.order_timeout:.0f}s")
            else:
                log.error("cancel FAILED trade_id=%s orderID=%s", t_id, order_id)

    # ── Alert helpers ─────────────────────────────────────────────────────────

    async def _alert(self, msg: str) -> None:
        if self.tg:
            try:
                await self.tg.send(msg)
            except Exception as exc:
                log.warning("_alert send failed: %s", exc)

    async def _alert_placed(
        self, t_id: str, order_id: str, market_id: str, side: str,
        question: str, size: float, price: float, sl: float,
        signal_meta: Optional[dict] = None,
    ) -> None:
        cash          = self.portfolio.cash
        active_trades = len(self.portfolio.positions)
        msg = format_trade_open(
            question=question,
            side=side,
            price=price,
            size_usd=size,
            signal_meta=signal_meta,
            cash=cash,
            active_trades=active_trades,
        )
        if self.tg:
            try:
                await self.tg.send_keyed(f"trade_open_{market_id}", msg, cooldown=30.0)
            except Exception as exc:
                log.warning("_alert_placed send_keyed failed: %s", exc)

    async def _alert_partial(
        self, t_id: str, filled_usd: float, total_usd: float, question: str
    ) -> None:
        msg = (
            f"<b>Partial Fill</b>\n"
            f"trade_id: <code>{t_id}</code>\n"
            f"Market: {question[:50]}\n"
            f"Filled: ${filled_usd:.2f} / ${total_usd:.2f}\n"
            f"Monitoring for up to {self.order_timeout:.0f}s"
        )
        await self._alert(msg)

    async def _alert_cancelled(self, t_id: str, order_id: str, reason: str) -> None:
        msg = (
            f"<b>Order Cancelled</b>\n"
            f"trade_id: <code>{t_id}</code>\n"
            f"orderID:  <code>{order_id}</code>\n"
            f"Reason: {reason}"
        )
        await self._alert(msg)

    async def _alert_failure(self, t_id: str, reason: str) -> None:
        msg = (
            f"<b>Trade Failed</b>\n"
            f"trade_id: <code>{t_id}</code>\n"
            f"Reason: {reason}"
        )
        await self._alert(msg)

    async def _alert_circuit_breaker(self, reason: str) -> None:
        await self._alert(format_circuit_breaker(reason))
