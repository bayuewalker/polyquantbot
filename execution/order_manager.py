"""
execution/order_manager.py  —  PolyBot v7.1  v2.0.0
=====================================================
LATEST UPDATE — v2.0.0:
NEW: alert_order_gtc() used for NOT_FILLED path (previously just send())
NEW: alert_partial_update() called every 20s during partial poll loop
NEW: alert_partial_resolved() called on successful partial resolution
NEW: attempt count passed to alert_failure()
IMPROVED: partial fill NOT_FILLED path uses tg.alert_order_gtc() directly
IMPROVED: partial monitor tracks elapsed time for progress updates
IMPROVED: _record_fill passes ev/z_score/strategy to alert_order_placed()
KEPT: triple safety check, FSM, position lock, atomic registration,
      _save_state() after PARTIAL_FILL, market_id captured before pop()
"""
from __future__ import annotations
import asyncio, logging, time
from typing import TYPE_CHECKING, Optional

from core.execution_guard import ExecutionGuard, TradeState
from core.position_manager import PositionManager, Portfolio

if TYPE_CHECKING:
    from integrations.polymarket_client import PolymarketClient
    from integrations.telegram_bot import TelegramBot

log = logging.getLogger("polybot.order_manager")


class OrderManager:

    def __init__(
        self,
        client: "PolymarketClient",
        guard: ExecutionGuard,
        position_manager: PositionManager,
        portfolio: Portfolio,
        telegram: Optional["TelegramBot"],
        cfg: dict,
    ) -> None:
        self.client   = client
        self.guard    = guard
        self.pm       = position_manager
        self.portfolio = portfolio
        self.tg       = telegram
        self.cfg      = cfg
        self.max_retries   = int(cfg.get("max_retries",             2))
        self.order_timeout = float(cfg.get("order_timeout_sec",    60.0))
        self.partial_poll  = float(cfg.get("partial_fill_poll_sec",  2.0))
        # Send partial progress updates every N seconds during monitor loop
        self._partial_update_interval = float(cfg.get("partial_update_interval_sec", 20.0))

    # ── Main execute ──────────────────────────────────────────────────────────

    async def execute(
        self,
        market_id: str,
        question: str,
        token_id: str,
        side: str,
        size_usd: float,
        price: float,
        strategy: str,
        ev: float = 0.0,
        z_score: float = 0.0,
    ) -> bool:
        # ── SAFETY TRIPLE-CHECK ───────────────────────────────────────────────
        if self.guard.is_halted:
            log.warning("BLOCKED: is_halted | %s %s", side, market_id[:12])
            return False

        if self.guard.has_open_position(market_id):
            log.info("BLOCKED: has_open_position | %s", market_id[:12])
            return False

        t_id = self.guard.trade_id(market_id, side)
        if self.guard.is_duplicate(t_id):
            log.info("BLOCKED: is_duplicate | %s", t_id)
            return False

        if not self.guard.is_cooled_down(market_id):
            log.info("BLOCKED: cooldown | %s", market_id[:12])
            return False

        async with self.guard._exec_sem:

            # Atomic registration
            async with self.guard._reg_lock:
                if self.guard.is_duplicate(t_id):
                    return False
                if self.guard.has_open_position(market_id):
                    return False
                try:
                    self.guard._fsm_set(t_id, TradeState.REGISTERED)
                    self.guard.set_trade_meta(t_id, market_id, side, size_usd)
                    log.info("AUDIT REGISTERED %s %s %s $%.2f @ %.4f strat=%s",
                             t_id, side, market_id[:12], size_usd, price, strategy)
                except RuntimeError as e:
                    log.error("FSM REGISTERED %s: %s", t_id, e)
                    return False

            market_lock = self.guard.get_market_lock(market_id, side)
            async with market_lock:

                # Portfolio guards
                can, reason = self.pm.can_trade()
                if not can:
                    log.warning("PRE-EXEC %s: %s", t_id, reason)
                    self.guard.fail(t_id); return False

                if self.portfolio.cash < size_usd:
                    log.warning("PRE-EXEC cash %s: need $%.2f have $%.2f",
                                t_id, size_usd, self.portfolio.cash)
                    self.guard.fail(t_id); return False

                # Pre-balance snapshot
                pre_balance = 0.0
                if not self.client.is_paper_trading():
                    try:
                        pre_balance = await self.client.get_balance()
                        if pre_balance < size_usd:
                            msg = f"Wallet ${pre_balance:.2f} < order ${size_usd:.2f}"
                            log.error("WALLET CHECK %s: %s", t_id, msg)
                            if self.tg:
                                await self.tg.alert_failure(t_id, msg)
                            self.guard.halt("wallet_balance_insufficient")
                            self.guard.fail(t_id); return False
                    except Exception as e:
                        log.warning("pre-balance %s: %s", t_id, e)

                try:
                    self.guard._fsm_set(t_id, TradeState.PLACED)
                except RuntimeError as e:
                    log.error("FSM PLACED %s: %s", t_id, e)
                    self.guard.fail(t_id); return False

                # Retry loop
                success = False
                for attempt in range(self.max_retries + 1):

                    # Order-existence check before retry
                    if attempt > 0 and not self.client.is_paper_trading():
                        try:
                            existing = await self.client.get_order_by_client_id(t_id)
                            if existing:
                                oid = existing.get("id") or existing.get("orderID") or t_id
                                log.info("AUDIT order_exists %s %s", t_id, oid)
                                success = await self._record_fill(
                                    t_id, oid, market_id, question, token_id,
                                    side, size_usd, price, pre_balance, ev, z_score, strategy,
                                )
                                break
                        except Exception as e:
                            log.warning("order_exists check attempt=%d: %s", attempt, e)

                    try:
                        resp = await self.client.place_order(
                            token_id, side, size_usd, price, client_order_id=t_id,
                        )
                        oid = resp.get("id") or resp.get("orderID") or f"pos_{int(time.time())}"
                        ok  = resp.get("status") == "paper" or resp.get("success", False)

                        if ok:
                            self.guard.set_order_id(t_id, oid)
                            resp_filled   = float(resp.get("filled_size",0) or resp.get("size_matched",0) or 0)
                            resp_original = float(resp.get("original_size",0) or resp.get("size", size_usd / max(price,1e-9)))

                            # NOT FILLED — GTC waiting
                            if resp_filled == 0 and not self.client.is_paper_trading():
                                log.info("AUDIT NOT_FILLED %s — GTC maintenance monitor", t_id)
                                success = True
                                if self.tg:
                                    await self.tg.alert_order_gtc(
                                        t_id, oid, side, question, size_usd, price,
                                        timeout_s=self.order_timeout,
                                    )
                                break

                            # PARTIAL FILL
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
                                self.guard.set_position_open(market_id)
                                self.guard._save_state()
                                log.info("AUDIT PARTIAL_FILL %s %.4f/%.4f", t_id, resp_filled, resp_original)
                                asyncio.create_task(
                                    self._partial_fill_monitor(
                                        t_id, oid, market_id, question, token_id,
                                        side, size_usd, price, resp_filled, ev, z_score, strategy,
                                    ),
                                    name=f"pf_{t_id}",
                                )
                                success = True
                                break

                            # FULL FILL
                            success = await self._record_fill(
                                t_id, oid, market_id, question, token_id,
                                side, size_usd, price, pre_balance, ev, z_score, strategy,
                            )
                            break

                        else:
                            log.warning("AUDIT attempt=%d/%d FAILED %s: %s",
                                        attempt+1, self.max_retries+1, t_id, resp)
                            if attempt < self.max_retries:
                                await self._alert(
                                    f"Order Retry {attempt+1}/{self.max_retries+1}\n"
                                    f"trade_id: {t_id}\n"
                                    f"Market: {question[:50]}\n"
                                    f"Error: {resp.get('errorMsg','unknown')}"
                                )

                    except Exception as e:
                        log.error("AUDIT attempt=%d/%d EXCEPTION %s: %s",
                                  attempt+1, self.max_retries+1, t_id, e)
                        if attempt < self.max_retries:
                            await self._alert(
                                f"Order Exception {attempt+1}/{self.max_retries+1}\n"
                                f"trade_id: {t_id}\n"
                                f"Error: {type(e).__name__}: {str(e)[:80]}"
                            )

                    if attempt < self.max_retries:
                        await asyncio.sleep(1.5 ** (attempt + 1))

                if not success:
                    self.guard.fail(t_id)
                    self.guard.record_failure()
                    reason = f"All {self.max_retries+1} attempts exhausted"
                    log.error("AUDIT EXHAUSTED %s %s %s", t_id, side, market_id[:12])
                    if self.tg:
                        await self.tg.alert_failure(t_id, reason, attempts=self.max_retries+1)
                    if self.guard.is_halted and self.tg:
                        await self.tg.alert_circuit_breaker(self.guard.circuit_breaker.trip_reason)
                return success

    # ── Record full fill ──────────────────────────────────────────────────────

    async def _record_fill(
        self,
        t_id: str, order_id: str, market_id: str, question: str, token_id: str,
        side: str, size_usd: float, price: float, pre_balance: float,
        ev: float = 0.0, z_score: float = 0.0, strategy: str = "",
    ) -> bool:
        try:
            self.guard._fsm_set(t_id, TradeState.FILLED)
        except RuntimeError as e:
            log.error("FSM FILLED %s: %s", t_id, e)

        self.guard.record_success(market_id)
        self.guard.set_position_open(market_id)

        pos = self.pm.open_position(
            order_id=order_id, market_id=market_id, question=question,
            side=side, price=price, size_usd=size_usd, token_id=token_id,
        )
        log.info("AUDIT FILLED %s %s %s %s $%.2f @ %.4f", t_id, order_id, side, question[:40], size_usd, price)

        # Post-balance check
        if not self.client.is_paper_trading() and pre_balance > 0:
            try:
                post = await self.client.get_balance()
                delta_actual   = pre_balance - post
                delta_expected = size_usd
                if abs(delta_actual - delta_expected) > size_usd * 0.20:
                    msg = f"Balance mismatch: expected ~${delta_expected:.2f} actual ~${delta_actual:.2f}"
                    log.error("POST-BALANCE MISMATCH %s: %s", t_id, msg)
                    await self._alert(f"Post-Trade Balance Mismatch\n{msg}\nTrading halted.")
                    self.guard.halt("post_balance_mismatch")
            except Exception as e:
                log.warning("post-balance %s: %s", t_id, e)

        if self.tg:
            await self.tg.alert_order_placed(
                t_id, order_id, side, question, size_usd, price,
                pos.stop_loss, ev=ev, z_score=z_score, strategy=strategy,
            )
        return True

    # ── Partial fill monitor ──────────────────────────────────────────────────

    async def _partial_fill_monitor(
        self,
        t_id: str, order_id: str, market_id: str, question: str, token_id: str,
        side: str, size_usd: float, price: float, filled: float,
        ev: float = 0.0, z_score: float = 0.0, strategy: str = "",
    ) -> None:
        deadline  = time.time() + self.order_timeout
        start_t   = time.time()
        filled_usd = round(filled * price, 2)
        last_update_t = start_t

        try:
            if self.tg:
                await self.tg.alert_partial_fill(
                    t_id, filled_usd, size_usd, question, timeout_s=self.order_timeout
                )

            while time.time() < deadline:
                await asyncio.sleep(self.partial_poll)
                elapsed = time.time() - start_t

                try:
                    order_data = await self.client.get_order_by_client_id(t_id)
                except Exception as e:
                    log.warning("partial_monitor fetch %s: %s", t_id, e)
                    continue

                if not order_data:
                    filled = size_usd / max(price, 1e-9)
                    log.info("partial_monitor: order gone full fill %s", t_id)
                    break

                try:
                    nf = float(order_data.get("size_matched",0) or order_data.get("filled_size",0) or filled)
                    no = float(order_data.get("original_size",0) or order_data.get("size", size_usd/max(price,1e-9)))
                    if nf >= no * 0.995:
                        filled = nf
                        log.info("partial_monitor: fully filled %s %.4f/%.4f", t_id, filled, no)
                        break
                    filled = nf
                except Exception as e:
                    log.warning("partial_monitor parse %s: %s", t_id, e)

                # Progress update every _partial_update_interval seconds
                if self.tg and (time.time() - last_update_t) >= self._partial_update_interval:
                    await self.tg.alert_partial_update(
                        t_id, round(filled * price, 2), size_usd, elapsed
                    )
                    last_update_t = time.time()

            else:
                # TIMEOUT
                log.warning("PARTIAL TIMEOUT %s — cancelling", t_id)
                try:
                    await self.client.cancel_order(order_id)
                except Exception as e:
                    log.error("partial cancel %s: %s", t_id, e)

                if self.tg:
                    await self.tg.alert_cancelled(
                        t_id, order_id, "partial_fill_timeout",
                        held_sec=time.time() - start_t,
                    )
                try:
                    self.guard._fsm_set(t_id, TradeState.CANCELLED)
                except RuntimeError:
                    pass
                self.guard.active_trades.pop(t_id, None)
                self.guard._save_state()

                actual_usd = round(filled * price, 2)
                if actual_usd < 1.0:
                    self.guard.set_position_closed(market_id)
                    return

            actual_usd = round(filled * price, 2)
            if actual_usd < 1.0:
                self.guard.set_position_closed(market_id)
                return

            try:
                self.guard._fsm_set(t_id, TradeState.FILLED)
            except RuntimeError:
                pass

            self.guard.record_success(market_id)
            self.guard.set_position_open(market_id)

            pos = self.pm.open_position(
                order_id=order_id, market_id=market_id, question=question,
                side=side, price=price, size_usd=actual_usd, token_id=token_id,
            )

            if self.tg:
                await self.tg.alert_partial_resolved(
                    t_id, actual_usd, size_usd, question, price, pos.stop_loss
                )

        except asyncio.CancelledError:
            log.info("partial_monitor cancelled %s", t_id)
        except Exception as e:
            log.error("partial_monitor error %s: %s", t_id, e)
            self.guard.fail(t_id)
            self.guard.set_position_closed(market_id)

    # ── Maintenance loop ──────────────────────────────────────────────────────

    async def maintenance_loop(self) -> None:
        interval = max(15.0, self.order_timeout / 2)
        log.info("maintenance_loop: interval=%.0fs", interval)
        while True:
            try:
                await asyncio.sleep(interval)
                await self._cancel_timed_out_orders()
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error("maintenance_loop error: %s", e, exc_info=True)
                await asyncio.sleep(5)

    async def _cancel_timed_out_orders(self) -> None:
        try:
            timed_out = self.guard.timed_out_orders()
        except Exception as e:
            log.error("timed_out_orders(): %s", e); return

        for t_id, order_id in timed_out:
            log.warning("TIMEOUT %s %s — cancelling", t_id, order_id)
            try:
                cancelled = await self.client.cancel_order(order_id)
            except Exception as e:
                log.error("cancel_order %s: %s", t_id, e); continue

            if cancelled:
                # Capture market_id BEFORE pop
                market_id = (self.guard.active_trades.get(t_id) or {}).get("market_id", "")
                placed_at = (self.guard.active_trades.get(t_id) or {}).get("placed_at", 0)
                held_sec  = time.time() - placed_at if placed_at else 0.0

                try:
                    self.guard._fsm_set(t_id, TradeState.CANCELLED)
                except RuntimeError:
                    self.guard.fail(t_id)
                else:
                    self.guard.active_trades.pop(t_id, None)
                    self.guard._save_state()

                if market_id:
                    self.guard.set_position_closed(market_id)

                if self.tg:
                    await self.tg.alert_cancelled(
                        t_id, order_id,
                        f"timeout_{self.order_timeout:.0f}s",
                        held_sec=held_sec,
                    )
            else:
                log.error("cancel FAILED %s %s", t_id, order_id)

    # ── Internal alert fallback ───────────────────────────────────────────────

    async def _alert(self, msg: str) -> None:
        if self.tg:
            try:
                await self.tg.send(msg)
            except Exception as e:
                log.warning("_alert failed: %s", e)
