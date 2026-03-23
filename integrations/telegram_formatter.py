"""
integrations/telegram_formatter.py  —  PolyBot v7.1

Centralized message formatter for all Telegram notifications.
All message assembly lives here; no formatting logic inside bot or order_manager methods.
Messages are HTML-formatted and capped at 4000 chars.
"""
from __future__ import annotations

import html
import time
from typing import Optional

_MAX_LEN = 4000


def _cap(text: str) -> str:
    if len(text) > _MAX_LEN:
        return text[:_MAX_LEN - 3] + "..."
    return text


def format_startup_report(
    mode: str,
    balance: float,
    cash: float,
    position_count: int,
    max_positions: int,
    market_count: int,
    scan_interval: int,
    intelligence_status: str,
    autostart: bool,
) -> str:
    trading_state = "ACTIVE" if autostart else "STANDBY \u2014 send /run to start"
    msg = (
        "\U0001f916 <b>POLYBOT v7.1 ONLINE</b>\n"
        "\u2500" * 22 + "\n\n"
        "\U0001f4ca <b>Portfolio</b>\n"
        f"  Balance:    <b>${balance:.2f}</b>\n"
        f"  Cash:       ${cash:.2f}\n"
        f"  Positions:  {position_count} / {max_positions}\n\n"
        "\u2699\ufe0f <b>System</b>\n"
        f"  Mode:         <b>{mode}</b>\n"
        f"  Markets:      {market_count}\n"
        f"  Scan interval: {scan_interval}s\n"
        f"  Intelligence:  {intelligence_status}\n"
        f"  Engine:        ACTIVE\n"
        f"  Risk:          ENABLED\n\n"
        f"\u26a1 Trading: <b>{trading_state}</b>"
    )
    return _cap(msg)


def format_trade_open(
    question: str,
    side: str,
    price: float,
    size_usd: float,
    signal_meta: Optional[dict] = None,
    cash: float = 0.0,
    active_trades: int = 0,
) -> str:
    q = html.escape(question[:70])
    meta = signal_meta or {}
    ev          = meta.get("ev")
    z_score     = meta.get("z_score")
    confidence  = meta.get("confidence")
    social_boost = meta.get("social_boost", 0.0) or 0.0
    liq_boost    = meta.get("liq_boost", 0.0) or 0.0

    signal_block = ""
    if ev is not None or z_score is not None or confidence is not None:
        conf_pct = f"{confidence * 100:.1f}%" if confidence is not None else "N/A"
        ev_str   = f"{ev:.3f}" if ev is not None else "N/A"
        z_str    = f"{z_score:.2f}" if z_score is not None else "N/A"
        signal_block = (
            "\n\U0001f4a1 <b>Signal</b>\n"
            f"  EV:         {ev_str}\n"
            f"  Z-Score:    {z_str}\n"
            f"  Confidence: {conf_pct}"
        )
        if social_boost != 0.0 or liq_boost != 0.0:
            label = "\U0001f525 STRONG" if (ev is not None and ev > 0.08) else "\U0001f7e2 GOOD"
            signal_block += (
                f"\n  Label:      {label}\n"
                "\n\U0001f680 <b>Boosts</b>\n"
                f"  Social:    {social_boost:+.3f}\n"
                f"  Liquidity: {liq_boost:+.3f}"
            )

    msg = (
        "\U0001f4c8 <b>TRADE OPENED</b>\n"
        "\u2500" * 22 + "\n\n"
        f"\U0001f3af Market: {q}\n"
        f"  Side:   <b>{side}</b>\n"
        f"  Price:  {price:.4f}\n"
        f"  Size:   <b>${size_usd:.2f}</b>"
        f"{signal_block}\n\n"
        "\U0001f4bc <b>Portfolio</b>\n"
        f"  Cash:       ${cash:.2f}\n"
        f"  Positions:  {active_trades}"
    )
    return _cap(msg)


def format_trade_close(
    question: str,
    side: str,
    exit_price: float,
    pnl: float,
    pnl_pct: float,
    reason: str,
    hold_minutes: int,
    balance: float,
    cash: float = 0.0,
    active_trades: int = 0,
) -> str:
    q = html.escape(question[:70])
    pnl_sign = "+" if pnl >= 0 else ""
    icon = "\U0001f7e2" if pnl >= 0 else "\U0001f534"
    msg = (
        f"{icon} <b>POSITION CLOSED</b>\n"
        "\u2500" * 22 + "\n\n"
        f"\U0001f3af Market: {q}\n"
        f"  Side:       {side}\n"
        f"  Exit Price: {exit_price:.4f}\n"
        f"  PnL:        <b>{pnl_sign}${pnl:.2f} ({pnl_sign}{pnl_pct:.1f}%)</b>\n"
        f"  Reason:     {reason}\n"
        f"  Duration:   {hold_minutes} min\n\n"
        "\U0001f4bc <b>Portfolio</b>\n"
        f"  Balance:    ${balance:.2f}\n"
        f"  Cash:       ${cash:.2f}\n"
        f"  Positions:  {active_trades}"
    )
    return _cap(msg)


def format_heartbeat(
    running: bool,
    cycle: int,
    markets_scanned: int,
    candidates_evaluated: int,
    signals_passed: int,
    trades_executed: int,
    balance: float,
    cash: float,
    active_trades: int,
    drawdown_pct: float,
    daily_pnl: float,
) -> str:
    status = "\u25b6\ufe0f RUNNING" if running else "\u23f9\ufe0f PAUSED"
    signal_quality = "HIGH" if signals_passed > 0 else ("MED" if candidates_evaluated > 5 else "LOW")
    msg = (
        "\U0001f493 <b>HEARTBEAT</b>\n"
        "\u2500" * 22 + "\n\n"
        f"  Status:   {status}\n"
        f"  Cycle:    #{cycle}\n\n"
        "\U0001f50d <b>Scan Stats</b>\n"
        f"  Markets scanned:   {markets_scanned}\n"
        f"  Candidates eval'd: {candidates_evaluated}\n"
        f"  Signals passed:    {signals_passed}\n"
        f"  Trades executed:   {trades_executed}\n"
        f"  Signal quality:    {signal_quality}\n\n"
        "\U0001f4bc <b>Portfolio</b>\n"
        f"  Balance:    ${balance:.2f}\n"
        f"  Cash:       ${cash:.2f}\n"
        f"  Positions:  {active_trades}\n"
        f"  Drawdown:   {drawdown_pct:.1f}%\n"
        f"  Daily PnL:  {daily_pnl:+.2f}"
    )
    return _cap(msg)


def format_status(
    running: bool,
    is_halted: bool,
    halt_reason: str,
    balance: float,
    cash: float,
    unrealized_pnl: float,
    daily_pnl: float,
    drawdown_pct: float,
    positions: list[dict],
    cycle_limit: int,
    scan_interval: int,
) -> str:
    status = "\u25b6\ufe0f RUNNING" if running else "\u23f9\ufe0f PAUSED"
    halt_txt = ("\U0001f6a8 YES \u2014 " + halt_reason) if is_halted else "\u2705 NO"

    pos_lines = []
    for i, p in enumerate(positions[:10], 1):
        pnl_sign = "+" if p.get("pnl", 0) >= 0 else ""
        pos_lines.append(
            f"  {i}. <b>{p.get('side','?')}</b> {html.escape(p.get('question','?')[:30])}\n"
            f"     Entry: {p.get('entry_price', 0):.4f}  "
            f"Cur: {p.get('current_price', 0):.4f}  "
            f"PnL: {pnl_sign}${p.get('pnl', 0):.2f}"
        )

    pos_block = "\n".join(pos_lines) if pos_lines else "  (none)"
    msg = (
        "\U0001f4ca <b>PolyBot Status</b>\n"
        "\u2500" * 22 + "\n\n"
        f"  Status:  {status}\n"
        f"  Halted:  {halt_txt}\n\n"
        "\U0001f4b0 <b>Portfolio</b>\n"
        f"  Balance:        ${balance:.2f}\n"
        f"  Cash:           ${cash:.2f}\n"
        f"  Unrealized PnL: {unrealized_pnl:+.2f}\n"
        f"  Daily PnL:      {daily_pnl:+.2f}\n"
        f"  Drawdown:       {drawdown_pct:.1f}%\n\n"
        "\U0001f4cb <b>Open Positions</b>\n"
        + pos_block + "\n\n"
        "\u2699\ufe0f <b>System</b>\n"
        f"  Scan interval: {scan_interval}s\n"
        f"  Max trades/cycle: {cycle_limit}"
    )
    return _cap(msg)


def format_alpha(signals: list[dict]) -> str:
    if not signals:
        return "\U0001f52d <b>No alpha signals</b> — waiting for next scan cycle."

    lines = [
        "\U0001f525 <b>TOP ALPHA SIGNALS</b>\n"
        "\u2500" * 22 + "\n"
    ]
    for i, s in enumerate(signals[:5], 1):
        ev   = s.get("ev", 0.0)
        z    = s.get("z", 0.0)
        prob = s.get("prob", 0.0)
        label = "\U0001f525 STRONG" if ev > 0.08 else ("\U0001f7e2 GOOD" if ev > 0.04 else "\U0001f7e1 WEAK")
        boost_txt = ""
        social = s.get("social", 0.0) or 0.0
        liq    = s.get("liq", 0.0) or 0.0
        if social != 0.0 or liq != 0.0:
            boost_txt = f"\n  Boosts: Social {social:+.3f}  Liq {liq:+.3f}"
        lines.append(
            f"\n{i}. {label} {html.escape(s.get('title','?')[:55])}\n"
            f"  EV: {ev:.3f}  Z: {z:.2f}  Prob: {prob:.2f}\n"
            f"  Size: ${s.get('size', 0):.2f} @ {s.get('price', 0):.4f}"
            f"{boost_txt}"
        )
    return _cap("\n".join(lines))


def format_error(
    exc_type: str,
    detail: str,
    module: str = "main_loop",
    action: str = "AUTO-RECOVERY",
) -> str:
    msg = (
        "\U0001f6a8 <b>CRITICAL ERROR</b>\n"
        "\u2500" * 22 + "\n\n"
        f"  Type:   <code>{html.escape(exc_type)}</code>\n"
        f"  Module: {module}\n"
        f"  Detail: {html.escape(detail[:200])}\n"
        f"  Action: {action}"
    )
    return _cap(msg)


def format_circuit_breaker(
    reason: str,
    cooldown_hint: str = "Send /reset to clear",
) -> str:
    msg = (
        "\U0001f6d1 <b>CIRCUIT BREAKER TRIPPED</b>\n"
        "\u2500" * 22 + "\n\n"
        f"  Type:     TRADING HALT\n"
        f"  Reason:   {html.escape(reason)}\n"
        f"  Action:   ALL TRADING HALTED\n"
        f"  Cooldown: {cooldown_hint}"
    )
    return _cap(msg)
