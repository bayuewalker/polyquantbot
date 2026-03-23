"""
integrations/intelligence_client.py  —  PolyBot v7.1
Prediction Market Intelligence API client.

Base URL: https://narrative.agent.heisenberg.so
Endpoint: POST /api/v2/semantic/retrieve/parameterized

Universal request structure:
{
  "agent_id": <number>,
  "params": { ... },             // all param values are strings unless noted
  "pagination": { "limit": 50, "offset": 0 },  // optional
  "formatter_config": { "format_type": "raw" }  // always "raw"
}

Agent IDs (per spec):
  574  — Polymarket Markets
  556  — Polymarket Trades
  568  — Polymarket Candlesticks
  572  — Polymarket Orderbook
  569  — Polymarket PnL
  579  — Polymarket Leaderboard
  584  — H-Score Leaderboard
  581  — Wallet 360
  575  — Market Insights
  565  — Kalshi Markets
  573  — Kalshi Trades
  585  — Social Pulse
"""
from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timezone, timedelta
from typing import Any, Optional

import aiohttp

log = logging.getLogger("polybot.intelligence_client")


class IntelligenceAPIError(Exception):
    """Raised when the Intelligence API returns an error or cannot be reached.

    Callers should catch this to distinguish transient failures (fail-open)
    from successful calls that returned zero results (genuine empty set).
    """


DEFAULT_BASE_URL = "https://narrative.agent.heisenberg.so"
_ENDPOINT = "/api/v2/semantic/retrieve/parameterized"
_TIMEOUT   = aiohttp.ClientTimeout(total=20)
_FORMATTER = {"format_type": "raw"}


class IntelligenceClient:

    def __init__(self, base_url: str = "", api_key: str = "") -> None:
        self._base = (base_url or os.getenv("INTELLIGENCE_API_BASE_URL", DEFAULT_BASE_URL)).rstrip("/")
        self._api_key = api_key or os.getenv("INTELLIGENCE_API_KEY", "")
        if not self._api_key:
            log.warning(
                "IntelligenceClient: INTELLIGENCE_API_KEY not set — "
                "intelligence API calls will be skipped"
            )
        self.session: Optional[aiohttp.ClientSession] = None

    async def start(self) -> None:
        connector    = aiohttp.TCPConnector(limit=20, ttl_dns_cache=300)
        self.session = aiohttp.ClientSession(connector=connector, timeout=_TIMEOUT)

    async def stop(self) -> None:
        if self.session and not self.session.closed:
            await self.session.close()

    @property
    def available(self) -> bool:
        return bool(self._api_key and self.session and not self.session.closed)

    # ── Internal ──────────────────────────────────────────────────────────────

    async def _call(
        self,
        agent_id: int,
        params: dict,
        limit: int = 50,
        offset: int = 0,
        paginate: bool = True,
    ) -> Optional[Any]:
        if not self.available:
            raise IntelligenceAPIError(
                f"agent={agent_id}: client not available (key missing or session closed)"
            )
        url = f"{self._base}{_ENDPOINT}"
        headers: dict[str, str] = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self._api_key}",
        }
        # Strip None values — spec says "Omit optional params entirely"
        clean_params = {k: v for k, v in params.items() if v is not None}
        payload: dict[str, Any] = {
            "agent_id": agent_id,
            "params": clean_params,
            "formatter_config": _FORMATTER,
        }
        if paginate:
            payload["pagination"] = {"limit": limit, "offset": offset}
        try:
            async with self.session.post(url, json=payload, headers=headers) as resp:
                if resp.status == 200:
                    return await resp.json()
                body = await resp.text()
                # Raise so callers can distinguish transient HTTP failures
                # from a successful response with zero matching records.
                raise IntelligenceAPIError(
                    f"agent={agent_id} HTTP {resp.status}: {body[:200]}"
                )
        except IntelligenceAPIError:
            raise
        except Exception as exc:
            raise IntelligenceAPIError(
                f"agent={agent_id} network error: {exc}"
            ) from exc

    async def _call_all_pages(
        self,
        agent_id: int,
        params: dict,
        page_size: int = 200,
        max_pages: int = 5,
    ) -> list[dict]:
        """
        Fetch all pages for a paginated endpoint, up to max_pages.
        Stops early when a page returns fewer items than page_size (last page).
        Max page_size per spec is 200.
        """
        all_items: list[dict] = []
        page_size = min(page_size, 200)
        for page in range(max_pages):
            result = await self._call(
                agent_id, params,
                limit=page_size, offset=page * page_size, paginate=True,
            )
            items = _extract_list(result)
            all_items.extend(items)
            if len(items) < page_size:
                break  # last page reached
        return all_items

    # ── Agent 574 — Polymarket Markets ────────────────────────────────────────

    async def get_markets(
        self,
        min_volume: Optional[str] = None,
        condition_id: Optional[str] = None,
        market_slug: Optional[str] = None,
        closed: Optional[str] = None,
        limit: int = 100,
    ) -> list[dict]:
        result = await self._call(574, {
            "min_volume": min_volume,
            "condition_id": condition_id,
            "market_slug": market_slug,
            "closed": closed,
        }, limit=limit)
        return _extract_list(result)

    # ── Agent 575 — Market Insights ───────────────────────────────────────────

    async def get_market_insights(
        self,
        volume_trend: str = "UP",
        min_liquidity_percentile: str = "60",
        min_volume_24h: Optional[str] = None,
        limit: int = 100,
        all_pages: bool = False,
    ) -> list[dict]:
        params = {
            "volume_trend": volume_trend,
            "min_liquidity_percentile": min_liquidity_percentile,
            "min_volume_24h": min_volume_24h,
        }
        if all_pages:
            rows = await self._call_all_pages(575, params, page_size=min(limit, 200))
            return rows[:limit] if limit else rows
        result = await self._call(575, params, limit=limit)
        return _extract_list(result)

    # ── Agent 568 — Candlesticks ───────────────────────────────────────────────

    async def get_candlesticks(
        self,
        token_id: str,
        interval: str = "1h",
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
    ) -> list[dict]:
        result = await self._call(568, {
            "token_id": token_id,
            "interval": interval,
            "start_time": start_time,
            "end_time": end_time,
        }, paginate=False)
        return _extract_list(result)

    # ── Agent 572 — Orderbook Snapshot ────────────────────────────────────────
    # Required param: token_id (NOT market_id per spec)

    async def get_orderbook_snapshot(
        self,
        token_id: str,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
    ) -> list[dict]:
        result = await self._call(572, {
            "token_id": token_id,
            "start_time": start_time,
            "end_time": end_time,
        }, paginate=False)
        return _extract_list(result)

    # ── Agent 556 — Polymarket Trades ─────────────────────────────────────────

    async def get_trades(
        self,
        wallet_proxy: Optional[str] = None,
        condition_id: Optional[str] = None,
        market_slug: Optional[str] = None,
        side: Optional[str] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        limit: int = 50,
        all_pages: bool = False,
    ) -> list[dict]:
        params = {
            "wallet_proxy": wallet_proxy,
            "condition_id": condition_id,
            "market_slug": market_slug,
            "side": side,
            "start_time": start_time,
            "end_time": end_time,
        }
        if all_pages:
            rows = await self._call_all_pages(556, params, page_size=min(limit, 200))
            return rows[:limit] if limit else rows
        result = await self._call(556, params, limit=limit)
        return _extract_list(result)

    # ── Agent 569 — PnL ───────────────────────────────────────────────────────
    # start_time / end_time are YYYY-MM-DD strings (NOT unix timestamps)

    async def get_pnl(
        self,
        wallet: str,
        granularity: str = "1d",
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        condition_id: Optional[str] = None,
    ) -> list[dict]:
        result = await self._call(569, {
            "wallet": wallet,
            "granularity": granularity,
            "start_time": start_time,
            "end_time": end_time,
            "condition_id": condition_id,
        }, paginate=False)
        return _extract_list(result)

    # ── Agent 579 — Polymarket Leaderboard ────────────────────────────────────

    async def get_leaderboard(
        self,
        leaderboard_period: str = "7d",
        wallet_address: str = "ALL",
        limit: int = 20,
        all_pages: bool = False,
    ) -> list[dict]:
        params = {
            "wallet_address": wallet_address,
            "leaderboard_period": leaderboard_period,
        }
        if all_pages:
            rows = await self._call_all_pages(579, params, page_size=min(limit, 200))
            return rows[:limit] if limit else rows
        result = await self._call(579, params, limit=limit)
        return _extract_list(result)

    # ── Agent 581 — Wallet 360 ────────────────────────────────────────────────
    # proxy_wallet is required; window_days must be "1", "3", "7", or "15"

    async def get_wallet_360(
        self,
        proxy_wallet: str,
        window_days: str = "7",
    ) -> list[dict]:
        result = await self._call(581, {
            "proxy_wallet": proxy_wallet,
            "window_days": window_days,
        }, limit=100)
        return _extract_list(result)

    # ── Agent 584 — H-Score Leaderboard ──────────────────────────────────────
    # All param values are strings

    async def get_hscore_leaderboard(
        self,
        min_win_rate_15d: str = "0.45",
        max_win_rate_15d: str = "0.92",
        min_roi_15d:      str = "0",
        min_total_trades_15d: str = "30",
        max_total_trades_15d: str = "5000",
        min_pnl_15d:      Optional[str] = None,
        sort_by:          str = "roi",
        limit:            int = 20,
    ) -> list[dict]:
        result = await self._call(584, {
            "min_win_rate_15d": min_win_rate_15d,
            "max_win_rate_15d": max_win_rate_15d,
            "min_roi_15d":      min_roi_15d,
            "min_total_trades_15d": min_total_trades_15d,
            "max_total_trades_15d": max_total_trades_15d,
            "min_pnl_15d":      min_pnl_15d,
            "sort_by":          sort_by,
        }, limit=limit)
        return _extract_list(result)

    # ── Agent 585 — Social Pulse ──────────────────────────────────────────────
    # keywords must be wrapped in curly braces: "{Trump,election,MAGA}"
    # hours_back is a string

    async def get_social_pulse(
        self,
        keywords: list[str],
        hours_back: str = "12",
    ) -> Optional[dict]:
        if not keywords:
            return None
        kw_str = "{" + ",".join(keywords) + "}"
        result = await self._call(585, {
            "keywords":   kw_str,
            "hours_back": hours_back,
        }, paginate=False)
        data = _extract_list(result)
        if data:
            return data[0]
        if isinstance(result, dict) and result:
            return result
        return None

    # ── Agent 565 — Kalshi Markets ────────────────────────────────────────────

    async def get_kalshi_markets(
        self,
        ticker: Optional[str] = None,
        title: Optional[str] = None,
        status: str = "open",
        limit: int = 50,
    ) -> list[dict]:
        result = await self._call(565, {
            "ticker": ticker,
            "title":  title,
            "status": status,
        }, limit=limit)
        return _extract_list(result)

    # ── Agent 573 — Kalshi Trades ─────────────────────────────────────────────

    async def get_kalshi_trades(
        self,
        ticker: Optional[str] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        limit: int = 50,
    ) -> list[dict]:
        result = await self._call(573, {
            "ticker":     ticker,
            "start_time": start_time,
            "end_time":   end_time,
        }, limit=limit)
        return _extract_list(result)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _extract_list(result: Any) -> list[dict]:
    if isinstance(result, list):
        return result
    if isinstance(result, dict):
        for key in ("data", "results", "items", "markets", "trades", "entries", "records"):
            v = result.get(key)
            if isinstance(v, list):
                return v
    return []


# ── Candlestick momentum ──────────────────────────────────────────────────────

def compute_momentum(candles: list[dict], ema_period: int = 6) -> Optional[float]:
    """
    Compute simple EMA-based momentum from candlestick data.
    Returns (last_close - EMA_n) / EMA_n or None if insufficient data.
    Positive = bullish momentum, negative = bearish.
    """
    closes: list[float] = []
    for c in candles:
        try:
            close = c.get("close") or c.get("c") or c.get("close_price") or 0
            closes.append(float(close))
        except Exception:
            continue
    if len(closes) < ema_period + 1:
        return None
    k   = 2.0 / (ema_period + 1)
    ema = closes[0]
    for price in closes[1:]:
        ema = price * k + ema * (1 - k)
    last = closes[-1]
    if ema == 0:
        return None
    return round((last - ema) / ema, 6)


# ── Social Pulse helpers ──────────────────────────────────────────────────────

def extract_keywords(question: str, max_words: int = 5) -> list[str]:
    """Extract meaningful words from a market question for Social Pulse queries."""
    import re
    stop = {
        "will", "the", "a", "an", "be", "is", "in", "on", "at", "to",
        "of", "and", "or", "by", "for", "with", "that", "this",
        "its", "than", "as", "have", "has", "had", "does", "did", "do",
        "win", "lose", "happen", "occur", "reach", "hit", "exceed",
        "before", "after", "more", "than", "over", "under", "any",
    }
    words = re.findall(r"[a-zA-Z]{3,}", question.lower())
    seen: list[str] = []
    for w in words:
        if w not in stop and w not in seen:
            seen.append(w)
        if len(seen) >= max_words:
            break
    return seen


# ── Date helpers for PnL endpoint ────────────────────────────────────────────

def days_ago_date(days: int) -> str:
    """Return YYYY-MM-DD date string for N days ago in UTC."""
    dt = datetime.now(timezone.utc) - timedelta(days=days)
    return dt.strftime("%Y-%m-%d")


def today_date() -> str:
    """Return today's date as YYYY-MM-DD in UTC."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def unix_seconds_ago(seconds: int) -> str:
    """Return unix timestamp (seconds) for N seconds ago, as string."""
    return str(int(time.time()) - seconds)
