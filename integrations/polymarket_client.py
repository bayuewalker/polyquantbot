"""
integrations/polymarket_client.py  —  PolyBot v7.1 FINAL
Full Polymarket CLOB client.

Methods:
  place_order()              GTC limit order with client_order_id
  cancel_order()             DELETE /order/<id>
  get_order_by_client_id()   idempotency key lookup
  get_order_status()         fill-status polling by orderID
  get_positions()            open positions for wallet
  get_balance()              USDC collateral balance
  get_markets()              active markets from Gamma API
  get_orderbook()            raw orderbook snapshot
"""
from __future__ import annotations

import json
import logging
import time
from typing import Optional

import aiohttp
from web3 import Web3

log = logging.getLogger("polybot.clob_client")

CLOB_BASE  = "https://clob.polymarket.com"
GAMMA_BASE = "https://gamma-api.polymarket.com"


class PolymarketClient:

    def __init__(
        self,
        cfg: dict,
        private_key:    str = "",
        maker_address:  str = "",
    ) -> None:
        self.cfg            = cfg
        self._private_key   = private_key.strip().lstrip("0x") if private_key else ""
        self._maker_address = maker_address.strip()
        self._acct          = None
        self.session: Optional[aiohttp.ClientSession] = None
        self._clob  = cfg.get("clob_url",  CLOB_BASE)
        self._gamma = cfg.get("gamma_url", GAMMA_BASE)

    # ── Lifecycle ──────────────────────────────────────────────────────────────

    async def start(self) -> None:
        connector    = aiohttp.TCPConnector(limit=50, ttl_dns_cache=300)
        timeout      = aiohttp.ClientTimeout(total=30)
        self.session = aiohttp.ClientSession(connector=connector, timeout=timeout)
        if self._private_key:
            try:
                self._acct = Web3().eth.account.from_key("0x" + self._private_key)
                log.info("Wallet loaded: %s", self._acct.address[:10] + "...")
            except Exception as exc:
                log.error("Wallet init failed: %s", exc)
                self._acct = None
        else:
            log.info("No private key — paper trading mode")

    async def stop(self) -> None:
        if self.session and not self.session.closed:
            await self.session.close()

    def is_paper_trading(self) -> bool:
        return self._acct is None

    @property
    def wallet_address(self) -> str:
        if self._maker_address:
            return self._maker_address
        if self._acct:
            return self._acct.address
        return ""

    # ── Order placement ────────────────────────────────────────────────────────

    async def place_order(
        self,
        token_id:        str,
        side:            str,
        size_usdc:       float,
        price:           float,
        client_order_id: str = "",
        order_type:      str = "GTC",
    ) -> dict:
        if self.is_paper_trading():
            oid = f"paper_{int(time.time() * 1000)}"
            log.info(
                "PAPER trade: %s %s $%.2f @ %.4f coid=%s",
                side, token_id[:12], size_usdc, price, client_order_id,
            )
            return {"status": "paper", "id": oid, "orderID": oid, "success": True}

        maker  = self._maker_address or self._acct.address
        signer = self._acct.address
        shares = round(size_usdc / price, 4) if price > 0 else 0.0

        order: dict = {
            "orderType":  order_type,
            "tokenID":    token_id,
            "price":      str(round(price, 4)),
            "size":       str(shares),
            "side":       side.lower(),
            "feeRateBps": "0",
            "nonce":      str(int(time.time() * 1000)),
            "signer":     signer,
            "maker":      maker,
        }
        if client_order_id:
            order["client_order_id"] = client_order_id

        try:
            from eth_account.messages import encode_defunct
            msg    = encode_defunct(Web3.keccak(text=json.dumps(order, sort_keys=True)))
            signed = self._acct.sign_message(msg)
            order["signature"] = signed.signature.hex()

            async with self.session.post(
                f"{self._clob}/order",
                json={"order": order, "owner": maker},
                headers=self._auth_headers(),
            ) as resp:
                data = await resp.json()
                if data.get("success"):
                    log.info("Order placed: orderID=%s coid=%s", data.get("orderID", ""), client_order_id)
                else:
                    log.error("Order failed (coid=%s): %s", client_order_id, data)
                return data
        except Exception as exc:
            log.error("place_order exception (coid=%s): %s", client_order_id, exc)
            return {"success": False, "errorMsg": str(exc)}

    # ── Cancel ─────────────────────────────────────────────────────────────────

    async def cancel_order(self, order_id: str) -> bool:
        if self.is_paper_trading():
            return True
        try:
            async with self.session.delete(
                f"{self._clob}/order/{order_id}",
                headers=self._auth_headers(),
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status in (200, 204):
                    log.info("cancel_order: %s cancelled (HTTP %d)", order_id, resp.status)
                    return True
                if resp.status == 404:
                    log.info("cancel_order: %s already gone (404)", order_id)
                    return True
                body = await resp.text()
                log.warning("cancel_order: %s HTTP %d: %s", order_id, resp.status, body[:120])
                return False
        except Exception as exc:
            log.error("cancel_order %s: %s", order_id, exc)
            return False

    # ── Order query ────────────────────────────────────────────────────────────

    async def get_order_by_client_id(self, client_order_id: str) -> Optional[dict]:
        if self.is_paper_trading() or not self.session:
            return None
        try:
            async with self.session.get(
                f"{self._clob}/data/orders",
                params={"client_order_id": client_order_id},
                headers=self._auth_headers(),
                timeout=aiohttp.ClientTimeout(total=8),
            ) as resp:
                if resp.status != 200:
                    return None
                data  = await resp.json()
                items = data if isinstance(data, list) else data.get("data", [])
                if items:
                    log.info("get_order_by_client_id: found order coid=%s", client_order_id)
                    return items[0]
                return None
        except Exception as exc:
            log.warning("get_order_by_client_id coid=%s: %s", client_order_id, exc)
            return None

    async def get_order_status(self, order_id: str) -> Optional[dict]:
        if self.is_paper_trading() or not self.session:
            return None
        try:
            async with self.session.get(
                f"{self._clob}/data/orders/{order_id}",
                headers=self._auth_headers(),
                timeout=aiohttp.ClientTimeout(total=8),
            ) as resp:
                return await resp.json() if resp.status == 200 else None
        except Exception as exc:
            log.warning("get_order_status %s: %s", order_id, exc)
            return None

    # ── Positions ──────────────────────────────────────────────────────────────

    async def get_positions(self) -> list[dict]:
        if self.is_paper_trading() or not self.session:
            return []
        addr = self.wallet_address
        try:
            async with self.session.get(
                f"{self._clob}/positions",
                params={"user": addr},
                headers=self._auth_headers(),
                timeout=aiohttp.ClientTimeout(total=12),
            ) as resp:
                if resp.status != 200:
                    log.warning("get_positions HTTP %d", resp.status)
                    return []
                data  = await resp.json()
                items = data if isinstance(data, list) else data.get("data", [])
                log.info("get_positions: %d position(s) for %s", len(items), addr[:10])
                return items
        except Exception as exc:
            log.error("get_positions: %s", exc)
            return []

    # ── Balance ────────────────────────────────────────────────────────────────

    async def get_balance(self) -> float:
        if self.is_paper_trading() or not self.session:
            return 0.0
        addr = self.wallet_address
        try:
            for params in [
                {"asset_type": "COLLATERAL", "account": addr},
                {"asset_type": "COLLATERAL"},
            ]:
                async with self.session.get(
                    f"{self._clob}/balance-allowance",
                    params=params,
                    headers=self._auth_headers(),
                    timeout=aiohttp.ClientTimeout(total=8),
                ) as resp:
                    if resp.status == 200:
                        d = await resp.json()
                        for key in ("balance", "allowance", "amount"):
                            raw = float(d.get(key) or 0)
                            if raw > 0:
                                return round(raw / 1_000_000 if raw > 1000 else raw, 2)
        except Exception as exc:
            log.warning("get_balance: %s", exc)
        return 0.0

    # ── Markets ────────────────────────────────────────────────────────────────

    async def get_markets(self, limit: int = 300) -> list[dict]:
        if not self.session:
            return []
        try:
            async with self.session.get(
                f"{self._gamma}/markets",
                params={"active": "true", "closed": "false", "limit": limit},
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                return await resp.json() if resp.status == 200 else []
        except Exception as exc:
            log.error("get_markets: %s", exc)
            return []

    # ── Orderbook ─────────────────────────────────────────────────────────────

    async def get_orderbook(self, token_id: str) -> Optional[dict]:
        if not self.session:
            return None
        try:
            async with self.session.get(
                f"{self._clob}/book",
                params={"token_id": token_id},
                timeout=aiohttp.ClientTimeout(total=8),
            ) as resp:
                return await resp.json() if resp.status == 200 else None
        except Exception as exc:
            log.warning("get_orderbook %s: %s", token_id[:12], exc)
            return None

    # ── Auth ───────────────────────────────────────────────────────────────────

    def _auth_headers(self) -> dict:
        return {
            "Content-Type": "application/json",
            "Accept":       "application/json",
        }
