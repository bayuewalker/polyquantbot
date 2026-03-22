# PolyBot v7.1 — Polymarket Trading Bot

## Overview
An automated trading bot for the [Polymarket](https://polymarket.com) prediction market platform. It scans markets, evaluates trading signals using a Bayesian model, and executes trades based on expected value (EV) and risk management parameters.

Supports **Paper Trading** (default, safe simulation) and **Live Trading** (requires private key and API credentials).

## Tech Stack
- **Language:** Python 3.12
- **Async Framework:** `asyncio` + `aiohttp`
- **Blockchain:** `web3.py`, `eth-account` (Polygon network)
- **Notifications:** `python-telegram-bot`
- **Config:** `pyyaml` + `python-dotenv`
- **Math/Data:** `numpy`

## Project Structure
```
polybot_v7_1.py          # Main entry point & trading loop
config.yaml              # All trading parameters
requirements.txt         # Python dependencies
core/
  circuit_breaker.py     # Halts trading on excessive losses/failures
  edge_filter.py         # EV & Z-score validation
  execution_guard.py     # FSM trade state, concurrency limits
  persistence.py         # Save/load trade state to JSON
  position_manager.py    # Portfolio & cash tracking
execution/
  order_manager.py       # Order lifecycle management
integrations/
  polymarket_client.py   # Polymarket REST/WebSocket API wrapper
  telegram_bot.py        # Telegram command interface
data/
  active_trades.json     # Persisted open positions
```

## Running the Bot
The workflow runs in **paper trading mode** by default (safe — no real money):
```bash
python polybot_v7_1.py --paper
```

For live trading, set environment variables:
- `PRIVATE_KEY` — Polygon wallet private key
- `TELEGRAM_BOT_TOKEN` — (optional) Telegram bot token
- `TELEGRAM_CHAT_ID` — (optional) Telegram chat ID
- `PAPER_TRADING=false` — to enable live mode

## Configuration
All trading parameters are in `config.yaml`:
- **Trading limits:** max position size, max positions, stop-loss, Kelly fraction
- **Edge filters:** EV threshold, Z-score threshold, spread limits
- **Circuit breaker:** failure rate thresholds, session loss limits
- **Notifications:** what events trigger Telegram alerts

## Workflow
- **Start application** — runs `python polybot_v7_1.py --paper` as a console process
