"""
ss18
pnl computation support
fetches Alpaca account JSON (cash/equity/etc.) via the shared HTTP client so PnL snapshots can reflect broker-reported equity.
"""
import os
from dotenv import load_dotenv

from http_client import request_json

load_dotenv(override=True)

BASE_URL = os.getenv("ALPACA_BASE_URL", "https://paper-api.alpaca.markets")

def get_account(run_id: str = "", mode: str = "") -> dict:
    """
    Returns Alpaca account JSON.
    Key fields: cash, equity, portfolio_value, buying_power.
    """
    return request_json(
        "GET",
        f"{BASE_URL}/v2/account",
        run_id=run_id,
        mode=mode,
        context={"component": "pnl", "op": "get_account"},
    )
