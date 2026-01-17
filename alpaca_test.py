"""
not really needed

checks if alpaca keys work, also connectivity test against /v2/account 
endpoint using requests and keys from .env 
"""

import os, requests
from dotenv import load_dotenv

load_dotenv(override=True)

BASE = os.getenv("ALPACA_BASE_URL", "https://paper-api.alpaca.markets")
KEY = os.getenv("ALPACA_API_KEY")
SEC = os.getenv("ALPACA_SECRET_KEY")

headers = {"APCA-API-KEY-ID": KEY, "APCA-API-SECRET-KEY": SEC}

r = requests.get(f"{BASE}/v2/account", headers=headers)
print("status:", r.status_code)
print(r.text)
