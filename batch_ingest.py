"""
ss9
ingests a universe of symbols by looping fetch_bars to store_bars with basic retry/backoff on HTTP 429.
"""


import time
import requests

from fetch_bars import fetch_bars
from db_store import store_bars
from datetime import datetime, timezone, timedelta

# A starter list — can probably expand this to 20–50 symbols
SYMBOLS = [

    #starter
    "AAPL", "MSFT", "GOOGL", "AMZN", "META",
    "NVDA", "TSLA", "JPM", "V", "MA",
    "UNH", "HD", "PG", "COST", "PEP",
    "AVGO", "ADBE", "NFLX", "CRM", "INTC",

    #tech
     "ORCL", "IBM", "CSCO", "INTU", "AMD",
    "QCOM", "TXN", "MU", "NOW", "SNOW",

    #financial
     "BAC", "WFC", "C", "GS", "MS",
    "AXP", "BLK", "SCHW", "USB", "PNC",

    #consumer
     "KO", "MNST", "KDP",
    "WMT", "TGT", "KR",
    "CL", "KMB", "MDLZ", "GIS",

    #industrial
    "CAT", "DE", "HON", "GE",
    "MMM", "RTX", "LMT", "BA",

    #etf
    "XLK", "XLF", "XLP"

]

START_DATE = (datetime.now(timezone.utc).date() - timedelta(days=10)).isoformat()
END_DATE = datetime.now(timezone.utc).date().isoformat()
TIMEFRAME = "1Day"

MAX_RETRIES = 3
BACKOFF_SECONDS = 4


def ingest_symbol(symbol):
    """
    try to fetch and store bars for ONE symbol.
    retry a few times if Alpaca says to slow down.
    """

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            bars = fetch_bars(
                symbol=symbol,
                start=START_DATE,
                end=END_DATE,
                timeframe=TIMEFRAME,
            )

            store_bars(symbol, bars)

            print(f"[SUCCESS] {symbol}: stored {len(bars)} bars")
            return

        except requests.exceptions.HTTPError as e:
            # Alpaca rate limit response
            if e.response is not None and e.response.status_code == 429:
                print(
                    f"[RATE LIMIT] {symbol}: attempt {attempt}/{MAX_RETRIES}, "
                    f"sleeping {BACKOFF_SECONDS}s"
                )
                sleep_time = BACKOFF_SECONDS * (2 ** (attempt - 1)) #exponential
                time.sleep(sleep_time)
            else:
                print(f"[HTTP ERROR] {symbol}: {e}")
                return

        except Exception as e:
            print(f"[ERROR] {symbol}: {e}")
            return

    print(f"[FAILED] {symbol}: exceeded retries")


def run_batch_ingestion():
    """
    loop over all symbols and ingest them one at a time.
    one failure should NOT stop the others.
    """

    for symbol in SYMBOLS:
        ingest_symbol(symbol)


if __name__ == "__main__":
    run_batch_ingestion()
