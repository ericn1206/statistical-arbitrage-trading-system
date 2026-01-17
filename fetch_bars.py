"""
ss7
fetch bars for a single symbol from Alpaca Market Data, returns raw bar list
"""

import os
import requests
from dotenv import load_dotenv

# Load values from .env into Python
load_dotenv(override=True)

# Get Alpaca credentials and base URL
API_KEY = os.getenv("ALPACA_API_KEY")
SECRET_KEY = os.getenv("ALPACA_SECRET_KEY")
DATA_URL = os.getenv("ALPACA_DATA_URL")

# Headers are to prove to Alpaca that we are who we are
HEADERS = {
    "APCA-API-KEY-ID": API_KEY,
    "APCA-API-SECRET-KEY": SECRET_KEY,
}


def fetch_bars(symbol, start, end, timeframe):
    """
    Ask Alpaca for historical price bars for ONE stock.
    """

    # alpaca endpoint for historical bars
    url = f"{DATA_URL}/v2/stocks/bars"

    #filters we sent to Alpaca
    params = {
        "symbols": symbol,
        "start": start,
        "end": end,
        "timeframe": timeframe,
        "feed": "iex",          
    }

    # make the HTTP request
    response = requests.get(url, headers=HEADERS, params=params)

    # if Alpaca says something went wrong, crash loudly
    response.raise_for_status()

    # convert response to Python dictionary
    data = response.json()

    # return just the bars for this symbol
    return data["bars"][symbol]

# more of a test
if __name__ == "__main__":
    bars = fetch_bars(
        symbol="AAPL",
        start="2024-01-01",
        end="2024-01-10",
        timeframe="1Day",
    )

    for bar in bars:
        print(bar)
