'''
not really needed

Docstring for session8_test
Tests if Rows after is bigger than Rows before
'''

from fetch_bars import fetch_bars
from db_store import store_bars, count_price_rows

if __name__ == "__main__":
    before = count_price_rows()
    print("Rows before:", before)

    symbol = "AAPL"
    bars = fetch_bars(
        symbol=symbol,
        start="2024-01-01",
        end="2024-01-10",
        timeframe="1Day",
    )

    store_bars(symbol, bars)

    after = count_price_rows()
    print("Rows after:", after)
    print("Inserted:", after - before)
