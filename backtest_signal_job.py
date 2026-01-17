"""
backtest-only

ss21
walks historical timestamps from the DB provider and prints non-HOLD signals, proving the strategy can run in backtest mode.
"""

import os
from datetime import datetime
from providers import BacktestDBProvider
from strategy import compute_pair_action

DATABASE_URL = os.getenv("DATABASE_URL")

provider = BacktestDBProvider(
    DATABASE_URL,
    start_ts=datetime.fromisoformat("2024-01-01T00:00:00"),
    end_ts=datetime.fromisoformat("2024-06-01T00:00:00"),
)

LOOKBACK = 120

sym_a, sym_b, hedge_ratio = "AAPL", "MSFT", 1.0

for t in provider.iter_times(symbol=sym_a):
    bars_a = provider.get_window(sym_a, end_ts=t, lookback=LOOKBACK)
    bars_b = provider.get_window(sym_b, end_ts=t, lookback=LOOKBACK)
    z, action = compute_pair_action(bars_a, bars_b, hedge_ratio)
    # for ss 21, just print, ss 22 will “fill”
    if action != "HOLD":
        print(t, action, z)
