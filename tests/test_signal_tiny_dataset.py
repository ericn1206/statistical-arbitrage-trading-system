from datetime import datetime, timedelta
from providers import Bar
from strategy import compute_pair_action

def test_signal_correctness_tiny_dataset():
    base = datetime(2025, 1, 1)
    bars_a, bars_b = [], []

    for i in range(40):
        ts = base + timedelta(days=i)
        a = 100.0
        b = 100.0
        if i == 39:
            a = 130.0
        bars_a.append(Bar(ts=ts, open=a, close=a))
        bars_b.append(Bar(ts=ts, open=b, close=b))

    z, action = compute_pair_action(bars_a, bars_b, hedge_ratio=1.0, entry_z=2.0, exit_z=0.5)
    assert z > 0
    assert action in ("ENTER_SHORT", "HOLD")
