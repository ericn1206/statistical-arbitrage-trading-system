'''
ss14
pure signal logic: aligns two bar windows, builds the spread using hedge_ratio, and returns (z, action) using entry/exit thresholds.
'''
import numpy as np

def compute_pair_action(
    bars_a,
    bars_b,
    hedge_ratio,
    entry_z=2.0,
    exit_z=0.5,
):
    # align by timestamp
    da = {b.ts: b.close for b in bars_a}
    db = {b.ts: b.close for b in bars_b}
    common = sorted(set(da) & set(db))

    if len(common) < 30:
        return 0.0, "HOLD"

    spread = np.array(
        [da[t] - hedge_ratio * db[t] for t in common],
        dtype=float,
    )

    mu = spread.mean()
    sd = spread.std(ddof=1) if len(spread) > 1 else 0.0

    if sd == 0.0:
        return 0.0, "HOLD"

    z = (spread[-1] - mu) / sd

    if z >= entry_z:
        return float(z), "ENTER_SHORT"
    if z <= -entry_z:
        return float(z), "ENTER_LONG"
    if abs(z) <= exit_z:
        return float(z), "EXIT"

    return float(z), "HOLD"
