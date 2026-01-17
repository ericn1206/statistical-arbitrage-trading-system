"""
ss14
live signal job zscore on spread
Generates live signals for enabled pairs by pulling latest closes, computing spread z-scores, and inserting into `signals` idempotently per (pair_id, ts, run_id).
"""

import os
from datetime import datetime, timezone
import uuid

import numpy as np
import pandas as pd
import psycopg2
from dotenv import load_dotenv
from providers import Bar

load_dotenv(dotenv_path=".env")
DATABASE_URL = os.getenv("DATABASE_URL")


LOOKBACK = 60
ENTRY_Z = 1.0
EXIT_Z = 0.2
MAX_PAIRS = 200

from typing import List, Tuple
import uuid
from logger import get_logger, log_event, log_error

log = get_logger("signals")
RUN_ID = os.getenv("RUN_ID", uuid.uuid4().hex[:12])
MODE = os.getenv("TRADING_MODE", "paper")

def compute_pair_action(
    bars_a: List[Bar],
    bars_b: List[Bar],
    hedge_ratio: float,
    entry_z: float = 2.0,
    exit_z: float = 0.5,
) -> Tuple[float, str]:
    # align by timestamp (simple dict join)
    da = {b.ts: b.close for b in bars_a}
    db = {b.ts: b.close for b in bars_b}
    common = sorted(set(da.keys()) & set(db.keys()))
    if len(common) < 30:
        return 0.0, "HOLD"

    spread = np.array([da[t] - hedge_ratio * db[t] for t in common], dtype=float)
    mu = spread.mean()
    sd = spread.std(ddof=1) if len(spread) > 1 else 0.0
    if sd == 0.0:
        return 0.0, "HOLD"

    z = float((spread[-1] - mu) / sd)

    # action rules (match live job)
    if z >= entry_z:
        return z, "ENTER_SHORT"
    if z <= -entry_z:
        return z, "ENTER_LONG"
    if abs(z) <= exit_z:
        return z, "EXIT"
    return z, "HOLD"


def get_conn():
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL missing in .env")
    return psycopg2.connect(DATABASE_URL)


def fetch_enabled_pairs(limit=MAX_PAIRS):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, symbol_1, symbol_2, hedge_ratio
                FROM pairs
                WHERE enabled = TRUE
                ORDER BY id
                LIMIT %s;
                """,
                (limit,),
            )
            return cur.fetchall()
    finally:
        conn.close()


def fetch_closes(symbol, n=LOOKBACK + 5):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT ts, close
                FROM prices
                WHERE symbol = %s
                ORDER BY ts DESC
                LIMIT %s;
                """,
                (symbol, n),
            )
            rows = cur.fetchall()
    finally:
        conn.close()

    if not rows:
        return pd.Series(dtype=float)

    df = pd.DataFrame(rows, columns=["ts", "close"])
    df["ts"] = pd.to_datetime(df["ts"], utc=True, errors="coerce")
    df["close"] = df["close"].astype(float)
    df = df.dropna(subset=["ts"]).sort_values("ts")
    return df.set_index("ts")["close"]


def align_series(s1: pd.Series, s2: pd.Series):
    df = pd.concat([s1.rename("a"), s2.rename("b")], axis=1).dropna()
    return df


def compute_zscore(spread: pd.Series, lookback=LOOKBACK):
    if len(spread) < lookback:
        return None

    roll_mean = spread.rolling(window=lookback).mean()
    roll_std = spread.rolling(window=lookback).std(ddof=0)

    z = (spread - roll_mean) / roll_std.replace(0.0, np.nan)
    latest_ts = z.index[-1]
    latest_z = z.iloc[-1]

    if pd.isna(latest_z):
        return None

    return float(latest_z), latest_ts


def action_from_z(z, entry=ENTRY_Z, exit_=EXIT_Z):
    if z >= entry:
        return "ENTER_SHORT"
    if z <= -entry:
        return "ENTER_LONG"
    if abs(z) <= exit_:
        return "EXIT"
    return "HOLD"


def insert_signal(pair_id, ts, zscore, action, run_id):
    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO signals (pair_id, ts, zscore, action, run_id)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (pair_id, ts, run_id) DO NOTHING;
                    """,
                    (pair_id, ts, zscore, action, run_id),
                )
    finally:
        conn.close()


def run_live_signals():
    run_id = f"live_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
    pairs = fetch_enabled_pairs()

    log_event(log, "signal_run_start", run_id=run_id, mode=MODE, enabled_pairs=len(pairs))

    if not pairs:
        print("[SIGNALS] No enabled pairs found.")
        log_event(log, "signal_run_done", run_id=run_id, mode=MODE, produced=0, skipped=0)
        return

    produced = 0
    skipped = 0

    try:
        for pair_id, sym1, sym2, hedge_ratio in pairs:
            s1 = fetch_closes(sym1)
            s2 = fetch_closes(sym2)

            if s1.empty or s2.empty:
                skipped += 1
                continue

            aligned = align_series(s1, s2)
            if len(aligned) < LOOKBACK:
                skipped += 1
                continue

            spread = aligned["a"] - float(hedge_ratio) * aligned["b"]
            z_out = compute_zscore(spread, lookback=LOOKBACK)
            if z_out is None:
                skipped += 1
                continue

            z, ts = z_out
            action = action_from_z(z)

            log_event(
                log,
                "signal_computed",
                run_id=run_id,
                mode=MODE,
                pair_id=pair_id,
                ts=ts.isoformat(),
                zscore=z,
                action=action,
            )

            insert_signal(pair_id, ts, z, action, run_id)

            log_event(
                log,
                "signal_written",
                run_id=run_id,
                mode=MODE,
                pair_id=pair_id,
                ts=ts.isoformat(),
            )

            produced += 1

        log_event(log, "signal_run_done", run_id=run_id, mode=MODE, produced=produced, skipped=skipped)

    except Exception as e:
        log_error(log, "signal_run_failed", e, run_id=run_id, mode=MODE)
        raise

    print(f"[DONE] run_id={run_id} produced={produced} skipped={skipped} enabled_pairs={len(pairs)}")


if __name__ == "__main__":
    run_live_signals()
