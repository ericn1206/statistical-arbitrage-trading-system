"""
research tool to create pairs

ss13
offline research script: 
loads recent closes from Postgres, runs Engle–Granger cointegration tests, and stores top pairs + hedge ratios.
"""
import os
import itertools
from datetime import datetime

import numpy as np
import pandas as pd
import psycopg2
from dotenv import load_dotenv

from statsmodels.tsa.stattools import coint

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

# ======== SETTINGS (tune later) ========
LOOKBACK_DAYS = 90          # how much history to use for pair testing
MIN_OVERLAP = 60            # minimum shared days required to test a pair
MAX_PAIRS_TO_STORE = 20      # shortlist size
PVAL_THRESHOLD = 0.05        # smaller = stronger evidence of cointegration
# =======================================


def get_conn():
    """ open a connection to Postgres."""
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL missing in .env")
    return psycopg2.connect(DATABASE_URL)


def get_symbols(limit=50):
    """
    Choose a universe of symbols that actually have data in DB.
    We take the symbols with the most stored bars.
    """
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT symbol
                FROM prices
                GROUP BY symbol
                ORDER BY COUNT(*) DESC
                LIMIT %s;
                """,
                (limit,),
            )
            return [r[0] for r in cur.fetchall()]
    finally:
        conn.close()


def load_close_series(symbol, lookback_days=180):
    """
    Load closing prices for one symbol from the DB (recent history).
    Returns a pandas Series indexed by date/time.
    """
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
                (symbol, lookback_days),
            )
            rows = cur.fetchall()
    finally:
        conn.close()

    if not rows:
        return None

    rows.reverse()  # oldest -> newest
    df = pd.DataFrame(rows, columns=["ts", "close"])
    df["ts"] = pd.to_datetime(df["ts"])
    df["close"] = df["close"].astype(float)
    return df.set_index("ts")["close"]


def align_series(s1, s2):
    """
    Keep only the timestamps where BOTH series have a price.
    (avoids comparing mismatched days.)
    """
    df = pd.concat([s1.rename("a"), s2.rename("b")], axis=1).dropna()
    return df["a"], df["b"]


def score_pair(symbol_a, symbol_b):
    """
    Run a cointegration test and return a score.
    The main score is p-value (smaller is better).
    """
    s1 = load_close_series(symbol_a, LOOKBACK_DAYS)
    s2 = load_close_series(symbol_b, LOOKBACK_DAYS)

    if s1 is None or s2 is None:
        return None

    a, b = align_series(s1, s2)

    if len(a) < MIN_OVERLAP:
        return None

    # Engle–Granger cointegration test
    # Returns: (test_statistic, p_value, critical_values)
    try:
        _, pval, _ = coint(a.values, b.values)
    except Exception:
        return None

    # Simple hedge ratio estimate: beta from linear regression a ~ beta*b
    # (This is a basic estimate; good enough for selection.)
    beta = np.polyfit(b.values, a.values, 1)[0]

    return {
        "symbol_1": symbol_a,
        "symbol_2": symbol_b,
        "pval": float(pval),
        "hedge_ratio": float(beta),
        "overlap": int(len(a)),
    }


def upsert_pair(symbol_1, symbol_2, hedge_ratio, enabled=True):
    """
    Save this pair into the pairs table.
    If the pair already exists, update its hedge_ratio and enabled flag.
    """
    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO pairs (symbol_1, symbol_2, hedge_ratio, enabled)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (symbol_1, symbol_2) DO UPDATE
                    SET hedge_ratio = EXCLUDED.hedge_ratio,
                        enabled = EXCLUDED.enabled;
                    """,
                    (symbol_1, symbol_2, hedge_ratio, enabled),
                )
    finally:
        conn.close()


def main():
    symbols = get_symbols(limit=50)
    print(f"[UNIVERSE] Using {len(symbols)} symbols from DB")

    results = []
    tested = 0

    for a, b in itertools.combinations(symbols, 2):
        tested += 1
        r = score_pair(a, b)
        if r is None:
            continue
        # keep only reasonably cointegrated pairs
        if r["pval"] <= PVAL_THRESHOLD:
            results.append(r)

    if not results:
        print("[RESULT] No pairs passed the p-value threshold.")
        return

    df = pd.DataFrame(results).sort_values("pval").head(MAX_PAIRS_TO_STORE)

    print(f"[TESTED] pairs tested: {tested}")
    print(f"[FOUND]  pairs passing threshold: {len(results)}")
    print(f"[STORE]  storing top {len(df)} pairs")

    for _, row in df.iterrows():
        upsert_pair(row["symbol_1"], row["symbol_2"], row["hedge_ratio"], enabled=True)
        print(
            f"  saved {row['symbol_1']}/{row['symbol_2']} "
            f"p={row['pval']:.4f} beta={row['hedge_ratio']:.4f} overlap={row['overlap']}"
        )

    print("[DONE] Pair shortlist saved to DB.")


if __name__ == "__main__":
    main()
