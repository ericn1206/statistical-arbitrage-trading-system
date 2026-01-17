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
from typing import Optional

from statsmodels.tsa.stattools import coint

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

# ======== SETTINGS (tune) ========
LOOKBACK_DAYS = 252          # how much history to use for pair testing
MIN_OVERLAP = 60            # minimum shared days required to test a pair
MAX_PAIRS_TO_STORE = 20      # shortlist size
PVAL_THRESHOLD = 0.05        # smaller = stronger evidence of cointegration
# =======================================


def get_conn():
    """ open a connection to Postgres."""
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL missing in .env")
    return psycopg2.connect(DATABASE_URL)


def get_symbols(limit=250):
    """
    Choose a universe of symbols that actually have data in DB.
    take the symbols with the most stored bars.
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
    load closing prices for one symbol from the DB (recent history).
    returns a pandas Series indexed by date/time.
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
    keep only the timestamps where BOTH series have a price.
    (avoids comparing mismatched days.)
    """
    df = pd.concat([s1.rename("a"), s2.rename("b")], axis=1).dropna()
    return df["a"], df["b"]


#adf, half-life, filter by category
from statsmodels.regression.linear_model import OLS
from statsmodels.tools.tools import add_constant
from statsmodels.tsa.stattools import adfuller, coint

def calc_half_life(spread: np.ndarray) -> Optional[float]:
    """
    half-life of mean reversion for a spread series.
    Uses: Δs_t = a + b*s_{t-1} + e_t; half_life = -ln(2)/b if b < 0
    """
    if len(spread) < 20:
        return None

    s = np.asarray(spread, dtype=float)
    lag = s[:-1]
    delta = s[1:] - s[:-1]

    X = add_constant(lag)  # include intercept
    model = OLS(delta, X).fit()
    b = model.params[1]  # coefficient on lag

    if b >= 0:
        return None

    hl = -np.log(2) / b
    if not np.isfinite(hl):
        return None
    return float(hl)

SECTOR_MAP = {
    # Financials
    "JPM": "financials", "BAC": "financials", "WFC": "financials",
    "C": "financials", "GS": "financials", "MS": "financials",
    "AXP": "financials", "USB": "financials", "SCHW": "financials",
    "BLK": "financials",

    # Tech
    "AAPL": "tech", "MSFT": "tech", "NVDA": "tech", "AMD": "tech",
    "INTC": "tech", "CSCO": "tech", "QCOM": "tech", "TXN": "tech",
    "AVGO": "tech", "ADBE": "tech", "CRM": "tech", "INTU": "tech",
    "SNOW": "tech", "NOW": "tech",

    # Consumer Staples
    "PG": "staples", "GIS": "staples", "MDLZ": "staples",
    "KO": "staples", "PEP": "staples", "KDP": "staples",

    # Consumer Discretionary
    "AMZN": "discretionary", "TSLA": "discretionary",
    "HD": "discretionary", "LOW": "discretionary",

    # Industrials
    "GE": "industrials", "HON": "industrials", "DE": "industrials",
    "CAT": "industrials", "MMM": "industrials", "RTX": "industrials",
    "LMT": "industrials", "BA": "industrials",

    # ETFs
    "XLF": "financials", "XLK": "tech", "XLP": "staples",
}

def sector(symbol: str) -> Optional[str]:
    return SECTOR_MAP.get(symbol)

def score_pair(symbol_a, symbol_b):
    if sector(symbol_a) != sector(symbol_b):
        return None
    s1 = load_close_series(symbol_a, LOOKBACK_DAYS)
    s2 = load_close_series(symbol_b, LOOKBACK_DAYS)
    if s1 is None or s2 is None:
        return None

    a, b = align_series(s1, s2)
    if len(a) < MIN_OVERLAP:
        return None

    # Convert to numpy once
    y = a.values.astype(float)
    x = b.values.astype(float)

    try:
        # Engle–Granger on price levels (OK as first filter)
        _, pval, _ = coint(y, x)

        # OLS hedge ratio WITH intercept
        X = add_constant(x)          # [const, x]
        ols = OLS(y, X).fit()
        alpha = float(ols.params[0])
        beta = float(ols.params[1])

        # Basic sanity filters for tradability
        if not np.isfinite(beta):
            return None
        if beta <= 0:
            return None               # usually avoid negative-beta pairs for simple statarb
        if not (0.3 <= abs(beta) <= 1.5):
            return None

        # Spread / residuals
        spread = y - (alpha + beta * x)

        # ADF on spread (stationarity check)
        adf_pval = float(adfuller(spread, autolag="AIC")[1])
        if adf_pval > 0.05:
            return None

        # Half-life
        half_life = calc_half_life(spread)
        if half_life is None or not (1.0 < half_life < 40.0):
            return None

    except Exception as e:
        print(f"Error scoring {symbol_a}/{symbol_b}: {e}")
        return None

    # Final filter: cointegration p-value threshold
    if pval > PVAL_THRESHOLD:
        return None

    return {
        "symbol_1": symbol_a,
        "symbol_2": symbol_b,
        "pval": float(pval),
        "adf_pval": float(adf_pval),
        "hedge_ratio": float(beta),   # store beta (s1 ~ alpha + beta*s2)
        "alpha": float(alpha),
        "half_life": float(half_life),
        "overlap": int(len(a)),
    }

def upsert_pair(symbol_1, symbol_2, hedge_ratio, enabled=True):
    """
    save this pair into the pairs table.
    if the pair already exists, update its hedge_ratio and enabled flag.
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
    symbols = get_symbols(limit=250)
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

    df_all = pd.DataFrame(results).sort_values("pval")

    MAX_PAIRS_PER_SYMBOL = 2

    used = {}
    chosen_rows = []

    for _, row in df_all.iterrows():
        a = row["symbol_1"]
        b = row["symbol_2"]

        if used.get(a, 0) >= MAX_PAIRS_PER_SYMBOL:
            continue
        if used.get(b, 0) >= MAX_PAIRS_PER_SYMBOL:
            continue

        chosen_rows.append(row)
        used[a] = used.get(a, 0) + 1
        used[b] = used.get(b, 0) + 1

        if len(chosen_rows) >= MAX_PAIRS_TO_STORE:
            break

    df = pd.DataFrame(chosen_rows)


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
