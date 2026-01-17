"""
not really needed

ss12
computes a simple spread z-score for a configured pair and writes BUY/SELL/HOLD signals into `signals`.
"""
import os
from datetime import datetime
import psycopg2
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")


def get_conn():
    """opens a connection to Postgres."""
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL missing in .env")
    return psycopg2.connect(DATABASE_URL)


def get_pair(pair_id):
    """look up which two symbols belong to this pair_id."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT symbol_1, symbol_2, enabled FROM pairs WHERE id = %s;",
                (pair_id,),
            )
            row = cur.fetchone()
            if not row:
                raise ValueError(f"No pair found with id={pair_id}")
            return row  # (symbol_1, symbol_2, enabled)
    finally:
        conn.close()


def get_last_n_closes(symbol, n=7):
    """
    get the most recent N closing prices we have saved for this symbol.
    Returns a list ordered oldest -> newest.
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
                (symbol, n),
            )
            rows = cur.fetchall()
    finally:
        conn.close()

    # rows came back newest -> oldest; reverse to oldest -> newest
    rows.reverse()
    return rows  # [(ts, close), ...]


def mean(values):
    """ average."""
    return sum(values) / len(values)


def std(values):
    """
    how spread out numbers are.
     a simple population std dev, good enough for a toy signal.)
    """
    m = mean(values)
    variance = sum((x - m) ** 2 for x in values) / len(values)
    return variance ** 0.5


def make_action(z, entry=1.0):
    """
    - If z is big positive, spread is high -> SELL (bet it comes back down)
    - If z is big negative, spread is low -> BUY (bet it comes back up)
    - Otherwise HOLD
    """
    if z >= entry:
        return "SELL"
    if z <= -entry:
        return "BUY"
    return "HOLD"


def insert_signal(pair_id, ts, zscore, action, run_id):
    """store the signal decision in the signals table."""
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


def toy_signal_for_pair(pair_id, lookback=7):
    """
    For one pair, compute spread = close1 - close2
    Then compute zscore of today's spread compared to history.
    """
    symbol_1, symbol_2, enabled = get_pair(pair_id)
    if not enabled:
        print(f"[SKIP] pair_id={pair_id} disabled")
        return

    closes_1 = get_last_n_closes(symbol_1, n=lookback)
    closes_2 = get_last_n_closes(symbol_2, n=lookback)

    if len(closes_1) < lookback or len(closes_2) < lookback:
        print(
            f"[NOT ENOUGH DATA] Need {lookback} bars each. "
            f"{symbol_1} has {len(closes_1)}, {symbol_2} has {len(closes_2)}"
        )
        return

    # assume timestamps line up well enough for this toy thingy
    spread_series = []
    for (_, c1), (_, c2) in zip(closes_1, closes_2):
        spread_series.append(float(c1) - float(c2))

    spread_today = spread_series[-1]
    spread_hist = spread_series[:-1]  # everything except today

    m = mean(spread_hist)
    s = std(spread_hist)

    # Avoid divide-by-zero if spread never changes
    z = 0.0 if s == 0 else (spread_today - m) / s

    action = make_action(z, entry=1.0)

    # timestamp = use the latest timestamp from symbol_1 
    ts_latest = closes_1[-1][0]

    run_id = f"toy_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"

    print(f"[PAIR] {pair_id}: {symbol_1}/{symbol_2}")
    print(f"[SPREAD TODAY] {spread_today:.4f}")
    print(f"[MEAN HIST]   {m:.4f}")
    print(f"[STD HIST]    {s:.4f}")
    print(f"[ZSCORE]      {z:.4f}")
    print(f"[ACTION]      {action}")
    print(f"[RUN_ID]      {run_id}")
    print(f"[TS]          {ts_latest}")

    insert_signal(pair_id, ts_latest, z, action, run_id)
    print("[DB] Signal saved.")


if __name__ == "__main__":
    # Change this to an actual pair id from DB (SELECT * FROM pairs;)
    PAIR_ID = 1
    toy_signal_for_pair(PAIR_ID, lookback=7)
