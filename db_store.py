"""
ss8
stores Alpaca bars into Postgres `prices` using INSERT ... ON CONFLICT DO NOTHING for idempotent ingestion.
"""

import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")


def get_conn():
    """
    Opens a connection to Postgres database.
    """
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL is missing. Add it to .env file.")
    return psycopg2.connect(DATABASE_URL)


def store_bars(symbol, bars):
    """
     Takes a list of Alpaca bars for ONE symbol
    and saves them into the prices table.

    If a (symbol, ts) already exists, it DOES NOT create a duplicate.
    """

    # This is the SQL UPSERT.
    # It means: insert the row, but if (symbol, ts) already exists, do nothing.
    upsert_sql = """
    INSERT INTO prices (symbol, ts, open, high, low, close, volume)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (symbol, ts) DO NOTHING;
    """

    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                for bar in bars:
                    # alpaca bar fields:
                    # t = timestamp, o = open, h = high, l = low, c = close, v = volume
                    cur.execute(
                        upsert_sql,
                        (
                            symbol,
                            bar["t"],
                            bar["o"],
                            bar["h"],
                            bar["l"],
                            bar["c"],
                            bar.get("v"),  # volume might be missing sometimes
                        ),
                    )
    finally:
        conn.close()


def count_price_rows():
    """
    Counts how many rows exist in the prices table. sanity check ig
    """
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM prices;")
            return cur.fetchone()[0]
    finally:
        conn.close()
