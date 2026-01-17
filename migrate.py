"""
ss5
Creates the project schema (prices/pairs/signals/orders/trades/positions/pnl) so the DB can be rebuilt from scratch.
"""

import os
import psycopg2
from dotenv import load_dotenv

load_dotenv(dotenv_path=".env")
DATABASE_URL = os.getenv("DATABASE_URL")

DDL = """
CREATE TABLE IF NOT EXISTS prices(
  symbol TEXT NOT NULL,
  ts TIMESTAMPTZ NOT NULL,
  open DOUBLE PRECISION NOT NULL,
  high DOUBLE PRECISION NOT NULL,
  low DOUBLE PRECISION NOT NULL,
  close DOUBLE PRECISION NOT NULL,
  volume DOUBLE PRECISION NOT NULL,
  PRIMARY KEY(symbol, ts)
);

CREATE TABLE IF NOT EXISTS pairs(
  id SERIAL PRIMARY KEY,
  symbol_1 TEXT NOT NULL,
  symbol_2 TEXT NOT NULL,
  hedge_ratio DOUBLE PRECISION NOT NULL,
  enabled BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS signals(
  pair_id INT NOT NULL REFERENCES pairs(id),
  ts TIMESTAMPTZ NOT NULL,
  zscore DOUBLE PRECISION NOT NULL,
  action TEXT NOT NULL,
  run_id TEXT NOT NULL,
  UNIQUE(pair_id, ts, run_id)
);

CREATE TABLE IF NOT EXISTS orders(
  alpaca_order_id TEXT,
  client_order_id TEXT UNIQUE,
  symbol TEXT NOT NULL,
  side TEXT NOT NULL,
  qty NUMERIC NOT NULL,
  order_type TEXT NOT NULL,
  time_in_force TEXT NOT NULL,
  status TEXT,
  submitted_at TIMESTAMPTZ,
  raw JSONB
);

CREATE TABLE IF NOT EXISTS trades(
  alpaca_trade_id TEXT UNIQUE,
  alpaca_order_id TEXT,
  symbol TEXT NOT NULL,
  side TEXT NOT NULL,
  qty NUMERIC NOT NULL,
  price NUMERIC NOT NULL,
  trade_ts TIMESTAMPTZ NOT NULL,
  raw JSONB
);

CREATE TABLE IF NOT EXISTS positions(
  symbol TEXT PRIMARY KEY,
  qty NUMERIC NOT NULL,
  avg_cost NUMERIC NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS pnl(
  ts TIMESTAMPTZ PRIMARY KEY,
  equity NUMERIC NOT NULL,
  cash NUMERIC NOT NULL,
  unrealized_pnl NUMERIC NOT NULL,
  realized_pnl NUMERIC NOT NULL,
  daily_pnl NUMERIC NOT NULL
);
"""

def main():
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL missing")
    conn = psycopg2.connect(DATABASE_URL)
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(DDL)
        print("[MIGRATE] ok")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
