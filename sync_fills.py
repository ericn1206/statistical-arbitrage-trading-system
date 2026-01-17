"""
ss17
trades & positions persistence
pulls recent orders + fills from Alpaca, inserts trades with ON CONFLICT DO NOTHING, then rebuilds `positions` by aggregating signed trade quantities.
"""

import os
import json
from datetime import datetime, timezone, timedelta
from http_client import request_json

import psycopg2
import requests
from dotenv import load_dotenv

load_dotenv(dotenv_path=".env")

DATABASE_URL = os.getenv("DATABASE_URL")
PAPER_KEY = os.getenv("ALPACA_PAPER_KEY")
PAPER_SECRET = os.getenv("ALPACA_PAPER_SECRET")
PAPER_BASE_URL = os.getenv("ALPACA_PAPER_BASE_URL", "https://paper-api.alpaca.markets")

import uuid
from logger import get_logger, log_event, log_error

log = get_logger("fills")
RUN_ID = os.getenv("RUN_ID", uuid.uuid4().hex[:12])
MODE = os.getenv("TRADING_MODE", "paper")

def info(event, **fields):
    log_event(log, event, run_id=RUN_ID, mode=MODE, **fields)

def err(event, exc, **fields):
    log_error(log, event, exc, run_id=RUN_ID, mode=MODE, **fields)


def get_conn():
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL missing")
    return psycopg2.connect(DATABASE_URL)


def alpaca_headers():
    if not PAPER_KEY or not PAPER_SECRET:
        raise ValueError("Paper Alpaca keys missing")
    return {
        "APCA-API-KEY-ID": PAPER_KEY,
        "APCA-API-SECRET-KEY": PAPER_SECRET,
        "Content-Type": "application/json",
    }


def list_recent_orders(minutes=180, limit=200):
    after = (datetime.now(timezone.utc) - timedelta(minutes=minutes)).isoformat().replace("+00:00", "Z")
    url = f"{PAPER_BASE_URL}/v2/orders"
    params = {"status": "all", "after": after, "limit": limit, "direction": "desc"}
    # resp = requests.get(url, headers=alpaca_headers(), params=params)
    # resp.raise_for_status()
    # return resp.json()
    return request_json(
        "GET",
        url,
        headers=alpaca_headers(),
        params=params,
        run_id=RUN_ID,
        mode=MODE,
        context={"component": "sync_fills", "op": "list_recent_orders"},
    )



def list_fills_for_order(alpaca_order_id):
    url = f"{PAPER_BASE_URL}/v2/orders/{alpaca_order_id}/fills"
    return request_json(
        "GET",
        url,
        headers=alpaca_headers(),
        run_id=RUN_ID,
        mode=MODE,
        context={"component": "sync_fills", "op": "list_fills_for_order", "alpaca_order_id": alpaca_order_id},
    )

def upsert_trade(fill, alpaca_order_id):
    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO trades (
                        alpaca_order_id,
                        alpaca_trade_id,
                        symbol,
                        side,
                        qty,
                        price,
                        trade_ts,
                        raw
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                    ON CONFLICT (alpaca_trade_id) DO NOTHING
                    RETURNING alpaca_trade_id;
                    """,
                    (
                        alpaca_order_id,
                        fill["id"],
                        fill["symbol"],
                        fill["side"],
                        str(fill["qty"]),
                        str(fill["price"]),
                        fill["timestamp"],
                        json.dumps(fill),
                    ),
                )
                return cur.fetchone() is not None
    finally:
        conn.close()


def recompute_positions_from_trades():
    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("TRUNCATE positions;")

                cur.execute(
                    """
                    WITH signed AS (
                        SELECT
                            symbol,
                            CASE WHEN side = 'buy' THEN qty ELSE -qty END AS signed_qty,
                            qty,
                            price,
                            trade_ts
                        FROM trades
                    ),
                    agg AS (
                        SELECT
                            symbol,
                            SUM(signed_qty) AS net_qty,
                            SUM(CASE WHEN signed_qty > 0 THEN qty * price ELSE 0 END) AS buy_notional,
                            SUM(CASE WHEN signed_qty > 0 THEN qty ELSE 0 END) AS buy_qty
                        FROM signed
                        GROUP BY symbol
                    )
                    INSERT INTO positions (symbol, qty, avg_cost, updated_at)
                    SELECT
                        symbol,
                        net_qty,
                        CASE WHEN buy_qty = 0 THEN 0 ELSE buy_notional / buy_qty END AS avg_cost,
                        NOW()
                    FROM agg
                    WHERE net_qty <> 0;
                    """
                )
    finally:
        conn.close()

def sync(minutes=180):
    info("fills_sync_start", minutes=minutes)

    try:
        orders = list_recent_orders(minutes=minutes)
        info("fills_fetched", count=len(orders))

        processed = 0
        fills_seen = 0
        trades_written = 0

        for o in orders:
            alpaca_order_id = o.get("id")
            filled_qty = float(o.get("filled_qty") or 0)
            if filled_qty <= 0:
                continue


            fills = list_fills_for_order(alpaca_order_id)
            fills_seen += len(fills)

            for fill in fills:
                inserted = upsert_trade(fill, alpaca_order_id)
                if inserted:
                    trades_written += 1
                    info(
                        "trade_written",
                        alpaca_trade_id=fill.get("id"),
                        alpaca_order_id=alpaca_order_id,
                        symbol=fill.get("symbol"),
                    )

            processed += 1

        recompute_positions_from_trades()
        info(
            "positions_rebuilt",
            orders_with_fills=processed,
            fills_seen=fills_seen,
            trades_written=trades_written,
        )

    except Exception as e:
        err("fills_sync_failed", e, minutes=minutes)
        raise

if __name__ == "__main__":
    sync(minutes=360)
