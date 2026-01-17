"""
ss15
trading api connectivity paper
submits a single Alpaca paper order and persists the returned order JSON into the `orders` table.
"""

import os
import json
import uuid
from datetime import datetime
from http_client import request_json

import psycopg2
import requests
from dotenv import load_dotenv
import os, uuid

RUN_ID = os.getenv("RUN_ID", uuid.uuid4().hex[:12])
MODE = os.getenv("TRADING_MODE", "paper")

load_dotenv(dotenv_path=".env")

DATABASE_URL = os.getenv("DATABASE_URL")
PAPER_KEY = os.getenv("ALPACA_PAPER_KEY")
PAPER_SECRET = os.getenv("ALPACA_PAPER_SECRET")
PAPER_BASE_URL = os.getenv("ALPACA_PAPER_BASE_URL", "https://paper-api.alpaca.markets")


def get_conn():
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL missing in .env")
    return psycopg2.connect(DATABASE_URL)


def submit_paper_order(symbol, qty, side="buy", order_type="market", time_in_force="day"):
    if not PAPER_KEY or not PAPER_SECRET:
        raise ValueError("Paper Alpaca keys missing in .env")

    url = f"{PAPER_BASE_URL}/v2/orders"
    headers = {
        "APCA-API-KEY-ID": PAPER_KEY,
        "APCA-API-SECRET-KEY": PAPER_SECRET,
        "Content-Type": "application/json",
    }

    client_order_id = f"statarb_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"

    payload = {
        "symbol": symbol,
        "qty": str(qty),
        "side": side,
        "type": order_type,
        "time_in_force": time_in_force,
        "client_order_id": client_order_id,
    }

    return request_json(
        "POST",
        url,
        headers=headers,
        data=json.dumps(payload),
        run_id=RUN_ID,
        mode=MODE,
        context={
            "component": "paper_order",
            "op": "submit_order",
            "client_order_id": payload.get("client_order_id"),
            "symbol": payload.get("symbol"),
            "side": payload.get("side"),
            "qty": payload.get("qty"),
        },
    )



def store_order(order_json, symbol, qty, side, order_type, time_in_force):
    alpaca_order_id = order_json.get("id")
    client_order_id = order_json.get("client_order_id")
    status = order_json.get("status")
    submitted_at = order_json.get("submitted_at")

    if not alpaca_order_id:
        raise ValueError("No alpaca order id returned")

    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO orders (
                        alpaca_order_id, client_order_id, symbol, side, qty,
                        order_type, time_in_force, status, submitted_at, raw
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                    ON CONFLICT (alpaca_order_id) DO UPDATE
                    SET status = EXCLUDED.status,
                        submitted_at = EXCLUDED.submitted_at,
                        raw = EXCLUDED.raw;
                    """,
                    (
                        alpaca_order_id,
                        client_order_id,
                        symbol,
                        side,
                        str(qty),
                        order_type,
                        time_in_force,
                        status,
                        submitted_at,
                        json.dumps(order_json),
                    ),
                )
    finally:
        conn.close()


if __name__ == "__main__":
    symbol = "AAPL"
    qty = 1
    side = "buy"
    order_type = "market"
    time_in_force = "day"

    order = submit_paper_order(symbol, qty, side=side, order_type=order_type, time_in_force=time_in_force)
    store_order(order, symbol, qty, side, order_type, time_in_force)

    print("submitted alpaca_order_id:", order.get("id"))
    print("status:", order.get("status"))
