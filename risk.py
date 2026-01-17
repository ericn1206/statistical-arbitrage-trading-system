"""
ss19
implements restart-safe risk gating by reading `positions` + latest `prices` to block orders when exposure is too high or data is stale.
"""
import os
from datetime import datetime, timezone

import psycopg2
from dotenv import load_dotenv

load_dotenv(dotenv_path=".env")
DATABASE_URL = os.getenv("DATABASE_URL")


def get_conn():
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL missing")
    return psycopg2.connect(DATABASE_URL)


def fetch_positions():
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT symbol, qty, avg_cost
                FROM positions;
                """
            )
            return cur.fetchall()
    finally:
        conn.close()


def fetch_latest_price(symbol):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT ts, close
                FROM prices
                WHERE symbol = %s
                ORDER BY ts DESC
                LIMIT 1;
                """,
                (symbol,),
            )
            row = cur.fetchone()
            return row
    finally:
        conn.close()


def compute_gross_exposure():
    positions = fetch_positions()
    gross = 0.0

    for symbol, qty, _ in positions:
        qty_f = float(qty)
        row = fetch_latest_price(symbol)
        if row is None:
            continue
        _, px = row
        gross += abs(qty_f) * float(px)

    return gross


def symbol_position_value(symbol):
    positions = fetch_positions()
    qty_f = 0.0
    for sym, qty, _ in positions:
        if sym == symbol:
            qty_f = float(qty)
            break

    row = fetch_latest_price(symbol)
    if row is None:
        return None

    _, px = row
    return abs(qty_f) * float(px)


def data_is_stale(symbols, stale_seconds):
    now = datetime.now(timezone.utc)
    worst_age = 0.0

    for symbol in symbols:
        row = fetch_latest_price(symbol)
        if row is None:
            return True, None
        ts, _ = row
        ts_dt = ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
        age = (now - ts_dt).total_seconds()
        worst_age = max(worst_age, age)

    return worst_age > float(stale_seconds), worst_age


def check_order_limits(orders_submitted, max_orders_per_run):
    return orders_submitted < int(max_orders_per_run)


def risk_check(
    symbols_for_order,
    max_gross_exposure,
    max_position_value_per_symbol,
    max_orders_per_run,
    stale_seconds,
    orders_submitted_in_run,
):
    reasons = []

    stale, age = data_is_stale(symbols_for_order, stale_seconds)
    if stale:
        reasons.append(f"data_stale age_seconds={None if age is None else int(age)}")

    gross = compute_gross_exposure()
    if gross > float(max_gross_exposure):
        reasons.append(f"max_gross_exposure gross={gross:.2f} limit={float(max_gross_exposure):.2f}")

    for symbol in symbols_for_order:
        v = symbol_position_value(symbol)
        if v is None:
            reasons.append(f"missing_price symbol={symbol}")
            continue
        if v > float(max_position_value_per_symbol):
            reasons.append(f"max_symbol_position symbol={symbol} value={v:.2f} limit={float(max_position_value_per_symbol):.2f}")

    if not check_order_limits(orders_submitted_in_run, max_orders_per_run):
        reasons.append(f"max_orders_per_run submitted={orders_submitted_in_run} limit={int(max_orders_per_run)}")

    allowed = len(reasons) == 0
    return allowed, reasons
