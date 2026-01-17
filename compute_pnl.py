"""
ss18 simple computation profit loss
marks positions to market using latest closes, pulls cash/equity from Alpaca, and UPSERTs a timestamped row into `pnl`.
"""

import os
from datetime import datetime, timezone, date, timedelta

import psycopg2
from dotenv import load_dotenv

import uuid
from alpaca_account import get_account

load_dotenv(dotenv_path=".env")

DATABASE_URL = os.getenv("DATABASE_URL")

from logger import get_logger, log_event, log_error

log = get_logger("pnl")
RUN_ID = os.getenv("RUN_ID", uuid.uuid4().hex[:12])
MODE = os.getenv("TRADING_MODE", "paper")

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
                FROM positions
                WHERE qty <> 0;
                """
            )
            return cur.fetchall()
    finally:
        conn.close()


def fetch_latest_close(symbol):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT close
                FROM prices
                WHERE symbol = %s
                ORDER BY ts DESC
                LIMIT 1;
                """,
                (symbol,),
            )
            row = cur.fetchone()
            return None if row is None else float(row[0])
    finally:
        conn.close()


def fetch_close_on_or_before(symbol, ts_cutoff):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT close
                FROM prices
                WHERE symbol = %s AND ts <= %s
                ORDER BY ts DESC
                LIMIT 1;
                """,
                (symbol, ts_cutoff),
            )
            row = cur.fetchone()
            return None if row is None else float(row[0])
    finally:
        conn.close()


def fetch_last_snapshot():
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT ts, equity
                FROM pnl
                ORDER BY ts DESC
                LIMIT 1;
                """
            )
            return cur.fetchone()
    finally:
        conn.close()


def upsert_snapshot(ts, equity, cash, unrealized, realized, daily_pnl):
    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO pnl (ts, equity, cash, unrealized_pnl, realized_pnl, daily_pnl)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (ts) DO UPDATE
                    SET equity = EXCLUDED.equity,
                        cash = EXCLUDED.cash,
                        unrealized_pnl = EXCLUDED.unrealized_pnl,
                        realized_pnl = EXCLUDED.realized_pnl,
                        daily_pnl = EXCLUDED.daily_pnl;
                    """,
                    (ts, equity, cash, unrealized, realized, daily_pnl),
                )
    finally:
        conn.close()


def compute_equity_and_daily_pnl():
    log_event(log, "pnl_start", run_id=RUN_ID, mode=MODE)

    try:
        positions = fetch_positions()

        now_ts = datetime.now(timezone.utc).replace(microsecond=0)
        today = datetime.now(timezone.utc).date()
        today_start_utc = datetime(today.year, today.month, today.day, tzinfo=timezone.utc)
        yesterday_start_utc = today_start_utc - timedelta(days=1)

        unrealized = 0.0
        daily = 0.0
        equity_positions = 0.0

        for symbol, qty, avg_cost in positions:
            qty_f = float(qty)
            avg_f = float(avg_cost)

            last_px = fetch_latest_close(symbol)
            if last_px is None:
                continue

            equity_positions += qty_f * last_px
            unrealized += qty_f * (last_px - avg_f)

            y_close = fetch_close_on_or_before(symbol, yesterday_start_utc)
            daily_component = 0.0
            if y_close is not None:
                daily_component = qty_f * (last_px - y_close)
                daily += daily_component

            log_event(
                log,
                "pnl_symbol_marked",
                run_id=RUN_ID,
                mode=MODE,
                symbol=symbol,
                qty=qty_f,
                last_px=last_px,
                y_close=y_close,
                daily_component=daily_component,
            )

        realized = 0.0
        # cash = 0.0
        # equity = equity_positions + cash
        acct = get_account(run_id=RUN_ID, mode=MODE)

        cash = float(acct.get("cash", 0.0))
        equity = float(acct.get("equity", 0.0))

        upsert_snapshot(
            ts=now_ts,
            equity=equity,
            cash=cash,
            unrealized=unrealized,
            realized=realized,
            daily_pnl=daily,
        )

        log_event(
            log,
            "pnl_snapshot_upserted",
            run_id=RUN_ID,
            mode=MODE,
            ts=now_ts.isoformat(),
            equity=equity,
            daily_pnl=daily,
            unrealized_pnl=unrealized,
            realized_pnl=realized,
            cash=cash,
        )

    except Exception as e:
        log_error(log, "pnl_failed", e, run_id=RUN_ID, mode=MODE)
        raise



if __name__ == "__main__":
    compute_equity_and_daily_pnl()
