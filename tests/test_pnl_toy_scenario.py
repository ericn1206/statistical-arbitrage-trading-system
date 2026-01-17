import os
import psycopg2
from datetime import datetime, timezone, timedelta, date
import importlib.util

def _load_compute_pnl():
    root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    path = os.path.join(root, "compute_pnl.py")
    spec = importlib.util.spec_from_file_location("compute_pnl_local", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod

compute_pnl = _load_compute_pnl()

def test_pnl_toy_scenario(db_url, test_symbol):
    conn = psycopg2.connect(db_url)

    d = datetime.now(timezone.utc).date()
    today_start_utc = datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
    yday_cutoff = today_start_utc - timedelta(days=1)

    yday_ts = yday_cutoff - timedelta(seconds=1)
    today_ts = today_start_utc + timedelta(hours=12)

    qty = 10.0
    avg_cost = 100.0
    y_close = 105.0
    last_close = 110.0

    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM pnl")
                cur.execute("DELETE FROM positions WHERE symbol=%s", (test_symbol,))
                cur.execute("DELETE FROM prices WHERE symbol=%s", (test_symbol,))

                cur.execute("""
                    INSERT INTO prices(symbol, ts, open, high, low, close, volume)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (symbol, ts) DO UPDATE SET close=EXCLUDED.close
                """, (test_symbol, yday_ts, y_close, y_close, y_close, y_close, 1000))

                cur.execute("""
                    INSERT INTO prices(symbol, ts, open, high, low, close, volume)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (symbol, ts) DO UPDATE SET close=EXCLUDED.close
                """, (test_symbol, today_ts, last_close, last_close, last_close, last_close, 1000))

                cur.execute("""
                    INSERT INTO positions(symbol, qty, avg_cost)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (symbol) DO UPDATE SET qty=EXCLUDED.qty, avg_cost=EXCLUDED.avg_cost
                """, (test_symbol, qty, avg_cost))

                cur.execute("""
                    SELECT close
                    FROM prices
                    WHERE symbol=%s AND ts <= %s
                    ORDER BY ts DESC
                    LIMIT 1
                """, (test_symbol, yday_cutoff))
                row = cur.fetchone()
                assert row is not None
                y_close_used = float(row[0])

        compute_pnl.DATABASE_URL = db_url
        compute_pnl.compute_equity_and_daily_pnl()

        with conn.cursor() as cur:
            cur.execute("""
                SELECT equity, daily_pnl, unrealized_pnl, realized_pnl, cash
                FROM pnl
                ORDER BY ts DESC
                LIMIT 1
            """)
            row = cur.fetchone()
            assert row is not None
            equity, daily, unreal, realized, cash = map(float, row)

        expected_equity = qty * last_close
        expected_unreal = qty * (last_close - avg_cost)
        expected_daily = qty * (last_close - y_close_used)

        assert abs(equity - expected_equity) < 1e-6
        assert abs(unreal - expected_unreal) < 1e-6
        assert abs(daily - expected_daily) < 1e-6
        assert abs(realized - 0.0) < 1e-6
        assert abs(cash - 0.0) < 1e-6

    finally:
        with conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM positions WHERE symbol=%s", (test_symbol,))
                cur.execute("DELETE FROM prices WHERE symbol=%s", (test_symbol,))
        conn.close()
