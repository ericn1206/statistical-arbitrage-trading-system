import os
import time
import psycopg2
from datetime import datetime, timezone, timedelta

def _wait_db(dsn, timeout_s=30):
    t0 = time.time()
    while time.time() - t0 < timeout_s:
        try:
            conn = psycopg2.connect(dsn)
            conn.close()
            return
        except Exception:
            time.sleep(1)
    raise RuntimeError("DB not ready")

def _setup_schema(conn):
    with conn:
        with conn.cursor() as cur:
            cur.execute("""
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
            """)
            cur.execute("""
            CREATE TABLE IF NOT EXISTS pairs(
              id SERIAL PRIMARY KEY,
              symbol_1 TEXT NOT NULL,
              symbol_2 TEXT NOT NULL,
              hedge_ratio DOUBLE PRECISION NOT NULL,
              enabled BOOLEAN NOT NULL DEFAULT TRUE
            );
            """)
            cur.execute("""
            CREATE TABLE IF NOT EXISTS signals(
              pair_id INT NOT NULL REFERENCES pairs(id),
              ts TIMESTAMPTZ NOT NULL,
              zscore DOUBLE PRECISION NOT NULL,
              action TEXT NOT NULL,
              run_id TEXT NOT NULL,
              UNIQUE(pair_id, ts, run_id)
            );
            """)
            cur.execute("""
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
            """)

def _insert_prices(conn, symbol, rows):
    with conn:
        with conn.cursor() as cur:
            for ts, px in rows:
                cur.execute("""
                INSERT INTO prices(symbol, ts, open, high, low, close, volume)
                VALUES (%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (symbol, ts) DO UPDATE SET close=EXCLUDED.close;
                """, (symbol, ts, px, px, px, px, 1000))

def _make_pair(conn, a, b, hedge=1.0):
    with conn:
        with conn.cursor() as cur:
            cur.execute("""
            INSERT INTO pairs(symbol_1, symbol_2, hedge_ratio, enabled)
            VALUES (%s,%s,%s,TRUE)
            RETURNING id;
            """, (a, b, hedge))
            return cur.fetchone()[0]

def _signal_job(conn, pair_id, ts, z, action, run_id):
    with conn:
        with conn.cursor() as cur:
            cur.execute("""
            INSERT INTO signals(pair_id, ts, zscore, action, run_id)
            VALUES (%s,%s,%s,%s,%s)
            ON CONFLICT DO NOTHING;
            """, (pair_id, ts, z, action, run_id))

def _execute_job_killswitch(conn, pair_id, ts, action, symbol, qty):
    client_order_id = f"itest_pair{pair_id}_{ts.strftime('%Y%m%dT%H%M%S')}_{action}"[:48]
    with conn:
        with conn.cursor() as cur:
            cur.execute("SELECT client_order_id FROM orders WHERE client_order_id=%s", (client_order_id,))
            if cur.fetchone():
                return client_order_id
            cur.execute("""
            INSERT INTO orders(client_order_id, symbol, side, qty, order_type, time_in_force, status)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (client_order_id) DO NOTHING;
            """, (client_order_id, symbol, "buy", qty, "market", "day", "KILL_SWITCH_BLOCKED"))
    return client_order_id

def test_integration_e2e_postgres_docker():
    dsn = os.getenv("INTEGRATION_DB_URL", "postgresql://statarb:statarb_password@localhost:55432/statarb_test_db")
    _wait_db(dsn)

    conn = psycopg2.connect(dsn)
    try:
        _setup_schema(conn)

        base = datetime(2025, 1, 1, tzinfo=timezone.utc)
        a = [(base + timedelta(days=i), 100 + i) for i in range(5)]
        b = [(base + timedelta(days=i), 100 + i) for i in range(5)]

        _insert_prices(conn, "AAA", a)
        _insert_prices(conn, "BBB", b)

        pair_id = _make_pair(conn, "AAA", "BBB", 1.0)

        run_id = "itest_run"
        sig_ts = a[-1][0]
        _signal_job(conn, pair_id, sig_ts, z=2.5, action="ENTER_LONG", run_id=run_id)

        client_id = _execute_job_killswitch(conn, pair_id, sig_ts, "ENTER_LONG", "AAA", 1)

        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM prices WHERE symbol IN ('AAA','BBB');")
            assert cur.fetchone()[0] == 10

            cur.execute("SELECT action FROM signals WHERE pair_id=%s;", (pair_id,))
            assert cur.fetchone()[0] == "ENTER_LONG"

            cur.execute("SELECT status FROM orders WHERE client_order_id=%s;", (client_id,))
            assert cur.fetchone()[0] == "KILL_SWITCH_BLOCKED"

    finally:
        conn.close()
