import psycopg2
from datetime import datetime, timezone

def test_prices_upsert_idempotency(db_url, test_symbol):
    conn = psycopg2.connect(db_url)
    try:
        ts = datetime(2025, 1, 3, tzinfo=timezone.utc)

        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO prices(symbol, ts, open, high, low, close, volume)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (symbol, ts) DO UPDATE
                    SET close = EXCLUDED.close
                """, (test_symbol, ts, 100.0, 101.0, 99.0, 100.0, 1000))

                cur.execute("""
                    INSERT INTO prices(symbol, ts, open, high, low, close, volume)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (symbol, ts) DO UPDATE
                    SET close = EXCLUDED.close
                """, (test_symbol, ts, 100.0, 101.0, 99.0, 100.0, 1000))

                cur.execute("SELECT COUNT(*) FROM prices WHERE symbol=%s", (test_symbol,))
                n = cur.fetchone()[0]
                assert n == 1
    finally:
        with conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM prices WHERE symbol=%s", (test_symbol,))
        conn.close()
