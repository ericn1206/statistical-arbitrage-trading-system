"""
opps diagnostics only

Validates the system end-to-end: schema exists, prices/signals are fresh, client_order_id is unique, pnl snapshots exist, and kill switch env is readable.
>>> import subprocess, sys
>>> out = subprocess.check_output([sys.executable, "health_check.py"]).decode()
>>> "SYSTEM HEALTH" in out
True
"""

import os, sys
import psycopg2
from datetime import datetime, timezone, timedelta

def ok(msg): print(f"[OK] {msg}")
def fail(msg):
    print(f"[FAIL] {msg}")
    sys.exit(1)

DB = os.getenv("DATABASE_URL")
if not DB:
    fail("DATABASE_URL missing")

conn = psycopg2.connect(DB)
cur = conn.cursor()

tables = ["prices","pairs","signals","orders","trades","positions","pnl"]
cur.execute("SELECT tablename FROM pg_tables WHERE schemaname='public'")
existing = {r[0] for r in cur.fetchall()}
missing = [t for t in tables if t not in existing]
if missing:
    fail(f"Missing tables: {missing}")
ok("DB schema present")

cur.execute("SELECT COUNT(*), MAX(ts) FROM prices")
price_rows, latest_price_ts = cur.fetchone()
if price_rows == 0:
    fail("No price data ingested")
ok(f"Prices ingested ({price_rows} rows)")
if latest_price_ts < datetime.now(timezone.utc) - timedelta(days=3):
    fail("Price data is stale")
ok("Price data is fresh")

cur.execute("SELECT COUNT(*), MAX(ts) FROM signals")
signal_rows, latest_signal_ts = cur.fetchone()
if signal_rows == 0:
    fail("No signals generated")
ok(f"Signals generated ({signal_rows} rows)")
if latest_signal_ts < datetime.now(timezone.utc) - timedelta(days=3):
    fail("Signals are stale")
ok("Signals are fresh")

cur.execute("SELECT COUNT(*) FROM orders")
ok(f"Orders table reachable ({cur.fetchone()[0]} rows)")

cur.execute("""
    SELECT client_order_id
    FROM orders
    GROUP BY client_order_id
    HAVING COUNT(*) > 1
""")
if cur.fetchall():
    fail("Client order idempotency broken")
ok("Order idempotency intact")

cur.execute("SELECT COUNT(*) FROM trades")
ok(f"Trades table reachable ({cur.fetchone()[0]} rows)")

cur.execute("SELECT COUNT(*) FROM positions")
ok(f"Positions table reachable ({cur.fetchone()[0]} rows)")

cur.execute("SELECT COUNT(*) FROM pnl")
pnl_rows = cur.fetchone()[0]
if pnl_rows == 0:
    fail("No PnL snapshots written")
ok(f"PnL snapshots written ({pnl_rows} rows)")

if os.getenv("TRADING_ENABLED") is None:
    fail("TRADING_ENABLED env not readable")
ok("Kill switch env readable")

conn.close()

print("\n==============================")
print("SYSTEM HEALTH: ALL CHECKS PASSED!! we did ittttt")
print("==============================")
