"""
ss16
Turns the latest signal into an Alpaca order using a deterministic client_order_id. re-runs fetch & upsert the existing order instead of duplicating.
"""
import os
import json
import uuid
from datetime import timezone

import psycopg2
import requests
from dotenv import load_dotenv

from logger import get_logger, log_event, log_error
from risk import risk_check
import risk_config as rc

load_dotenv(override=True)

DATABASE_URL = os.getenv("DATABASE_URL")
ALPACA_KEY = os.getenv("ALPACA_API_KEY")
ALPACA_SECRET = os.getenv("ALPACA_SECRET_KEY")
ALPACA_BASE_URL = os.getenv("ALPACA_BASE_URL", "https://paper-api.alpaca.markets")

log = get_logger("exec")
RUN_ID = os.getenv("RUN_ID", uuid.uuid4().hex[:12])
MODE = os.getenv("TRADING_MODE", "paper").strip().lower()

def env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on")

TRADING_ENABLED = env_bool("TRADING_ENABLED", True)

def get_conn():
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL missing")
    return psycopg2.connect(DATABASE_URL)

def alpaca_headers():
    if not ALPACA_KEY or not ALPACA_SECRET:
        raise ValueError("Alpaca keys missing (ALPACA_API_KEY / ALPACA_SECRET_KEY)")
    return {
        "APCA-API-KEY-ID": ALPACA_KEY,
        "APCA-API-SECRET-KEY": ALPACA_SECRET,
        "Content-Type": "application/json",
    }

def fetch_order_by_alpaca_id(alpaca_order_id: str):
    url = f"{ALPACA_BASE_URL}/v2/orders/{alpaca_order_id}"
    resp = requests.get(url, headers=alpaca_headers())
    resp.raise_for_status()
    return resp.json()

def fetch_order_by_client_id(client_order_id: str):
    url = f"{ALPACA_BASE_URL}/v2/orders:by_client_order_id"
    resp = requests.get(
        url,
        headers=alpaca_headers(),
        params={"client_order_id": client_order_id},
    )
    resp.raise_for_status()
    return resp.json()


def db_find_by_client_id(client_order_id: str):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT alpaca_order_id, status
                FROM orders
                WHERE client_order_id = %s;
                """,
                (client_order_id,),
            )
            return cur.fetchone()
    finally:
        conn.close()

def db_upsert_order(order_json: dict):
    """
    Store what Alpaca actually returned.
    (Do not overwrite side/symbol/qty with guessed values.)
    """
    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO orders (
                        alpaca_order_id,
                        client_order_id,
                        symbol,
                        side,
                        qty,
                        order_type,
                        time_in_force,
                        status,
                        submitted_at,
                        raw
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                    ON CONFLICT (client_order_id) DO UPDATE
                    SET alpaca_order_id = EXCLUDED.alpaca_order_id,
                        status = EXCLUDED.status,
                        submitted_at = EXCLUDED.submitted_at,
                        raw = EXCLUDED.raw;
                    """,
                    (
                        order_json.get("id"),
                        order_json.get("client_order_id"),
                        order_json.get("symbol"),
                        order_json.get("side"),
                        order_json.get("qty"),
                        order_json.get("type"),
                        order_json.get("time_in_force"),
                        order_json.get("status"),
                        order_json.get("submitted_at"),
                        json.dumps(order_json),
                    ),
                )
    finally:
        conn.close()

def submit_order(symbol, qty, side, order_type, tif, client_order_id):
    url = f"{ALPACA_BASE_URL}/v2/orders"
    payload = {
        "symbol": symbol,
        "qty": str(qty),
        "side": side,
        "type": order_type,
        "time_in_force": tif,
        "client_order_id": client_order_id,
    }

    resp = requests.post(url, headers=alpaca_headers(), data=json.dumps(payload))

    # if Alpaca already has this client_order_id, recover instead of failing. hate error codes.. took me so long
    if resp.status_code == 422:
        try:
            body = resp.json()
        except Exception:
            body = {"message": resp.text}

        msg = str(body.get("message", ""))
        if "client_order_id" in msg and "unique" in msg:
            existing = fetch_order_by_client_id(client_order_id)
            return existing

        print("[ORDER ERROR]", resp.status_code, body)
        resp.raise_for_status()

    if resp.status_code >= 400:
        print("[ORDER ERROR]", resp.status_code, resp.text)
        resp.raise_for_status()

    return resp.json()


def build_client_order_id(pair_id: int, ts_iso: str, action: str) -> str:
    ts_str = ts_iso.replace(":", "").replace("-", "")
    suffix = os.getenv("CLIENT_ID_SUFFIX", "")

    return f"statarb_pair{pair_id}_{ts_str}_{action}"[:48]

def action_to_side(action: str):
    if action == "ENTER_LONG":
        return "buy"
    if action == "ENTER_SHORT":
        return "sell"
    return None  # HOLD / EXIT / unknown

def execute_decision(pair_id: int, ts_iso: str, action: str, symbol: str, qty: int, orders_submitted_in_run: int):
    client_order_id = build_client_order_id(pair_id, ts_iso, action)

    log_event(
        log, "exec_decision_start",
        run_id=RUN_ID, mode=MODE,
        pair_id=pair_id, action=action, ts=ts_iso,
        client_order_id=client_order_id, symbol=symbol, qty=qty
    )

    existing = db_find_by_client_id(client_order_id)
    if existing:
        alpaca_order_id, _status = existing
        log_event(
            log, "exec_fetch_existing",
            run_id=RUN_ID, mode=MODE,
            pair_id=pair_id, client_order_id=client_order_id,
            alpaca_order_id=alpaca_order_id
        )
        latest = fetch_order_by_alpaca_id(alpaca_order_id)
        db_upsert_order(latest)
        return {"mode": "fetch_existing", "alpaca_order_id": alpaca_order_id}

    # decide side from action
    side = action_to_side(action)
    if side is None:
        return {"mode": "no_action", "client_order_id": client_order_id, "action": action}

    allowed, reasons = risk_check(
        symbols_for_order=[symbol],
        max_gross_exposure=rc.MAX_GROSS_EXPOSURE,
        max_position_value_per_symbol=rc.MAX_POSITION_VALUE_PER_SYMBOL,
        max_orders_per_run=rc.MAX_ORDERS_PER_RUN,
        stale_seconds=rc.DATA_STALE_SECONDS,
        orders_submitted_in_run=orders_submitted_in_run,
    )

    if not allowed:
        log_event(
            log, "exec_risk_block",
            run_id=RUN_ID, mode=MODE,
            pair_id=pair_id, client_order_id=client_order_id,
            reasons=reasons
        )
        return {"mode": "blocked", "client_order_id": client_order_id, "reasons": reasons}

    if not TRADING_ENABLED:
        log_event(
            log, "exec_kill_switch_block",
            run_id=RUN_ID, mode=MODE,
            pair_id=pair_id, client_order_id=client_order_id
        )
        return {"mode": "trading_disabled", "client_order_id": client_order_id}

    try:
        created = submit_order(
            symbol=symbol,
            qty=qty,
            side=side,
            order_type="market",
            tif="day",
            client_order_id=client_order_id,
        )
        log_event(
            log, "exec_submitted",
            run_id=RUN_ID, mode=MODE,
            pair_id=pair_id, client_order_id=client_order_id,
            alpaca_order_id=created.get("id"),
            side=side,
        )
        db_upsert_order(created)
        return {"mode": "submitted_new", "alpaca_order_id": created.get("id")}
    except Exception as e:
        log_error(
            log, "exec_submit_failed", e,
            run_id=RUN_ID, mode=MODE,
            pair_id=pair_id, client_order_id=client_order_id
        )
        raise

if __name__ == "__main__":
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT s.pair_id, s.ts, s.action
                FROM signals s
                ORDER BY s.ts DESC
                LIMIT 1;
                """
            )
            row = cur.fetchone()
            if not row:
                print("[EXEC] no signals found")
                raise SystemExit(0)

            pair_id, ts, action = row

        result = execute_decision(
            pair_id=int(pair_id),
            ts_iso=ts.isoformat(),
            action=action,
            symbol="AAPL",   # still single-leg for now
            qty=1,
            orders_submitted_in_run=0,
        )
        print(result)
    finally:
        conn.close()
