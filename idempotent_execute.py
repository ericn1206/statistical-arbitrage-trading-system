"""
ss16 (updated)
Executes the latest-per-pair signals by submitting two-leg Alpaca orders using deterministic client_order_id(s).
- ENTER_LONG  > buy symbol_1, sell symbol_2
- ENTER_SHORT > sell symbol_1, buy symbol_2
- EXIT/HOLD   > no-op for now (you can wire EXIT to close positions later)
Idempotent: each leg has its own client_order_id; reruns fetch & upsert instead of duplicating.

FIXES INCLUDED:
1) client_order_id collisions fixed (no more base[:48] truncation collisions) via SHA1 hash suffix.
2) open orders fetched once per run (instead of per-symbol per-pair) to avoid rate limits + weird behavior.
"""

import os
import sys
import json
import uuid
import hashlib
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
    Store what Alpaca returned.
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

    # If Alpaca already has this client_order_id, recover instead of failing.
    if resp.status_code == 422:
        try:
            body = resp.json()
        except Exception:
            body = {"message": resp.text}

        msg = str(body.get("message", ""))
        if "client_order_id" in msg and "unique" in msg:
            return fetch_order_by_client_id(client_order_id)

        print("[ORDER ERROR]", resp.status_code, body, file=sys.stderr, flush=True)
        resp.raise_for_status()

    # Handle wash-trade guardrail gracefully (do NOT crash the whole job)
    if resp.status_code == 403:
        try:
            body = resp.json()
        except Exception:
            body = {"message": resp.text}

        if body.get("code") == 40310000 and body.get("existing_order_id"):
            existing_id = body["existing_order_id"]
            existing_order = fetch_order_by_alpaca_id(existing_id)
            db_upsert_order(existing_order)
            log_event(
                log,
                "exec_leg_blocked_wash_trade",
                run_id=RUN_ID,
                mode=MODE,
                symbol=symbol,
                side=side,
                client_order_id=client_order_id,
                existing_order_id=existing_id,
                reject_reason=body.get("reject_reason"),
                message=body.get("message"),
            )
            # Return the existing order so caller can treat as "blocked" or "existing"
            return existing_order

        print("[ORDER ERROR]", resp.status_code, body, file=sys.stderr, flush=True)
        resp.raise_for_status()

    if resp.status_code >= 400:
        try:
            body = resp.json()
        except Exception:
            body = resp.text
        print("[ORDER ERROR]", resp.status_code, body, file=sys.stderr, flush=True)
        resp.raise_for_status()

    return resp.json()


def build_client_order_id(pair_id: int, ts_iso: str, action: str, leg: str, symbol: str) -> str:
    """
    Deterministic per-leg id, collision-resistant:
      statarb_p{pair}_{YYYYMMDDHHMMSS}_{action}_{leg}_{symbol}_{hash}

    The old base[:48] truncation can collide (L1/L2 or different symbols) because
    timestamp dominates the prefix. This version keeps uniqueness via hash suffix.
    """
    ts_digits = "".join(ch for ch in ts_iso if ch.isdigit())[:14]  # YYYYMMDDHHMMSS
    core = f"p{pair_id}_{ts_digits}_{action}_{leg}_{symbol}"
    h = hashlib.sha1(core.encode("utf-8")).hexdigest()[:10]
    return f"statarb_{core}_{h}"[:48]


def pair_action_to_legs(action: str):
    """
    Return (side_leg1, side_leg2) for (symbol_1, symbol_2), or None if no-operation.
    """
    if action == "ENTER_LONG":
        return ("buy", "sell")
    if action == "ENTER_SHORT":
        return ("sell", "buy")
    return None  # HOLD / EXIT / unknown


def fetch_open_order_symbols() -> set[str]:
    """
    Fetch open orders ONCE per run (much cheaper than per-symbol checks).
    """
    url = f"{ALPACA_BASE_URL}/v2/orders"
    resp = requests.get(url, headers=alpaca_headers(), params={"status": "open", "limit": 500})
    resp.raise_for_status()
    orders = resp.json() or []
    return {o.get("symbol") for o in orders if o.get("symbol")}

def execute_leg(
    pair_id: int,
    ts_iso: str,
    action: str,
    leg: str,
    symbol: str,
    qty: int,
    side: str,
    orders_submitted_in_run: int,
):
    """
    Execute one leg idempotently.

    Behaviors:
    - If this leg's client_order_id already exists in *our DB*, fetch latest from Alpaca and upsert.
    - If TRADING_ENABLED is off, no-op.
    - If Alpaca rejects with wash-trade protection (40310000), we fetch the existing conflicting order,
      upsert it, and return mode=blocked_existing_open_order instead of crashing the run.
    - Otherwise, submit a new market order, upsert, and return submitted_new.
    """
    client_order_id = build_client_order_id(pair_id, ts_iso, action, leg, symbol)

    log_event(
        log,
        "exec_leg_start",
        run_id=RUN_ID,
        mode=MODE,
        pair_id=pair_id,
        action=action,
        ts=ts_iso,
        leg=leg,
        client_order_id=client_order_id,
        symbol=symbol,
        side=side,
        qty=qty,
    )

    # 1) DB-level idempotency first: if we already recorded this leg, just refresh it.
    existing = db_find_by_client_id(client_order_id)
    if existing:
        alpaca_order_id, _status = existing
        log_event(
            log,
            "exec_leg_fetch_existing",
            run_id=RUN_ID,
            mode=MODE,
            pair_id=pair_id,
            leg=leg,
            client_order_id=client_order_id,
            alpaca_order_id=alpaca_order_id,
        )
        latest = fetch_order_by_alpaca_id(alpaca_order_id)
        db_upsert_order(latest)
        return {
            "mode": "fetch_existing",
            "alpaca_order_id": alpaca_order_id,
            "client_order_id": client_order_id,
            "status": (latest.get("status") or "").lower(),
            "filled_qty": float(latest.get("filled_qty", "0") or 0),
            "side": latest.get("side"),
            "symbol": latest.get("symbol"),
        }

    # 2) Kill switch
    if not TRADING_ENABLED:
        log_event(
            log,
            "exec_leg_kill_switch_block",
            run_id=RUN_ID,
            mode=MODE,
            pair_id=pair_id,
            leg=leg,
            client_order_id=client_order_id,
            symbol=symbol,
        )
        return {"mode": "trading_disabled", "client_order_id": client_order_id}

    # 3) Submit (submit_order() should already handle 422 uniqueness and 403 wash-trade gracefully
    # by returning an existing order JSON instead of throwing, when possible).
    created = submit_order(
        symbol=symbol,
        qty=qty,
        side=side,
        order_type="market",
        tif="day",
        client_order_id=client_order_id,
    )

    # 4) If Alpaca returned an EXISTING conflicting order (wash-trade / opposite-side open order),
    # it will not match our client_order_id. Treat that as blocked, not submitted.
    if (created.get("client_order_id") or "") != client_order_id:
        db_upsert_order(created)
        log_event(
            log,
            "exec_leg_blocked_existing_open_order",
            run_id=RUN_ID,
            mode=MODE,
            pair_id=pair_id,
            leg=leg,
            client_order_id=client_order_id,
            symbol=symbol,
            side=side,
            existing_alpaca_order_id=created.get("id"),
            existing_status=created.get("status"),
            existing_client_order_id=created.get("client_order_id"),
        )
        return {
            "mode": "blocked_existing_open_order",
            "alpaca_order_id": created.get("id"),
            "client_order_id": client_order_id,  # what we tried to use
            "status": (created.get("status") or "").lower(),
            "filled_qty": float(created.get("filled_qty", "0") or 0),
            "side": created.get("side"),
            "symbol": created.get("symbol"),
            "existing_client_order_id": created.get("client_order_id"),
        }

    # 5) Normal success path: submitted a brand new order with our client_order_id
    log_event(
        log,
        "exec_leg_submitted",
        run_id=RUN_ID,
        mode=MODE,
        pair_id=pair_id,
        leg=leg,
        client_order_id=client_order_id,
        alpaca_order_id=created.get("id"),
        side=created.get("side"),
        symbol=created.get("symbol"),
        status=created.get("status"),
    )
    db_upsert_order(created)

    return {
        "mode": "submitted_new",
        "alpaca_order_id": created.get("id"),
        "client_order_id": client_order_id,
        "status": (created.get("status") or "").lower(),
        "filled_qty": float(created.get("filled_qty", "0") or 0),
        "side": created.get("side"),
        "symbol": created.get("symbol"),
    }



def latest_price(symbol: str) -> float:
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
            if not row:
                raise ValueError(f"No price for {symbol}")
            return float(row[0])
    finally:
        conn.close()


def safe_float(x, default=0.0) -> float:
    try:
        return float(x)
    except Exception:
        return float(default)


def opposite_side(side: str) -> str:
    return "sell" if side == "buy" else "buy"


def is_order_done(status: str) -> bool:
    # Alpaca statuses vary, these are the common terminal ones.
    return status in ("filled", "canceled", "rejected", "expired", "done_for_day")


def flatten_if_filled(alpaca_order_id: str):
    """
    If an order has any filled_qty, submit an opposite-side market order
    to flatten that filled amount immediately.
    """
    o = fetch_order_by_alpaca_id(alpaca_order_id)
    db_upsert_order(o)

    status = (o.get("status") or "").lower()
    filled_qty = safe_float(o.get("filled_qty", 0))
    symbol = o.get("symbol")
    side = o.get("side")

    if filled_qty <= 0:
        return {"flattened": False, "reason": "no_fills", "status": status}

    flatten_side = opposite_side(side)

    # Unique deterministic-ish client id for flatten action
    ts_raw = (o.get("updated_at") or o.get("submitted_at") or "")
    ts_digits = "".join(ch for ch in ts_raw if ch.isdigit())[:14] or "00000000000000"
    core = f"flatten_{alpaca_order_id[:10]}_{ts_digits}_{symbol}_{flatten_side}_{int(round(filled_qty))}"
    h = hashlib.sha1(core.encode("utf-8")).hexdigest()[:10]
    flatten_client_id = f"statarb_{core}_{h}"[:48]

    created = submit_order(
        symbol=symbol,
        qty=int(round(filled_qty)),
        side=flatten_side,
        order_type="market",
        tif="day",
        client_order_id=flatten_client_id,
    )
    db_upsert_order(created)

    log_event(
        log,
        "exec_flatten_submitted",
        run_id=RUN_ID,
        mode=MODE,
        alpaca_order_id=alpaca_order_id,
        symbol=symbol,
        filled_qty=filled_qty,
        flatten_side=flatten_side,
        flatten_order_id=created.get("id"),
    )

    return {
        "flattened": True,
        "symbol": symbol,
        "filled_qty": filled_qty,
        "flatten_side": flatten_side,
        "flatten_order_id": created.get("id"),
    }


def cancel_order(alpaca_order_id: str):
    url = f"{ALPACA_BASE_URL}/v2/orders/{alpaca_order_id}"
    resp = requests.delete(url, headers=alpaca_headers())
    # Alpaca returns 204 on success; if already filled/canceled, ignore
    if resp.status_code not in (204, 404):
        resp.raise_for_status()


def execute_pair_signal(
    pair_id: int,
    ts_iso: str,
    action: str,
    symbol_1: str,
    symbol_2: str,
    hedge_ratio: float,
    orders_submitted_in_run: int,
    open_symbols_with_orders: set[str],
):
    # Must have room for both legs (2 orders) or we skip the pair entirely
    if orders_submitted_in_run + 2 > rc.MAX_ORDERS_PER_RUN:
        return {"mode": "blocked_pair", "reason": "max_orders_pair"}

    legs = pair_action_to_legs(action)
    if legs is None:
        return {"mode": "no_action", "pair_id": pair_id, "action": action}

    # --- PAIR-ATOMIC GATE: all-or-nothing ---
    # 1) Don't trade if either symbol already has open orders
    if symbol_1 in open_symbols_with_orders or symbol_2 in open_symbols_with_orders:
        log_event(
            log,
            "exec_pair_block_open_orders",
            run_id=RUN_ID,
            mode=MODE,
            pair_id=pair_id,
            action=action,
            symbol_1=symbol_1,
            symbol_2=symbol_2,
        )
        return {"mode": "blocked_pair", "reason": "open_orders_exist", "pair_id": pair_id}

    # 2) Ensure we have room for *two* orders in this run
    if orders_submitted_in_run + 2 > rc.MAX_ORDERS_PER_RUN:
        log_event(
            log,
            "exec_pair_block_max_orders",
            run_id=RUN_ID,
            mode=MODE,
            pair_id=pair_id,
            action=action,
            orders_submitted_in_run=orders_submitted_in_run,
            max_orders_per_run=rc.MAX_ORDERS_PER_RUN,
        )
        return {"mode": "blocked_pair", "reason": "max_orders_pair", "pair_id": pair_id}

    # 3) One risk check for both symbols (pair-level)
    allowed, reasons = risk_check(
        symbols_for_order=[symbol_1, symbol_2],
        max_gross_exposure=rc.MAX_GROSS_EXPOSURE,
        max_position_value_per_symbol=rc.MAX_POSITION_VALUE_PER_SYMBOL,
        max_orders_per_run=rc.MAX_ORDERS_PER_RUN,
        stale_seconds=rc.DATA_STALE_SECONDS,
        orders_submitted_in_run=orders_submitted_in_run,
    )
    if not allowed:
        log_event(
            log,
            "exec_pair_risk_block",
            run_id=RUN_ID,
            mode=MODE,
            pair_id=pair_id,
            action=action,
            reasons=reasons,
        )
        return {"mode": "blocked_pair", "reason": "risk", "pair_id": pair_id, "reasons": reasons}

    if not TRADING_ENABLED:
        return {"mode": "blocked_pair", "reason": "trading_disabled", "pair_id": pair_id}
    # --- END PAIR-ATOMIC GATE ---

    side1, side2 = legs

    # hedge_ratio sizing (linear combination)
    BASE_NOTIONAL = 1000.0  # dollars per pair (tune later)

    price1 = latest_price(symbol_1)
    price2 = latest_price(symbol_2)

    qty1 = BASE_NOTIONAL / price1
    qty2 = abs(hedge_ratio) * BASE_NOTIONAL / price2

    qty1 = max(1, int(qty1))
    qty2 = max(1, int(qty2))

    r1 = execute_leg(
        pair_id=pair_id,
        ts_iso=ts_iso,
        action=action,
        leg="L1",
        symbol=symbol_1,
        qty=qty1,
        side=side1,
        orders_submitted_in_run=orders_submitted_in_run,
    )
    if r1.get("mode") == "submitted_new":
        orders_submitted_in_run += 1

    try:
        r2 = execute_leg(
            pair_id=pair_id,
            ts_iso=ts_iso,
            action=action,
            leg="L2",
            symbol=symbol_2,
            qty=qty2,
            side=side2,
            orders_submitted_in_run=orders_submitted_in_run,
        )
    except Exception as e:
        repair = None

        # If leg1 was submitted, try cancel; if already filled/partial, flatten what filled.
        if r1.get("mode") == "submitted_new" and r1.get("alpaca_order_id"):
            try:
                cancel_order(r1["alpaca_order_id"])
            except Exception:
                pass

            try:
                repair = flatten_if_filled(r1["alpaca_order_id"])
            except Exception as e2:
                log_error(log, "exec_repair_failed", e2, run_id=RUN_ID, mode=MODE, pair_id=pair_id)

        log_error(
            log,
            "exec_pair_leg2_failed",
            e,
            run_id=RUN_ID,
            mode=MODE,
            pair_id=pair_id,
            repair=repair,
        )
        raise

    if r2.get("mode") == "submitted_new":
        orders_submitted_in_run += 1

    return {
        "mode": "pair_executed",
        "pair_id": pair_id,
        "action": action,
        "leg1": r1,
        "leg2": r2,
        "orders_submitted_in_run": orders_submitted_in_run,
    }


if __name__ == "__main__":
    conn = get_conn()
    orders_submitted = 0
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT ON (s.pair_id)
                s.pair_id, s.ts, s.action,
                p.symbol_1, p.symbol_2, p.hedge_ratio
                FROM signals s
                JOIN pairs p ON p.id = s.pair_id
                WHERE p.enabled = true
                ORDER BY s.pair_id, s.ts DESC, s.run_id DESC;
                """
            )
            rows = cur.fetchall()

        if not rows:
            print("[EXEC] no signals found")
            raise SystemExit(0)

        # Only attempt ENTER actions for now (avoid EXIT/HOLD until you wire closes)
        rows = [r for r in rows if r[2] in ("ENTER_LONG", "ENTER_SHORT")]
        if not rows:
            print("[EXEC] no tradable signals (ENTER_LONG/ENTER_SHORT) found")
            raise SystemExit(0)

        # Fetch open orders ONCE per run
        try:
            open_symbols = fetch_open_order_symbols()
        except Exception as e:
            # If this fails, be safe and block trading (prevents accidental duplicate exposure)
            log_error(log, "exec_fetch_open_orders_failed", e, run_id=RUN_ID, mode=MODE)
            open_symbols = set()

        for pair_id, ts, action, sym1, sym2, hedge_ratio in rows:
            out = execute_pair_signal(
                pair_id=int(pair_id),
                ts_iso=ts.isoformat(),
                action=str(action),
                symbol_1=str(sym1),
                symbol_2=str(sym2),
                hedge_ratio=float(hedge_ratio),
                orders_submitted_in_run=orders_submitted,
                open_symbols_with_orders=open_symbols,
            )
            print(out)

            orders_submitted = out.get("orders_submitted_in_run", orders_submitted)

    finally:
        conn.close()
