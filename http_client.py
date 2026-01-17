"""
ss28
thin HTTP wrapper that throttles requests, retries on 429/5xx with backoff+jitter, and writes a dead-letter JSONL entry on final failure.
"""

import json
import os
import random
import time
from typing import Optional

import requests
from logger import get_logger, log_event, log_error
from dotenv import load_dotenv

load_dotenv(override=True)


log = get_logger("http")

_MIN_INTERVAL_S = float(os.getenv("HTTP_MIN_INTERVAL_S", "0.25"))
_last_ts = 0.0

BASE_URL = os.getenv("ALPACA_BASE_URL", "")
API_KEY = os.getenv("ALPACA_API_KEY", "")
API_SECRET = os.getenv("ALPACA_SECRET_KEY", "")

DEFAULT_HEADERS = {
    "APCA-API-KEY-ID": API_KEY,
    "APCA-API-SECRET-KEY": API_SECRET,
}

'''
DEBUG 401 ERROR code! see if the urls and keys match

import os

BASE_URL = os.getenv("ALPACA_BASE_URL", "")
API_KEY = os.getenv("ALPACA_API_KEY", "")
API_SECRET = os.getenv("ALPACA_SECRET_KEY", "")

def _mask(s: str) -> str:
    if not s:
        return "<EMPTY>"
    if len(s) <= 8:
        return f"{s[:2]}...{s[-2:]}"
    return f"{s[:4]}...{s[-4:]}"

print("[ALPACA CONFIG]",
      "base_url=", BASE_URL,
      "api_key=", _mask(API_KEY),
      "secret_present=", bool(API_SECRET))
'''
def _throttle():
    global _last_ts
    now = time.time()
    wait = _MIN_INTERVAL_S - (now - _last_ts)
    if wait > 0:
        time.sleep(wait)
    _last_ts = time.time()

def _dead_letter(event: str, payload: dict):
    path = os.getenv("DEAD_LETTER_PATH", "dead_letter.jsonl")
    with open(path, "a") as f:
        f.write(json.dumps({"event": event, **payload}, default=str) + "\n")

def request_json(
        
    method: str,
    url: str,
    *,
    headers: Optional[dict] = None,
    params: Optional[dict] = None,
    data: Optional[str] = None,
    timeout_s: float = 15.0,
    max_retries: int = 6,
    base_backoff_s: float = 0.5,
    run_id: str = "",
    mode: str = "",
    context: Optional[dict] = None,
):
    
    ctx = context or {}
    for attempt in range(max_retries + 1):
        try:
            _throttle()
            req_headers = dict(DEFAULT_HEADERS)
            if headers:
                req_headers.update(headers)
            # DEBUG (temporary)
            if url.startswith("https://paper-api.alpaca.markets"):
                print("[DEBUG HEADERS]", {
                    "APCA-API-KEY-ID": (req_headers.get("APCA-API-KEY-ID") or "<MISSING>")[:4] + "...",
                    "APCA-API-SECRET-KEY": "<PRESENT>" if req_headers.get("APCA-API-SECRET-KEY") else "<MISSING>",
                })

            if "/v2/orders" in url:
                print("[DEBUG URL]", url)
                print("[DEBUG KEY PREFIX]", (req_headers.get("APCA-API-KEY-ID") or "")[:8])


            resp = requests.request(
                method,
                url,
                headers=req_headers,
                params=params,
                data=data,
                timeout=timeout_s,
            )

            if resp.status_code in (429,) or 500 <= resp.status_code <= 599:
                raise requests.HTTPError(f"HTTP {resp.status_code}", response=resp)

            resp.raise_for_status()
            return resp.json()

        except Exception as e:
            status = None
            retry_after = None
            if isinstance(e, requests.HTTPError) and getattr(e, "response", None) is not None:
                status = e.response.status_code
                retry_after = e.response.headers.get("Retry-After")

            if attempt == max_retries:
                log_error(log, "http_dead_letter", e, run_id=run_id, mode=mode, url=url, method=method, status=status, **ctx)
                _dead_letter("http_dead_letter", {
                    "run_id": run_id,
                    "mode": mode,
                    "url": url,
                    "method": method,
                    "status": status,
                    "error": str(e),
                    "headers": headers,
                    "params": params,
                    "data": data,
                    "context": ctx,
                })
                raise

            if retry_after:
                sleep_s = float(retry_after)
            else:
                jitter = random.uniform(0.0, 0.25)
                sleep_s = min(30.0, base_backoff_s * (2 ** attempt) + jitter)

            log_event(
                log,
                "http_retry",
                run_id=run_id,
                mode=mode,
                url=url,
                method=method,
                status=status,
                attempt=attempt + 1,
                sleep_s=sleep_s,
                **ctx,
            )
            time.sleep(sleep_s)
