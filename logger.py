"""
ss26
JSON-structured logging helpers that standardize fields like event, ts, run_id, and mode across jobs for replay/debugging.
"""
import json
import logging
import os
import sys
from datetime import datetime, timezone

def get_logger(name: str = "statarb"):
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    log = logging.getLogger(name)
    if log.handlers:
        return log
    log.setLevel(level)
    h = logging.StreamHandler(sys.stdout)
    h.setLevel(level)
    log.addHandler(h)
    log.propagate = False
    return log

def log_event(log, event: str, **fields):
    payload = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "event": event,
        **fields,
    }
    log.info(json.dumps(payload, default=str))

def log_error(log, event: str, exc: Exception, **fields):
    payload = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "event": event,
        "error_type": type(exc).__name__,
        "error": str(exc),
        **fields,
    }
    log.error(json.dumps(payload, default=str))
