"""
ss27
deployment run mode
single-process “job runner” that stamps a run_id and repeatedly runs migrate/ingest/signals/exec/fills/pnl on configurable intervals (which are set in .env).
"""
import os
import time
import uuid
import subprocess
from logger import get_logger, log_event, log_error

log = get_logger("runner")
RUN_ID = os.getenv("RUN_ID", uuid.uuid4().hex[:12])
MODE = os.getenv("TRADING_MODE", "paper")

def every_seconds(name, seconds, last_run):
    now = time.time()
    if last_run.get(name) is None or now - last_run[name] >= seconds:
        last_run[name] = now
        return True
    return False

def run_script(path):
    env = os.environ.copy()
    env["RUN_ID"] = RUN_ID
    env["TRADING_MODE"] = MODE
    p = subprocess.run(["python", path], env=env, capture_output=True, text=True)
    if p.returncode != 0:
        raise RuntimeError(f"{path} failed: {p.stderr.strip()}")
    return p.stdout.strip()

def main():
    ingest_s = int(os.getenv("INGEST_EVERY_SECONDS", "900"))
    signal_s = int(os.getenv("SIGNALS_EVERY_SECONDS", "300"))
    exec_s = int(os.getenv("EXEC_EVERY_SECONDS", "300"))
    fills_s = int(os.getenv("FILLS_EVERY_SECONDS", "120"))
    pnl_s = int(os.getenv("PNL_EVERY_SECONDS", "300"))

    last = {}
    log_event(log, "runner_migrate_start", run_id=RUN_ID, mode=MODE)
    out = run_script("migrate.py")
    log_event(log, "runner_migrate_done", run_id=RUN_ID, mode=MODE, out=out)

    while True:
        try:
            if every_seconds("ingest", ingest_s, last):
                log_event(log, "job_start", run_id=RUN_ID, mode=MODE, job="ingest")
                out = run_script("batch_ingest.py")
                log_event(log, "job_done", run_id=RUN_ID, mode=MODE, job="ingest", out=out)

            if every_seconds("signals", signal_s, last):
                log_event(log, "job_start", run_id=RUN_ID, mode=MODE, job="signals")
                out = run_script("live_signal_job.py")
                log_event(log, "job_done", run_id=RUN_ID, mode=MODE, job="signals", out=out)

            if every_seconds("exec", exec_s, last):
                log_event(log, "job_start", run_id=RUN_ID, mode=MODE, job="exec")
                out = run_script("idempotent_execute.py")
                log_event(log, "job_done", run_id=RUN_ID, mode=MODE, job="exec", out=out)

            if every_seconds("fills", fills_s, last):
                log_event(log, "job_start", run_id=RUN_ID, mode=MODE, job="fills")
                out = run_script("sync_fills.py")
                log_event(log, "job_done", run_id=RUN_ID, mode=MODE, job="fills", out=out)

            if every_seconds("pnl", pnl_s, last):
                log_event(log, "job_start", run_id=RUN_ID, mode=MODE, job="pnl")
                out = run_script("compute_pnl.py")
                log_event(log, "job_done", run_id=RUN_ID, mode=MODE, job="pnl", out=out)

        except Exception as e:
            log_error(log, "runner_loop_error", e, run_id=RUN_ID, mode=MODE)

        time.sleep(1)

if __name__ == "__main__":
    main()
