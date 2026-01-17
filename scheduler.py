"""
not needed if run_jobs.py exists

ss10
suns batch ingestion on an APScheduler interval (dev-friendly “runs forever” loop).
"""

import time
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler

from batch_ingest import run_batch_ingestion


def ingest_job():
    """
    This function is what runs on a schedule.
    It wraps ingestion, so that it can log start/end and catch errors.
    """
    start = datetime.utcnow()
    print(f"[INGEST START] {start.isoformat()}")

    try:
        run_batch_ingestion()
        end = datetime.utcnow()
        print(f"[INGEST SUCCESS] {end.isoformat()} (duration: {end - start})")
    except Exception as e:
        end = datetime.utcnow()
        print(f"[INGEST FAIL] {end.isoformat()} (duration: {end - start}) error={e}")


if __name__ == "__main__":
    # this sets up a scheduler in the background
    scheduler = BackgroundScheduler()

    # run ingest_job every 10 minutes
    scheduler.add_job(ingest_job, "interval", minutes=10)

    # start scheduler
    scheduler.start()
    print("[SCHEDULER] Started. Ingestion will run every 10 minutes.")

    # keep the script alive forever
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("[SCHEDULER] Stopping...")
        scheduler.shutdown()
