"""
Scheduler — runs the FMCSA scraper every 24 hours continuously.
Railway keeps this process alive 24/7.
"""

import schedule
import time
import logging
from scraper import run_scraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)


def job():
    try:
        result = run_scraper()
        # Safety: run_scraper() should return an int but guard against list/None
        count = result if isinstance(result, int) else (len(result) if isinstance(result, list) else 0)
        log.info("Scheduled run complete. New leads: %d", count)
    except Exception:
        log.exception("Scraper job failed")


if __name__ == "__main__":
    log.info("Scheduler started. Running scraper now, then every 24 hours.")
    job()
    schedule.every(24).hours.do(job)
    while True:
        schedule.run_pending()
        time.sleep(60)
