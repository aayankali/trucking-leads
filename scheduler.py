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
        count = run_scraper()
        log.info("Scheduled run complete. New leads: %d", count)
    except Exception as e:
        log.error("Scraper job failed: %s", e)


if __name__ == "__main__":
    log.info("Scheduler started. Running scraper now, then every 24 hours.")
    job()
    schedule.every(24).hours.do(job)
    while True:
        schedule.run_pending()
        time.sleep(60)
