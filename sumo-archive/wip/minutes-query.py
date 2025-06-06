#!/usr/bin/env python3
import os
import sys
import time
import gzip
import json
import argparse
import logging
import requests

from datetime import datetime, timedelta, timezone
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Semaphore
from collections import defaultdict

# === Constants ===
PROTOCOL = "https"
HOST = os.getenv("SUMO_HOST", "api.us2.sumologic.com")
SUMO_API_URL = f"{PROTOCOL}://{HOST}/api/v1/search/jobs"
SEARCH_JOB_RESULTS_LIMIT = 10000   # max messages per “get messages” call
API_RATE_LIMIT_DELAY = 2           # seconds to sleep on 429

# Credentials must be set as environment variables
API_ACCESS_ID = os.getenv("SUMO_ACCESS_ID")
API_ACCESS_KEY = os.getenv("SUMO_ACCESS_KEY")


def configure_logging(logfile, log_level):
    loglevel = getattr(logging, log_level.upper(), logging.INFO)
    logging.basicConfig(
        level=loglevel,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[logging.FileHandler(logfile), logging.StreamHandler()]
    )
    logging.info("Logging initialized.")

if not API_ACCESS_ID or not API_ACCESS_KEY:
    print("ERROR: Please set SUMO_ACCESS_ID and SUMO_ACCESS_KEY environment variables.", file=sys.stderr)
    sys.exit(1)


def parse_iso_minute(ts: str) -> datetime:
    """
    Parse an ISO‐8601 timestamp and floor to the whole minute (drop seconds).
    Returns a UTC datetime with second=0, microsecond=0.
    """
    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    dt = dt.astimezone(timezone.utc)
    return dt.replace(second=0, microsecond=0)


def get_minute_filepath(base_dir: str, dt: datetime) -> Path:
    """
    Compute the full path to the <YYYY>/<MM>/<DD>/<HH>/<MM>.json.gz file for a given minute.
    """
    year = dt.strftime("%Y")
    month = dt.strftime("%m")
    day = dt.strftime("%d")
    hour = dt.strftime("%H")
    minute = dt.strftime("%M")
    return Path(base_dir) / year / month / day / hour / f"{minute}.json.gz"


def ensure_directory_exists(path: Path):
    """
    Create the directory (and parents) if it does not exist.
    """
    path.mkdir(parents=True, exist_ok=True)


def write_minute_atomically(msgs: list, out_path: Path):
    """
    Write messages to a temporary .json.gz file and fsync before renaming to the final path.
    This prevents leaving a partially written .json.gz if interrupted.
    """
    tmp_path = out_path.with_suffix(".json.gz.tmp")
    # Ensure parent directory exists
    ensure_directory_exists(out_path.parent)

    try:
        with gzip.open(tmp_path, "wt", encoding="utf-8") as f:
            json.dump(msgs, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, out_path)
        logging.info(f"✅ Wrote {len(msgs)} messages to {out_path}")
    except Exception:
        # Cleanup any leftover temp file if something failed
        try:
            tmp_path.unlink()
        except Exception:
            pass
        raise


def create_search_job(query: str, start_time: datetime, end_time: datetime) -> str:
    """
    Submit a new Sumo Logic search job for the given time window.
    Retries on HTTP 429 (rate-limit).
    Returns the job ID.
    """
    payload = {
        "query": query,
        "from": start_time.isoformat(),
        "to":   end_time.isoformat(),
        "timeZone": "UTC"
    }
    response = requests.post(
        SUMO_API_URL,
        json=payload,
        auth=(API_ACCESS_ID, API_ACCESS_KEY),
        timeout=30
    )
    if response.status_code == 429:
        logging.warning("Rate limit hit creating job. Sleeping before retry...")
        time.sleep(API_RATE_LIMIT_DELAY)
        return create_search_job(query, start_time, end_time)
    response.raise_for_status()
    return response.json()["id"]


def wait_for_job_completion(job_id: str):
    """
    Poll the Sumo Logic job status until it is 'DONE GATHERING RESULTS'.
    Retries on HTTP 429. Handles 404 when the job is canceled due to inactivity.
    """
    status_url = f"{SUMO_API_URL}/{job_id}"
    while True:
        response = requests.get(status_url, auth=(API_ACCESS_ID, API_ACCESS_KEY), timeout=30)
        if response.status_code == 429:
            logging.warning("Rate limit hit checking job status. Retrying...")
            time.sleep(API_RATE_LIMIT_DELAY)
            continue
        if response.status_code == 404:
            raise RuntimeError(f"Search job {job_id} not found (possibly expired or canceled).")
        response.raise_for_status()

        state = response.json().get("state")
        if state == "DONE GATHERING RESULTS":
            return
        elif state in ("CANCELLED", "FAILED"):
            raise RuntimeError(f"Search job {job_id} failed with state: {state}")
        time.sleep(5)


def fetch_all_messages(job_id: str) -> list:
    """
    Fetch all messages for a completed search job in pages of up to SEARCH_JOB_RESULTS_LIMIT.
    Retries on HTTP 429. Handles 404 if job is no longer available.
    """
    messages = []
    offset = 0
    while True:
        response = requests.get(
            f"{SUMO_API_URL}/{job_id}/messages",
            params={"limit": SEARCH_JOB_RESULTS_LIMIT, "offset": offset},
            auth=(API_ACCESS_ID, API_ACCESS_KEY),
            timeout=60
        )
        if response.status_code == 429:
            logging.warning("Rate limit hit fetching messages. Sleeping before retry...")
            time.sleep(API_RATE_LIMIT_DELAY)
            continue
        if response.status_code == 404:
            raise RuntimeError(f"Search job {job_id} not found while fetching messages (possibly expired).")
        response.raise_for_status()

        batch = response.json().get("messages", [])
        logging.debug(f"    → fetched batch of {len(batch)} messages (offset {offset}).")
        if not batch:
            break
        messages.extend(batch)
        offset += len(batch)
        if len(batch) < SEARCH_JOB_RESULTS_LIMIT:
            break
    return messages


def process_minute(dt: datetime, args, sem: Semaphore):
    """
    Process a single minute:
      1) Skip if output file already exists.
      2) Create a search job for that minute window.
      3) Wait for it to complete.
      4) Fetch all messages.
      5) If count > message_limit, raise.
      6) Otherwise, write them atomically.
    """
    out_path = get_minute_filepath(args.output_dir, dt)
    if out_path.exists():
        logging.debug(f"⏭  Skipping {dt.isoformat()} (file exists).")
        return

    sem.acquire()
    try:
        next_dt = dt + timedelta(minutes=1)
        logging.info(f"🔍  Creating job for {dt.strftime('%Y-%m-%dT%H:%M')} → {next_dt.strftime('%Y-%m-%dT%H:%M')}")
        job_id = create_search_job(args.query, dt, next_dt)

        wait_for_job_completion(job_id)

        # Fetch messages and count
        msgs = fetch_all_messages(job_id)
        total_messages = len(msgs)
        logging.info(f"ℹ️  Minute {dt.strftime('%Y-%m-%dT%H:%M')} returned {total_messages} messages.")
        if total_messages > args.message_limit:
            raise RuntimeError(
                f"🚫 Minute {dt.strftime('%Y-%m-%dT%H:%M')} exceeds MESSAGE_LIMIT ({args.message_limit})."
            )

        # Atomically write to disk
        write_minute_atomically(msgs, out_path)

    finally:
        sem.release()


def main():
    parser = argparse.ArgumentParser(
        description="Archive Sumo Logic messages minute-by-minute, skipping existing files, using concurrency."
    )
    parser.add_argument("--query", required=True,
                        help="Sumo Logic query (e.g. '_index=foo | ...')")
    parser.add_argument("--start-date", required=True,
                        help="ISO8601 start (e.g. '2023-01-01T00:00:00Z'). Seconds will be dropped.")
    parser.add_argument("--end-date", required=True,
                        help="ISO8601 end   (e.g. '2023-01-01T02:15:30Z'). Seconds will be dropped.")
    parser.add_argument("--output-dir", required=True,
                        help="Base directory under which to write YYYY/MM/DD/HH/MM.json.gz")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG","INFO","WARNING","ERROR"])
    parser.add_argument("--logfile", default="sumo-query.log")
    parser.add_argument("--message-limit", type=int, default=200000,
                        help="Max messages allowed per minute (default: 200000).")
    parser.add_argument("--max-concurrent-jobs", type=int, default=4,
                        help="Number of concurrent Sumo Logic jobs (default: 4).")
    args = parser.parse_args()

    # logging.basicConfig(
    #     format="%(asctime)s [%(levelname)s] %(message)s",
    #     level=getattr(logging, args.log_level),
    #     datefmt="%Y-%m-%d %H:%M:%S"
    # )
    configure_logging(args.logfile, args.log_level)

    # Floor start/end to whole minutes
    start_dt = parse_iso_minute(args.start_date)
    end_dt = parse_iso_minute(args.end_date)
    if end_dt <= start_dt:
        logging.error("End date must be strictly after start date (after flooring to the minute).")
        sys.exit(1)

    logging.info(f"⏱ Archiving from {start_dt.isoformat()} to {end_dt.isoformat()} (minute precision).")

    # Build list of all minute datetimes
    all_minutes = []
    current = start_dt
    while current < end_dt:
        all_minutes.append(current)
        current += timedelta(minutes=1)

    # Semaphore to throttle concurrent Sumo Logic jobs
    sem = Semaphore(args.max_concurrent_jobs)

    # Create ThreadPoolExecutor outside of 'with' so we can shut it down on KeyboardInterrupt
    executor = ThreadPoolExecutor(max_workers=args.max_concurrent_jobs)
    future_to_minute = {}

    try:
        for minute_dt in all_minutes:
            out_path = get_minute_filepath(args.output_dir, minute_dt)
            if out_path.exists():
                logging.debug(f"⏭  Skipping {minute_dt.isoformat()} (file exists).")
                continue

            future = executor.submit(process_minute, minute_dt, args, sem)
            future_to_minute[future] = minute_dt

        # As each task finishes, handle exceptions
        for future in as_completed(future_to_minute):
            minute_dt = future_to_minute[future]
            try:
                future.result()
            except Exception as e:
                logging.error(f"❌ Error processing minute {minute_dt.strftime('%Y-%m-%dT%H:%M')}: {e}")
                # Abort entire run on first failure
                raise

    except KeyboardInterrupt:
        logging.info("⚠️  Caught Ctrl-C, shutting down executor...")
        executor.shutdown(wait=False)
        sys.exit(1)

    except Exception:
        # Any other exception: shut down immediately
        executor.shutdown(wait=False)
        sys.exit(1)

    else:
        # No exceptions, allow all threads to finish cleanly
        executor.shutdown(wait=True)
        logging.info("✅ Done archiving all minutes.")


if __name__ == "__main__":
    main()
