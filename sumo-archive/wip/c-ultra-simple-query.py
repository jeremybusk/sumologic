#!/usr/bin/env python3
import os
import json
import time
import gzip
import logging
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from collections import defaultdict
import calendar
import argparse
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Semaphore

# Constants
PROTOCOL = "https"
HOST = os.getenv("SUMO_HOST", "api.us2.sumologic.com")
SUMO_API_URL = f"{PROTOCOL}://{HOST}/api/v1/search/jobs"
API_ACCESS_ID = os.getenv("SUMO_ACCESS_ID")
API_ACCESS_KEY = os.getenv("SUMO_ACCESS_KEY")
SEARCH_JOB_RESULTS_LIMIT = 10000
MAX_CONCURRENT_JOBS = 18
API_RATE_LIMIT_DELAY = 9
MESSAGE_LIMIT = 200000
DB_FILE = "sumo-query.db"

# Semaphore for concurrency control
job_semaphore = Semaphore(MAX_CONCURRENT_JOBS)

def configure_logging(logfile, log_level):
    loglevel = getattr(logging, log_level.upper(), logging.INFO)
    logging.basicConfig(
        level=loglevel,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.FileHandler(logfile),
            logging.StreamHandler()
        ]
    )

def ensure_directory_exists(path):
    Path(path).mkdir(parents=True, exist_ok=True)

def iterate_time_blocks(year_range, month_range, day_range=None):
    for year in range(year_range[0], year_range[1] + 1):
        for month in range(month_range[0], month_range[1] + 1):
            _, days_in_month = calendar.monthrange(year, month)
            d_start, d_end = day_range if day_range else (1, days_in_month)
            for day in range(d_start, d_end + 1):
                for hour in range(24):
                    yield datetime(year, month, day, hour, 0, tzinfo=timezone.utc)

def initialize_database():
    with sqlite3.connect(DB_FILE) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS query_limits (
                query TEXT PRIMARY KEY,
                max_minutes_per_query INTEGER,
                last_updated TIMESTAMP
            )
        """)
        conn.commit()

def get_max_minutes_from_db(query):
    with sqlite3.connect(DB_FILE) as conn:
        row = conn.execute(
            "SELECT max_minutes_per_query FROM query_limits WHERE query = ?", (query,)
        ).fetchone()
        return row[0] if row else None

def save_max_minutes_to_db(query, max_minutes):
    with sqlite3.connect(DB_FILE) as conn:
        conn.execute("""
            INSERT INTO query_limits (query, max_minutes_per_query, last_updated)
            VALUES (?, ?, ?)
            ON CONFLICT(query) DO UPDATE SET
                max_minutes_per_query = excluded.max_minutes_per_query,
                last_updated = excluded.last_updated
        """, (query, max_minutes, datetime.now()))
        conn.commit()

def discover_max_minutes(query, sample_time):
    logging.info("Discovering max_minutes_per_query...")
    try:
        job_id = create_search_job(query, sample_time, sample_time + timedelta(minutes=1))
        wait_for_job_completion(job_id)
        messages = fetch_all_messages(job_id)
        estimated = max(1, MESSAGE_LIMIT // max(1, len(messages)))
        logging.info(f"Estimated max_minutes_per_query = {estimated}")
    except Exception as e:
        logging.warning(f"Discovery failed: {e}")
        estimated = 60
    save_max_minutes_to_db(query, estimated)
    return estimated

def create_search_job(query, start_time, end_time, retry=0):
    payload = {
        "query": query,
        "from": start_time.isoformat(),
        "to": end_time.isoformat(),
        "timeZone": "UTC"
    }
    try:
        response = requests.post(SUMO_API_URL, json=payload, auth=(API_ACCESS_ID, API_ACCESS_KEY))
        if response.status_code == 429:
            time.sleep(API_RATE_LIMIT_DELAY * (retry + 1))
            return create_search_job(query, start_time, end_time, retry + 1)
        response.raise_for_status()
        return response.json()["id"]
    except Exception as e:
        raise RuntimeError(f"Error creating search job: {e}")

def wait_for_job_completion(job_id):
    url = f"{SUMO_API_URL}/{job_id}"
    while True:
        resp = requests.get(url, auth=(API_ACCESS_ID, API_ACCESS_KEY))
        if resp.status_code == 429:
            time.sleep(API_RATE_LIMIT_DELAY)
            continue
        resp.raise_for_status()
        data = resp.json()
        if data["state"] == "DONE GATHERING RESULTS":
            return
        if data["state"] in ("CANCELLED", "FAILED"):
            raise RuntimeError(f"Job {job_id} failed with state: {data['state']}")
        time.sleep(5)

def fetch_all_messages(job_id):
    all_msgs, offset = [], 0
    url = f"{SUMO_API_URL}/{job_id}/messages"
    while True:
        resp = requests.get(url, params={"limit": SEARCH_JOB_RESULTS_LIMIT, "offset": offset},
                            auth=(API_ACCESS_ID, API_ACCESS_KEY))
        if resp.status_code == 429:
            time.sleep(API_RATE_LIMIT_DELAY)
            continue
        resp.raise_for_status()
        data = resp.json().get("messages", [])
        all_msgs.extend(data)
        if len(data) < SEARCH_JOB_RESULTS_LIMIT:
            break
        offset += SEARCH_JOB_RESULTS_LIMIT
    return all_msgs

def save_messages_by_minute(messages, output_dir):
    grouped = defaultdict(list)
    for msg in messages:
        try:
            t = datetime.fromtimestamp(int(msg["map"]["_messagetime"]) // 1000, tz=timezone.utc)
            key = (t.year, t.month, t.day, t.hour, t.minute)
            grouped[key].append(msg)
        except Exception as e:
            logging.warning(f"Invalid message: {e}")

    for (y, m, d, h, mi), msgs in grouped.items():
        path = Path(output_dir) / f"{y}/{m:02}/{d:02}/{h:02}"
        ensure_directory_exists(path)
        fpath = path / f"{mi:02}.json.gz"
        with gzip.open(fpath, "wt") as f:
            json.dump(msgs, f)
        logging.info(f"✅ Saved {len(msgs)} messages to {fpath}")

def all_minute_files_exist(start, end, output_dir):
    t = start
    while t < end:
        path = Path(output_dir) / f"{t.year}/{t.month:02}/{t.day:02}/{t.hour:02}/{t.minute:02}.json.gz"
        if not path.exists() or path.stat().st_size == 0:
            return False
        t += timedelta(minutes=1)
    return True

def process_time_range(query, start, end, output_dir, max_minutes, dry_run=False):
    duration = (end - start).total_seconds() / 60
    if all_minute_files_exist(start, end, output_dir):
        logging.info(f"⏭️ Skipping {start}–{end}, all minute files exist.")
        return
    if duration > max_minutes:
        mid = start + timedelta(minutes=max_minutes)
        process_time_range(query, start, mid, output_dir, max_minutes, dry_run)
        process_time_range(query, mid, end, output_dir, max_minutes, dry_run)
        return

    if dry_run:
        logging.info(f"[DRY-RUN] Would query {start} to {end}")
        return

    try:
        job_id = create_search_job(query, start, end)
        wait_for_job_completion(job_id)
        messages = fetch_all_messages(job_id)
        if len(messages) > MESSAGE_LIMIT:
            mid = start + timedelta(minutes=max(1, int(duration / 2)))
            if mid <= start or mid >= end:
                logging.warning(f"⚠️ Cannot split further: {start} → {end}")
                return
            process_time_range(query, start, mid, output_dir, max_minutes, dry_run)
            process_time_range(query, mid, end, output_dir, max_minutes, dry_run)
        else:
            save_messages_by_minute(messages, output_dir)
            logging.info(f"✅ Processed {start} to {end}")
    except Exception as e:
        logging.error(f"❌ Error in range {start}–{end}: {e}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--query", required=True)
    parser.add_argument("--year-range", nargs=2, type=int, required=True, help="Inclusive year range (e.g., 2023 2024)")
    parser.add_argument("--month-range", nargs=2, type=int, required=True)
    parser.add_argument("--day-range", nargs=2, type=int)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--logfile", default="sumo-query.log")
    parser.add_argument("--log-level", default="INFO")
    parser.add_argument("--only-missing-minutes", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    configure_logging(args.logfile, args.log_level)
    initialize_database()

    sample_time = datetime(args.year_range[0], args.month_range[0], 1, 0, tzinfo=timezone.utc)
    max_minutes = get_max_minutes_from_db(args.query) or discover_max_minutes(args.query, sample_time)

    with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_JOBS) as executor:
        futures = []
        for block_start in iterate_time_blocks(args.year_range, args.month_range, args.day_range):
            block_end = block_start + timedelta(hours=1)
            futures.append(executor.submit(
                process_time_range, args.query, block_start, block_end,
                args.output_dir, max_minutes, args.dry_run
            ))
        for f in as_completed(futures):
            try:
                f.result()
            except Exception as e:
                logging.error(f"Unhandled exception in thread: {e}")

if __name__ == "__main__":
    main()

