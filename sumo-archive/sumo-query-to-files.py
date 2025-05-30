#!/usr/bin/env python3
import os
import json
import time
import gzip
import logging
import sqlite3
from datetime import datetime, timedelta, timezone
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Semaphore
import calendar
from pathlib import Path
from collections import defaultdict
import argparse
import sys

# === Constants ===
PROTOCOL = "https"
HOST = os.getenv("SUMO_HOST", "api.us2.sumologic.com")
SUMO_API_URL = f"{PROTOCOL}://{HOST}/api/v1/search/jobs"
API_ACCESS_ID = os.getenv("SUMO_ACCESS_ID")
API_ACCESS_KEY = os.getenv("SUMO_ACCESS_KEY")
SEARCH_JOB_RESULTS_LIMIT = 10000
DEFAULT_MAX_CONCURRENT_JOBS = 1
API_RATE_LIMIT_DELAY = 2
MESSAGE_LIMIT = 199999
DEFAULT_DISCOVER_TIME = datetime(2024, 3, 1, 15, tzinfo=timezone.utc)
MIN_SPLIT_MINUTES = 1  # don't split ranges shorter than 1 minute


# === Globals ===
from threading import Lock
db_lock = Lock()
conn = None



# === Logging ===
def configure_logging(logfile, log_level):
    loglevel = getattr(logging, log_level.upper(), logging.INFO)
    logging.basicConfig(
        level=loglevel,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[logging.FileHandler(logfile), logging.StreamHandler()]
    )
    logging.info("Logging initialized.")


# === DB Functions ===
def init_db(db_path):
    global conn
    conn = sqlite3.connect(db_path, check_same_thread=False)
    with db_lock:
        c = conn.cursor()
        c.execute("""
            CREATE TABLE IF NOT EXISTS message_counts (
                query TEXT,
                timestamp TEXT,
                message_count INTEGER
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS query_splits (
                query TEXT,
                original_start TEXT,
                original_end TEXT,
                sub_start TEXT,
                sub_end TEXT
            )
        """)
        conn.commit()


def save_message_counts(query, messages):
    if conn is None:
        return
    grouped = defaultdict(int)
    for message in messages:
        try:
            ts = int(message["map"]["_messagetime"]) // 1000
            dt = datetime.fromtimestamp(ts, tz=timezone.utc).replace(second=0, microsecond=0)
            grouped[dt.isoformat()] += 1
        except Exception as e:
            logging.warning(f"Failed to parse timestamp: {e}")

    with db_lock:
        c = conn.cursor()
        for minute, count in grouped.items():
            c.execute("INSERT INTO message_counts (query, timestamp, message_count) VALUES (?, ?, ?)",
                      (query, minute, count))
        conn.commit()


def log_query_split(query, original_start, original_end, sub_start, sub_end):
    if conn is None:
        return
    with db_lock:
        c = conn.cursor()
        c.execute("""INSERT INTO query_splits
                     (query, original_start, original_end, sub_start, sub_end)
                     VALUES (?, ?, ?, ?, ?)""",
                  (query, original_start.isoformat(), original_end.isoformat(),
                   sub_start.isoformat(), sub_end.isoformat()))
        conn.commit()

# === Core Functions ===
def ensure_directory_exists(path):
    os.makedirs(path, exist_ok=True)


def create_search_job(query, start_time, end_time):
    payload = {
        "query": query,
        "from": start_time.isoformat(),
        "to": end_time.isoformat(),
        "timeZone": "UTC"
    }
    response = requests.post(SUMO_API_URL, json=payload,
                             auth=(API_ACCESS_ID, API_ACCESS_KEY))
    if response.status_code == 429:
        logging.warning("Rate limit hit. Sleeping before retry...")
        time.sleep(API_RATE_LIMIT_DELAY)
        return create_search_job(query, start_time, end_time)
    response.raise_for_status()
    return response.json()["id"]


def wait_for_job_completion(job_id):
    status_url = f"{SUMO_API_URL}/{job_id}"
    while True:
        response = requests.get(status_url, auth=(
            API_ACCESS_ID, API_ACCESS_KEY))
        if response.status_code == 429:
            logging.warning("Rate limit hit. Retrying status check...")
            time.sleep(API_RATE_LIMIT_DELAY)
            continue
        response.raise_for_status()
        state = response.json()["state"]
        if state == "DONE GATHERING RESULTS":
            return
        elif state in ["CANCELLED", "FAILED"]:
            raise Exception(f"Search job {job_id} failed with state: {state}")
        time.sleep(5)


def fetch_all_messages(job_id):
    messages = []
    offset = 0
    while True:
        response = requests.get(
            f"{SUMO_API_URL}/{job_id}/messages",
            params={"limit": SEARCH_JOB_RESULTS_LIMIT, "offset": offset},
            auth=(API_ACCESS_ID, API_ACCESS_KEY)
        )
        if response.status_code == 429:
            logging.warning("Rate limit hit fetching messages. Retrying...")
            time.sleep(API_RATE_LIMIT_DELAY)
            continue
        response.raise_for_status()
        data = response.json().get("messages", [])
        messages.extend(data)
        if len(data) < SEARCH_JOB_RESULTS_LIMIT:
            break
        offset += SEARCH_JOB_RESULTS_LIMIT
    return messages


def save_messages_by_minute(messages, output_dir):
    messages_by_minute = defaultdict(list)
    for message in messages:
        try:
            ts = int(message["map"]["_messagetime"]) // 1000
            dt = datetime.fromtimestamp(ts, tz=timezone.utc)
            key = (dt.year, dt.month, dt.day, dt.hour, dt.minute)
            messages_by_minute[key].append(message)
        except Exception as e:
            logging.error(f"Invalid message timestamp: {e}")

    for (y, m, d, h, minute), msgs in messages_by_minute.items():
        path = Path(output_dir) / f"{y}/{m:02}/{d:02}/{h:02}"
        ensure_directory_exists(path)
        file_path = path / f"{minute:02}.json.gz"
        try:
            with gzip.open(file_path, "wt") as f:
                json.dump(msgs, f)
            logging.info(f"✅ Saved {len(msgs)} messages to {file_path}")
        except Exception as e:
            logging.error(f"Failed to write {file_path}: {e}")


def discover_max_minutes(query):
    logging.info("Discovering optimal minutes per query...")
    start = DEFAULT_DISCOVER_TIME
    try:
        job_id = create_search_job(query, start, start + timedelta(minutes=1))
        wait_for_job_completion(job_id)
        messages = fetch_all_messages(job_id)
        per_minute = len(messages)
        if per_minute > 0:
            return max(1, MESSAGE_LIMIT // per_minute)
        return 60
    except Exception as e:
        logging.warning(f"Discovery failed, defaulting to 60 min: {e}")
        return 60


def all_minute_files_exist(start, end, output_dir):
    t = start
    while t < end:
        path = Path(
            output_dir) / f"{t.year}/{t.month:02}/{t.day:02}/{t.hour:02}/{t.minute:02}.json.gz"
        if not path.exists() or path.stat().st_size == 0:
            return False
        t += timedelta(minutes=1)
    return True


def process_time_range(query, start, end, output_dir, max_minutes, dry_run, split_factor, failed, depth=0):
    duration = (end - start).total_seconds() / 60
    logging.debug(
        f"{'  '*depth}↪ process_time_range: {start} → {end} ({duration:.1f} min)")
    logging.debug(
        f"{'  '*depth}⏱ max_minutes = {max_minutes}, duration = {duration}")

    if all_minute_files_exist(start, end, output_dir):
        logging.info(
            f"{'  '*depth}✅ Skipping {start}–{end}, all minute files exist.")
        return


    if duration > max_minutes and duration > MIN_SPLIT_MINUTES:
        # Compute how many splits we want: min between split_factor and ceil(duration / max_minutes)
        desired_chunks = min(split_factor, int((duration + max_minutes - 1) // max_minutes))
        step_minutes = duration / desired_chunks
        for i in range(desired_chunks):
        # step_minutes = duration / split_factor
        # for i in range(split_factor):
            s = start + timedelta(minutes=round(i * step_minutes))
            e = start + timedelta(minutes=round((i + 1) * step_minutes))
            if e > end:
                e = end
            if s >= e:
                continue
            log_query_split(query, start, end, s, e)
            process_time_range(query, s, e, output_dir, max_minutes,
                               dry_run, split_factor, failed, depth + 1)
        return


    if dry_run:
        logging.info(f"{'  '*depth}[DRY-RUN] Would query {start} to {end}")
        return

    try:
        logging.debug(f"{'  '*depth}📤 Running job for {start} → {end}")
        job_id = create_search_job(query, start, end)
        wait_for_job_completion(job_id)
        messages = fetch_all_messages(job_id)
        save_message_counts(query, messages)

        if len(messages) >= MESSAGE_LIMIT and duration > MIN_SPLIT_MINUTES:
            logging.warning(
                f"{'  '*depth}⚠️ Max messages hit ({len(messages)}): {start} → {end}")
            step = int(duration / split_factor)
            for i in range(split_factor):
                s = start + timedelta(minutes=i * step)
                e = min(start + timedelta(minutes=(i + 1) * step), end)
                if (e - s).total_seconds() / 60 < MIN_SPLIT_MINUTES:
                    continue
                log_query_split(query, start, end, s, e)
                process_time_range(
                    query, s, e, output_dir, max_minutes, dry_run, split_factor, failed, depth + 1)
        # else:
        #     save_messages_by_minute(messages, output_dir)
        else:
            if messages:
                save_messages_by_minute(messages, output_dir)
            else:
                if write_files_with_zero_messages and (end - start) == timedelta(minutes=1):
                    write_empty_file_if_needed(start, output_dir)



    except Exception as e:
        logging.error(f"{'  '*depth}❌ Error {start} → {end}: {e}")
        failed.append((start, end))


# def query_first_minute_per_day(year, day, query):
def query_first_minute_per_day(args):
    summary = []
    year_start, year_end = args.year_range
    month_start, month_end = args.month_range
    day_start, day_end = args.day_range if args.day_range else (1, 31)
    for year in range(year_start, year_end + 1):
        for month in range(month_start, month_end + 1):
        # for month in range(1, 13):
            # for day in range(1, 32):
            _, max_days = calendar.monthrange(year, month)
            for day in range(day_start, min(day_end, max_days) + 1):
                # print(day)
                # time.sleep(5)
                # for day in range(1, 2):
                try:
                    start = datetime(year, month, day, 0, 0, 0, tzinfo=timezone.utc)
                    end = start + timedelta(minutes=1)
                except ValueError:
                    continue  # Skip invalid dates
                logging.info(f"🔍 Querying {start.isoformat()} to {end.isoformat()}")
                try:
                    job_id = create_search_job(args.query, start, end)
                    wait_for_job_completion(job_id)
                    messages = fetch_all_messages(job_id)
                    count = len(messages)
                    summary.append((start.date(), count))
                    print(f"{start.date()}: {count} messages")
                except Exception as e:
                    print(f"{start.date()}: error - {e}")
                    continue

    return summary


def write_empty_file_if_needed(start, output_dir):
    path = Path(output_dir) / f"{start.year}/{start.month:02}/{start.day:02}/{start.hour:02}"
    ensure_directory_exists(path)
    file_path = path / f"{start.minute:02}.json.gz"
    try:
        with gzip.open(file_path, "wt") as f:
            json.dump([], f)
        logging.info(f"✅ Wrote empty file: {file_path}")
    except Exception as e:
        logging.error(f"Failed to write empty file {file_path}: {e}")



# === Main ===
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--query", required=True)
    parser.add_argument("--year-range", nargs=2, type=int, required=True)
    parser.add_argument("--month-range", nargs=2, type=int, required=True)
    parser.add_argument("--day-range", nargs=2, type=int)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--logfile", default="sumo-query.log")
    parser.add_argument("--log-level", default="INFO")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--query-first-minute-per-day", action="store_true")
    parser.add_argument("--split-factor", type=int, default=10)
    parser.add_argument("--write-files-with-zero-messages", action="store_true")
    parser.add_argument("--max-concurrent-jobs", type=int,
                        default=DEFAULT_MAX_CONCURRENT_JOBS)
    parser.add_argument("--max-minutes", type=int)
    parser.add_argument(
        "--sqlite-db", help="Path to SQLite DB to store message counts and splits.")
    args = parser.parse_args()

    configure_logging(args.logfile, args.log_level)

    global write_files_with_zero_messages
    write_files_with_zero_messages = args.write_files_with_zero_messages

    if args.query_first_minute_per_day:
        year_start, year_end = args.year_range
        for year in range(year_start, year_end + 1):
            query_first_minute_per_day(args)
        return


    if args.sqlite_db:
        init_db(args.sqlite_db)

    max_minutes = args.max_minutes or discover_max_minutes(args.query)
    logging.info(f"💡 Using {max_minutes} minutes per query")
    print("Proceeding in 5")
    time.sleep(5)

    year_start, year_end = args.year_range
    month_start, month_end = args.month_range
    day_start, day_end = args.day_range if args.day_range else (1, 31)


    failed = []
    with ThreadPoolExecutor(max_workers=args.max_concurrent_jobs) as executor:
        futures = []
        for year in range(year_start, year_end + 1):
            for month in range(month_start, month_end + 1):
                _, max_days = calendar.monthrange(year, month)
                for day in range(day_start, min(day_end, max_days) + 1):
                    for hour in range(24):
                        start = datetime(year, month, day, hour,
                                         0, tzinfo=timezone.utc)
                        end = start + timedelta(hours=1)
                        futures.append(executor.submit(
                            process_time_range, args.query, start, end,
                            args.output_dir, max_minutes, args.dry_run, args.split_factor, failed
                        ))
        for f in as_completed(futures):
            f.result()

    if failed:
        logging.warning("⚠️ Some ranges failed:")
        for s, e in failed:
            logging.warning(f"- {s} → {e}")
    else:
        logging.info("🎉 All queries completed successfully.")


if __name__ == "__main__":
    main()
