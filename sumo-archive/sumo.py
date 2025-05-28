#!/usr/bin/env python3
import os
import json
import time
import gzip
import logging
from datetime import datetime, timedelta, timezone
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Semaphore
import calendar
from pathlib import Path
from collections import defaultdict
import sys
import argparse

# Constants
PROTOCOL = "https"
HOST = os.getenv("SUMO_HOST", "api.us2.sumologic.com")
SUMO_API_URL = f"{PROTOCOL}://{HOST}/api/v1/search/jobs"
API_ACCESS_ID = os.getenv("SUMO_ACCESS_ID")
API_ACCESS_KEY = os.getenv("SUMO_ACCESS_KEY")
SEARCH_JOB_RESULTS_LIMIT = 10000  # Maximum messages per request
DEFAULT_MAX_CONCURRENT_JOBS = 1  # Limit for concurrent active jobs
API_RATE_LIMIT_DELAY = 2  # Delay in seconds between API calls to avoid rate limiting
MESSAGE_LIMIT = 200000  # Maximum messages per query. Max appears to be 199999 while API docs specify 100K
# https://help.sumologic.com/docs/api/search-job/
DEFAULT_DISCOVER_MINUTES_MONTH = 1  # If None it will use start query month
DEFAULT_DISCOVER_MINUTES_DAY = 2  # 2 is Monday
DEFAULT_DISCOVER_MINUTES_HOUR = 15  # 15 UTC is 11am MDT, trying to get peak
MAX_MINUTES_PER_QUERY = 9  # None, 9. If None will be dynamically calculated on start
SPLIT_FACTOR = 10  # Set to any N >= 2


# Ensure output directories exist
def ensure_directory_exists(path):
    os.makedirs(path, exist_ok=True)


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
    logging.info("Logging initialized.")


def all_minute_files_exist(start, end, output_dir):
    t = start
    while t < end:
        path = Path(
            output_dir) / f"{t.year}/{t.month:02}/{t.day:02}/{t.hour:02}/{t.minute:02}.json.gz"
        if not path.exists() or path.stat().st_size == 0:
            return False
        t += timedelta(minutes=1)
    return True


# Discover the maximum number of minutes per query
def discover_max_minutes(query, year, month, day, hour):
    global MAX_MINUTES_PER_QUERY
    logging.info("Discovering maximum minutes per query...")
    start_time = datetime(year, month, day, hour, 0, tzinfo=timezone.utc)

    try:
        # Query for 1 minute to estimate the number of messages per minute
        end_time = start_time + timedelta(minutes=1)
        job_id = create_search_job(query, start_time, end_time)
        wait_for_job_completion(job_id)
        messages = fetch_all_messages(job_id)

        # Calculate the maximum number of minutes per query
        if len(messages) > 0:
            MAX_MINUTES_PER_QUERY = max(1, MESSAGE_LIMIT // len(messages))
            logging.info(f"Max minutes per query set to {
                         MAX_MINUTES_PER_QUERY} minutes.")
        else:
            MAX_MINUTES_PER_QUERY = 60  # Default to 60 if no messages are returned
            logging.warning(
                "No messages returned. Defaulting to 60 minutes per query.")

    except Exception as e:
        logging.error(f"Error during discovery: {e}")
        MAX_MINUTES_PER_QUERY = 60  # Default to 60 if an error occurs
        logging.info(f"Defaulting to 60 minutes per query.")

# Create a search job


def create_search_job(query, start_time, end_time):
    payload = {
        "query": query,
        "from": start_time.isoformat(),
        "to": end_time.isoformat(),
        "timeZone": "UTC"
    }
    try:
        response = requests.post(
            SUMO_API_URL, json=payload, auth=(API_ACCESS_ID, API_ACCESS_KEY))
        if response.status_code == 429:  # Rate limit exceeded
            logging.warning("Rate limit exceeded. Retrying after delay...")
            time.sleep(API_RATE_LIMIT_DELAY)
            return create_search_job(query, start_time, end_time)
        response.raise_for_status()
        return response.json()["id"]
    except Exception as e:
        logging.error(f"Error creating search job: {e}")
        raise

# Wait for the search job to complete


def wait_for_job_completion(job_id):
    status_url = f"{SUMO_API_URL}/{job_id}"
    while True:
        try:
            response = requests.get(status_url, auth=(
                API_ACCESS_ID, API_ACCESS_KEY))
            if response.status_code == 429:  # Rate limit exceeded
                logging.warning(
                    "Rate limit exceeded while waiting for job completion. Retrying...")
                time.sleep(API_RATE_LIMIT_DELAY)
                continue
            response.raise_for_status()
            data = response.json()
            if data["state"] == "DONE GATHERING RESULTS":
                return
            elif data["state"] in ["CANCELLED", "FAILED"]:
                raise Exception(
                    f"Search job {job_id} failed with state: {data['state']}")
            time.sleep(5)
        except Exception as e:
            logging.error(f"Error waiting for job completion: {e}")
            raise

# Fetch all paginated messages from the search job


def fetch_all_messages(job_id):
    messages = []
    job_results_url = f"{SUMO_API_URL}/{job_id}/messages"
    offset = 0
    while True:
        try:
            params = {"limit": SEARCH_JOB_RESULTS_LIMIT, "offset": offset}
            response = requests.get(job_results_url, params=params, auth=(
                API_ACCESS_ID, API_ACCESS_KEY))
            if response.status_code == 429:  # Rate limit exceeded
                logging.warning(
                    "Rate limit exceeded while fetching messages. Retrying...")
                time.sleep(API_RATE_LIMIT_DELAY)
                continue
            response.raise_for_status()
            data = response.json()
            messages.extend(data.get("messages", []))
            if len(data.get("messages", [])) < SEARCH_JOB_RESULTS_LIMIT:
                break
            offset += SEARCH_JOB_RESULTS_LIMIT
        except Exception as e:
            logging.error(f"Error fetching messages: {e}")
            raise
    return messages

# Save messages incrementally to disk


def save_messages_by_minute(messages, output_dir):
    messages_by_minute = defaultdict(list)
    for message in messages:
        try:
            timestamp = int(message["map"]["_messagetime"]
                            ) // 1000  # Convert to seconds
            message_time = datetime.fromtimestamp(timestamp, tz=timezone.utc)
            minute_key = (message_time.year, message_time.month,
                          message_time.day, message_time.hour, message_time.minute)
            messages_by_minute[minute_key].append(message)
        except Exception as e:
            logging.error(f"Error processing message: {e}")

    for (year, month, day, hour, minute), minute_messages in messages_by_minute.items():
        directory = f"{output_dir}/{year}/{month:02}/{day:02}/{hour:02}"
        file_name = f"{minute:02}.json.gz"
        full_path = os.path.join(directory, file_name)
        ensure_directory_exists(directory)
        try:
            with gzip.open(full_path, "wt") as f:
                json.dump(minute_messages, f, indent=2)
            logging.info(f"✅ Saved {len(minute_messages)
                                    } messages to {full_path}")
        except Exception as e:
            logging.error(f"Error writing to file {full_path}: {e}")


def is_file_non_empty(file_path):
    return os.path.exists(file_path) and os.path.getsize(file_path) > 0

# Process a time range dynamically
def process_time_range(query, start, end, output_dir, max_minutes, dry_run=False):
    duration = (end - start).total_seconds() / 60
    if all_minute_files_exist(start, end, output_dir):
        logging.info(f"✅ Skipping {start}–{end}, all minute files exist.")
        return

    if duration > max_minutes:
        step = int((end - start).total_seconds() / 60 / SPLIT_FACTOR)
        for i in range(SPLIT_FACTOR):
            sub_start = start + timedelta(minutes=step * i)
            sub_end = start + timedelta(minutes=step * (i + 1))
            if sub_end > end:
                sub_end = end
            process_time_range(query, sub_start, sub_end,
                               output_dir, max_minutes, dry_run)
        return

    if dry_run:
        logging.info(f"[DRY-RUN] Would query {start} to {end}")
        return

    try:
        job_id = create_search_job(query, start, end)
        wait_for_job_completion(job_id)
        messages = fetch_all_messages(job_id)
        if len(messages) >= MESSAGE_LIMIT:
            logging.warning(f"⚠️ Number of messages {len(messages)} is >= {
                            MESSAGE_LIMIT} for {start} to {end}")
            step = int(duration / SPLIT_FACTOR)
            for i in range(SPLIT_FACTOR):
                sub_start = start + timedelta(minutes=step * i)
                sub_end = start + timedelta(minutes=step * (i + 1))
                logging.warning(f"⛔ Max Messages hit. Splitting original query {
                                start} -> {end}  to: {sub_start} → {sub_end}")
                if sub_end > end:
                    sub_end = end
                if sub_start >= sub_end:
                    logging.warning(f"⛔ Cannot split further: {
                                    sub_start} → {sub_end}")
                    return
                process_time_range(query, sub_start, sub_end,
                                   output_dir, max_minutes, dry_run)
        else:
            save_messages_by_minute(messages, output_dir)
            logging.info(f"✅ Processed {start} to {end}")
    except Exception as e:
        logging.error(f"❌ Error in range {start} to {end}: {e}")


# Main function


def main():
    # global DEFAULT_MAX_CONCURRENT_JOBS
    parser = argparse.ArgumentParser(
        description="Collect Sumo Logic messages and save to gzip-compressed files.")
    parser.add_argument("--query", required=True,
                        help="Sumo Logic query to execute.")
    parser.add_argument("--year-range", nargs=2, type=int,
                        required=True, help="Year range to query (e.g., 2023 2024).")
    parser.add_argument("--month-range", nargs=2, type=int,
                        required=True, help="Month range to query (e.g., 1 12).")
    parser.add_argument("--day-range", nargs=2, type=int,
                        help="Day range to query (e.g., 1 31). If not set, all days of the month are processed.")
    parser.add_argument("--output-dir", required=True,
                        help="Directory to save the output files.")
    parser.add_argument("--logfile", default="sumo-query.log",
                        help="Path to the log file (default: sumo-query.log).")
    parser.add_argument('--log-level', default='WARNING',
                        help='Set the logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)')
    parser.add_argument('--max-concurrent-jobs', type=int,
                        default=DEFAULT_MAX_CONCURRENT_JOBS, help='Max concurrent jobs 20 is max for org)')
    parser.add_argument("--only-missing-minutes", action="store_true",
                        help="Process only missing minute files.")
    args = parser.parse_args()

    # Configure logging
    configure_logging(args.logfile, args.log_level)

    global MAX_CONCURRENT_JOBS
    MAX_CONCURRENT_JOBS = args.max_concurrent_jobs
    # Semaphore to limit concurrent jobs
    global job_semaphore
    job_semaphore = Semaphore(MAX_CONCURRENT_JOBS)

    year_start, year_end = args.year_range
    month_start, month_end = args.month_range
    day_start, day_end = args.day_range if args.day_range else (1, 31)

    # Discover the maximum minutes per query on the first day of the month
    if not MAX_MINUTES_PER_QUERY:
        if DEFAULT_DISCOVER_MINUTES_MONTH:
            discover_max_minutes(args.query, year_start, DEFAULT_DISCOVER_MINUTES_MONTH,
                                 DEFAULT_DISCOVER_MINUTES_DAY, DEFAULT_DISCOVER_MINUTES_HOUR)
        else:
            discover_max_minutes(args.query, year_start, month_start,
                                 DEFAULT_DISCOVER_MINUTES_DAY, DEFAULT_DISCOVER_MINUTES_HOUR)

    if args.only_missing_minutes:
        logging.info("�~_~T~M Running in only-missing-minutes mode...")
        with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_JOBS) as executor:
            futures = []
            for hour_start in iterate_time_blocks(args.year_range, args.month_range, args.day_range):
                hour_end = hour_start + timedelta(hours=1)
                missing_minutes = find_missing_minutes(
                    hour_start, hour_end, args.output_dir)
                for minute_time in missing_minutes:
                    futures.append(executor.submit(
                        process_missing_minute, args.query, minute_time, args.output_dir
                    ))
            for f in as_completed(futures):
                try:
                    f.result()
                except Exception as e:
                    logging.error(f"Error processing a missing minute: {e}")
        return  # exit after one-minute-only mode

    with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_JOBS) as executor:
        futures = []
        for year in range(year_start, year_end + 1):
            for month in range(month_start, month_end + 1):
                _, days_in_month = calendar.monthrange(year, month)
                for day in range(day_start, min(day_end, days_in_month) + 1):
                    for hour in range(24):
                        start_time = datetime(
                            year, month, day, hour, 0, tzinfo=timezone.utc)
                        end_time = start_time + timedelta(hours=1)
                        futures.append(
                            executor.submit(
                                process_time_range, args.query, start_time, end_time, args.output_dir, MAX_MINUTES_PER_QUERY
                            )
                        )

        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logging.error(f"❌ Error processing a time range: {e}")


# Process missing minutes
def find_missing_minutes(start_time, end_time, output_dir):
    """
    Return a list of minute datetimes where files are missing or empty.
    """
    missing_minutes = []
    current_time = start_time
    while current_time < end_time:
        path = Path(output_dir) / f"{current_time.year}/{current_time.month:02}/{
            current_time.day:02}/{current_time.hour:02}/{current_time.minute:02}.json.gz"
        if not path.exists() or path.stat().st_size == 0:
            missing_minutes.append(current_time)
        current_time += timedelta(minutes=1)
    return missing_minutes


def process_missing_minute(query, minute_time, output_dir):
    start_time = minute_time
    end_time = start_time + timedelta(minutes=1)
    try:
        job_id = create_search_job(query, start_time, end_time)
        wait_for_job_completion(job_id)
        messages = fetch_all_messages(job_id)
        save_messages_by_minute(messages, output_dir)
        logging.info(f"Filled missing minute: {start_time}")
    except Exception as e:
        logging.error(f"Failed to process missing minute {start_time}: {e}")


if __name__ == "__main__":
    main()
