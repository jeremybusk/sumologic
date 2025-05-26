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

# Constants
PROTOCOL = "https"
HOST = os.getenv("SUMO_HOST", "api.us2.sumologic.com")
SUMO_API_URL = f"{PROTOCOL}://{HOST}/api/v1/search/jobs"
API_ACCESS_ID = os.getenv("SUMO_ACCESS_ID")
API_ACCESS_KEY = os.getenv("SUMO_ACCESS_KEY")
SEARCH_JOB_RESULTS_LIMIT = 10000  # Maximum messages per request
MAX_CONCURRENT_JOBS = 18  # Limit for concurrent active jobs
API_RATE_LIMIT_DELAY = 2  # Delay in seconds between API calls to avoid rate limiting

# Semaphore to limit concurrent jobs
job_semaphore = Semaphore(MAX_CONCURRENT_JOBS)

# Ensure output directories exist
def ensure_directory_exists(path):
    os.makedirs(path, exist_ok=True)

# Configure logging
def configure_logging(logfile, verbose):
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.FileHandler(logfile),
            logging.StreamHandler()  # Log to stdout
        ]
    )
    logging.info("Logging initialized.")

# Check if a file exists and is not empty
def is_file_non_empty(file_path):
    return os.path.exists(file_path) and os.path.getsize(file_path) > 0

# Create a search job
def create_search_job(query, start_time, end_time):
    payload = {
        "query": query,
        "from": start_time.isoformat(),
        "to": end_time.isoformat(),
        "timeZone": "UTC"
    }
    try:
        response = requests.post(SUMO_API_URL, json=payload, auth=(API_ACCESS_ID, API_ACCESS_KEY))
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
            response = requests.get(status_url, auth=(API_ACCESS_ID, API_ACCESS_KEY))
            if response.status_code == 429:  # Rate limit exceeded
                logging.warning("Rate limit exceeded while waiting for job completion. Retrying...")
                time.sleep(API_RATE_LIMIT_DELAY)
                continue
            response.raise_for_status()
            data = response.json()
            if data["state"] == "DONE GATHERING RESULTS":
                return
            elif data["state"] in ["CANCELLED", "FAILED"]:
                raise Exception(f"Search job {job_id} failed with state: {data['state']}")
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
            response = requests.get(job_results_url, params=params, auth=(API_ACCESS_ID, API_ACCESS_KEY))
            if response.status_code == 429:  # Rate limit exceeded
                logging.warning("Rate limit exceeded while fetching messages. Retrying...")
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

# Filter messages to ensure they fall within the exact minute
def filter_messages_by_minute(messages, start_time, end_time):
    filtered_messages = []
    for message in messages:
        try:
            timestamp = int(message["map"]["_messagetime"]) // 1000  # Convert to seconds
            message_time = datetime.fromtimestamp(timestamp, tz=timezone.utc)
            if start_time <= message_time < end_time:
                filtered_messages.append(message)
            else:
                logging.warning(f"Message outside range: {message_time} (expected {start_time} to {end_time})")
        except Exception as e:
            logging.error(f"Error filtering message: {e}")
    return filtered_messages

# Append messages to a gzip-compressed file
def append_messages_to_gzip_file(messages, file_path):
    try:
        if os.path.exists(file_path):
            # Append to the existing gzip file
            with gzip.open(file_path, "rt") as f:
                existing_messages = json.load(f)
            existing_messages.extend(messages)
            with gzip.open(file_path, "wt") as f:
                json.dump(existing_messages, f, indent=2)
        else:
            # Write a new gzip file
            with gzip.open(file_path, "wt") as f:
                json.dump(messages, f, indent=2)
        logging.info(f"✅ Flushed {len(messages)} messages to {file_path}")
    except Exception as e:
        logging.error(f"Error writing to file {file_path}: {e}")

# Save messages incrementally to disk
def save_messages_incrementally(messages, year, month, day, hour, minute, output_dir):
    directory = f"{output_dir}/{year}/{month:02}/{day:02}/{hour:02}"
    file_name = f"{minute:02}.json.gz"
    full_path = os.path.join(directory, file_name)
    ensure_directory_exists(directory)
    append_messages_to_gzip_file(messages, full_path)

# Process a single minute range
def process_minute_range(query, start_time, end_time, year, month, day, hour, minute, output_dir):
    directory = f"{output_dir}/{year}/{month:02}/{day:02}/{hour:02}"
    file_name = f"{minute:02}.json.gz"
    full_path = os.path.join(directory, file_name)

    # Skip query if the file already exists and is not empty
    if is_file_non_empty(full_path):
        logging.info(f"⏩ Skipping query for {full_path} (file already exists and is not empty).")
        return

    with job_semaphore:  # Limit concurrent jobs
        job_id = create_search_job(query, start_time, end_time)
        wait_for_job_completion(job_id)
        all_messages = fetch_all_messages(job_id)
        filtered_messages = filter_messages_by_minute(all_messages, start_time, end_time)
        save_messages_incrementally(filtered_messages, year, month, day, hour, minute, output_dir)

# Main function
def main():
    import argparse
    parser = argparse.ArgumentParser(description="Collect Sumo Logic messages and save to gzip-compressed files.")
    parser.add_argument("--query", required=True, help="Sumo Logic query to execute.")
    parser.add_argument("--year-range", nargs=2, type=int, required=True, help="Year range to query (e.g., 2023 2024).")
    parser.add_argument("--month-range", nargs=2, type=int, required=True, help="Month range to query (e.g., 1 12).")
    parser.add_argument("--day-range", nargs=2, type=int, help="Day range to query (e.g., 1 31). If not set, all days of the month are processed.")
    parser.add_argument("--output-dir", required=True, help="Directory to save the output files.")
    parser.add_argument("--logfile", default="sumo-query.log", help="Path to the log file (default: sumo-query.log).")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging for debugging.")
    args = parser.parse_args()

    # Configure logging
    configure_logging(args.logfile, args.verbose)

    year_start, year_end = args.year_range
    month_start, month_end = args.month_range
    day_start, day_end = args.day_range if args.day_range else (1, 31)

    for year in range(year_start, year_end + 1):
        for month in range(month_start, month_end + 1):
            _, days_in_month = calendar.monthrange(year, month)
            for day in range(day_start, min(day_end, days_in_month) + 1):
                for hour in range(24):
                    for minute in range(60):
                        start_time = datetime(year, month, day, hour, minute, tzinfo=timezone.utc)
                        end_time = start_time + timedelta(minutes=1)
                        process_minute_range(args.query, start_time, end_time, year, month, day, hour, minute, args.output_dir)

if __name__ == "__main__":
    main()
