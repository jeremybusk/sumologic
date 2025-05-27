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
from collections import defaultdict

# Constants
PROTOCOL = "https"
HOST = os.getenv("SUMO_HOST", "api.us2.sumologic.com")
SUMO_API_URL = f"{PROTOCOL}://{HOST}/api/v1/search/jobs"
API_ACCESS_ID = os.getenv("SUMO_ACCESS_ID")
API_ACCESS_KEY = os.getenv("SUMO_ACCESS_KEY")
SEARCH_JOB_RESULTS_LIMIT = 10000  # Maximum messages per request
# MAX_CONCURRENT_JOBS = 18  # Limit for concurrent active jobs
MAX_CONCURRENT_JOBS = 18  # Limit for concurrent active jobs
# API_RATE_LIMIT_DELAY = MAX_CONCURRENT_JOBS // 2   # Delay in seconds between API calls to avoid rate limiting
API_RATE_LIMIT_DELAY = 9  # Delay in seconds between API calls to avoid rate limiting
MESSAGE_LIMIT = 200000  # Maximum messages per query
# MESSAGE_LIMIT = 100000  # Maximum messages per query 100000 in docs 200000 you can miss messages
MAX_MINUTES_PER_QUERY = None  # Will be dynamically calculated or retrieved from the database
# DB_FILE = "sumologic_default.query_limits.db"  # SQLite database file
# DB_FILE = "sumo-query100k.db"  # SQLite database file
# DB_FILE = "sumo-queryM10-100k.db"  # SQLite database file
DB_FILE = "sumo-query.db"  # SQLite database file
OVERWRITE_EMPTY_FILES = False

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

# Find all missing minute files
def find_missing_minutes(start_time, end_time, output_dir):
    """
    Identify all missing or empty minute files in the given time range.
    """
    missing_minutes = []
    current_time = start_time
    while current_time < end_time:
        year, month, day, hour, minute = (
            current_time.year,
            current_time.month,
            current_time.day,
            current_time.hour,
            current_time.minute,
        )
        directory = f"{output_dir}/{year}/{month:02}/{day:02}/{hour:02}"
        file_name = f"{minute:02}.json.gz"
        file_path = os.path.join(directory, file_name)

        # Check if the file exists and is non-empty
        if not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
            missing_minutes.append(current_time)
            logging.debug(f"Missing or empty file: {file_path}")

        # Move to the next minute
        current_time += timedelta(minutes=1)

    logging.info(f"Found {len(missing_minutes)} missing minutes in range {start_time} to {end_time}.")
    return missing_minutes

# Process missing minutes
def process_missing_minutes(query, missing_minutes, output_dir):
    """
    Process queries for all missing minutes and save the results using futures.
    """
    with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_JOBS) as executor:
        futures = []
        for minute in missing_minutes:
            start_time = minute
            end_time = start_time + timedelta(minutes=1)
            logging.info(f"Submitting task for missing minute: {start_time} to {end_time}")

            # Submit the task to the executor
            futures.append(
                executor.submit(process_missing_minute, query, start_time, end_time, output_dir)
            )

        # Wait for all futures to complete
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logging.error(f"❌ Error processing a missing minute: {e}")


def process_missing_minute(query, start_time, end_time, output_dir):
    """
    Process a single missing minute query and save the results.
    """
    try:
        job_id = create_search_job(query, start_time, end_time)
        wait_for_job_completion(job_id)
        messages = fetch_all_messages(job_id)
        save_messages_by_minute(messages, output_dir)
        logging.info(f"✅ Successfully processed missing minute: {start_time} to {end_time}")
    except Exception as e:
        logging.error(f"❌ Error processing missing minute {start_time} to {end_time}: {e}")


# Initialize SQLite database
def initialize_database():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS query_limits (
            query TEXT PRIMARY KEY,
            max_minutes_per_query INTEGER,
            last_updated TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()

# Get the last value of MAX_MINUTES_PER_QUERY from the database
def get_max_minutes_from_db(query):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT max_minutes_per_query FROM query_limits WHERE query = ?", (query,))
    row = cursor.fetchone()
    conn.close()
    if row:
        logging.info(f"Retrieved MAX_MINUTES_PER_QUERY from database: {row[0]} minutes.")
        return row[0]
    return None

# Save the MAX_MINUTES_PER_QUERY value to the database
def save_max_minutes_to_db(query, max_minutes):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO query_limits (query, max_minutes_per_query, last_updated)
        VALUES (?, ?, ?)
        ON CONFLICT(query) DO UPDATE SET
            max_minutes_per_query = excluded.max_minutes_per_query,
            last_updated = excluded.last_updated
    """, (query, max_minutes, datetime.now()))
    conn.commit()
    conn.close()
    logging.info(f"Saved MAX_MINUTES_PER_QUERY to database: {max_minutes} minutes.")

# Check if all minute files exist for a given time range
def all_minute_files_exist(start_time, end_time, output_dir):
    """
    Check if all minute files for the given time range exist and are non-empty.
    """
    current_time = start_time
    while current_time < end_time:
        year, month, day, hour, minute = (
            current_time.year,
            current_time.month,
            current_time.day,
            current_time.hour,
            current_time.minute,
        )
        directory = f"{output_dir}/{year}/{month:02}/{day:02}/{hour:02}"
        file_name = f"{minute:02}.json.gz"
        file_path = os.path.join(directory, file_name)

        # Check if the file exists and is non-empty
        if not os.path.exists(file_path):
            logging.debug(f"File does not exist: {file_path}")
            return False
        if os.path.getsize(file_path) == 0:
            logging.debug(f"File is empty: {file_path}")
            if OVERWRITE_EMPTY_FILES:
                logging.debug(f"Overwriting empty file: {file_path}")
                return False

        # Move to the next minute
        current_time += timedelta(minutes=1)

    logging.info(f"All minute files exist for range {start_time} to {end_time}.")
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
            logging.info(f"Max minutes per query set to {MAX_MINUTES_PER_QUERY} minutes.")
        else:
            MAX_MINUTES_PER_QUERY = 60  # Default to 60 if no messages are returned
            logging.warning("No messages returned. Defaulting to 60 minutes per query.")

    except Exception as e:
        logging.error(f"Error during discovery: {e}")
        MAX_MINUTES_PER_QUERY = 60  # Default to 60 if an error occurs
        logging.info(f"Defaulting to 60 minutes per query.")

    # Save the discovered value to the database
    save_max_minutes_to_db(query, MAX_MINUTES_PER_QUERY)

# Create a search job
def create_search_job(query, start_time, end_time):
    payload = {
        "query": query,
        "from": start_time.isoformat(),
        "to": end_time.isoformat(),
        "timeZone": "UTC"
    }
    try:
        logging.info(f"Creating search job {start_time.isoformat()} {end_time.isoformat()} ")
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

# Save messages incrementally to disk
def save_messages_by_minute(messages, output_dir):
    messages_by_minute = defaultdict(list)
    for message in messages:
        try:
            timestamp = int(message["map"]["_messagetime"]) // 1000  # Convert to seconds
            message_time = datetime.fromtimestamp(timestamp, tz=timezone.utc)
            minute_key = (message_time.year, message_time.month, message_time.day, message_time.hour, message_time.minute)
            messages_by_minute[minute_key].append(message)
        except Exception as e:
            logging.error(f"Error processing message: {e}")

    for (year, month, day, hour, minute), minute_messages in messages_by_minute.items():
        directory = f"{output_dir}/{year}/{month:02}/{day:02}/{hour:02}"
        file_name = f"{minute:02}.json.gz"
        full_path = os.path.join(directory, file_name)
        ensure_directory_exists(directory)

        # Check if the file already exists
        if os.path.exists(full_path):
            logging.info(f"Overwriting existing file: {full_path}")

        try:
            with gzip.open(full_path, "wt") as f:
                json.dump(minute_messages, f, indent=2)
            logging.info(f"✅ Saved {len(minute_messages)} messages to {full_path}")
        except Exception as e:
            logging.error(f"Error writing to file {full_path}: {e}")

# Process a time range dynamically
def process_time_range(query, start_time, end_time, output_dir):
    """
    Process a time range dynamically. If the number of messages exceeds MESSAGE_LIMIT,
    split the query into smaller chunks and reprocess.
    """
    global MAX_MINUTES_PER_QUERY

    # Check if all minute files exist
    if all_minute_files_exist(start_time, end_time, output_dir):
        logging.info(f"⏩ Skipping query for {start_time} to {end_time} (all minute files exist).")
        return

    duration = (end_time - start_time).total_seconds() / 60

    # If the duration exceeds MAX_MINUTES_PER_QUERY, split the range
    if duration > MAX_MINUTES_PER_QUERY:
        mid_time = start_time + timedelta(minutes=MAX_MINUTES_PER_QUERY)
        process_time_range(query, start_time, mid_time, output_dir)
        process_time_range(query, mid_time, end_time, output_dir)
        return

    # Create the search job
    try:
        job_id = create_search_job(query, start_time, end_time)
        wait_for_job_completion(job_id)
        messages = fetch_all_messages(job_id)

        # If the number of messages exceeds MESSAGE_LIMIT, split the range into smaller chunks
        if len(messages) > MESSAGE_LIMIT:
            logging.warning(
                f"Query for {start_time} to {end_time} returned {len(messages)} messages, "
                f"exceeding MESSAGE_LIMIT ({MESSAGE_LIMIT}). Splitting into smaller chunks."
            )
            mid_time = start_time + timedelta(minutes=(duration // 2))
            process_time_range(query, start_time, mid_time, output_dir)
            process_time_range(query, mid_time, end_time, output_dir)
        else:
            # Save the messages if within the limit
            save_messages_by_minute(messages, output_dir)
            logging.info(f"✅ Successfully processed range: {start_time} to {end_time}")

    except Exception as e:
        logging.error(f"❌ Error processing range {start_time} to {end_time}: {e}")


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
    parser.add_argument("--only-missing-minutes", action="store_true", help="Process only missing minute files.")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging for debugging.")
    args = parser.parse_args()

    # Configure logging
    configure_logging(args.logfile, args.verbose)

    # Process only missing minutes if the argument is specified
    if args.only_missing_minutes:
        logging.info("Processing only missing minute files.")
        for year in range(args.year_range[0], args.year_range[1] + 1):
            for month in range(args.month_range[0], args.month_range[1] + 1):
                _, days_in_month = calendar.monthrange(year, month)
                day_start, day_end = args.day_range if args.day_range else (1, days_in_month)
                for day in range(day_start, day_end + 1):
                    for hour in range(24):
                        start_time = datetime(year, month, day, hour, 0, tzinfo=timezone.utc)
                        end_time = start_time + timedelta(hours=1)
                        missing_minutes = find_missing_minutes(start_time, end_time, args.output_dir)
                        if missing_minutes:
                            process_missing_minutes(args.query, missing_minutes, args.output_dir)
        return
    # End minutes


    # Initialize the database
    initialize_database()

    # Retrieve MAX_MINUTES_PER_QUERY from the database or discover it
    global MAX_MINUTES_PER_QUERY
    MAX_MINUTES_PER_QUERY = get_max_minutes_from_db(args.query)
    if MAX_MINUTES_PER_QUERY is None:
        discover_max_minutes(args.query, args.year_range[0], args.month_range[0], 1, 0)

    # Continue with the rest of the script...
    logging.info(f"Using MAX_MINUTES_PER_QUERY: {MAX_MINUTES_PER_QUERY} minutes.")

    with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_JOBS) as executor:
        futures = []
        for year in range(args.year_range[0], args.year_range[1] + 1):
            for month in range(args.month_range[0], args.month_range[1] + 1):
                _, days_in_month = calendar.monthrange(year, month)
                day_start, day_end = args.day_range if args.day_range else (1, days_in_month)
                for day in range(day_start, day_end + 1):
                    logging.info(f"Starting year:{year}, month:{month}, day:{day} in 5 seconds.")
                    time.sleep(5)
                    for hour in range(24):
                        start_time = datetime(year, month, day, hour, 0, tzinfo=timezone.utc)
                        end_time = start_time + timedelta(hours=1)
                        futures.append(
                            executor.submit(
                                process_time_range, args.query, start_time, end_time, args.output_dir
                            )
                        )

        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logging.error(f"❌ Error processing a time range: {e}")

if __name__ == "__main__":
    main()
