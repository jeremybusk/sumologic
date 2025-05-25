#!/usr/bin/env python3
import os
import json
import time
import gzip
from datetime import datetime, timedelta, timezone
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Semaphore

# Constants
PROTOCOL = "https"
HOST = os.getenv("SUMO_HOST", "api.us2.sumologic.com")
SUMO_API_URL = f"{PROTOCOL}://{HOST}/api/v1/search/jobs"
API_ACCESS_ID = os.getenv("SUMO_ACCESS_ID")
API_ACCESS_KEY = os.getenv("SUMO_ACCESS_KEY")
SEARCH_JOB_RESULTS_LIMIT = 10000  # Maximum messages per request
MAX_CONCURRENT_JOBS = 5  # Limit for concurrent active jobs
API_RATE_LIMIT_DELAY = 2  # Delay in seconds between API calls to avoid rate limiting

# Semaphore to limit concurrent jobs
job_semaphore = Semaphore(MAX_CONCURRENT_JOBS)

# Ensure output directories exist
def ensure_directory_exists(path):
    os.makedirs(path, exist_ok=True)

# Create a search job
def create_search_job(query, start_time, end_time):
    payload = {
        "query": query,
        "from": start_time.isoformat(),
        "to": end_time.isoformat(),
        "timeZone": "UTC"
    }
    response = requests.post(SUMO_API_URL, json=payload, auth=(API_ACCESS_ID, API_ACCESS_KEY))
    if response.status_code == 429:  # Rate limit exceeded
        time.sleep(API_RATE_LIMIT_DELAY)
        return create_search_job(query, start_time, end_time)
    response.raise_for_status()
    return response.json()["id"]

# Wait for the search job to complete
def wait_for_job_completion(job_id):
    status_url = f"{SUMO_API_URL}/{job_id}"
    while True:
        response = requests.get(status_url, auth=(API_ACCESS_ID, API_ACCESS_KEY))
        if response.status_code == 429:  # Rate limit exceeded
            time.sleep(API_RATE_LIMIT_DELAY)
            continue
        response.raise_for_status()
        data = response.json()
        if data["state"] == "DONE GATHERING RESULTS":
            return
        elif data["state"] in ["CANCELLED", "FAILED"]:
            raise Exception(f"Search job {job_id} failed with state: {data['state']}")
        time.sleep(5)

# Fetch all paginated messages from the search job
def fetch_all_messages(job_id):
    messages = []
    job_results_url = f"{SUMO_API_URL}/{job_id}/messages"
    offset = 0
    while True:
        params = {"limit": SEARCH_JOB_RESULTS_LIMIT, "offset": offset}
        response = requests.get(job_results_url, params=params, auth=(API_ACCESS_ID, API_ACCESS_KEY))
        if response.status_code == 429:  # Rate limit exceeded
            time.sleep(API_RATE_LIMIT_DELAY)
            continue
        response.raise_for_status()
        data = response.json()
        messages.extend(data.get("messages", []))
        if len(data.get("messages", [])) < SEARCH_JOB_RESULTS_LIMIT:
            break
        offset += SEARCH_JOB_RESULTS_LIMIT
    return messages

# Filter messages to ensure they fall within the exact minute
def filter_messages_by_minute(messages, start_time, end_time):
    filtered_messages = []
    for message in messages:
        timestamp = int(message["map"]["_messagetime"]) // 1000  # Convert to seconds
        message_time = datetime.fromtimestamp(timestamp, tz=timezone.utc)
        if start_time <= message_time < end_time:
            filtered_messages.append(message)
        else:
            print(f"⚠️ Message outside range: {message_time} (expected {start_time} to {end_time})")
    return filtered_messages

# Append messages to a gzip-compressed file
def append_messages_to_gzip_file(messages, file_path):
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
    print(f"✅ Flushed {len(messages)} messages to {file_path}")

# Save messages incrementally to disk
def save_messages_incrementally(messages, year, month, day, hour, minute, output_dir):
    directory = f"{output_dir}/{year}/{month:02}/{day:02}/{hour:02}"
    file_name = f"{minute:02}.json.gz"
    full_path = os.path.join(directory, file_name)
    ensure_directory_exists(directory)
    append_messages_to_gzip_file(messages, full_path)

# Process a single minute range
def process_minute_range(query, start_time, end_time, year, month, day, hour, minute, output_dir):
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
    parser.add_argument("--year", type=int, required=True, help="Year to query.")
    parser.add_argument("--month", type=int, required=True, help="Month to query (1-12).")
    parser.add_argument("--day", type=int, required=True, help="Day to query (1-31).")
    parser.add_argument("--output-dir", required=True, help="Directory to save the output files.")
    args = parser.parse_args()

    year = args.year
    month = args.month
    day = args.day
    query = args.query
    output_dir = args.output_dir

    # Iterate through each hour and minute of the specified day
    with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_JOBS) as executor:
        futures = []
        for hour in range(24):
            for minute in range(60):
                start_time = datetime(year, month, day, hour, minute, tzinfo=timezone.utc)
                end_time = start_time + timedelta(minutes=1)
                futures.append(
                    executor.submit(
                        process_minute_range, query, start_time, end_time, year, month, day, hour, minute, output_dir
                    )
                )

        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"❌ Error processing a time range: {e}")

if __name__ == "__main__":
    main()
