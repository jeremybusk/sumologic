import os
import sys
import json
import time
import logging
import calendar
import requests
import zstandard as zstd
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from threading import Semaphore
from concurrent.futures import ThreadPoolExecutor, as_completed
import argparse

LOG_FILE = None
SUMO_HTTP_API_BACKOFF_SECONDS = None
GLOBAL_OPTIMAL_CHUNK_MINUTES = None # This will store the adaptively determined chunk size
GLOBAL_CONSECUTIVE_GROW_COUNT = 0 # Helps in adaptive logic for growing chunk size
FILES_STORED_BY = "minute" # This remains constant, indicating output file granularity

def must_env(key):
    """Retrieves an environment variable or exits if not found."""
    val = os.getenv(key)
    if not val:
        logging.critical(f"Missing required environment variable: {key}")
        sys.exit(1)
    return val

def build_output_path(container, prefix, year, month_abbr, day, hour, minute):
    """Constructs the hierarchical output directory path."""
    path_parts = [container, prefix, str(year), month_abbr, f"{day:02}", f"H{hour:02}", f"M{minute:02}"]
    return os.path.join(*path_parts)

def check_file_exists(args, year, month, day, hour, minute):
    """Checks if an archived file for a given minute already exists."""
    month_abbr = calendar.month_abbr[month]
    output_dir = build_output_path(args.container_name, args.prefix, year, month_abbr, day, hour, minute)
    filename = f"{args.prefix}_{year:04}{month_abbr}{day:02}H{hour:02}M{minute:02}.json.zst"
    full_path = os.path.join(output_dir, filename)
    return os.path.exists(full_path)

class SumoExporter:
    """Handles communication with the Sumo Logic Search Job API."""
    def __init__(self, access_id, access_key, api_endpoint, api_request_rate_limit, api_rate_limit_backoff_seconds):
        self.session = requests.Session()
        self.session.auth = (access_id, access_key)
        self.session.headers.update({"Content-Type": "application/json", "Accept": "application/json"})
        self.api_endpoint = api_endpoint.rstrip('/')
        # Semaphore controls the number of concurrent API *requests* per second.
        # Sumo Logic API general rate limit is typically 4 requests/sec.
        self.semaphore = Semaphore(api_request_rate_limit)
        self.backoff_seconds = api_rate_limit_backoff_seconds

    def _request_with_retry(self, method, url, **kwargs):
        """Executes an HTTP request with retry logic for timeouts and rate limits."""
        with self.semaphore:
            retry_count = 0
            while retry_count <= 5: # Max 5 retries for transient errors
                try:
                    resp = self.session.request(method, url, **kwargs)
                    if resp.status_code == 429: # Too Many Requests
                        logging.warning(f"Rate limit hit for {method} {url}. Backing off for {self.backoff_seconds}s. Retry count: {retry_count}")
                        time.sleep(self.backoff_seconds * (retry_count + 1))
                        retry_count += 1
                        continue
                    resp.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
                    return resp
                except requests.exceptions.Timeout as e:
                    logging.warning(f"Timeout for {method} {url}: {e}. Retrying in {self.backoff_seconds}s...")
                    time.sleep(self.backoff_seconds)
                except requests.RequestException as e:
                    # Catch all other requests.exceptions (e.g., ConnectionError, HTTPError)
                    logging.error(f"RequestException for {method} {url}: {e}. Retrying in 10s...")
                    time.sleep(10)
                retry_count += 1

            logging.error(f"Max retries exceeded for {method} {url}.")
            raise requests.RequestException(f"Max retries exceeded for {method} {url}")

    def create_job(self, query, time_from, time_to):
        """Creates a Sumo Logic search job."""
        payload = {"query": query, "from": time_from, "to": time_to, "timeZone": "UTC"}
        url = f"{self.api_endpoint}/api/v1/search/jobs"
        logging.debug(f"Creating job: {query} from {time_from} to {time_to}")
        resp = self._request_with_retry("POST", url, json=payload)
        return resp.json()["id"]

    def get_job_status(self, job_id):
        """Gets the status of a Sumo Logic search job."""
        url = f"{self.api_endpoint}/api/v1/search/jobs/{job_id}"
        resp = self._request_with_retry("GET", url)
        return resp.json()

    def wait_for_completion(self, job_id, poll_interval=5, initial_delay=5):
        """Polls a Sumo Logic job until it completes."""
        logging.debug(f"Waiting for job {job_id} completion. Initial delay: {initial_delay}s")
        time.sleep(initial_delay)
        while True:
            status = self.get_job_status(job_id)
            state = status.get("state")
            logging.debug(f"Job {job_id} state: {state}, Messages: {status.get('messageCount', 'N/A')}")
            if state == "DONE GATHERING RESULTS":
                return status
            if state in ["CANCELLED", "FAILED"]:
                error_msg = status.get("error", "Unknown error")
                logging.error(f"Job {job_id} {state}. Error: {error_msg}. Trace: {status.get('trace', 'N/A')}")
                raise Exception(f"Job {job_id} {state}. Error: {error_msg}")
            # Log warnings/errors from Sumo Logic job itself
            if state == "GATHERING RESULTS" and status.get("pendingWarnings"):
                logging.warning(f"Job {job_id} has pending warnings: {status.get('pendingWarnings')}")
            if state == "GATHERING RESULTS" and status.get("pendingErrors"):
                logging.error(f"Job {job_id} has pending errors: {status.get('pendingErrors')}")
            time.sleep(poll_interval)

    def stream_messages(self, job_id, limit_per_request=10000):
        """Streams all messages from a completed Sumo Logic search job."""
        # Sumo Logic API typically returns up to 10,000 messages per /messages endpoint call.
        # This function ensures all messages for a completed job are streamed by
        # continuously fetching with increasing offsets.
        offset = 0
        messages_fetched_count = 0
        logging.debug(f"Streaming messages for job {job_id}. Limit per request: {limit_per_request}")
        while True:
            params = {"limit": limit_per_request, "offset": offset}
            url = f"{self.api_endpoint}/api/v1/search/jobs/{job_id}/messages"

            resp = self._request_with_retry("GET", url, params=params)
            messages = resp.json().get("messages", [])

            if not messages:
                logging.debug(f"No more messages for job {job_id} at offset {offset}.")
                break

            for m in messages:
                yield m
                messages_fetched_count += 1

            offset += len(messages)
            logging.debug(f"Fetched {len(messages)} messages for job {job_id}. Total this stream: {messages_fetched_count}. Next offset: {offset}")

            # If the number of messages returned is less than the requested limit,
            # it means we've likely received all available messages from this job.
            if len(messages) < limit_per_request:
                break
        logging.info(f"Finished streaming for job {job_id}. Total messages yielded: {messages_fetched_count}")


def get_optimal_query_minutes(
    exporter, args, query, current_start_dt,
    current_chunk_minutes,
    adaptive_chunk_state
):
    """
    Determines an optimal time chunk size for queries based on message count,
    to stay within Sumo Logic's API message limit per job.
    """
    global GLOBAL_OPTIMAL_CHUNK_MINUTES, GLOBAL_CONSECUTIVE_GROW_COUNT

    # Use a unique key for the adaptive state, based on the query itself
    query_key_for_adaptive = f"{query[:50]}_adaptive_check" # Use first 50 chars as a identifier for this query

    if query_key_for_adaptive not in adaptive_chunk_state:
        adaptive_chunk_state[query_key_for_adaptive] = {
            'last_chunk_minutes': current_chunk_minutes,
            'last_message_count': -1, # Indicates no test run yet for this query key
            'consecutive_shrink_count': 0,
            'last_adaptive_check_time': datetime.now(timezone.utc)
        }

    state = adaptive_chunk_state[query_key_for_adaptive]

    # Use the current_chunk_minutes passed in as the test chunk size
    test_chunk_minutes = current_chunk_minutes
    if test_chunk_minutes == 0:
        test_chunk_minutes = 1 # Ensure minimum 1 minute for a test

    test_chunk_end_time = current_start_dt + timedelta(minutes=test_chunk_minutes) - timedelta(seconds=1)

    job_id = None
    message_count = 0
    try:
        job_id = exporter.create_job(query, current_start_dt.isoformat(), test_chunk_end_time.isoformat())
        status = exporter.wait_for_completion(job_id, initial_delay=args.job_status_poll_initial_delay_seconds)
        message_count = status.get("messageCount", 0)
        logging.info(f"Adaptive test: {current_start_dt.strftime('%Y-%m-%d %H:%M')} - {test_chunk_end_time.strftime('%H:%M')} (Test Chunk {test_chunk_minutes} min) yielded {message_count} messages (Limit: {args.job_message_limit}).")

    except Exception as e:
        logging.error(f"Error during adaptive chunk size test (size {test_chunk_minutes} min) for {current_start_dt.isoformat()}: {e}")
        if job_id: logging.error(f"Associated Job ID: {job_id}")

        # If the test query fails (e.g., Bad Request 400, Timeout), it's a strong signal to shrink.
        # Shrink more aggressively on hard errors, but not below 1 minute.
        proposed_chunk_minutes = max(1, test_chunk_minutes // 4)
        if proposed_chunk_minutes == test_chunk_minutes: # Avoid infinite loop if already 1 minute
            proposed_chunk_minutes = max(1, test_chunk_minutes - 1) # Just try 1 minute less if it's already small

        logging.info(f"Adaptive test failed. Shrinking test chunk from {test_chunk_minutes} to {proposed_chunk_minutes} min.")
        GLOBAL_OPTIMAL_CHUNK_MINUTES = proposed_chunk_minutes # Update global optimal
        GLOBAL_CONSECUTIVE_GROW_COUNT = 0 # Reset global grow count on failure
        state['consecutive_shrink_count'] += 1 # Increment local shrink count for this query key
        state['last_chunk_minutes'] = proposed_chunk_minutes
        state['last_message_count'] = message_count # Could be 0 or N/A on error
        state['last_adaptive_check_time'] = datetime.now(timezone.utc) # Reset timer for current check
        return proposed_chunk_minutes

    # Logic for shrinking or growing based on message_count
    proposed_chunk_minutes = test_chunk_minutes

    if message_count >= args.job_message_limit:
        logging.info(f"Messages ({message_count}) at {test_chunk_minutes} min exceed API limit. Shrinking.")
        proposed_chunk_minutes = max(1, test_chunk_minutes // 2)
        if proposed_chunk_minutes == test_chunk_minutes: # Ensure it actually shrinks if it was 1 minute
            proposed_chunk_minutes = 1 # Can't shrink below 1 minute

        state['consecutive_shrink_count'] += 1
        GLOBAL_CONSECUTIVE_GROW_COUNT = 0 # Reset global grow count on any shrink
        state['last_adaptive_check_time'] = datetime.now(timezone.utc) # Reset timer on a change

        if state['consecutive_shrink_count'] >= args.adaptive_shrink_consecutive_count:
            logging.info(f"Consecutive shrink limit reached ({args.adaptive_shrink_consecutive_count}). Settling global optimal to {proposed_chunk_minutes} minutes.")
            GLOBAL_OPTIMAL_CHUNK_MINUTES = proposed_chunk_minutes
            return proposed_chunk_minutes

    elif message_count > 0 and message_count < args.job_message_limit * (1 - args.adaptive_grow_trigger_message_percent / 100.0):
        # Only grow if we are significantly below the limit, as defined by adaptive_grow_trigger_message_percent
        logging.info(f"Messages ({message_count}) are well below API limit. Trying to grow chunk size.")

        new_size_candidate = test_chunk_minutes * 2
        proposed_chunk_minutes = min(args.search_job_time_window_initial_minutes, new_size_candidate) # Don't exceed the configured max initial chunk size
        proposed_chunk_minutes = max(1, proposed_chunk_minutes) # Ensure not less than 1

        GLOBAL_CONSECUTIVE_GROW_COUNT += 1 # Increment global grow count
        state['consecutive_shrink_count'] = 0 # Reset local shrink count on grow
        state['last_adaptive_check_time'] = datetime.now(timezone.utc) # Reset timer on a change

        if GLOBAL_CONSECUTIVE_GROW_COUNT >= args.adaptive_grow_consecutive_count:
            logging.info(f"Consecutive grow limit reached ({args.adaptive_grow_consecutive_count}). Settling global optimal to {proposed_chunk_minutes} minutes.")
            GLOBAL_OPTIMAL_CHUNK_MINUTES = proposed_chunk_minutes
            GLOBAL_CONSECUTIVE_GROW_COUNT = 0 # Reset after reaching optimal grow to stabilize
            return proposed_chunk_minutes
    else:
        # Messages are in an acceptable range (not too high, not too low).
        logging.debug(f"Messages ({message_count}) at {test_chunk_minutes} min are in acceptable range. Maintaining chunk size.")
        state['consecutive_shrink_count'] = 0
        GLOBAL_CONSECUTIVE_GROW_COUNT = 0 # Reset if in acceptable range, it's not "consecutively" growing/shrinking
        proposed_chunk_minutes = test_chunk_minutes
        state['last_adaptive_check_time'] = datetime.now(timezone.utc) # Update last check time if we are stable

    state['last_chunk_minutes'] = proposed_chunk_minutes
    state['last_message_count'] = message_count

    return proposed_chunk_minutes

def write_file_for_minute(data, args, dt_minute):
    """Writes the collected messages for a specific minute to a Zstandard compressed JSON file."""
    if not data:
        logging.debug(f"No data to write for minute {dt_minute.isoformat()}.")
        if args.no_file_if_zero_messages:
            logging.info("Skipping file write due to no_file_if_zero_messages set and zero messages.")
            return

    current_year = dt_minute.year
    current_month_abbr = calendar.month_abbr[dt_minute.month]
    current_day = dt_minute.day
    current_hour = dt_minute.hour
    current_minute = dt_minute.minute

    output_dir_path = build_output_path(args.container_name, args.prefix,
                                         current_year, current_month_abbr,
                                         current_day, current_hour, current_minute)
    os.makedirs(output_dir_path, exist_ok=True)

    filename = f"{args.prefix}_{current_year:04}{current_month_abbr}{current_day:02}H{current_hour:02}M{current_minute:02}.json.zst"
    final_file_path = os.path.join(output_dir_path, filename)

    if os.path.exists(final_file_path):
        if args.overwrite_if_archive_file_exists:
            logging.info(f"Output file {final_file_path} exists. Overwriting as requested.")
        else:
            logging.info(f"Output file {final_file_path} exists. Skipping write (use --overwrite-if-archive-file-exists to force overwrite).")
            return

    logging.info(f"Attempting to save {len(data)} messages to: {final_file_path}")
    cctx = zstd.ZstdCompressor()
    try:
        with open(final_file_path, "wb") as f:
            json_data = json.dumps(data, indent=2).encode("utf-8")
            compressed_data = cctx.compress(json_data)
            f.write(compressed_data)
        logging.info(f"Saved: {final_file_path} ({len(data)} messages)")
    except TypeError as e:
        logging.error(f"Error serializing data for {final_file_path}: {e}. First item (partial): {str(data[0])[:200] if data else 'N/A'}")
    except Exception as e:
        logging.error(f"Error writing file {final_file_path}: {e}")

def process_query_chunk_recursive(exp, args, query, chunk_start_dt, chunk_end_dt, job_identifier_suffix, current_chunk_minutes, adaptive_chunk_state):
    """
    Recursively processes a time chunk, splitting it if the message count exceeds the API limit.
    This ensures all messages are fetched and archived, handling Sumo Logic's 100,000 message limit per job.
    """
    logging.info(f"Processing chunk {job_identifier_suffix}: {chunk_start_dt.isoformat()} -> {chunk_end_dt.isoformat()}")

    job_id = None
    try:
        job_id = exp.create_job(query, chunk_start_dt.isoformat(), chunk_end_dt.isoformat())
        status = exp.wait_for_completion(job_id, initial_delay=args.job_status_poll_initial_delay_seconds)
        total_messages_for_job = status.get("messageCount", 0)

        logging.info(f"Sumo Logic job {job_identifier_suffix} ({chunk_start_dt.strftime('%H:%M')} - {chunk_end_dt.strftime('%H:%M')}) completed with {total_messages_for_job} messages.")

        if total_messages_for_job > args.job_message_limit:
            logging.warning(f"Messages ({total_messages_for_job}) for job {job_identifier_suffix} exceed job-message-limit ({args.job_message_limit}). Splitting this chunk recursively.")
            
            # Calculate duration in minutes for splitting
            duration_seconds = (chunk_end_dt - chunk_start_dt).total_seconds()
            # Ensure duration is at least 1 minute, handle partial minutes
            duration_minutes = int(duration_seconds / 60) + (1 if duration_seconds % 60 > 0 else 0)
            
            if duration_minutes <= 1:
                # This is a critical case: a 1-minute chunk still exceeds the message limit.
                # Data will be truncated by Sumo Logic's API if the limit is truly hard on a single job.
                # We fetch what's available and warn severely.
                logging.critical(f"A 1-minute chunk ({chunk_start_dt.isoformat()}) still exceeds {args.job_message_limit} messages. Data WILL BE TRUNCATED for this minute. Fetched {total_messages_for_job} available messages.")
                all_messages = list(exp.stream_messages(job_id, limit_per_request=10000)) # Fetch all available
                logging.info(f"Fetched {len(all_messages)} available messages for critically large 1-minute chunk {job_identifier_suffix}.")
            else:
                # Determine new smaller chunk size (halve it, but ensure at least 1 minute)
                new_chunk_minutes = max(1, duration_minutes // 2)
                
                # Split the current chunk into two parts
                mid_point = chunk_start_dt + timedelta(minutes=new_chunk_minutes)
                
                # Recursively process the two halves
                process_query_chunk_recursive(exp, args, query, chunk_start_dt, mid_point - timedelta(seconds=1), f"{job_identifier_suffix}-P1", new_chunk_minutes, adaptive_chunk_state)
                process_query_chunk_recursive(exp, args, query, mid_point, chunk_end_dt, f"{job_identifier_suffix}-P2", duration_minutes - new_chunk_minutes, adaptive_chunk_state)
                return # Crucial: don't process messages for this parent job as its children will handle it

        # If we reach here, the job's message count is within limits or it's a 1-minute chunk that was critically large
        # In either case, stream all available messages from this job.
        all_messages = list(exp.stream_messages(job_id, limit_per_request=10000))
        logging.info(f"Received {len(all_messages)} messages for job {job_identifier_suffix}.")

        # Group messages by their exact minute timestamp and write to files
        grouped_by_minute = defaultdict(list)
        for m in all_messages:
            raw_ts = m.get("map", {}).get("_messagetime", 0)
            try:
                ts = int(raw_ts) // 1000 # Convert milliseconds to seconds
                dt_utc = datetime.fromtimestamp(ts, timezone.utc)
                minute_start_dt = dt_utc.replace(second=0, microsecond=0)
                grouped_by_minute[minute_start_dt].append(m)
            except (ValueError, TypeError):
                logging.warning(f"Invalid timestamp '{raw_ts}' in message. Skipping message for file grouping.")
                continue

        current_minute_dt = chunk_start_dt.replace(second=0, microsecond=0)
        chunk_end_minute_dt = chunk_end_dt.replace(second=0, microsecond=0)
        # Iterate through every minute within the chunk's boundaries to ensure all minutes are considered
        # and files are created even if empty (unless --no-file-if-zero-messages is set).
        while current_minute_dt <= chunk_end_minute_dt:
            messages_for_minute = grouped_by_minute[current_minute_dt]
            write_file_for_minute(messages_for_minute, args, current_minute_dt)
            current_minute_dt += timedelta(minutes=1)

    except Exception as e:
        logging.error(f"Failed to process chunk {job_identifier_suffix} ({chunk_start_dt.isoformat()} to {chunk_end_dt.isoformat()}): {e}")
        if job_id: logging.error(f"Associated Job ID: {job_id}")


def main():
    global LOG_FILE, SUMO_HTTP_API_BACKOFF_SECONDS, GLOBAL_OPTIMAL_CHUNK_MINUTES, GLOBAL_CONSECUTIVE_GROW_COUNT, FILES_STORED_BY

    parser = argparse.ArgumentParser(description="Extract data from SumoLogic to compressed JSON files with dynamic chunk sizing and minute-level deduplication.")
    parser.add_argument("--query", required=True,
                        help="SumoLogic query string. IMPORTANT: To target specific data tiers, include '_dataTier=Frequent' or '_dataTier=Infrequent' at the beginning of your query, e.g., '_dataTier=Frequent | my_log_query'. If omitted, it defaults to Continuous Tier or inferred partitions.")
    parser.add_argument("--years", nargs="+", type=int, required=True, help="Year(s) to process.")
    parser.add_argument("--months", nargs="+", help="Month abbreviation(s) e.g., Jan Feb. Processes all if omitted.")
    parser.add_argument("--days", nargs="+", type=int, help="Day(s) of the month. Processes all if omitted.")

    parser.add_argument("--prefix", default="sumo_export", help="Prefix for output filenames and directories.")
    parser.add_argument("--container-name", default="sumo-archive", help="Root directory for archives.")

    parser.add_argument("--api-request-rate-limit", type=int, default=4,
                        help="Max concurrent API requests per second to SumoLogic. This helps manage the overall API rate limit (typically 4 req/sec). Max concurrent search jobs are higher (200, or 20 for Frequent tier queries), but this controls the API call rate.")
    parser.add_argument("--job-message-limit", type=int, default=100000,
                        help="Maximum messages allowed per single Sumo Logic search job (Sumo Logic's hard limit is 100,000). If a query chunk's message count exceeds this, it will be recursively split into smaller time ranges to ensure no data is lost.")
    parser.add_argument("--search-job-time-window-initial-minutes", type=int, default=60,
                        help="Initial duration in minutes for a search job's time window. This is the starting point for dynamic adjustments and the maximum allowed chunk size. For Infrequent Tier data, consider a larger value (e.g., 120, 240) if your data density is typically low to potentially reduce search job count and cost.")
    parser.add_argument("--job-status-poll-initial-delay-seconds", type=int, default=10, help="Initial delay (seconds) before polling a search job's status for the first time.")

    parser.add_argument("--overwrite-if-archive-file-exists", action="store_true", help="Overwrite the output file if it already exists.")
    parser.add_argument("--no-file-if-zero-messages", action="store_true", help="Do not create a file if a minute's processed data contains zero messages.")

    parser.add_argument("--adaptive-shrink-consecutive-count", type=int, default=1,
                        help="Number of consecutive query chunk size reductions (due to exceeding message limit) before the 'global optimal' chunk size is set to the newly shrunk size.")
    parser.add_argument("--adaptive-grow-trigger-message-percent", type=int, default=50,
                        help="Percentage threshold (e.g., 50 means < 50% of job-message-limit) below which a chunk's message count will trigger the adaptive logic to attempt to GROW the chunk size. This prevents unnecessarily small chunks for sparse data.")
    parser.add_argument("--adaptive-grow-consecutive-count", type=int, default=2,
                        help="Number of consecutive query chunk size increases (due to low message count) before the 'global optimal' chunk size is set to the newly grown size.")

    parser.add_argument("--adaptive-re-evaluation-interval-hours", type=int, default=12,
                        help="Interval in hours to periodically re-evaluate the global optimal chunk size, even if consecutive grow/shrink conditions aren't met. This helps adapt to changing data volumes over long periods. For very stable data or cost-sensitive Infrequent Tier, consider increasing this.")


    parser.add_argument("--log-file", type=str, default="sumo-query-to-files.log",
                        help="Path to the log file for script execution logs.")
    parser.add_argument("--api-rate-limit-backoff-seconds", type=int, default=8,
                        help="Seconds to back off when Sumo Logic API rate limit (429 Too Many Requests) is hit. This helps prevent being throttled.")

    args = parser.parse_args()

    LOG_FILE = args.log_file
    SUMO_HTTP_API_BACKOFF_SECONDS = args.api_rate_limit_backoff_seconds

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler(sys.stdout)],
    )

    sumo_access_id = must_env("SUMO_ACCESS_ID")
    sumo_access_key = must_env("SUMO_ACCESS_KEY")
    sumo_api_endpoint = must_env("SUMO_API_ENDPOINT")

    exporter = SumoExporter(sumo_access_id, sumo_access_key, sumo_api_endpoint,
                            api_request_rate_limit=args.api_request_rate_limit,
                            api_rate_limit_backoff_seconds=args.api_rate_limit_backoff_seconds)

    tasks_to_queue = []
    # adaptive_chunk_state will store the adaptive history per query string (or a hash of it)
    adaptive_chunk_state = {}

    for year in args.years:
        for month_num in range(1, 13):
            month_abbr = calendar.month_abbr[month_num]
            if args.months and month_abbr not in args.months:
                continue

            days_in_month = calendar.monthrange(year, month_num)[1]
            selected_days = args.days if args.days else range(1, days_in_month + 1)

            for day_num in selected_days:
                if day_num < 1 or day_num > days_in_month:
                    logging.warning(f"Day {day_num} is out of range for {month_abbr} {year}. Skipping.")
                    continue

                for hour_num in range(24):
                    current_hour_start_dt = datetime(year, month_num, day_num, hour_num, 0, 0, tzinfo=timezone.utc)
                    
                    current_block_start_dt = None # Marks the start of a contiguous block of missing minutes
                    
                    for minute_num in range(60):
                        current_minute_to_check_dt = current_hour_start_dt + timedelta(minutes=minute_num)
                        
                        file_exists_for_minute = check_file_exists(args, current_minute_to_check_dt.year, 
                                                                    current_minute_to_check_dt.month, 
                                                                    current_minute_to_check_dt.day, 
                                                                    current_minute_to_check_dt.hour, 
                                                                    current_minute_to_check_dt.minute)
                        
                        if not file_exists_for_minute:
                            if current_block_start_dt is None:
                                current_block_start_dt = current_minute_to_check_dt
                        
                        # Conditions to trigger processing of the current missing block:
                        # 1. It's the last minute of the hour.
                        # 2. The next minute's file already exists (meaning the current block of missing data ends here).
                        # 3. We have an active `current_block_start_dt` (meaning we've found at least one missing minute).
                        is_last_minute_of_hour = (minute_num == 59)
                        is_end_of_block = file_exists_for_minute or is_last_minute_of_hour
                        
                        if is_end_of_block and current_block_start_dt is not None:
                            # Determine the actual end of the block that needs to be queried.
                            # If file_exists_for_minute is True, the block ends *before* this minute.
                            # Otherwise (last minute of hour), it ends *at* this minute.
                            block_end_dt = current_minute_to_check_dt.replace(second=59, microsecond=999999)
                            if file_exists_for_minute:
                                block_end_dt = (current_minute_to_check_dt - timedelta(minutes=1)).replace(second=59, microsecond=999999)

                            # Determine the initial chunk size for this block using adaptive logic.
                            # We use GLOBAL_OPTIMAL_CHUNK_MINUTES which is updated by get_optimal_query_minutes.
                            current_adaptive_test_size = GLOBAL_OPTIMAL_CHUNK_MINUTES if GLOBAL_OPTIMAL_CHUNK_MINUTES is not None else args.search_job_time_window_initial_minutes

                            # Check if it's time for an adaptive re-evaluation for this specific query.
                            # This ensures we don't re-test too often for the same query.
                            query_adaptive_state = adaptive_chunk_state.get(f"{args.query[:50]}_adaptive_check", {})
                            last_adaptive_check_time = query_adaptive_state.get('last_adaptive_check_time', datetime.min.replace(tzinfo=timezone.utc))

                            if (datetime.now(timezone.utc) - last_adaptive_check_time) >= timedelta(hours=args.adaptive_re_evaluation_interval_hours) or GLOBAL_OPTIMAL_CHUNK_MINUTES is None:
                                logging.info(f"Triggering periodic adaptive chunk size re-evaluation for query beginning at {current_block_start_dt}. Last check: {last_adaptive_check_time.isoformat()}")
                                current_adaptive_test_size = get_optimal_query_minutes(
                                    exporter, args, args.query, current_block_start_dt,
                                    current_adaptive_test_size, adaptive_chunk_state
                                )
                                GLOBAL_OPTIMAL_CHUNK_MINUTES = current_adaptive_test_size # Update global after test
                            else:
                                logging.debug(f"Using current GLOBAL_OPTIMAL_CHUNK_MINUTES: {GLOBAL_OPTIMAL_CHUNK_MINUTES} for {current_block_start_dt}.")
                                current_adaptive_test_size = GLOBAL_OPTIMAL_CHUNK_MINUTES

                            # Now, use the (potentially updated) GLOBAL_OPTIMAL_CHUNK_MINUTES to segment the missing block
                            chunk_start_for_api = current_block_start_dt
                            while chunk_start_for_api <= block_end_dt:
                                chunk_end_for_api = min(
                                    chunk_start_for_api + timedelta(minutes=current_adaptive_test_size) - timedelta(seconds=1),
                                    block_end_dt
                                )
                                # Ensure chunk_end_for_api doesn't go into the next hour/day if current_adaptive_test_size crosses boundary
                                # This handles cases where adaptive_test_size might cause it to jump past the end of the current hour/day.
                                # Example: chunk_start_for_api is 10:50, test_size is 30. end should be 11:19, but should clip to 10:59 if block ends at hour.
                                if chunk_end_for_api.hour != chunk_start_for_api.hour and chunk_end_for_api.minute < chunk_start_for_api.minute:
                                     # This means the chunk crosses an hour boundary and the end minute is 'before' the start minute's hour (e.g., 10:50 -> 11:19).
                                     # This specific logic is more relevant if block_end_dt was precisely hourly.
                                     # Given block_end_dt is already correctly calculated to boundary, this might be redundant or could be simplified.
                                     # For robustness, if you want strictly hourly boundaries for sub-chunks:
                                     # chunk_end_for_api = min(chunk_end_for_api, chunk_start_for_api.replace(minute=59, second=59, microsecond=999999))
                                     pass # block_end_dt already handles overall boundary correctly

                                job_id_suffix_base = f"{chunk_start_for_api.year}{calendar.month_abbr[chunk_start_for_api.month]}{chunk_start_for_api.day:02}"
                                job_id_suffix = f"{job_id_suffix_base}H{chunk_start_for_api.hour:02}M{chunk_start_for_api.minute:02}"

                                # Add to queue for processing by the recursive function
                                tasks_to_queue.append((exporter, args, args.query, chunk_start_for_api, chunk_end_for_api, job_id_suffix, current_adaptive_test_size, adaptive_chunk_state))
                                
                                chunk_start_for_api += timedelta(minutes=current_adaptive_test_size)
                            
                            current_block_start_dt = None # Reset for next missing block

                        elif file_exists_for_minute:
                            # If we encounter an existing file, it breaks any contiguous block of missing data
                            current_block_start_dt = None

    logging.info(f"Prepared {len(tasks_to_queue)} initial tasks for data export.")
    if tasks_to_queue:
        # The ThreadPoolExecutor submits initial chunks. The recursive calls are then handled
        # by the same executor's threads, potentially creating more sub-tasks if chunks are split.
        with ThreadPoolExecutor(max_workers=args.api_request_rate_limit) as executor:
            futures = [executor.submit(process_query_chunk_recursive, *task_args) for task_args in tasks_to_queue]
            for i, future in enumerate(as_completed(futures)):
                try:
                    future.result()
                    logging.info(f"Initial Task {i+1}/{len(tasks_to_queue)} completed.")
                except Exception as e:
                    logging.error(f"Initial Task {i+1}/{len(tasks_to_queue)} failed: {e}")
    else:
        logging.info("No tasks to execute for data export based on the provided parameters.")

if __name__ == "__main__":
    main()
