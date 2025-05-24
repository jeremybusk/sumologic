import argparse
import datetime
import calendar
import json
import logging
import os
import sqlite3
import time
import zstandard
import threading
import math # Import math for ceil and exp
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Tuple, Optional, Set

import requests
from requests.exceptions import RequestException, HTTPError

# --- Configuration Constants ---
DEFAULT_MESSAGES_PER_API_REQUEST = 10000
DEFAULT_JOB_POLL_INITIAL_DELAY_SECONDS = 5
DEFAULT_JOB_POLL_MAX_RETRIES = 60
DEFAULT_JOB_POLL_RETRY_INTERVAL_SECONDS = 10
DEFAULT_MAX_MESSAGES_PER_FILE = 100000
DEFAULT_CHUNK_MINUTES_IF_NOT_FOUND = 60
DEFAULT_ADAPTIVE_SHRINK_CONSECUTIVE_COUNT = 3
DEFAULT_ADAPTIVE_GROW_TRIGGER_MESSAGE_PERCENT = 0.5
DEFAULT_ADAPTIVE_GROW_CONSECUTIVE_COUNT = 5
DEFAULT_SPLIT_INTERVALS = "60,30,15,5,1"
DEFAULT_DB_PATH = "trun.db"
DEFAULT_MAX_CONCURRENT_API_CALLS = 5

# --- New Backoff Constants ---
DEFAULT_API_RETRY_INITIAL_DELAY_SECONDS = 1
DEFAULT_API_RETRY_MAX_DELAY_SECONDS = 60
DEFAULT_API_MAX_RETRIES = 10 # Total attempts including the first one
DEFAULT_API_RETRY_BACKOFF_FACTOR = 2 # Exponential backoff (delay * factor)

# --- Logging Setup ---
log = logging.getLogger(__name__)
log.setLevel(logging.INFO) # Default level, can be overridden by user args

# --- Helper for Environment Variables ---
def get_env_var(name: str, default: Optional[str] = None) -> Optional[str]:
    """Fetches an environment variable."""
    return os.environ.get(name, default)

def must_env(name: str) -> str:
    """Fetches an environment variable or raises an error if not found."""
    value = get_env_var(name)
    if value is None:
        raise ValueError(f"Environment variable '{name}' is not set.")
    return value

# --- Database Setup (for optimal chunk sizes) ---
class OptimalChunksDB:
    def __init__(self, db_path: str = DEFAULT_DB_PATH):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS optimal_chunks (
                query_hash TEXT NOT NULL,
                year INTEGER NOT NULL,
                month INTEGER NOT NULL,
                day INTEGER NOT NULL,
                hour INTEGER NOT NULL,
                optimal_minutes INTEGER NOT NULL,
                last_updated TEXT NOT NULL,
                PRIMARY KEY (query_hash, year, month, day, hour)
            )
        ''')
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS adaptive_metrics (
                query_hash TEXT NOT NULL,
                year INTEGER NOT NULL,
                month INTEGER NOT NULL,
                day INTEGER NOT NULL,
                hour INTEGER NOT NULL,
                consecutive_over_limit_count INTEGER DEFAULT 0,
                consecutive_under_limit_count INTEGER DEFAULT 0,
                PRIMARY KEY (query_hash, year, month, day, hour)
            )
        ''')
        self.conn.commit()

    def get_optimal_chunk_minutes(self, query_hash: str, dt: datetime) -> Optional[int]:
        self.cursor.execute(
            "SELECT optimal_minutes FROM optimal_chunks WHERE query_hash=? AND year=? AND month=? AND day=? AND hour=?",
            (query_hash, dt.year, dt.month, dt.day, dt.hour)
        )
        result = self.cursor.fetchone()
        return result[0] if result else None

    def set_optimal_chunk_minutes(self, query_hash: str, dt: datetime, minutes: int):
        self.cursor.execute(
            """
            INSERT OR REPLACE INTO optimal_chunks
            (query_hash, year, month, day, hour, optimal_minutes, last_updated)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (query_hash, dt.year, dt.month, dt.day, dt.hour, minutes, datetime.now(timezone.utc).isoformat())
        )
        self.conn.commit()

    def get_adaptive_metrics(self, query_hash: str, dt: datetime) -> Tuple[int, int]:
        self.cursor.execute(
            "SELECT consecutive_over_limit_count, consecutive_under_limit_count FROM adaptive_metrics WHERE query_hash=? AND year=? AND month=? AND day=? AND hour=?",
            (query_hash, dt.year, dt.month, dt.day, dt.hour)
        )
        result = self.cursor.fetchone()
        return result if result else (0, 0)

    def update_adaptive_metrics(self, query_hash: str, dt: datetime,
                                over_limit_increment: int = 0, under_limit_increment: int = 0,
                                reset_over: bool = False, reset_under: bool = False):
        over_count, under_count = self.get_adaptive_metrics(query_hash, dt)

        if reset_over:
            over_count = 0
        else:
            over_count += over_limit_increment

        if reset_under:
            under_count = 0
        else:
            under_count += under_limit_increment

        self.cursor.execute(
            """
            INSERT OR REPLACE INTO adaptive_metrics
            (query_hash, year, month, day, hour, consecutive_over_limit_count, consecutive_under_limit_count)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (query_hash, dt.year, dt.month, dt.day, dt.hour, over_count, under_count)
        )
        self.conn.commit()

    def close(self):
        self.conn.close()

# --- Sumo Logic API Client ---
class SumoExporter:
    def __init__(self, access_id: str, access_key: str, endpoint: str, dry_run: bool = False,
                 api_retry_initial_delay: int = DEFAULT_API_RETRY_INITIAL_DELAY_SECONDS,
                 api_retry_max_delay: int = DEFAULT_API_RETRY_MAX_DELAY_SECONDS,
                 api_max_retries: int = DEFAULT_API_MAX_RETRIES,
                 api_retry_backoff_factor: int = DEFAULT_API_RETRY_BACKOFF_FACTOR):
        self.auth = (access_id, access_key)
        # Ensure endpoint ends with /api/v1 if not already present
        if not endpoint.endswith("/api/v1"):
            self.endpoint = endpoint.rstrip('/') + "/api/v1"
        else:
            self.endpoint = endpoint
            
        self.dry_run = dry_run
        self.headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}
        self.session = requests.Session() # Use a session for connection pooling

        self.api_retry_initial_delay = api_retry_initial_delay
        self.api_retry_max_delay = api_retry_max_delay
        self.api_max_retries = api_max_retries
        self.api_retry_backoff_factor = api_retry_backoff_factor
        self.retry_on_status_codes = {429, 500, 502, 503, 504} # Add 5xx for general server errors

    def _make_request(self, method: str, path: str, **kwargs) -> Dict[str, Any]:
        url = f"{self.endpoint}{path}"
        
        for attempt in range(self.api_max_retries):
            log.debug(f"Making {method} request to {url} (Attempt {attempt + 1}/{self.api_max_retries}) with kwargs: {kwargs}")
            try:
                response = self.session.request(method, url, auth=self.auth, headers=self.headers, **kwargs)
                response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
                return response.json()
            except HTTPError as e:
                if e.response is not None and e.response.status_code in self.retry_on_status_codes and attempt < self.api_max_retries - 1:
                    delay = min(self.api_retry_initial_delay * (self.api_retry_backoff_factor ** attempt), self.api_retry_max_delay)
                    log.warning(f"API request failed with status {e.response.status_code}. Retrying in {delay:.2f} seconds. URL: {url}")
                    log.debug(f"Response body: {e.response.text}") # Log body for debugging
                    time.sleep(delay)
                else:
                    log.error(f"API request failed after {attempt + 1} attempts: {e}")
                    if hasattr(e, 'response') and e.response is not None:
                        log.error(f"Response status: {e.response.status_code}")
                        log.error(f"Response body: {e.response.text}")
                    raise # Re-raise if not retryable or max retries reached
            except RequestException as e:
                log.error(f"API request failed (non-HTTP error): {e}")
                raise # Re-raise for other request exceptions

        # This part should ideally not be reached if max_retries is handled correctly
        # but is here for robustness.
        raise RequestException(f"API request failed after {self.api_max_retries} attempts.")


    def create_search_job(self, query: str, from_time: datetime, to_time: datetime) -> str:
        if self.dry_run:
            log.info(f"[Dry Run] Creating job for query: {query}, {from_time.isoformat()} to {to_time.isoformat()}")
            return f"DRYRUN_JOB_{int(time.time())}"

        data = {
            "query": query,
            "from": from_time.isoformat(timespec='seconds'),
            "to": to_time.isoformat(timespec='seconds'),
            "timeZone": "UTC"
        }
        response = self._make_request("POST", "/search/jobs", json=data)
        job_id = response.get("id")
        if not job_id:
            raise ValueError("Failed to get job ID from Sumo Logic response.")
        log.info(f"➕ Created Sumo Logic job: {job_id}")
        return job_id

    def get_job_status(self, job_id: str) -> Dict[str, Any]:
        if self.dry_run and "DRYRUN_JOB_" in job_id:
            time.sleep(1)
            return {"state": "DONE GATHERING RESULTS", "messageCount": 0, "recordCount": 0}

        return self._make_request("GET", f"/search/jobs/{job_id}")

    def stream_job_messages(self, job_id: str, limit: int = DEFAULT_MESSAGES_PER_API_REQUEST, max_messages_to_fetch: Optional[int] = None):
        offset = 0
        total_fetched = 0
        while True:
            if max_messages_to_fetch is not None and total_fetched >= max_messages_to_fetch:
                break

            current_limit = limit
            if max_messages_to_fetch is not None:
                current_limit = min(limit, max_messages_to_fetch - total_fetched)
                if current_limit <= 0:
                    break

            try:
                response = self._make_request("GET", f"/search/jobs/{job_id}/messages?limit={current_limit}&offset={offset}")
                messages = response.get("messages", [])
                if not messages:
                    break

                for msg_data in messages:
                    yield msg_data.get("map")
                    total_fetched += 1
                    if max_messages_to_fetch is not None and total_fetched >= max_messages_to_fetch:
                        break

                offset += len(messages)
                if len(messages) < limit:
                    break

            except RequestException as e:
                log.error(f"Error streaming messages for job {job_id}: {e}")
                break

        log.info(f"Finished streaming for job {job_id}. Total messages yielded: {total_fetched}")

    def delete_search_job(self, job_id: str):
        if self.dry_run and "DRYRUN_JOB_" in job_id:
            log.info(f"[Dry Run] Deleting job: {job_id}")
            return
        # Deletion attempts should also have some retry logic, but usually simpler
        # For simplicity, we'll use the general _make_request which now includes backoff
        self._make_request("DELETE", f"/search/jobs/{job_id}")
        log.info(f"🗑️ Deleted Sumo Logic job: {job_id}")

# --- Helper Functions ---
def generate_time_suffix(dt: datetime, granularity: str) -> str:
    """Generates a time suffix for filenames based on granularity."""
    if granularity == "minute":
        # Modified to include 'M' prefix for minutes
        return dt.strftime("%Y%b%dH%HM%M")
    elif granularity == "hour":
        return dt.strftime("%Y%b%dH%H")
    elif granularity == "day":
        return dt.strftime("%Y%b%d")
    elif granularity == "month":
        return dt.strftime("%Y%b")
    else:
        raise ValueError(f"Unsupported granularity: {granularity}")

def get_query_hash(query: str) -> str:
    """Generates a simple hash for the query string for DB keying."""
    import hashlib
    return hashlib.md5(query.encode('utf-8')).hexdigest()

def build_output_path(base_output_directory: str, file_prefix: str, output_granularity: str,
                      current_year: int, current_month_abbr: str, current_day: Optional[int],
                      current_hour: Optional[int], current_minute: Optional[int]) -> Tuple[str, str]:
    """Builds the full output directory and file path based on granularity."""
    current_month_num = list(calendar.month_abbr).index(current_month_abbr)

    output_dir_path = os.path.join(base_output_directory, str(current_year))

    if output_granularity in ["minute", "hour", "day", "month"]:
        output_dir_path = os.path.join(output_dir_path, current_month_abbr)
        if output_granularity in ["minute", "hour", "day"]:
            output_dir_path = os.path.join(output_dir_path, f"{current_day:02d}")
            if output_granularity in ["minute", "hour"]:
                output_dir_path = os.path.join(output_dir_path, f"H{current_hour:02d}")
                # No separate directory for minute, as it's part of the filename suffix
                # if output_granularity == "minute":
                #     output_dir_path = os.path.join(output_dir_path, f"M{current_minute:02d}")

    os.makedirs(output_dir_path, exist_ok=True)

    # Use the generate_time_suffix with the specific datetime object for this file
    filename_time_suffix = generate_time_suffix(
        datetime(current_year,
                 current_month_num,
                 current_day or 1,
                 current_hour or 0,
                 current_minute or 0,
                 tzinfo=timezone.utc),
        output_granularity
    )
    final_file_path = os.path.join(output_dir_path, f"{file_prefix}_{filename_time_suffix}.json.zst")

    return output_dir_path, final_file_path

def get_expected_file_paths_for_range(
    base_output_directory: str,
    file_prefix: str,
    output_granularity: str,
    chunk_start_time: datetime,
    chunk_end_time: datetime
) -> Set[str]:
    """
    Generates a set of all expected file paths for a given time range and output granularity.
    This is used to check for existing files before running a query.
    """
    expected_files = set()
    current_dt = chunk_start_time

    # Determine the step size based on output_granularity
    if output_granularity == "minute":
        step_delta = timedelta(minutes=1)
    elif output_granularity == "hour":
        step_delta = timedelta(hours=1)
    elif output_granularity == "day":
        step_delta = timedelta(days=1)
    elif output_granularity == "month":
        # Month step is more complex, handle below
        step_delta = timedelta(days=1) # Initial dummy, will adjust
    else:
        raise ValueError(f"Unsupported output granularity for file path check: {output_granularity}")

    while current_dt <= chunk_end_time:
        current_year = current_dt.year
        current_month_abbr = calendar.month_abbr[current_dt.month]
        current_day = current_dt.day if output_granularity in ["minute", "hour", "day"] else 1
        current_hour = current_dt.hour if output_granularity in ["minute", "hour"] else 0
        current_minute = current_dt.minute if output_granularity == "minute" else 0

        # build_output_path handles directory creation and filename suffix correctly
        # We only need the full file_path here.
        _, file_path = build_output_path(
            base_output_directory, file_prefix, output_granularity,
            current_year, current_month_abbr, current_day, current_hour, current_minute
        )
        expected_files.add(file_path)

        # Advance current_dt based on granularity
        if output_granularity == "month":
            # For month, increment month by month, handling year rollover
            if current_dt.month == 12:
                current_dt = current_dt.replace(year=current_dt.year + 1, month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
            else:
                current_dt = current_dt.replace(month=current_dt.month + 1, day=1, hour=0, minute=0, second=0, microsecond=0)
        else:
            current_dt += step_delta
            # Ensure we don't go past chunk_end_time's minute/hour/day if step is larger
            # This is implicitly handled by the while loop condition `current_dt <= chunk_end_time`
            # and by the fact that `build_output_path` uses the `current_dt`
            # The only edge case is if step_delta causes current_dt to jump way past chunk_end_time
            # before the loop finishes, which is fine, as it won't add more files.

    return expected_files


def write_messages_to_files(messages: List[Dict[str, Any]],
                            base_output_directory: str,
                            file_prefix: str,
                            output_granularity: str,
                            if_zero_messages_skip_file_write: bool,
                            overwrite_archive_file_if_exists: bool,
                            compressor: zstandard.ZstdCompressor,
                            dry_run: bool = False):
    if not messages and if_zero_messages_skip_file_write:
        log.info("Skipping file write as 0 messages received and skip_if_zero_messages is true.")
        return

    grouped_messages = {}
    for msg in messages:
        msg_timestamp_ms = msg.get('_messagetime')
        
        if msg_timestamp_ms is None:
            log.debug(f"'_messagetime' not found for message, using current UTC time. Message: {msg}")
            msg_dt = datetime.now(timezone.utc)
        else:
            try:
                msg_timestamp_ms = int(msg_timestamp_ms) 
                msg_dt = datetime.fromtimestamp(msg_timestamp_ms / 1000, tz=timezone.utc)
            except (ValueError, TypeError):
                log.warning(f"Could not parse '_messagetime' '{msg_timestamp_ms}' for message. Using current UTC time. Message: {msg}")
                msg_dt = datetime.now(timezone.utc)

        current_year = msg_dt.year
        current_month_abbr = calendar.month_abbr[msg_dt.month]
        current_day = msg_dt.day
        current_hour = msg_dt.hour
        current_minute = msg_dt.minute

        # Key by the appropriate granularity for grouping messages into files
        if output_granularity == "minute":
            key_dt = datetime(current_year, msg_dt.month, current_day, current_hour, current_minute, tzinfo=timezone.utc)
        elif output_granularity == "hour":
            key_dt = datetime(current_year, msg_dt.month, current_day, current_hour, tzinfo=timezone.utc)
        elif output_granularity == "day":
            key_dt = datetime(current_year, msg_dt.month, current_day, tzinfo=timezone.utc)
        elif output_granularity == "month":
            key_dt = datetime(current_year, msg_dt.month, 1, tzinfo=timezone.utc)
        else:
            raise ValueError(f"Unsupported output granularity: {output_granularity}")

        # Use the specific datetime object for the key to ensure correct suffix generation
        key = generate_time_suffix(key_dt, output_granularity)
        
        if key not in grouped_messages:
            grouped_messages[key] = {
                'year': current_year,
                'month_abbr': calendar.month_abbr[key_dt.month], # Use key_dt's month
                'day': key_dt.day,
                'hour': key_dt.hour,
                'minute': key_dt.minute,
                'messages': []
            }
        grouped_messages[key]['messages'].append(msg)

    for key, data in grouped_messages.items():
        output_dir, file_path = build_output_path(
            base_output_directory, file_prefix, output_granularity,
            data['year'], data['month_abbr'], data['day'], data['hour'], data['minute']
        )

        if dry_run:
            log.info(f"[Dry Run] Would write {len(data['messages'])} messages to {file_path}")
            continue

        if os.path.exists(file_path):
            if overwrite_archive_file_if_exists:
                log.warning(f"Overwriting existing file: {file_path}")
            else:
                log.error(f"File already exists and overwrite is false: {file_path}. Skipping.")
                continue

        try:
            with open(file_path, 'wb') as f:
                with compressor.stream_writer(f) as zstd_writer:
                    for msg in data['messages']:
                        json_line = json.dumps(msg) + '\n'
                        zstd_writer.write(json_line.encode('utf-8'))
            log.info(f"📦 Wrote {len(data['messages'])} messages to {file_path}")
        except IOError as e:
            log.error(f"Error writing to file {file_path}: {e}")

# --- Core Logic for Processing Chunks ---
def process_query_chunk(
    exporter: SumoExporter,
    sumo_query: str,
    chunk_start_time: datetime,
    chunk_end_time: datetime,
    job_marker_suffix: str,
    max_messages_per_file: int,
    messages_per_api_request: int,
    poll_initial_delay: int,
    base_output_directory: str,
    file_prefix: str,
    output_granularity: str,
    overwrite_archive_file_if_exists: bool,
    if_zero_messages_skip_file_write: bool,
    adaptive_shrink_consecutive_count: int,
    adaptive_grow_trigger_message_percent: float,
    adaptive_grow_consecutive_count: int,
    compressor: zstandard.ZstdCompressor,
    optimal_chunks_db: Optional[OptimalChunksDB],
    query_hash: Optional[str],
    optimal_minutes_for_hour: Optional[int], # This is the "parent" optimal minutes
    current_optimal_minutes_for_this_chunk: int, # This is the optimal minutes for the current chunk being processed
    split_intervals: List[int],
    depth: int = 0
) -> Tuple[int, bool, bool]:
    indent = "    " * depth
    job_id = None
    messages_count_for_chunk = 0
    was_split = False
    was_skipped = False
    query_executed = False

    # --- FILE EXISTENCE CHECK ---
    if not overwrite_archive_file_if_exists:
        expected_files = get_expected_file_paths_for_range(
            base_output_directory, file_prefix, output_granularity,
            chunk_start_time, chunk_end_time
        )
        
        # Only check if expected_files is not empty. If it's empty, it means the time range is invalid
        # or the granularity makes no sense for the time range (e.g. asking for minute files over a 0-duration span).
        # An empty set of expected files should not cause a skip.
        if expected_files: 
            all_expected_files_exist = True
            for fpath in expected_files:
                if not os.path.exists(fpath):
                    all_expected_files_exist = False
                    break
            
            if all_expected_files_exist: # Ensure there's at least one file to check
                log.info(f"{indent}✅ All expected files for chunk {job_marker_suffix} already exist. Skipping Sumo Logic query.")
                return 0, False, True # 0 messages, not split, was skipped

    try:
        job_id = exporter.create_search_job(sumo_query, chunk_start_time, chunk_end_time)
        query_executed = True

        for i in range(DEFAULT_JOB_POLL_MAX_RETRIES):
            status = exporter.get_job_status(job_id)
            state = status.get("state")
            messages_count_for_chunk = status.get("messageCount", 0)
            record_count = status.get("recordCount", 0)

            log.info(f"{indent}Status for job {job_marker_suffix}: State='{state}', Messages={messages_count_for_chunk}, Records={record_count}")

            if state == "DONE GATHERING RESULTS":
                break
            elif state in ["CANCELLED", "FAILED"]:
                log.error(f"{indent}Sumo Logic job {job_marker_suffix} failed or was cancelled. State: {state}")
                raise RequestException(f"Sumo Logic job failed or cancelled: {state}")
            else:
                time.sleep(poll_initial_delay if i == 0 else DEFAULT_JOB_POLL_RETRY_INTERVAL_SECONDS)
        else:
            log.error(f"{indent}Sumo Logic job {job_marker_suffix} did not complete in time. Last state: {state}")
            raise TimeoutError(f"Sumo Logic job {job_marker_suffix} timed out.")

        duration = chunk_end_time - chunk_start_time + timedelta(seconds=1)

        if messages_count_for_chunk > max_messages_per_file:
            log.info(f"{indent}Messages ({messages_count_for_chunk}) exceed max_messages_per_file ({max_messages_per_file}). Splitting chunk.")
            was_split = True

            new_split_duration_minutes = 1 # Fallback to 1 if no suitable interval found
            current_duration_minutes = int(duration.total_seconds() / 60)

            # Find the largest split interval that is smaller than the current duration
            for interval_minutes in split_intervals:
                if interval_minutes < current_duration_minutes and interval_minutes >= 1:
                    new_split_duration_minutes = interval_minutes
                    break
            
            split_granularity_duration = timedelta(minutes=new_split_duration_minutes)

            if new_split_duration_minutes == current_duration_minutes or current_duration_minutes == 1:
                log.warning(f"{indent}⚠️ Chunk {job_marker_suffix} ({chunk_start_time.isoformat()} → {chunk_end_time.isoformat()}) is at minimum effective granularity ({current_duration_minutes} min) but has {messages_count_for_chunk} messages (limit {max_messages_per_file}). Writing first {max_messages_per_file} messages.")
                # When at min granularity but still over limit, just fetch and write what we can
                messages_to_fetch = min(messages_count_for_chunk, max_messages_per_file) # Fetch up to max_messages_per_file
                messages = list(exporter.stream_job_messages(job_id, messages_per_api_request, max_messages_to_fetch=messages_to_fetch))
                write_messages_to_files(
                    messages, base_output_directory, file_prefix, output_granularity,
                    if_zero_messages_skip_file_write, overwrite_archive_file_if_exists, compressor, dry_run=exporter.dry_run
                )
            else:
                # Calculate number of sub-chunks based on the new split duration
                # Use math.ceil for accurate division
                num_sub_chunks = math.ceil(duration.total_seconds() / 60 / new_split_duration_minutes)
                num_sub_chunks = int(max(1, num_sub_chunks))

                messages_sum_from_subchunks = 0 # To accumulate messages from recursive calls

                for i in range(num_sub_chunks):
                    sub_chunk_start = chunk_start_time + i * split_granularity_duration
                    if sub_chunk_start > chunk_end_time: # Stop if sub-chunk start exceeds parent end
                        break

                    # Ensure sub_chunk_end doesn't go past parent chunk_end_time
                    sub_chunk_end = min(sub_chunk_start + split_granularity_duration - timedelta(seconds=1), chunk_end_time)

                    # Determine marker granularity for sub-jobs
                    if new_split_duration_minutes < 60:
                         marker_granularity = "minute"
                    else:
                         marker_granularity = "hour"
                    
                    sub_job_marker_suffix = generate_time_suffix(sub_chunk_start, marker_granularity)


                    sub_messages_count, sub_was_split, sub_was_skipped = process_query_chunk(
                        exporter, sumo_query, sub_chunk_start, sub_chunk_end,
                        sub_job_marker_suffix, max_messages_per_file, messages_per_api_request,
                        poll_initial_delay,
                        base_output_directory, file_prefix, output_granularity,
                        overwrite_archive_file_if_exists, if_zero_messages_skip_file_write,
                        adaptive_shrink_consecutive_count, adaptive_grow_trigger_message_percent,
                        adaptive_grow_consecutive_count, compressor,
                        optimal_chunks_db, query_hash, optimal_minutes_for_hour, # Pass parent optimal_minutes_for_hour
                        new_split_duration_minutes, # Pass the new (smaller) optimal minutes for this sub-chunk
                        split_intervals,
                        depth + 1
                    )
                    messages_sum_from_subchunks += sub_messages_count # Sum messages from all sub-chunks
                    if sub_was_split:
                        was_split = True
                    if sub_was_skipped:
                        was_skipped = True
                messages_count_for_chunk = messages_sum_from_subchunks # Update parent's count with sum of sub-chunks

        else: # messages_count_for_chunk <= max_messages_per_file
            if messages_count_for_chunk == 0 and if_zero_messages_skip_file_write:
                log.info(f"{indent}📬 Received 0 messages for job {job_marker_suffix}. Skipping file write.")
                was_skipped = True
            else:
                # If messages are within limit, stream and write all of them
                messages = list(exporter.stream_job_messages(job_id, messages_per_api_request, max_messages_to_fetch=messages_count_for_chunk))
                write_messages_to_files(
                    messages, base_output_directory, file_prefix, output_granularity,
                    if_zero_messages_skip_file_write, overwrite_archive_file_if_exists, compressor, dry_run=exporter.dry_run
                )

    except (RequestException, TimeoutError, ValueError) as e:
        log.error(f"{indent}Error processing job {job_marker_suffix}: {e}")
        messages_count_for_chunk = 0 # Indicate failure for adaptive logic

    finally:
        if job_id and query_executed:
            try:
                exporter.delete_search_job(job_id)
            except Exception as e:
                log.error(f"{indent}Failed to delete Sumo Logic job {job_id}: {e}")

    # Adaptive logic for the _original_ hour-level chunk (depth == 0)
    if optimal_chunks_db and query_hash and optimal_minutes_for_hour is not None and depth == 0:
        dt_for_metrics = datetime(chunk_start_time.year, chunk_start_time.month, chunk_start_time.day,
                                  chunk_start_time.hour, tzinfo=timezone.utc)
        current_over_count, current_under_count = optimal_chunks_db.get_adaptive_metrics(query_hash, dt_for_metrics)

        # For adaptive logic:
        # If was_split is True, it means the *initial* job (or any sub-job that led to this recursive call chain)
        # was too big and had to be broken down.
        # This means the current_optimal_minutes_for_this_hour (the parent's optimal) might be too large.
        if was_split: # This indicates the initial chunk was too large and triggered a split
            optimal_chunks_db.update_adaptive_metrics(query_hash, dt_for_metrics,
                                                      over_limit_increment=1, reset_under=True)
            new_over_count, _ = optimal_chunks_db.get_adaptive_metrics(query_hash, dt_for_metrics)
            
            if new_over_count >= adaptive_shrink_consecutive_count and optimal_minutes_for_hour > min(split_intervals):
                # Find the next smallest interval in split_intervals
                new_optimal_minutes = min(split_intervals) # Smallest possible fallback
                for interval in sorted(split_intervals, reverse=True): # Iterate descending
                    if interval < optimal_minutes_for_hour:
                        new_optimal_minutes = interval
                        break
                
                log.info(f"{indent}Adaptive: Shrinking optimal chunk size for {dt_for_metrics.isoformat()} from {optimal_minutes_for_hour} to {new_optimal_minutes} minutes due to consecutive over-limit. Resetting over-limit count.")
                optimal_chunks_db.set_optimal_chunk_minutes(query_hash, dt_for_metrics, new_optimal_minutes)
                optimal_chunks_db.update_adaptive_metrics(query_hash, dt_for_metrics, reset_over=True)

        elif not was_skipped and messages_count_for_chunk > 0: 
            # If the original chunk was not split, and it had messages, we can evaluate for growth
            # messages_count_for_chunk here represents the total messages for the *original* top-level chunk.
            
            if messages_count_for_chunk <= max_messages_per_file * adaptive_grow_trigger_message_percent:
                optimal_chunks_db.update_adaptive_metrics(query_hash, dt_for_metrics,
                                                          under_limit_increment=1, reset_over=True)
                new_under_count, _ = optimal_chunks_db.get_adaptive_metrics(query_hash, dt_for_metrics)

                # Check if the current optimal_minutes_for_hour can actually grow
                if new_under_count >= adaptive_grow_consecutive_count and optimal_minutes_for_hour < max(split_intervals):
                    new_optimal_minutes = max(split_intervals) # Largest possible fallback
                    # Find the smallest interval greater than current_optimal_minutes_for_this_hour
                    for interval in sorted(split_intervals): # Iterate ascending
                        if interval > optimal_minutes_for_hour:
                            new_optimal_minutes = interval
                            break
                    
                    log.info(f"{indent}Adaptive: Growing optimal chunk size for {dt_for_metrics.isoformat()} from {optimal_minutes_for_hour} to {new_optimal_minutes} minutes due to consecutive under-limit. Resetting under-limit count.")
                    optimal_chunks_db.set_optimal_chunk_minutes(query_hash, dt_for_metrics, new_optimal_minutes)
                    optimal_chunks_db.update_adaptive_metrics(query_hash, dt_for_metrics, reset_under=True)
            else: # If messages were within the "good" range or zero (and not skipped)
                optimal_chunks_db.update_adaptive_metrics(query_hash, dt_for_metrics, reset_over=True, reset_under=True)
        else: # Chunk was skipped (0 messages, or due to existing files) or was a failure scenario
            optimal_chunks_db.update_adaptive_metrics(query_hash, dt_for_metrics, reset_over=True, reset_under=True)

    # Return total messages yielded by this chunk (and its sub-chunks if split)
    return messages_count_for_chunk, was_split, was_skipped

def find_optimal_chunk_size(
    exporter: SumoExporter,
    sumo_query: str,
    search_start_time: datetime,
    max_minutes_for_search_window: int,
    max_messages_per_file: int,
    poll_initial_delay: int,
    dry_run: bool = False
) -> int:
    """
    Determines optimal chunk size in minutes for a given query and start time by iteratively testing.
    This is typically used for initial discovery or re-evaluation.
    """
    log.info(f"🔍 Determining optimal chunk size for query hash (partial), starting at {search_start_time.isoformat()} (max {max_minutes_for_search_window} min search window)")

    current_test_chunk_size_minutes = max_minutes_for_search_window
    
    # Ensure current_test_chunk_size_minutes is not 0
    if current_test_chunk_size_minutes == 0:
        current_test_chunk_size_minutes = 1

    while current_test_chunk_size_minutes >= 1:
        test_end_time = search_start_time + timedelta(minutes=current_test_chunk_size_minutes) - timedelta(seconds=1)
        job_id = None
        messages_count = 0

        try:
            job_id = exporter.create_search_job(sumo_query, search_start_time, test_end_time)

            for i in range(DEFAULT_JOB_POLL_MAX_RETRIES):
                status = exporter.get_job_status(job_id)
                state = status.get("state")
                messages_count = status.get("messageCount", 0)

                if state == "DONE GATHERING RESULTS":
                    break
                elif state in ["CANCELLED", "FAILED"]:
                    log.error(f"Job failed during optimal chunk size discovery: {state}")
                    return 1
                time.sleep(poll_initial_delay if i == 0 else DEFAULT_JOB_POLL_RETRY_INTERVAL_SECONDS)
            else:
                log.warning(f"Optimal chunk size discovery job timed out. State: {state}. Falling back to 1 minute.")
                return 1

            log.info(f"  Tested {current_test_chunk_size_minutes} minutes: {messages_count} messages.")

            if messages_count <= max_messages_per_file:
                return current_test_chunk_size_minutes

            # Shrink the test interval
            current_test_chunk_size_minutes //= 2
            if current_test_chunk_size_minutes == 0:
                current_test_chunk_size_minutes = 1

        except (RequestException, ValueError, TimeoutError) as e:
            log.error(f"Error during optimal chunk size discovery: {e}. Falling back to 1 minute.")
            return 1
        finally:
            if job_id:
                # The delete operation is also now handled by the _make_request with backoff
                exporter.delete_search_job(job_id)

    return 1

# --- Main Export Logic ---
def run_export(args):
    sumo_access_id = args.sumo_access_id or get_env_var("SUMO_ACCESS_ID")
    sumo_access_key = args.sumo_access_key or get_env_var("SUMO_ACCESS_KEY")
    sumo_api_endpoint = args.sumo_api_endpoint or get_env_var("SUMO_API_ENDPOINT", "https://api.sumologic.com") # Use root URL, SumoExporter will add /api/v1

    if not sumo_access_id:
        raise ValueError("Sumo Logic Access ID not provided. Use --sumo-access-id or set SUMO_ACCESS_ID environment variable.")
    if not sumo_access_key:
        raise ValueError("Sumo Logic Access Key not provided. Use --sumo-access-key or set SUMO_ACCESS_KEY environment variable.")
    
    db = OptimalChunksDB(args.db_path)
    # Initialize the semaphore for concurrent API *jobs* at the top-level hourly chunk
    api_job_semaphore = threading.Semaphore(args.max_concurrent_api_calls)
    exporter = SumoExporter(
        access_id=sumo_access_id, 
        access_key=sumo_access_key, 
        endpoint=sumo_api_endpoint, 
        dry_run=args.dry_run,
        api_retry_initial_delay=args.api_retry_initial_delay_seconds,
        api_retry_max_delay=args.api_retry_max_delay_seconds,
        api_max_retries=args.api_max_retries,
        api_retry_backoff_factor=args.api_retry_backoff_factor
    ) 
    query_hash = get_query_hash(args.sumo_query)
    compressor = zstandard.ZstdCompressor(level=3)

    split_intervals_parsed = []
    try:
        raw_intervals = [int(x.strip()) for x in args.split_intervals.split(',')]
        # Ensure 1 is always available as a minimum interval and sort descending for shrinking
        split_intervals_parsed = sorted(list(set([x for x in raw_intervals if x >= 1] + [1])), reverse=True)
        if not split_intervals_parsed:
            raise ValueError("No valid split intervals provided after parsing.")
    except ValueError as e:
        log.error(f"Invalid --split-intervals: {e}. Using default: {DEFAULT_SPLIT_INTERVALS}")
        split_intervals_parsed = sorted([int(x) for x in DEFAULT_SPLIT_INTERVALS.split(',')], reverse=True)

    log.info(f"Using split intervals: {split_intervals_parsed}")


    total_tasks = 0
    exported_messages_total = 0
    split_tasks_total = 0
    skipped_tasks_total = 0
    task_counter = 0

    target_years = args.years
    target_months = [m.lower() for m in args.months] if args.months else list(calendar.month_abbr)[1:]
    target_days = args.days
    target_hours = args.hours

    for year in target_years:
        for month_num, month_abbr in enumerate(calendar.month_abbr):
            if month_num == 0 or month_abbr.lower() not in target_months:
                continue
            
            if target_days:
                days_in_month = [d for d in target_days if 1 <= d <= calendar.monthrange(year, month_num)[1]]
            else:
                days_in_month = range(1, calendar.monthrange(year, month_num)[1] + 1)

            for day in days_in_month:
                hours_to_process = target_hours if target_hours else range(24)
                for hour in hours_to_process:
                    total_tasks += 1

    log.info(f"Prepared {total_tasks} tasks for data export.")

    for year in target_years:
        for month_num, month_abbr in enumerate(calendar.month_abbr):
            if month_num == 0 or month_abbr.lower() not in target_months:
                continue

            days_in_month = []
            if target_days:
                days_in_month = [d for d in target_days if 1 <= d <= calendar.monthrange(year, month_num)[1]]
            else:
                days_in_month = range(1, calendar.monthrange(year, month_num)[1] + 1)

            for day in days_in_month:
                hours_to_process = target_hours if target_hours else range(24)
                for hour in hours_to_process:
                    task_counter += 1
                    current_hour_start_dt = datetime(year, month_num, day, hour, 0, 0, tzinfo=timezone.utc)
                    current_hour_end_dt = datetime(year, month_num, day, hour, 59, 59, tzinfo=timezone.utc)

                    log.info(f"Processing Task {task_counter}/{total_tasks} for {current_hour_start_dt.isoformat()}")

                    current_optimal_minutes_for_this_hour = db.get_optimal_chunk_minutes(query_hash, current_hour_start_dt)
                    if current_optimal_minutes_for_this_hour is None:
                        if args.discover_optimal_chunk_sizes:
                            max_search_window_minutes = min(args.initial_optimal_chunk_search_minutes, 60)
                            # Acquire semaphore for optimal chunk size discovery
                            with api_job_semaphore:
                                current_optimal_minutes_for_this_hour = find_optimal_chunk_size(
                                    exporter, args.sumo_query, current_hour_start_dt,
                                    max_search_window_minutes, args.max_messages_per_file,
                                    args.job_poll_initial_delay_seconds, 
                                    dry_run=args.dry_run
                                )
                            db.set_optimal_chunk_minutes(query_hash, current_hour_start_dt, current_optimal_minutes_for_this_hour)
                        else:
                            current_optimal_minutes_for_this_hour = DEFAULT_CHUNK_MINUTES_IF_NOT_FOUND
                            log.info(f"No optimal chunk size found for {current_hour_start_dt.isoformat()}. Using default: {current_optimal_minutes_for_this_hour} minutes.")

                    minutes_to_process_in_this_pass = current_optimal_minutes_for_this_hour
                    num_sub_chunks_in_hour = math.ceil(60 / minutes_to_process_in_this_pass) # Use ceil for exact number
                    num_sub_chunks_in_hour = int(max(1, num_sub_chunks_in_hour))

                    messages_for_this_hour_total = 0
                    hour_was_split = False
                    hour_was_skipped = False
                    
                    # Acquire semaphore here for the entire hourly processing, including its splits
                    with api_job_semaphore: 
                        for i in range(num_sub_chunks_in_hour):
                            chunk_start_time = current_hour_start_dt + i * timedelta(minutes=minutes_to_process_in_this_pass)
                            if chunk_start_time > current_hour_end_dt:
                                break
                            chunk_end_time = min(chunk_start_time + timedelta(minutes=minutes_to_process_in_this_pass) - timedelta(seconds=1), current_hour_end_dt)

                            chunk_duration_minutes = int((chunk_end_time - chunk_start_time + timedelta(seconds=1)).total_seconds() / 60)
                            
                            # The marker granularity for sub-jobs should align with how time_suffix is generated
                            # If the chunk duration is < 60 minutes, it's typically a minute-level granularity for the marker.
                            # If it's 60 minutes, it's hour.
                            if chunk_duration_minutes < 60:
                                marker_granularity = "minute"
                            else:
                                marker_granularity = "hour"
                            
                            job_marker_suffix = generate_time_suffix(chunk_start_time, marker_granularity)


                            messages_count, was_split_this_subchunk, was_skipped_this_subchunk = process_query_chunk(
                                exporter, args.sumo_query, chunk_start_time, chunk_end_time,
                                job_marker_suffix, args.max_messages_per_file, args.messages_per_api_request,
                                args.job_poll_initial_delay_seconds,
                                args.base_output_directory, args.file_prefix, args.output_granularity,
                                args.overwrite_archive_file_if_exists, args.if_zero_messages_skip_file_write,
                                args.adaptive_shrink_consecutive_count, args.adaptive_grow_trigger_message_percent,
                                args.adaptive_grow_consecutive_count, compressor,
                                db, query_hash, current_optimal_minutes_for_this_hour, # Pass parent optimal_minutes_for_hour
                                minutes_to_process_in_this_pass,
                                split_intervals_parsed,
                                0 # Top level call for this specific sub-hour chunk (depth 0 for adaptive logic application)
                            )

                            messages_for_this_hour_total += messages_count
                            if was_split_this_subchunk:
                                hour_was_split = True
                            if was_skipped_this_subchunk:
                                hour_was_skipped = True

                    exported_messages_total += messages_for_this_hour_total
                    if hour_was_split:
                        split_tasks_total += 1
                    if hour_was_skipped:
                        skipped_tasks_total += 1

                    log.info(f"Task {task_counter}/{total_tasks} completed. Messages: {messages_for_this_hour_total}. Skipped: {hour_was_skipped}. Split: {hour_was_split}")

    db.close()
    log.info("--- Data Export Summary ---")
    log.info(f"Total tasks prepared: {total_tasks}")
    log.info(f"Total messages exported: {exported_messages_total}")
    log.info(f"Total tasks that required splitting: {split_tasks_total}")
    log.info(f"Total tasks skipped (0 messages): {skipped_tasks_total}")
    log.info("🚀 Sumo Logic data export script finished.")


def main():
    parser = argparse.ArgumentParser(description="Sumo Logic data export script.")
    parser.add_argument("--sumo-access-id", help="Sumo Logic Access ID (or set SUMO_ACCESS_ID env var).")
    parser.add_argument("--sumo-access-key", help="Sumo Logic Access Key (or set SUMO_ACCESS_KEY env var).")
    parser.add_argument("--sumo-api-endpoint",
                        help="Sumo Logic API endpoint (e.g., https://api.sumologic.com/ or https://api.us2.sumologic.com). Defaults to 'https://api.sumologic.com'. The script will append '/api/v1'.")
    parser.add_argument("--sumo-query", required=True, help="The Sumo Logic query string.")

    parser.add_argument("--base-output-directory", default="sumo-archive",
                        help="Base directory to save exported data.")
    parser.add_argument("--file-prefix", default="sumo_export",
                        help="Prefix for output filenames (e.g., 'sumo_export').")
    parser.add_argument("--output-granularity", choices=["month", "day", "hour", "minute"], default="hour",
                        help="Granularity for output file organization (e.g., 'hour' will create hourly files).")
    parser.add_argument("--overwrite-archive-file-if-exists", action="store_true",
                        help="Overwrite output file if it already exists.")
    parser.add_argument("--if-zero-messages-skip-file-write", action="store_true",
                        help="Skip writing a file if a chunk yields zero messages.")

    parser.add_argument("--years", nargs='+', type=int, default=[datetime.now().year],
                        help="One or more years to export data from (e.g., 2023 2024).")
    parser.add_argument("--months", nargs='+', type=str,
                        choices=[m.lower() for m in calendar.month_abbr[1:]],
                        help="One or more months (e.g., jan feb). Defaults to all months if not specified.")
    parser.add_argument("--days", nargs='+', type=int,
                        help="One or more days (1-31). Defaults to all days in specified months if not specified.")
    parser.add_argument("--hours", nargs='+', type=int, choices=range(24),
                        help="One or more hours (0-23). Defaults to all hours in specified days if not specified.")

    parser.add_argument("--max-messages-per-file", type=int, default=DEFAULT_MAX_MESSAGES_PER_FILE,
                        help=f"Maximum messages per output file before splitting time chunk. Default: {DEFAULT_MAX_MESSAGES_PER_FILE}")
    parser.add_argument("--messages-per-api-request", type=int, default=DEFAULT_MESSAGES_PER_API_REQUEST,
                        help=f"Messages to fetch per Sumo Logic 'get messages' API call. Default: {DEFAULT_MESSAGES_PER_API_REQUEST}")
    parser.add_argument("--job-poll-initial-delay-seconds", type=int, default=DEFAULT_JOB_POLL_INITIAL_DELAY_SECONDS,
                        help=f"Initial delay in seconds before polling Sumo Logic job status. Default: {DEFAULT_JOB_POLL_INITIAL_DELAY_SECONDS}")

    parser.add_argument("--discover-optimal-chunk-sizes", action="store_true",
                        help="Enable initial discovery of optimal chunk sizes for each hour before export.")
    parser.add_argument("--initial-optimal-chunk-search-minutes", type=int, default=DEFAULT_CHUNK_MINUTES_IF_NOT_FOUND,
                        help=f"Initial max minutes to test when discovering optimal chunk size. Default: {DEFAULT_CHUNK_MINUTES_IF_NOT_FOUND}")
    parser.add_argument("--adaptive-shrink-consecutive-count", type=int, default=DEFAULT_ADAPTIVE_SHRINK_CONSECUTIVE_COUNT,
                        help=f"Number of consecutive over-limit chunks before optimal size shrinks. Default: {DEFAULT_ADAPTIVE_SHRINK_CONSECUTIVE_COUNT}")
    parser.add_argument("--adaptive-grow-trigger-message-percent", type=float, default=DEFAULT_ADAPTIVE_GROW_TRIGGER_MESSAGE_PERCENT,
                        help=f"Percentage of max_messages_per_file (0.0-1.0) to trigger optimal size growth. Default: {DEFAULT_ADAPTIVE_GROW_TRIGGER_MESSAGE_PERCENT}")
    parser.add_argument("--adaptive-grow-consecutive-count", type=int, default=DEFAULT_ADAPTIVE_GROW_CONSECUTIVE_COUNT,
                        help=f"Number of consecutive under-limit chunks before optimal size grows. Default: {DEFAULT_ADAPTIVE_GROW_CONSECUTIVE_COUNT}")
    parser.add_argument("--default-chunk-minutes-if-not-found", type=int, default=DEFAULT_CHUNK_MINUTES_IF_NOT_FOUND,
                        help=f"Default chunk size in minutes to use if no optimal size is found in DB. Default: {DEFAULT_CHUNK_MINUTES_IF_NOT_FOUND}")

    parser.add_argument("--split-intervals", type=str, default=DEFAULT_SPLIT_INTERVALS,
                        help=f"Comma-separated list of minute intervals (e.g., '60,30,15,5,1') to try when splitting a chunk that exceeds max-messages-per-file. Intervals should be sorted largest to smallest. Default: '{DEFAULT_SPLIT_INTERVALS}'")

    parser.add_argument("--dry-run", action="store_true",
                        help="Simulate the process without making actual API calls or writing files.")

    parser.add_argument("--max-concurrent-api-calls", type=int, default=DEFAULT_MAX_CONCURRENT_API_CALLS,
                        help=f"Maximum number of concurrent Sumo Logic API calls. Default: {DEFAULT_MAX_CONCURRENT_API_CALLS}")
    parser.add_argument("--db-path", type=str, default=DEFAULT_DB_PATH,
                        help=f"Path to the SQLite database file for storing optimal chunk sizes. Default: {DEFAULT_DB_PATH}")
    parser.add_argument("--log-file", type=str,
                        help="Path to a file where logs will be written in addition to console output.")
    
    # New arguments for API backoff
    parser.add_argument("--api-retry-initial-delay-seconds", type=int, default=DEFAULT_API_RETRY_INITIAL_DELAY_SECONDS,
                        help=f"Initial delay for API call retries in seconds. Default: {DEFAULT_API_RETRY_INITIAL_DELAY_SECONDS}")
    parser.add_argument("--api-retry-max-delay-seconds", type=int, default=DEFAULT_API_RETRY_MAX_DELAY_SECONDS,
                        help=f"Maximum delay for API call retries in seconds. Default: {DEFAULT_API_RETRY_MAX_DELAY_SECONDS}")
    parser.add_argument("--api-max-retries", type=int, default=DEFAULT_API_MAX_RETRIES,
                        help=f"Maximum number of retries for API calls (excluding the first attempt). Default: {DEFAULT_API_MAX_RETRIES}")
    parser.add_argument("--api-retry-backoff-factor", type=int, default=DEFAULT_API_RETRY_BACKOFF_FACTOR,
                        help=f"Factor by which to increase the delay between API retries (e.g., 2 for exponential). Default: {DEFAULT_API_RETRY_BACKOFF_FACTOR}")

    args = parser.parse_args()

    # Configure logging: clear existing handlers, then add new ones based on args
    for handler in log.handlers[:]: 
        log.removeHandler(handler)
    log.propagate = False 

    # Always add a console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    log.addHandler(console_handler)

    # Add file handler if specified
    if args.log_file:
        file_handler = logging.FileHandler(args.log_file)
        file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        log.addHandler(file_handler)

    if args.months:
        args.months = [m.lower() for m in args.months]

    if not (0.0 <= args.adaptive_grow_trigger_message_percent <= 1.0):
        parser.error("--adaptive-grow-trigger-message-percent must be between 0.0 and 1.0.")

    log.info("🚀 Starting Sumo Logic data export script...")
    log.info(f"🗃️ Optimal chunks DB will be used at: {args.db_path}")
    log.info(f"⚡ Max concurrent API calls: {args.max_concurrent_api_calls}")
    log.info(f"♻️ API Retry Policy: Initial Delay={args.api_retry_initial_delay_seconds}s, Max Delay={args.api_retry_max_delay_seconds}s, Max Retries={args.api_max_retries}, Backoff Factor={args.api_retry_backoff_factor}")


    run_export(args)

if __name__ == "__main__":
    main()
