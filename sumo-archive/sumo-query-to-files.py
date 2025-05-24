import os
import sys
import json
import time
import logging
import calendar
import requests
import zstandard as zstd
import sqlite3
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from threading import Semaphore
from concurrent.futures import ThreadPoolExecutor, as_completed
import argparse

# --- Configuration Constants ---
DEFAULT_BACKOFF_SECONDS = 8
DEFAULT_LOG_FILE = "sumo-query-to-files.log"
DEFAULT_DB_PATH = "sumo_export.db"
DEFAULT_FILE_PREFIX = "sumo_export"
DEFAULT_BASE_OUTPUT_DIRECTORY = "sumo-archive"
DEFAULT_OUTPUT_GRANULARITY = "month"
DEFAULT_MAX_CONCURRENT_API_CALLS = 4
DEFAULT_MAX_MESSAGES_PER_FILE = 100000
DEFAULT_INITIAL_OPTIMAL_CHUNK_SEARCH_MINUTES = 60
DEFAULT_JOB_POLL_INITIAL_DELAY_SECONDS = 10
DEFAULT_DEFAULT_CHUNK_MINUTES_IF_NOT_FOUND = 60
DEFAULT_ADAPTIVE_SHRINK_CONSECUTIVE_COUNT = 1
DEFAULT_ADAPTIVE_GROW_TRIGGER_MESSAGE_PERCENT = 50
DEFAULT_ADAPTIVE_GROW_CONSECUTIVE_COUNT = 2
DEFAULT_ADAPTIVE_RE_EVALUATION_INTERVAL_HOURS = 12

# API Paths
SEARCH_JOBS_API_PATH = "/api/v1/search/jobs"
MESSAGES_API_PATH_FORMAT = "/api/v1/search/jobs/{job_id}/messages"

# Global DB connection (will be initialized in main)
DB_CONN: sqlite3.Connection | None = None
ADAPTIVE_METRICS_BUFFER: list[tuple] = []
ADAPTIVE_METRICS_BUFFER_SIZE = 100 # Flush buffer every 100 entries

# --- Utility Functions ---
def must_env(key: str) -> str:
    """
    Retrieves an environment variable, exiting if it's not set.
    """
    val = os.getenv(key)
    if not val:
        logging.critical(f"🚨 Missing required environment variable: {key}")
        sys.exit(1)
    return val

def build_output_path(
    base_output_directory: str,
    file_prefix: str,
    year: int,
    month_abbr: str,
    day: int | None = None,
    hour: int | None = None,
    minute: int | None = None,
    output_granularity: str = DEFAULT_OUTPUT_GRANULARITY,
) -> str:
    """
    Constructs the file system path for storing exported data based on granularity.
    """
    path_parts = [base_output_directory, file_prefix, str(year), month_abbr]
    if output_granularity in ["day", "hour", "minute"] and day is not None:
        path_parts.append(f"{day:02}")
    if output_granularity in ["hour", "minute"] and hour is not None:
        path_parts.append(f"H{hour:02}")
    if output_granularity == "minute" and minute is not None:
        path_parts.append(f"M{minute:02}")
    return os.path.join(*path_parts)

def check_job_marker_exists(
    base_output_directory: str,
    file_prefix: str,
    job_marker_suffix: str,
    year: int,
    month_abbr: str,
    day: int | None = None,
    hour: int | None = None,
    minute: int | None = None,
    output_granularity: str = DEFAULT_OUTPUT_GRANULARITY,
) -> bool:
    """
    Checks if a marker file for a specific job chunk already exists.
    This marker indicates that the data for that chunk was processed.
    """
    path = build_output_path(
        base_output_directory, file_prefix, year, month_abbr, day, hour, minute, output_granularity
    )
    # The job_marker_suffix should uniquely identify the time chunk of the job.
    file_path = os.path.join(path, f"{file_prefix}_{job_marker_suffix}.json.zst")
    return os.path.exists(file_path)

def generate_time_suffix(dt: datetime, granularity: str) -> str:
    """
    Generates a time-based suffix (e.g., YYYYMonDD, YYYYMonDDHH, YYYYMonDDHHMM).
    """
    suffix = f"{dt.year:04}{calendar.month_abbr[dt.month]}{dt.day:02}"
    if granularity in ("hour", "minute"):
        suffix += f"H{dt.hour:02}"
    if granularity == "minute":
        suffix += f"M{dt.minute:02}"
    return suffix

# --- Database Functions ---
def init_db(db_path: str):
    """
    Initializes the SQLite database connection and creates necessary tables for
    optimal chunk sizes and adaptive metrics.
    """
    global DB_CONN
    try:
        DB_CONN = sqlite3.connect(db_path, timeout=10)
        cursor = DB_CONN.cursor()

        # Table for optimal chunk sizes
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS optimal_chunks (
                query_hash TEXT NOT NULL, /* Hashed query for keying */
                date TEXT NOT NULL, /* YYYY-MM-DD */
                hour INTEGER NOT NULL,
                chunk_minutes INTEGER NOT NULL,
                last_evaluated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (query_hash, date, hour)
            )
        """)
        # Add column if it doesn't exist for existing databases
        try:
            cursor.execute("ALTER TABLE optimal_chunks ADD COLUMN last_evaluated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
        except sqlite3.OperationalError:
            pass # Column already exists
        DB_CONN.commit()
        logging.info(f"🗃️ Optimal chunks DB initialized at {db_path}")

        # Table for adaptive metrics
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS adaptive_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                query_hash TEXT NOT NULL,
                date TEXT NOT NULL, /* YYYY-MM-DD */
                hour INTEGER NOT NULL,
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                actual_chunk_minutes INTEGER NOT NULL, /* The chunk size attempted for this specific run */
                messages_count INTEGER NOT NULL,
                is_split BOOLEAN NOT NULL DEFAULT 0, /* Was this chunk further split? */
                is_skipped BOOLEAN NOT NULL DEFAULT 0 /* Was this chunk skipped due to existing file? */
            )
        """)
        DB_CONN.commit()
        logging.info(f"🗃️ Adaptive metrics table initialized (part of {db_path})")

    except sqlite3.Error as e:
        logging.error(f"🚨 Database initialization error: {e}")
        sys.exit(1)

def get_optimal_chunk_info_from_db(query: str, date_str: str, hour: int) -> tuple[int, datetime] | None:
    """
    Retrieves the optimal chunk size and last evaluation time for a given query, date, and hour.
    Returns (chunk_minutes, last_evaluated_at) or None.
    """
    if not DB_CONN:
        logging.error("🚨 DB connection not initialized for get_optimal_chunk_info_from_db")
        return None
    query_hash = str(hash(query))[:255]
    try:
        cursor = DB_CONN.cursor()
        cursor.execute("SELECT chunk_minutes, last_evaluated_at FROM optimal_chunks WHERE query_hash = ? AND date = ? AND hour = ?",
                       (query_hash, date_str, hour))
        row = cursor.fetchone()
        if row:
            chunk_minutes = row[0]
            last_evaluated_at = datetime.fromisoformat(row[1]) if row[1] else datetime.min
            return (chunk_minutes, last_evaluated_at)
        return None
    except sqlite3.Error as e:
        logging.error(f"🚨 DB get_optimal_chunk_info_from_db error: {e}")
        return None

def store_optimal_chunk_minutes_in_db(query: str, date_str: str, hour: int, minutes: int, dry_run: bool = False):
    """
    Stores or updates the optimal chunk size for a given query, date, and hour in the database,
    updating last_evaluated_at.
    """
    if dry_run:
        logging.info(f"DRY RUN: Would store optimal chunk: Query hash (partial), Date {date_str}, Hour {hour} -> {minutes} min")
        return
    if not DB_CONN:
        logging.error("🚨 DB connection not initialized for store_optimal_chunk_minutes_in_db")
        return
    query_hash = str(hash(query))[:255]
    try:
        cursor = DB_CONN.cursor()
        cursor.execute("""
            INSERT OR REPLACE INTO optimal_chunks (query_hash, date, hour, chunk_minutes, last_evaluated_at)
            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
        """, (query_hash, date_str, hour, minutes))
        DB_CONN.commit()
        logging.debug(f"💾 Stored optimal chunk: Query hash {query_hash}, Date {date_str}, Hour {hour} -> {minutes} min")
    except sqlite3.Error as e:
        logging.error(f"🚨 DB store_optimal_chunk_minutes_in_db error: {e}")

def flush_adaptive_metrics_buffer(dry_run: bool = False):
    """
    Flushes the buffered adaptive metrics to the database.
    """
    global ADAPTIVE_METRICS_BUFFER
    if dry_run:
        if ADAPTIVE_METRICS_BUFFER:
            logging.info(f"DRY RUN: Would flush {len(ADAPTIVE_METRICS_BUFFER)} adaptive metrics to DB.")
        ADAPTIVE_METRICS_BUFFER = [] # Clear buffer even in dry run
        return

    if not ADAPTIVE_METRICS_BUFFER:
        return
    if not DB_CONN:
        logging.error("🚨 DB connection not initialized for flush_adaptive_metrics_buffer")
        return

    try:
        cursor = DB_CONN.cursor()
        cursor.executemany("""
            INSERT INTO adaptive_metrics
            (query_hash, date, hour, processed_at, actual_chunk_minutes, messages_count, is_split, is_skipped)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP, ?, ?, ?, ?)
        """, ADAPTIVE_METRICS_BUFFER)
        DB_CONN.commit()
        logging.debug(f"📊 Flushed {len(ADAPTIVE_METRICS_BUFFER)} adaptive metrics to DB.")
        ADAPTIVE_METRICS_BUFFER = [] # Clear buffer after successful flush
    except sqlite3.Error as e:
        logging.error(f"🚨 DB flush_adaptive_metrics_buffer error: {e}")
        # Keep buffer to retry later or for manual inspection if needed, or clear based on policy
        ADAPTIVE_METRICS_BUFFER = [] # For simplicity, clear on error to prevent infinite retries

def add_adaptive_metric_entry(query: str, date_str: str, hour: int,
                              actual_chunk_minutes: int, messages_count: int,
                              is_split: bool, is_skipped: bool, dry_run: bool = False):
    """
    Adds an entry to the adaptive_metrics buffer. Flushes if buffer size is reached.
    """
    if dry_run:
        logging.info(f"DRY RUN: Would add metric: {date_str} H{hour:02}, Chunk:{actual_chunk_minutes}, Msgs:{messages_count}, Split:{is_split}, Skipped:{is_skipped}")
        return

    query_hash = str(hash(query))[:255]
    ADAPTIVE_METRICS_BUFFER.append((query_hash, date_str, hour, actual_chunk_minutes, messages_count, is_split, is_skipped))
    if len(ADAPTIVE_METRICS_BUFFER) >= ADAPTIVE_METRICS_BUFFER_SIZE:
        flush_adaptive_metrics_buffer(dry_run=dry_run)

def get_recent_adaptive_metrics(query: str, date_str: str, hour: int, limit: int = 5) -> list[tuple]:
    """
    Retrieves recent adaptive metrics for a given query, date, and hour.
    Returns a list of (actual_chunk_minutes, messages_count, is_split, is_skipped) tuples.
    """
    if not DB_CONN:
        logging.error("🚨 DB connection not initialized for get_recent_adaptive_metrics")
        return []
    query_hash = str(hash(query))[:255]
    try:
        cursor = DB_CONN.cursor()
        cursor.execute("""
            SELECT actual_chunk_minutes, messages_count, is_split, is_skipped
            FROM adaptive_metrics
            WHERE query_hash = ? AND date = ? AND hour = ?
            ORDER BY processed_at DESC
            LIMIT ?
        """, (query_hash, date_str, hour, limit))
        return cursor.fetchall()
    except sqlite3.Error as e:
        logging.error(f"🚨 DB get_recent_adaptive_metrics error: {e}")
        return []

# --- SumoExporter Class ---
class SumoExporter:
    def __init__(self, access_id: str, access_key: str, api_endpoint: str, rate_limit: int, backoff_seconds: int, dry_run: bool = False):
        self.session = requests.Session()
        self.session.auth = (access_id, access_key)
        self.session.headers.update({"Content-Type": "application/json", "Accept": "application/json"})
        self.api_endpoint = api_endpoint.rstrip('/')
        self.semaphore = Semaphore(rate_limit)
        self.backoff_seconds = backoff_seconds
        self.dry_run = dry_run

    def _request_with_retry(self, method: str, url: str, **kwargs) -> requests.Response:
        """
        Handles HTTP requests with retries for rate limiting and transient errors.
        """
        if self.dry_run:
            logging.info(f"DRY RUN: Would make {method} request to {url}")
            # Simulate a successful response for dry run
            class MockResponse:
                status_code = 200
                def json(self): return {"id": "dry_run_job_id", "state": "DONE GATHERING RESULTS", "messageCount": 0}
                def raise_for_status(self): pass
            return MockResponse()

        with self.semaphore:
            retry_count = 0
            max_retries = 5
            while retry_count <= max_retries:
                try:
                    resp = self.session.request(method, url, **kwargs)
                    if resp.status_code == 429:  # Rate limit
                        logging.warning(f"🚦 Rate limit hit for {method} {url}. Backing off for {self.backoff_seconds * (retry_count + 1)}s. Retry count: {retry_count}")
                        time.sleep(self.backoff_seconds * (retry_count + 1))
                        retry_count += 1
                        continue
                    resp.raise_for_status()
                    return resp
                except requests.exceptions.Timeout as e:
                    logging.warning(f"⏱️ Timeout for {method} {url}: {e}. Retrying in {self.backoff_seconds}s...")
                    time.sleep(self.backoff_seconds)
                except requests.RequestException as e:
                    logging.error(f"🚨 RequestException for {method} {url}: {e}. Retrying in 10s...")
                    time.sleep(10)
                retry_count += 1

            logging.error(f"🚨 Max retries ({max_retries}) exceeded for {method} {url}.")
            raise requests.RequestException(f"Max retries exceeded for {method} {url}")

    def create_search_job(self, query: str, start_time: str, end_time: str) -> str:
        """
        Creates a Sumo Logic search job.
        """
        payload = {"query": query, "from": start_time, "to": end_time, "timeZone": "UTC"}
        url = f"{self.api_endpoint}{SEARCH_JOBS_API_PATH}"
        logging.debug(f"Creating job: {query} from {start_time} to {end_time}")
        resp = self._request_with_retry("POST", url, json=payload)
        return resp.json()["id"]

    def get_job_status(self, job_id: str) -> dict:
        """
        Retrieves the status of a Sumo Logic search job.
        """
        url = f"{self.api_endpoint}{SEARCH_JOBS_API_PATH}/{job_id}"
        resp = self._request_with_retry("GET", url)
        return resp.json()

    def wait_for_job_completion(self, job_id: str, poll_interval: int, initial_delay: int) -> dict:
        """
        Polls a Sumo Logic search job until it completes.
        Returns the final job status dictionary.
        """
        if self.dry_run:
            logging.info(f"DRY RUN: Would wait for job {job_id} completion.")
            return {"state": "DONE GATHERING RESULTS", "messageCount": 0}

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
                logging.error(f"🚨 Job {job_id} {state}. Error: {error_msg}. Trace: {status.get('trace', 'N/A')}")
                raise Exception(f"Job {job_id} {state}. Error: {error_msg}")
            if state == "GATHERING RESULTS" and status.get("pendingWarnings"):
                logging.warning(f"Job {job_id} has pending warnings: {status.get('pendingWarnings')}")
            if state == "GATHERING RESULTS" and status.get("pendingErrors"):
                logging.error(f"Job {job_id} has pending errors: {status.get('pendingErrors')}")
            time.sleep(poll_interval)

    def stream_job_messages(self, job_id: str, limit_per_request: int, max_messages_to_fetch: int | None = None):
        """
        Streams messages from a completed Sumo Logic search job.
        Yields individual message dictionaries.
        """
        if self.dry_run:
            logging.info(f"DRY RUN: Would stream messages for job {job_id}.")
            yield {} # Yield empty message to simulate a single iteration
            return

        offset = 0
        messages_fetched_count = 0
        logging.debug(f"Streaming messages for job {job_id}. Limit per request: {limit_per_request}")
        while True:
            actual_limit = limit_per_request
            if max_messages_to_fetch is not None:
                remaining_to_fetch = max_messages_to_fetch - messages_fetched_count
                if remaining_to_fetch <= 0:
                    break
                actual_limit = min(limit_per_request, remaining_to_fetch)

            if actual_limit == 0 and max_messages_to_fetch is not None:
                 break

            params = {"limit": actual_limit, "offset": offset}
            url = f"{self.api_endpoint}{MESSAGES_API_PATH_FORMAT.format(job_id=job_id)}"

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

            if len(messages) < actual_limit:
                break
            if max_messages_to_fetch is not None and messages_fetched_count >= max_messages_to_fetch:
                break
        logging.info(f"Finished streaming for job {job_id}. Total messages yielded: {messages_fetched_count}")

    def delete_search_job(self, job_id: str):
        """
        Deletes a Sumo Logic search job.
        """
        if self.dry_run:
            logging.info(f"DRY RUN: Would delete job {job_id}.")
            return
        url = f"{self.api_endpoint}{SEARCH_JOBS_API_PATH}/{job_id}"
        try:
            self._request_with_retry("DELETE", url)
            logging.info(f"🗑️ Deleted Sumo Logic job: {job_id}")
        except requests.RequestException as e:
            logging.error(f"🚨 Failed to delete Sumo Logic job {job_id}: {e}")


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
    Stores the optimal size in the database. This is typically used for initial discovery or re-evaluation.
    """
    logging.info(f"🔍 Determining optimal chunk size for query hash (partial), starting at {search_start_time.isoformat()} (max {max_minutes_for_search_window} min search window)")

    current_test_chunk_size_minutes = max_minutes_for_search_window
    date_str = search_start_time.strftime("%Y-%m-%d")
    hour = search_start_time.hour

    while current_test_chunk_size_minutes >= 1:
        logging.info(f"⏱️ Trying chunk size: {current_test_chunk_size_minutes} minutes from {search_start_time.isoformat()}")
        search_chunk_end_time = search_start_time + timedelta(minutes=current_test_chunk_size_minutes) - timedelta(seconds=1)

        job_id = None
        try:
            job_id = exporter.create_search_job(sumo_query, search_start_time.isoformat(), search_chunk_end_time.isoformat())
            status = exporter.wait_for_job_completion(job_id, poll_initial_delay)
            message_count = status.get("messageCount", 0)
            logging.info(f"📊 Chunk size {current_test_chunk_size_minutes} minutes yielded {message_count} messages (Limit: {max_messages_per_file}).")

            if message_count < max_messages_per_file:
                store_optimal_chunk_minutes_in_db(sumo_query, date_str, hour, current_test_chunk_size_minutes, dry_run=dry_run)
                return current_test_chunk_size_minutes

            if current_test_chunk_size_minutes == 1:
                logging.warning(f"⚠️ Optimal chunk size is 1 minute, but it still yields {message_count} messages (>= {max_messages_per_file}). Proceeding with 1 minute.")
                break

        except Exception as e:
            logging.error(f"🚨 Error during optimal chunk size test (size {current_test_chunk_size_minutes} min): {e}")
            if job_id:
                logging.info(f"Job ID was {job_id}")
            current_test_chunk_size_minutes //= 2
            continue
        finally:
            if job_id and not exporter.dry_run:
                exporter.delete_search_job(job_id)

        current_test_chunk_size_minutes //= 2

    logging.info(f"ℹ️ Defaulting to 1 minute chunk size for query hash (partial) at {search_start_time.isoformat()} after search.")
    store_optimal_chunk_minutes_in_db(sumo_query, date_str, hour, 1, dry_run=dry_run)
    return 1


def write_messages_to_files(
    messages: list[dict],
    base_output_directory: str,
    file_prefix: str,
    output_granularity: str,
    if_zero_messages_skip_file_write: bool,
    overwrite_archive_file_if_exists: bool,
    compressor: zstd.ZstdCompressor, # Pass pre-initialized compressor
    dry_run: bool = False
):
    """
    Writes a list of messages to compressed JSON files, grouping them by timestamp
    according to the specified output granularity.
    """
    if not messages and if_zero_messages_skip_file_write:
        logging.debug("No messages to write for this chunk and 'if_zero_messages_skip_file_write' is True. Skipping.")
        return

    grouped_by_file_target = defaultdict(list)
    for m in messages:
        raw_ts = m.get("map", {}).get("_messagetime", 0)
        try:
            ts = int(raw_ts) // 1000
            dt_utc = datetime.fromtimestamp(ts, timezone.utc)
        except (ValueError, TypeError):
            logging.warning(f"⚠️ Invalid timestamp '{raw_ts}' in message: {str(m)[:100]}. Skipping message for file grouping.")
            continue

        msg_year = dt_utc.year
        msg_month_abbr = calendar.month_abbr[dt_utc.month]
        msg_day = dt_utc.day
        msg_hour = dt_utc.hour
        msg_minute = dt_utc.minute

        if output_granularity == "minute":
            file_group_key = (msg_year, msg_month_abbr, msg_day, msg_hour, msg_minute)
        elif output_granularity == "hour":
            file_group_key = (msg_year, msg_month_abbr, msg_day, msg_hour)
        elif output_granularity == "day":
            file_group_key = (msg_year, msg_month_abbr, msg_day)
        else:  # month
            file_group_key = (msg_year, msg_month_abbr)

        grouped_by_file_target[file_group_key].append(m)

    for key_tuple, items_for_file in grouped_by_file_target.items():
        current_year = key_tuple[0]
        current_month_abbr = key_tuple[1]
        current_day = key_tuple[2] if len(key_tuple) > 2 else None
        current_hour = key_tuple[3] if len(key_tuple) > 3 else None
        current_minute = key_tuple[4] if len(key_tuple) > 4 else None

        output_dir_path = build_output_path(
            base_output_directory,
            file_prefix,
            current_year,
            current_month_abbr,
            current_day,
            current_hour,
            current_minute,
            output_granularity,
        )

        filename_time_suffix = generate_time_suffix(
            datetime(current_year,
                     list(calendar.month_abbr).index(current_month_abbr),
                     current_day or 1,
                     current_hour or 0,
                     current_minute or 0,
                     tzinfo=timezone.utc),
            output_granularity
        )
        final_file_path = os.path.join(output_dir_path, f"{file_prefix}_{filename_time_suffix}.json.zst")

        if if_zero_messages_skip_file_write and not items_for_file:
            logging.info(f"💨 Skipping empty file target: {final_file_path}")
            continue

        if not overwrite_archive_file_if_exists and os.path.exists(final_file_path):
            logging.info(f"☑️ Final target file {final_file_path} already exists and --overwrite-archive-file-if-exists is not set. Skipping write.")
            continue

        if dry_run:
            logging.info(f"DRY RUN: Would create directory {output_dir_path} and save {len(items_for_file)} messages to: {final_file_path}")
            continue

        os.makedirs(output_dir_path, exist_ok=True)
        logging.info(f"💾 Attempting to save {len(items_for_file)} messages to: {final_file_path}")
        try:
            with open(final_file_path, "wb") as f:
                json_data = json.dumps(items_for_file, indent=2).encode("utf-8")
                compressed_data = compressor.compress(json_data)
                f.write(compressed_data)
            logging.info(f"✅ Saved: {final_file_path} ({len(items_for_file)} messages)")
        except TypeError as e:
            logging.error(f"🚨 Error serializing data for {final_file_path}: {e}. First item (partial): {str(items_for_file[0])[:200] if items_for_file else 'N/A'}")
        except Exception as e:
            logging.error(f"🚨 Error writing file {final_file_path}: {e}")


def process_query_chunk(
    exporter: SumoExporter,
    sumo_query: str,
    chunk_start_time: datetime,
    chunk_end_time: datetime,
    job_marker_suffix: str,
    max_messages_per_file: int,
    poll_initial_delay: int,
    base_output_directory: str,
    file_prefix: str,
    output_granularity: str,
    overwrite_archive_file_if_exists: bool,
    if_zero_messages_skip_file_write: bool,
    adaptive_shrink_consecutive_count: int,
    adaptive_grow_trigger_message_percent: int,
    adaptive_grow_consecutive_count: int,
    compressor: zstd.ZstdCompressor, # Pass pre-initialized compressor
    # Context for adaptive metrics (passed from main, not for recursive calls)
    db_date_str: str | None,
    hour_num: int | None,
    query_hash_for_db: str | None,
    current_optimal_minutes_for_this_chunk: int, # The chunk size we are attempting for this run
    depth: int = 0,
) -> tuple[int, bool, bool] | None: # Return (messages_count, was_split, was_skipped) or None
    """
    Processes a single time chunk by querying Sumo Logic, retrieving messages,
    and writing them to files, potentially splitting if too many messages are found.
    Returns (message_count, was_split, was_skipped) or None if job failed.
    """
    indent = "  " * depth
    duration = chunk_end_time - chunk_start_time + timedelta(seconds=1)

    s_year, s_month_abbr, s_day, s_hour, s_minute = (
        chunk_start_time.year, calendar.month_abbr[chunk_start_time.month], chunk_start_time.day,
        chunk_start_time.hour, chunk_start_time.minute
    )

    was_skipped = False
    if not overwrite_archive_file_if_exists and check_job_marker_exists(
        base_output_directory, file_prefix, job_marker_suffix, s_year, s_month_abbr, s_day, s_hour, s_minute, output_granularity
    ):
        logging.info(f"{indent}⏭️ Skipping job {job_marker_suffix} ({chunk_start_time.isoformat()} to {chunk_end_time.isoformat()}) as its marker/output file exists and --overwrite-archive-file-if-exists is not set.")
        was_skipped = True
        # Only add metric if it's a top-level chunk (has DB context)
        if db_date_str and hour_num is not None and query_hash_for_db:
            add_adaptive_metric_entry(query_hash_for_db, db_date_str, hour_num, current_optimal_minutes_for_this_chunk, 0, False, True, dry_run=exporter.dry_run)
        return 0, False, was_skipped

    logging.info(f"{indent}🔎 Querying for job {job_marker_suffix}: {chunk_start_time.isoformat()} → {chunk_end_time.isoformat()}")

    job_id = None
    messages_count_for_chunk = 0
    was_split = False
    try:
        job_id = exporter.create_search_job(sumo_query, chunk_start_time.isoformat(), chunk_end_time.isoformat())
        status = exporter.wait_for_job_completion(job_id, poll_initial_delay)
        messages_count_for_chunk = status.get("messageCount", 0) # Use messageCount from status for consistency
        logging.info(f"{indent}📬 Received {messages_count_for_chunk} messages for job {job_marker_suffix}.")

        if messages_count_for_chunk > max_messages_per_file:
            logging.info(f"{indent}Messages ({messages_count_for_chunk}) exceed max_messages_per_file ({max_messages_per_file}). Splitting chunk.")
            was_split = True
            if duration > timedelta(minutes=1):
                split_granularity_duration = timedelta(hours=1)
                if duration <= timedelta(hours=1):
                    split_granularity_duration = timedelta(minutes=1)

                num_sub_chunks = (duration.total_seconds() + split_granularity_duration.total_seconds() - 1) // split_granularity_duration.total_seconds()
                num_sub_chunks = int(max(1, num_sub_chunks))

                for i in range(num_sub_chunks):
                    sub_chunk_start = chunk_start_time + i * split_granularity_duration
                    if sub_chunk_start > chunk_end_time:
                        break

                    sub_chunk_end = min(sub_chunk_start + split_granularity_duration - timedelta(seconds=1), chunk_end_time) # Corrected variable name

                    sub_job_marker_suffix = generate_time_suffix(sub_chunk_start, "minute")

                    # Recursive calls do not contribute to adaptive metrics for the parent chunk,
                    # so we don't pass the db_date_str, hour_num, query_hash_for_db to them.
                    # This ensures adaptive logic only uses the *initial* chunk's performance.
                    process_query_chunk(
                        exporter, sumo_query, sub_chunk_start, sub_chunk_end,
                        sub_job_marker_suffix, max_messages_per_file, poll_initial_delay,
                        base_output_directory, file_prefix, output_granularity,
                        overwrite_archive_file_if_exists, if_zero_messages_skip_file_write,
                        adaptive_shrink_consecutive_count, adaptive_grow_trigger_message_percent,
                        adaptive_grow_consecutive_count, compressor,
                        None, None, None, current_optimal_minutes_for_this_chunk, # No DB context for recursive calls
                        depth + 1
                    )
                # Only add metric for top-level chunk (has DB context)
                if db_date_str and hour_num is not None and query_hash_for_db:
                    add_adaptive_metric_entry(query_hash_for_db, db_date_str, hour_num, current_optimal_minutes_for_this_chunk, messages_count_for_chunk, was_split, was_skipped, dry_run=exporter.dry_run)
                return messages_count_for_chunk, was_split, was_skipped
            else:
                logging.warning(f"{indent}⚠️ Chunk {job_marker_suffix} ({chunk_start_time.isoformat()}) is at minimum 1-min granularity but has {messages_count_for_chunk} messages (limit {max_messages_per_file}). Writing first {max_messages_per_file} messages.")
                # Fetch messages again, but limited to max_messages_per_file for writing
                messages = list(exporter.stream_job_messages(job_id, max_messages_to_fetch=max_messages_per_file))

        else: # messages_count_for_chunk <= max_messages_per_file
            # Only fetch messages if not split and within limit
            messages = list(exporter.stream_job_messages(job_id, max_messages_to_fetch=messages_count_for_chunk))


        write_messages_to_files(
            messages, base_output_directory, file_prefix, output_granularity,
            if_zero_messages_skip_file_write, overwrite_archive_file_if_exists, compressor, dry_run=exporter.dry_run
        )
        # Only add metric for top-level chunk (has DB context)
        if db_date_str and hour_num is not None and query_hash_for_db:
            add_adaptive_metric_entry(query_hash_for_db, db_date_str, hour_num, current_optimal_minutes_for_this_chunk, messages_count_for_chunk, was_split, was_skipped, dry_run=exporter.dry_run)
        return messages_count_for_chunk, was_split, was_skipped

    except Exception as e:
        logging.error(f"{indent}🚨 Failed to process chunk {job_marker_suffix} ({chunk_start_time.isoformat()} to {chunk_end_time.isoformat()}): {e}")
        if job_id: logging.error(f"{indent}Associated Job ID: {job_id}")
        # Only add metric for top-level chunk (has DB context)
        if db_date_str and hour_num is not None and query_hash_for_db:
            add_adaptive_metric_entry(query_hash_for_db, db_date_str, hour_num, current_optimal_minutes_for_this_chunk, 0, False, False, dry_run=exporter.dry_run) # Record as 0 messages on failure
        return None # Indicate failure
    finally:
        if job_id and not exporter.dry_run:
            exporter.delete_search_job(job_id)


# --- Main Execution Helper Functions ---
def _setup_logging(log_file: str, verbose: bool, debug: bool):
    """Configures the logging system."""
    log_level = logging.INFO
    if verbose:
        log_level = logging.DEBUG
    if debug:
        log_level = logging.DEBUG # Can be refined for more verbose debug

    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )
    logging.info("🚀 Starting Sumo Logic data export script...")

def _initialize_exporter(args: argparse.Namespace) -> SumoExporter:
    """Initializes and returns the SumoExporter instance."""
    sumo_access_id = must_env("SUMO_ACCESS_ID")
    sumo_access_key = must_env("SUMO_ACCESS_KEY")
    sumo_api_endpoint = must_env("SUMO_API_ENDPOINT")
    return SumoExporter(
        access_id=sumo_access_id,
        access_key=sumo_access_key,
        api_endpoint=sumo_api_endpoint,
        rate_limit=args.max_concurrent_api_calls,
        backoff_seconds=args.backoff_seconds,
        dry_run=args.dry_run
    )

def _prepare_export_tasks(args: argparse.Namespace, exporter: SumoExporter) -> list[tuple]:
    """Prepares a list of tasks for data export or optimal chunk discovery."""
    tasks = []
    valid_months = {calendar.month_abbr[i] for i in range(1, 13)}

    for year in args.years:
        for month_num in range(1, 13):
            month_abbr = calendar.month_abbr[month_num]
            if args.months and month_abbr not in args.months:
                continue
            # No need to validate month_abbr against valid_months here
            # as it's directly derived from calendar.month_abbr

            days_in_month = calendar.monthrange(year, month_num)[1]
            selected_days = args.days if args.days else range(1, days_in_month + 1)

            for day_num in selected_days:
                if day_num < 1 or day_num > days_in_month:
                    logging.warning(f"⚠️ Day {day_num} is out of range for {month_abbr} {year}. Skipping.")
                    continue

                for hour_num in range(24):
                    current_hour_start_dt = datetime(year, month_num, day_num, hour_num, 0, 0, tzinfo=timezone.utc)
                    db_date_str = current_hour_start_dt.strftime("%Y-%m-%d")
                    query_hash_for_db = str(hash(args.sumo_query))[:255]

                    if args.discover_optimal_chunk_sizes:
                        logging.info(f"🔬 Discovery Mode: Finding optimal chunk size for query (truncated), date {db_date_str}, hour {hour_num:02}.")
                        max_search_window_minutes = min(args.initial_optimal_chunk_search_minutes, 60)
                        find_optimal_chunk_size(
                            exporter, args.sumo_query, current_hour_start_dt,
                            max_search_window_minutes, args.max_messages_per_file,
                            args.job_poll_initial_delay_seconds, dry_run=args.dry_run
                        )
                        logging.info(f"🔬 Discovery for {db_date_str} H{hour_num:02} complete.")
                    else:
                        optimal_chunk_info = get_optimal_chunk_info_from_db(args.sumo_query, db_date_str, hour_num)
                        current_optimal_minutes = args.default_chunk_minutes_if_not_found
                        last_evaluated_at = datetime.min

                        if optimal_chunk_info:
                            current_optimal_minutes, last_evaluated_at = optimal_chunk_info
                            logging.info(f"💾 DB: Found optimal chunk size for query (truncated) / {db_date_str} H{hour_num:02}: {current_optimal_minutes} min (Last eval: {last_evaluated_at.isoformat()}).")

                        re_evaluate_due_to_time = False
                        if args.adaptive_re_evaluation_interval_hours > 0 and last_evaluated_at != datetime.min:
                            time_since_last_eval = (datetime.now(timezone.utc) - last_evaluated_at).total_seconds() / 3600
                            if time_since_last_eval >= args.adaptive_re_evaluation_interval_hours:
                                logging.info(f"🔄 Re-evaluation triggered for {db_date_str} H{hour_num:02} "
                                             f"(last eval {time_since_last_eval:.1f} hrs ago).")
                                re_evaluate_due_to_time = True

                        if re_evaluate_due_to_time:
                            new_optimal_minutes = find_optimal_chunk_size(
                                exporter, args.sumo_query, current_hour_start_dt,
                                args.initial_optimal_chunk_search_minutes, args.max_messages_per_file,
                                args.job_poll_initial_delay_seconds, dry_run=args.dry_run
                            )
                            logging.info(f"🔄 Re-evaluation updated chunk size to {new_optimal_minutes} minutes for {db_date_str} H{hour_num:02}.")
                            current_optimal_minutes = new_optimal_minutes

                        actual_chunk_delta = timedelta(minutes=current_optimal_minutes)
                        loop_chunk_start_dt = current_hour_start_dt
                        end_of_hour_dt = current_hour_start_dt.replace(minute=59, second=59, microsecond=999999)

                        while loop_chunk_start_dt <= end_of_hour_dt:
                            loop_chunk_end_dt = min(loop_chunk_start_dt + actual_chunk_delta - timedelta(seconds=1), end_of_hour_dt)

                            job_marker_suffix = generate_time_suffix(loop_chunk_start_dt, "minute")

                            tasks.append((
                                exporter, args.sumo_query, loop_chunk_start_dt, loop_chunk_end_dt,
                                job_marker_suffix, args.max_messages_per_file,
                                args.job_poll_initial_delay_seconds, args.base_output_directory,
                                args.file_prefix, args.output_granularity,
                                args.overwrite_archive_file_if_exists,
                                args.if_zero_messages_skip_file_write,
                                args.adaptive_shrink_consecutive_count,
                                args.adaptive_grow_trigger_message_percent,
                                args.adaptive_grow_consecutive_count,
                                db_date_str, # DB context for metrics tracking
                                hour_num, # DB context for metrics tracking
                                query_hash_for_db, # DB context for metrics tracking
                                current_optimal_minutes # The optimal minutes passed to this specific chunk for logging
                            ))
                            loop_chunk_start_dt += actual_chunk_delta
    return tasks

def _execute_export_tasks(args: argparse.Namespace, tasks: list[tuple], compressor: zstd.ZstdCompressor):
    """Executes the prepared export tasks using a thread pool."""
    logging.info(f"Prepared {len(tasks)} tasks for data export.")
    if not tasks:
        logging.info("No tasks to execute for data export based on the provided parameters.")
        return

    def submit_and_track_task(task_args):
        # Unpack all args, including the DB context ones at the end
        (exporter, sumo_query, chunk_start_time, chunk_end_time,
         job_marker_suffix, max_messages_per_file, poll_initial_delay,
         base_output_directory, file_prefix, output_granularity,
         overwrite_archive_file_if_exists, if_zero_messages_skip_file_write,
         adaptive_shrink_consecutive_count, adaptive_grow_trigger_message_percent,
         adaptive_grow_consecutive_count,
         db_date_str, hour_num, query_hash_for_db, current_optimal_minutes_for_this_chunk) = task_args

        # Call the process_query_chunk
        result = process_query_chunk(
            exporter, sumo_query, chunk_start_time, chunk_end_time,
            job_marker_suffix, max_messages_per_file, poll_initial_delay,
            base_output_directory, file_prefix, output_granularity,
            overwrite_archive_file_if_exists, if_zero_messages_skip_file_write,
            adaptive_shrink_consecutive_count, adaptive_grow_trigger_message_percent,
            adaptive_grow_consecutive_count, compressor, # Pass compressor
            db_date_str, hour_num, query_hash_for_db, current_optimal_minutes_for_this_chunk
        )

        if result is None: # Job failed
            # Metric for failure already added inside process_query_chunk
            return None
        else:
            messages_count, was_split, was_skipped = result
            # Metric for success/skip already added inside process_query_chunk
            return messages_count, was_split, was_skipped, db_date_str, hour_num, query_hash_for_db, max_messages_per_file, current_optimal_minutes_for_this_chunk


    with ThreadPoolExecutor(max_workers=args.max_concurrent_api_calls) as executor:
        futures = [executor.submit(submit_and_track_task, task_args) for task_args in tasks]
        for i, future in enumerate(as_completed(futures)):
            try:
                result = future.result()
                if result is None:
                    logging.info(f"Task {i+1}/{len(tasks)} failed or was skipped. No adaptive update for optimal chunk size.")
                    continue

                messages_count, was_split, was_skipped, db_date_str, hour_num, query_hash_for_db, max_messages_per_file, original_chunk_minutes = result
                logging.info(f"Task {i+1}/{len(tasks)} completed. Messages: {messages_count}. Skipped: {was_skipped}. Split: {was_split}")

                if was_skipped: # If the chunk was skipped, no new data for adaptive logic
                    continue

                # Retrieve recent metrics for this hour to make an adaptive decision
                recent_metrics = get_recent_adaptive_metrics(args.sumo_query, db_date_str, hour_num,
                                                              max(args.adaptive_shrink_consecutive_count, args.adaptive_grow_consecutive_count))

                if not recent_metrics:
                    logging.warning(f"No recent metrics for {db_date_str} H{hour_num:02}. Skipping adaptive update for optimal chunk size.")
                    continue

                # Filter for metrics relevant to grow/shrink decisions
                # For grow: We want to see if recent actual chunks are consistently small and NOT split.
                # For shrink: We want to see if recent actual chunks consistently led to splits or exceeded max.
                recent_for_grow = [
                    m for m in recent_metrics
                    if not m[3] and not m[2] and m[1] < (max_messages_per_file * args.adaptive_grow_trigger_message_percent / 100.0)
                ]
                recent_for_shrink = [
                    m for m in recent_metrics
                    if not m[3] and (m[2] or m[1] >= max_messages_per_file)
                ]

                current_stored_optimal_info = get_optimal_chunk_info_from_db(args.sumo_query, db_date_str, hour_num)
                if not current_stored_optimal_info:
                     logging.warning(f"Cannot retrieve current optimal info for {db_date_str} H{hour_num:02}. Skipping adaptive update.")
                     continue
                current_stored_optimal_minutes, _ = current_stored_optimal_info

                # --- Shrink Logic ---
                if len(recent_for_shrink) >= args.adaptive_shrink_consecutive_count:
                    new_chunk_minutes = max(1, current_stored_optimal_minutes // 2)
                    if new_chunk_minutes != current_stored_optimal_minutes:
                        logging.info(f"⬇️ ADAPTIVE SHRINK: {len(recent_for_shrink)} recent chunks (out of {len(recent_metrics)}) were large/split for {db_date_str} H{hour_num:02}. "
                                     f"Shrinking optimal from {current_stored_optimal_minutes} to {new_chunk_minutes} minutes.")
                        store_optimal_chunk_minutes_in_db(args.sumo_query, db_date_str, hour_num, new_chunk_minutes, dry_run=args.dry_run)
                    else:
                        logging.debug(f"Chunk size for {db_date_str} H{hour_num:02} already 1 min, cannot shrink further.")
                # --- Grow Logic ---
                elif len(recent_for_grow) >= args.adaptive_grow_consecutive_count:
                    new_chunk_minutes = min(60, current_stored_optimal_minutes * 2)
                    if new_chunk_minutes != current_stored_optimal_minutes:
                        logging.info(f"⬆️ ADAPTIVE GROW: {len(recent_for_grow)} recent chunks (out of {len(recent_metrics)}) were small for {db_date_str} H{hour_num:02}. "
                                     f"Growing optimal from {current_stored_optimal_minutes} to {new_chunk_minutes} minutes.")
                        store_optimal_chunk_minutes_in_db(args.sumo_query, db_date_str, hour_num, new_chunk_minutes, dry_run=args.dry_run)
                    else:
                        logging.debug(f"Chunk size for {db_date_str} H{hour_num:02} already 60 min, cannot grow further.")
                else:
                    logging.debug(f"No adaptive change needed for {db_date_str} H{hour_num:02}. Recent metrics: {len(recent_metrics)}.")

            except Exception as e:
                logging.error(f"🚨 Task {i+1}/{len(tasks)} failed or adaptive update issue: {e}")


# --- Main Execution ---
def main():
    parser = argparse.ArgumentParser(description="Extract data from SumoLogic to compressed JSON files.")

    # General Configuration Arguments
    parser.add_argument("--backoff-seconds", type=int, default=DEFAULT_BACKOFF_SECONDS,
                        help="Seconds to back off when SumoLogic API rate limit is hit.")
    parser.add_argument("--log-file", type=str, default=DEFAULT_LOG_FILE,
                        help="Path to the log file.")
    parser.add_argument("--db-path", type=str, default=DEFAULT_DB_PATH,
                        help="Path to the SQLite database file for storing optimal chunk sizes and adaptive metrics.")

    # SumoLogic Query Arguments
    parser.add_argument("--sumo-query", required=True, help="The SumoLogic query string.")
    parser.add_argument("--years", nargs="+", type=int, required=True, help="Year(s) to process (e.g., 2023 2024).")
    parser.add_argument("--months", nargs="+", help="Month abbreviation(s) to process (e.g., Jan Feb). Processes all if omitted.")
    parser.add_argument("--days", nargs="+", type=int, help="Day(s) of the month to process (e.g., 1 15). Processes all if omitted.")

    # Output File Arguments
    parser.add_argument("--file-prefix", default=DEFAULT_FILE_PREFIX, help="Prefix for output filenames and directories.")
    parser.add_argument("--base-output-directory", default=DEFAULT_BASE_OUTPUT_DIRECTORY, help="Root directory where archives will be stored.")
    parser.add_argument("--output-granularity", choices=["month", "day", "hour", "minute"], default=DEFAULT_OUTPUT_GRANULARITY,
                        help="Granularity of subdirectories and file names for storing data.")

    # Performance and Throttling Arguments
    parser.add_argument("--max-concurrent-api-calls", type=int, default=DEFAULT_MAX_CONCURRENT_API_CALLS,
                        help="Maximum concurrent API calls to SumoLogic (rate limit).")
    parser.add_argument("--max-messages-per-file", type=int, default=DEFAULT_MAX_MESSAGES_PER_FILE,
                        help="Maximum messages per output file. If a query chunk exceeds this, it may be split or truncated.")
    parser.add_argument("--initial-optimal-chunk-search-minutes", type=int, default=DEFAULT_INITIAL_OPTIMAL_CHUNK_SEARCH_MINUTES,
                        help="Max duration in minutes for the initial search window when determining optimal chunk size (e.g., 60 for 1hr).")
    parser.add_argument("--job-poll-initial-delay-seconds", type=int, default=DEFAULT_JOB_POLL_INITIAL_DELAY_SECONDS,
                        help="Initial delay (seconds) before polling Sumo Logic job status.")

    # Behavior Control Arguments
    parser.add_argument("--overwrite-archive-file-if-exists", action="store_true",
                        help="If set, existing output files will be overwritten. By default, processing is skipped if the file/marker exists.")
    parser.add_argument("--if-zero-messages-skip-file-write", action="store_true",
                        help="If set, a file will not be created if a chunk query returns zero messages.")

    # Optimal Chunk Discovery Arguments
    parser.add_argument("--discover-optimal-chunk-sizes", action="store_true",
                        help="If set, only find and store optimal chunk sizes for the specified "
                             "query and time ranges to the database. Does not export actual data.")
    parser.add_argument("--default-chunk-minutes-if-not-found", type=int, default=DEFAULT_DEFAULT_CHUNK_MINUTES_IF_NOT_FOUND,
                        help="Default chunk size in minutes to use for data export if an optimal size "
                             "is not found in the database (and not in discovery mode).")

    # Adaptive Sizing Arguments
    parser.add_argument("--adaptive-shrink-consecutive-count", type=int, default=DEFAULT_ADAPTIVE_SHRINK_CONSECUTIVE_COUNT,
                        help="Number of consecutive query chunk size reductions (due to exceeding message limit or being split) before the 'global optimal' chunk size is set to the newly shrunk size.")
    parser.add_argument("--adaptive-grow-trigger-message-percent", type=int, default=DEFAULT_ADAPTIVE_GROW_TRIGGER_MESSAGE_PERCENT,
                        help="Percentage threshold (e.g., 50 means < 50%% of max-messages-per-file) below which a chunk's message count will trigger the adaptive logic to attempt to GROW the chunk size. This prevents unnecessarily small chunks for sparse data.")
    parser.add_argument("--adaptive-grow-consecutive-count", type=int, default=DEFAULT_ADAPTIVE_GROW_CONSECUTIVE_COUNT,
                        help="Number of consecutive query chunk size increases (due to low message count) before the 'global optimal' chunk size is set to the newly grown size.")
    parser.add_argument("--adaptive-re-evaluation-interval-hours", type=int, default=DEFAULT_ADAPTIVE_RE_EVALUATION_INTERVAL_HOURS,
                        help="Interval in hours to periodically re-evaluate the global optimal chunk size, even if consecutive grow/shrink conditions aren't met. This helps adapt to changing data volumes over long periods. For very stable data or cost-sensitive Infrequent Tier, consider increasing this.")

    # Debugging and Dry Run Arguments
    parser.add_argument("--dry-run", action="store_true",
                        help="Perform a dry run without making Sumo Logic API calls, writing files, or modifying the database.")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging (DEBUG level).")


    args = parser.parse_args()

    # Setup logging
    _setup_logging(args.log_file, args.verbose, False) # Pass False for debug, as verbose handles it

    # Initialize DB connections
    init_db(args.db_path)

    # Initialize SumoExporter
    exporter = _initialize_exporter(args)

    # Initialize Zstandard Compressor once
    global_compressor = zstd.ZstdCompressor()

    if args.discover_optimal_chunk_sizes:
        logging.info("Running in optimal chunk size discovery mode only.")
        _prepare_export_tasks(args, exporter) # This will run the discovery logic directly
        flush_adaptive_metrics_buffer(dry_run=args.dry_run) # Flush any metrics collected during discovery
    else:
        tasks_to_execute = _prepare_export_tasks(args, exporter)
        _execute_export_tasks(args, tasks_to_execute, global_compressor)
        flush_adaptive_metrics_buffer(dry_run=args.dry_run) # Flush any remaining metrics after all tasks

    if DB_CONN:
        try:
            DB_CONN.close()
            logging.info("🗃️ Database connection closed.")
        except sqlite3.Error as e:
            logging.error(f"🚨 Error closing database connection: {e}")

if __name__ == "__main__":
    main()
