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
SUMO_HTTP_API_BACKOFF_SECONDS = 8
LOG_FILE = "sumo-query-to-files.log"
SQLITE_PATH = "query_chunk_sizes.db"
DB_CONN: sqlite3.Connection | None = None  # Global DB connection with type hint

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler(sys.stdout)],
)

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
    output_granularity: str = "month",
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
    output_granularity: str = "month",
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
def init_db(db_path: str = SQLITE_PATH):
    """
    Initializes the SQLite database connection and creates the table for optimal chunk sizes.
    """
    global DB_CONN
    try:
        DB_CONN = sqlite3.connect(db_path, timeout=10)
        DB_CONN.execute("""
            CREATE TABLE IF NOT EXISTS optimal_chunks (
                query_hash TEXT NOT NULL, /* Hashed query for keying */
                date TEXT NOT NULL, /* YYYY-MM-DD */
                hour INTEGER NOT NULL,
                chunk_minutes INTEGER NOT NULL,
                PRIMARY KEY (query_hash, date, hour)
            )
        """)
        DB_CONN.commit()
        logging.info(f"🗃️ Database initialized at {db_path}")
    except sqlite3.Error as e:
        logging.error(f"🚨 Database initialization error: {e}")
        sys.exit(1)

def get_optimal_chunk_minutes_from_db(query: str, date_str: str, hour: int) -> int | None:
    """
    Retrieves the optimal chunk size for a given query, date, and hour from the database.
    """
    if not DB_CONN:
        logging.error("🚨 DB connection not initialized for get_optimal_chunk_minutes_from_db")
        return None
    # Use a hash of the query for the DB key to handle potentially very long queries
    query_hash = str(hash(query))[:255] # Truncate hash if it's too long for the column
    try:
        cursor = DB_CONN.cursor()
        cursor.execute("SELECT chunk_minutes FROM optimal_chunks WHERE query_hash = ? AND date = ? AND hour = ?",
                       (query_hash, date_str, hour))
        row = cursor.fetchone()
        return row[0] if row else None
    except sqlite3.Error as e:
        logging.error(f"🚨 DB get_optimal_chunk_minutes_from_db error: {e}")
        return None

def store_optimal_chunk_minutes_in_db(query: str, date_str: str, hour: int, minutes: int):
    """
    Stores the optimal chunk size for a given query, date, and hour in the database.
    """
    if not DB_CONN:
        logging.error("🚨 DB connection not initialized for store_optimal_chunk_minutes_in_db")
        return
    query_hash = str(hash(query))[:255] # Truncate hash
    try:
        cursor = DB_CONN.cursor()
        cursor.execute("INSERT OR REPLACE INTO optimal_chunks (query_hash, date, hour, chunk_minutes) VALUES (?, ?, ?, ?)",
                       (query_hash, date_str, hour, minutes))
        DB_CONN.commit()
        logging.debug(f"💾 Stored optimal chunk: Query hash {query_hash}, Date {date_str}, Hour {hour} -> {minutes} min")
    except sqlite3.Error as e:
        logging.error(f"🚨 DB store_optimal_chunk_minutes_in_db error: {e}")

# --- SumoExporter Class ---
class SumoExporter:
    def __init__(self, access_id: str, access_key: str, api_endpoint: str, rate_limit: int = 4):
        self.session = requests.Session()
        self.session.auth = (access_id, access_key)
        self.session.headers.update({"Content-Type": "application/json", "Accept": "application/json"})
        self.api_endpoint = api_endpoint.rstrip('/')
        self.semaphore = Semaphore(rate_limit)

    def _request_with_retry(self, method: str, url: str, **kwargs) -> requests.Response:
        """
        Handles HTTP requests with retries for rate limiting and transient errors.
        """
        with self.semaphore:
            retry_count = 0
            max_retries = 5
            while retry_count <= max_retries:
                try:
                    resp = self.session.request(method, url, **kwargs)
                    if resp.status_code == 429:  # Rate limit
                        logging.warning(f"🚦 Rate limit hit for {method} {url}. Backing off for {SUMO_HTTP_API_BACKOFF_SECONDS * (retry_count + 1)}s. Retry count: {retry_count}")
                        time.sleep(SUMO_HTTP_API_BACKOFF_SECONDS * (retry_count + 1))  # Exponential-ish backoff
                        retry_count += 1
                        continue
                    resp.raise_for_status()  # Raises HTTPError for bad responses (4xx or 5xx)
                    return resp
                except requests.exceptions.Timeout as e:
                    logging.warning(f"⏱️ Timeout for {method} {url}: {e}. Retrying in {SUMO_HTTP_API_BACKOFF_SECONDS}s...")
                    time.sleep(SUMO_HTTP_API_BACKOFF_SECONDS)
                except requests.RequestException as e:
                    logging.error(f"🚨 RequestException for {method} {url}: {e}. Retrying in 10s...")
                    time.sleep(10)
                retry_count += 1

            logging.error(f"🚨 Max retries exceeded for {method} {url}.")
            raise requests.RequestException(f"Max retries exceeded for {method} {url}")

    def create_search_job(self, query: str, start_time: str, end_time: str) -> str:
        """
        Creates a Sumo Logic search job.
        Args:
            query (str): The Sumo Logic query string.
            start_time (str): Start time in ISO 8601 format (e.g., "2023-01-01T00:00:00Z").
            end_time (str): End time in ISO 8601 format.
        Returns:
            str: The ID of the created search job.
        """
        payload = {"query": query, "from": start_time, "to": end_time, "timeZone": "UTC"}
        url = f"{self.api_endpoint}/api/v1/search/jobs"
        logging.debug(f"Creating job: {query} from {start_time} to {end_time}")
        resp = self._request_with_retry("POST", url, json=payload)
        return resp.json()["id"]

    def get_job_status(self, job_id: str) -> dict:
        """
        Retrieves the status of a Sumo Logic search job.
        """
        url = f"{self.api_endpoint}/api/v1/search/jobs/{job_id}"
        resp = self._request_with_retry("GET", url)
        return resp.json()

    def wait_for_job_completion(self, job_id: str, poll_interval: int = 5, initial_delay: int = 5) -> dict:
        """
        Polls a Sumo Logic search job until it completes.
        Returns the final job status dictionary.
        """
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

    def stream_job_messages(self, job_id: str, limit_per_request: int = 10000, max_messages_to_fetch: int | None = None):
        """
        Streams messages from a completed Sumo Logic search job.
        Yields individual message dictionaries.
        """
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

            if actual_limit == 0 and max_messages_to_fetch is not None :
                 break

            params = {"limit": actual_limit, "offset": offset}
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

            if len(messages) < actual_limit:
                break
            if max_messages_to_fetch is not None and messages_fetched_count >= max_messages_to_fetch:
                break
        logging.info(f"Finished streaming for job {job_id}. Total messages yielded: {messages_fetched_count}")


def find_optimal_chunk_size(
    exporter: SumoExporter,
    sumo_query: str,
    search_start_time: datetime,
    max_minutes_for_search_window: int,
    max_messages_per_file: int,
    poll_initial_delay: int,
) -> int:
    """
    Determines optimal chunk size in minutes for a given query and start time by iteratively testing.
    Stores the optimal size in the database.
    """
    logging.info(f"🔍 Determining optimal chunk size for query hash (partial), starting at {search_start_time.isoformat()} (max {max_minutes_for_search_window} min search window)")

    current_test_chunk_size_minutes = max_minutes_for_search_window
    date_str = search_start_time.strftime("%Y-%m-%d")
    hour = search_start_time.hour

    while current_test_chunk_size_minutes >= 1:
        logging.info(f"⏱️ Trying chunk size: {current_test_chunk_size_minutes} minutes from {search_start_time.isoformat()}")
        # Ensure the test chunk does not exceed the hour boundary or overall process boundaries if relevant
        search_chunk_end_time = search_start_time + timedelta(minutes=current_test_chunk_size_minutes) - timedelta(seconds=1)

        job_id = None
        try:
            job_id = exporter.create_search_job(sumo_query, search_start_time.isoformat(), search_chunk_end_time.isoformat())
            status = exporter.wait_for_job_completion(job_id, initial_delay=poll_initial_delay)
            message_count = status.get("messageCount", 0)
            logging.info(f"📊 Chunk size {current_test_chunk_size_minutes} minutes yielded {message_count} messages (Limit: {max_messages_per_file}).")

            if message_count < max_messages_per_file:
                store_optimal_chunk_minutes_in_db(sumo_query, date_str, hour, current_test_chunk_size_minutes)
                return current_test_chunk_size_minutes

            if current_test_chunk_size_minutes == 1 and message_count >= max_messages_per_file:
                logging.warning(f"⚠️ Optimal chunk size is 1 minute, but it still yields {message_count} messages (>= {max_messages_per_file}). Proceeding with 1 minute.")
                break # Will store 1 minute outside loop

        except Exception as e:
            logging.error(f"🚨 Error during optimal chunk size test (size {current_test_chunk_size_minutes} min): {e}")
            if job_id:
                logging.info(f"Job ID was {job_id}")

        current_test_chunk_size_minutes //= 2 # Try half the size

    logging.info(f"ℹ️ Defaulting to 1 minute chunk size for query hash (partial) at {search_start_time.isoformat()} after search.")
    store_optimal_chunk_minutes_in_db(sumo_query, date_str, hour, 1)
    return 1


def write_messages_to_files(
    messages: list[dict],
    base_output_directory: str,
    file_prefix: str,
    output_granularity: str,
    no_file_if_zero_messages: bool,
    skip_if_archive_exists: bool,
):
    """
    Writes a list of messages to compressed JSON files, grouping them by timestamp
    according to the specified output granularity.
    """
    if not messages and no_file_if_zero_messages:
        logging.debug("No messages to write for this chunk and 'no_file_if_zero_messages' is True. Skipping.")
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

        # Key for grouping messages into the same file
        if output_granularity == "minute":
            file_group_key = (msg_year, msg_month_abbr, msg_day, msg_hour, msg_minute)
        elif output_granularity == "hour":
            file_group_key = (msg_year, msg_month_abbr, msg_day, msg_hour)
        elif output_granularity == "day":
            file_group_key = (msg_year, msg_month_abbr, msg_day)
        else:  # month
            file_group_key = (msg_year, msg_month_abbr)

        grouped_by_file_target[file_group_key].append(m)

    cctx = zstd.ZstdCompressor()
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
        os.makedirs(output_dir_path, exist_ok=True)

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

        if no_file_if_zero_messages and not items_for_file:
            logging.info(f"💨 Skipping empty file target: {final_file_path}")
            continue

        if skip_if_archive_exists and os.path.exists(final_file_path):
            logging.info(f"☑️ Final target file {final_file_path} already exists. Skipping write.")
            continue

        logging.info(f"💾 Attempting to save {len(items_for_file)} messages to: {final_file_path}")
        try:
            with open(final_file_path, "wb") as f:
                json_data = json.dumps(items_for_file, indent=2).encode("utf-8")
                compressed_data = cctx.compress(json_data)
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
    job_marker_suffix: str, # This is the argument you're using
    max_messages_per_file: int,
    poll_initial_delay: int,
    base_output_directory: str,
    file_prefix: str,
    output_granularity: str,
    skip_if_archive_exists: bool,
    no_file_if_zero_messages: bool,
    depth: int = 0,
):
    """
    Processes a single time chunk by querying Sumo Logic, retrieving messages,
    and writing them to files, potentially splitting if too many messages are found.
    """
    indent = "  " * depth
    duration = chunk_end_time - chunk_start_time + timedelta(seconds=1)

    s_year, s_month_abbr, s_day, s_hour, s_minute = (
        chunk_start_time.year, calendar.month_abbr[chunk_start_time.month], chunk_start_time.day,
        chunk_start_time.hour, chunk_start_time.minute
    )

    if skip_if_archive_exists and check_job_marker_exists(
        base_output_directory, file_prefix, job_marker_suffix, s_year, s_month_abbr, s_day, s_hour, s_minute, output_granularity
    ):
        logging.info(f"{indent}⏭️ Skipping job {job_marker_suffix} ({chunk_start_time.isoformat()} to {chunk_end_time.isoformat()}) as its marker/output file exists.")
        return

    logging.info(f"{indent}🔎 Querying for job {job_marker_suffix}: {chunk_start_time.isoformat()} → {chunk_end_time.isoformat()}")

    job_id = None
    try:
        job_id = exporter.create_search_job(sumo_query, chunk_start_time.isoformat(), chunk_end_time.isoformat())
        exporter.wait_for_job_completion(job_id, initial_delay=poll_initial_delay)

        messages = list(exporter.stream_job_messages(job_id, max_messages_to_fetch=max_messages_per_file + 1))
        # FIX: Use job_marker_suffix here instead of the undefined job_id_suffix
        logging.info(f"{indent}📬 Received {len(messages)} messages for job {job_marker_suffix}.")

        if len(messages) > max_messages_per_file:
            logging.info(f"{indent}Messages ({len(messages)}) exceed max_messages_per_file ({max_messages_per_file}). Splitting chunk.")
            if duration > timedelta(minutes=1):
                # Split logic: prefer hourly splits, then minute splits if within an hour.
                split_granularity_duration = timedelta(hours=1)
                if duration <= timedelta(hours=1):
                    split_granularity_duration = timedelta(minutes=1)

                # Calculate number of sub-chunks. Ensure at least one.
                num_sub_chunks = (duration.total_seconds() + split_granularity_duration.total_seconds() - 1) // split_granularity_duration.total_seconds()
                num_sub_chunks = int(max(1, num_sub_chunks))

                for i in range(num_sub_chunks):
                    sub_chunk_start = chunk_start_time + i * split_granularity_duration
                    if sub_chunk_start > chunk_end_time:
                        break

                    sub_chunk_end = min(sub_chunk_start + split_granularity_duration - timedelta(seconds=1), chunk_end_time)

                    sub_job_marker_suffix = generate_time_suffix(sub_chunk_start, "minute")

                    process_query_chunk(
                        exporter, sumo_query, sub_chunk_start, sub_chunk_end,
                        sub_job_marker_suffix, max_messages_per_file, poll_initial_delay,
                        base_output_directory, file_prefix, output_granularity,
                        skip_if_archive_exists, no_file_if_zero_messages, depth + 1
                    )
                return
            else:
                logging.warning(f"{indent}⚠️ Chunk {job_marker_suffix} ({chunk_start_time.isoformat()}) is at minimum 1-min granularity but has {len(messages)} messages (limit {max_messages_per_file}). Writing first {max_messages_per_file} messages.")
                messages = messages[:max_messages_per_file]

        write_messages_to_files(
            messages, base_output_directory, file_prefix, output_granularity,
            no_file_if_zero_messages, skip_if_archive_exists
        )

    except Exception as e:
        logging.error(f"{indent}🚨 Failed to process chunk {job_marker_suffix} ({chunk_start_time.isoformat()} to {chunk_end_time.isoformat()}): {e}")
        if job_id: logging.error(f"{indent}Associated Job ID: {job_id}")


# --- Main Execution ---
def main():
    parser = argparse.ArgumentParser(description="Extract data from SumoLogic to compressed JSON files.")

    parser.add_argument("--sumo-query", required=True, help="The SumoLogic query string.")
    parser.add_argument("--years", nargs="+", type=int, required=True, help="Year(s) to process (e.g., 2023 2024).")
    parser.add_argument("--months", nargs="+", help="Month abbreviation(s) to process (e.g., Jan Feb). Processes all if omitted.")
    parser.add_argument("--days", nargs="+", type=int, help="Day(s) of the month to process (e.g., 1 15). Processes all if omitted.")

    parser.add_argument("--file-prefix", default="sumo_export", help="Prefix for output filenames and directories.")
    parser.add_argument("--base-output-directory", default="sumo-archive", help="Root directory where archives will be stored.")
    parser.add_argument("--output-granularity", choices=["month", "day", "hour", "minute"], default="month",
                        help="Granularity of subdirectories and file names for storing data.")

    parser.add_argument("--max-concurrent-api-calls", type=int, default=4,
                        help="Maximum concurrent API calls to SumoLogic (rate limit).")
    parser.add_argument("--max-messages-per-file", type=int, default=100000,
                        help="Maximum messages per output file. If a query chunk exceeds this, it may be split or truncated.")
    parser.add_argument("--initial-optimal-chunk-search-minutes", type=int, default=60,
                        help="Max duration in minutes for the initial search window when determining optimal chunk size (e.g., 60 for 1hr).")
    parser.add_argument("--job-poll-initial-delay-seconds", type=int, default=10,
                        help="Initial delay (seconds) before polling Sumo Logic job status.")

    parser.add_argument("--skip-if-archive-exists", action="store_true",
                        help="If set, skips processing for a time chunk if its corresponding output file/marker already exists.")
    parser.add_argument("--no-file-if-zero-messages", action="store_true",
                        help="If set, do not create a file if a chunk query returns zero messages.")

    parser.add_argument("--discover-optimal-chunk-sizes", action="store_true",
                        help="If set, only find and store optimal chunk sizes for the specified "
                             "query and time ranges to the database. Does not export actual data.")
    parser.add_argument("--default-chunk-minutes-if-not-found", type=int, default=60,
                        help="Default chunk size in minutes to use for data export if an optimal size "
                             "is not found in the database (and not in discovery mode).")

    args = parser.parse_args()

    init_db()

    sumo_access_id = must_env("SUMO_ACCESS_ID")
    sumo_access_key = must_env("SUMO_ACCESS_KEY")
    sumo_api_endpoint = must_env("SUMO_API_ENDPOINT")

    exporter = SumoExporter(sumo_access_id, sumo_access_key, sumo_api_endpoint, rate_limit=args.max_concurrent_api_calls)

    export_tasks = []

    for year in args.years:
        for month_num in range(1, 13):
            month_abbr = calendar.month_abbr[month_num]
            if args.months and month_abbr not in args.months:
                continue

            days_in_month = calendar.monthrange(year, month_num)[1]
            selected_days = args.days if args.days else range(1, days_in_month + 1)

            for day_num in selected_days:
                if day_num < 1 or day_num > days_in_month:
                    logging.warning(f"⚠️ Day {day_num} is out of range for {month_abbr} {year}. Skipping.")
                    continue

                for hour_num in range(24):
                    current_hour_start_dt = datetime(year, month_num, day_num, hour_num, 0, 0, tzinfo=timezone.utc)
                    db_date_str = current_hour_start_dt.strftime("%Y-%m-%d")

                    if args.discover_optimal_chunk_sizes:
                        logging.info(f"🔬 Discovery Mode: Finding optimal chunk size for query (truncated), date {db_date_str}, hour {hour_num:02}.")
                        max_search_window_minutes = min(args.initial_optimal_chunk_search_minutes, 60)
                        find_optimal_chunk_size(
                            exporter, args.sumo_query, current_hour_start_dt,
                            max_search_window_minutes, args.max_messages_per_file,
                            args.job_poll_initial_delay_seconds
                        )
                        logging.info(f"🔬 Discovery for {db_date_str} H{hour_num:02} complete.")
                    else:
                        optimal_minutes_for_hour = get_optimal_chunk_minutes_from_db(args.sumo_query, db_date_str, hour_num)
                        if not optimal_minutes_for_hour:
                            optimal_minutes_for_hour = args.default_chunk_minutes_if_not_found
                            logging.warning(f"⚠️ Optimal chunk size not in DB for query (truncated) / {db_date_str} H{hour_num:02}. "
                                            f"Using default: {optimal_minutes_for_hour} minutes.")
                        else:
                            logging.info(f"💾 DB: Found optimal chunk size for query (truncated) / {db_date_str} H{hour_num:02}: "
                                         f"{optimal_minutes_for_hour} minutes.")

                        actual_chunk_delta = timedelta(minutes=optimal_minutes_for_hour)
                        loop_chunk_start_dt = current_hour_start_dt
                        end_of_hour_dt = current_hour_start_dt.replace(minute=59, second=59, microsecond=999999)

                        while loop_chunk_start_dt <= end_of_hour_dt:
                            loop_chunk_end_dt = min(loop_chunk_start_dt + actual_chunk_delta - timedelta(seconds=1), end_of_hour_dt)

                            job_marker_suffix = generate_time_suffix(loop_chunk_start_dt, "minute")

                            export_tasks.append((
                                exporter, args.sumo_query, loop_chunk_start_dt, loop_chunk_end_dt,
                                job_marker_suffix, args.max_messages_per_file,
                                args.job_poll_initial_delay_seconds, args.base_output_directory,
                                args.file_prefix, args.output_granularity, args.skip_if_archive_exists,
                                args.no_file_if_zero_messages
                            ))
                            loop_chunk_start_dt += actual_chunk_delta

    if args.discover_optimal_chunk_sizes:
        logging.info("✅ Optimal chunk size discovery mode complete. Results stored in database.")
    else:
        logging.info(f"Prepared {len(export_tasks)} tasks for data export.")
        if export_tasks:
            with ThreadPoolExecutor(max_workers=args.max_concurrent_api_calls) as executor:
                futures = [executor.submit(process_query_chunk, *task_args) for task_args in export_tasks]
                for i, future in enumerate(as_completed(futures)):
                    try:
                        future.result()
                        logging.info(f"Task {i+1}/{len(export_tasks)} completed.")
                    except Exception as e:
                        logging.error(f"🚨 Task {i+1}/{len(export_tasks)} failed: {e}")
        else:
            logging.info("No tasks to execute for data export based on the provided parameters.")

    if DB_CONN:
        try:
            DB_CONN.close()
            logging.info("🗃️ Database connection closed.")
        except sqlite3.Error as e:
            logging.error(f"🚨 Error closing database connection: {e}")

if __name__ == "__main__":
    main()
