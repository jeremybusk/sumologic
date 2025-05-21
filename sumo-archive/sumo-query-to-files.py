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
import argparse # Moved import to top

# === Configuration Constants ===
SUMO_HTTP_API_BACKOFF_SECONDS = 15
LOG_FILE = "sumo-query-to-files.log"
SQLITE_PATH = "query_chunk_sizes.db"
DB_CONN = None # Global DB connection

# === Logging Setup ===
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s", # Added levelname
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler(sys.stdout)],
)

# === Utility Functions ===
def must_env(key):
    val = os.getenv(key)
    if not val:
        logging.critical(f"🚨 Missing required environment variable: {key}")
        sys.exit(1)
    return val

def build_output_path(container, prefix, year, month_abbr, day=None, hour=None, minute=None, files_stored_by="month"):
    # Ensure year is a string for os.path.join
    path_parts = [container, prefix, str(year), month_abbr]
    if files_stored_by in ["day", "hour", "minute"] and day is not None:
        path_parts.append(f"{day:02}")
    if files_stored_by in ["hour", "minute"] and hour is not None:
        path_parts.append(f"H{hour:02}")
    if files_stored_by == "minute" and minute is not None:
        path_parts.append(f"M{minute:02}")
    return os.path.join(*path_parts)

def file_exists(args, job_specific_suffix, year, month_abbr, day=None, hour=None, minute=None):
    # This function checks for a marker file related to a specific job chunk,
    # using the job_specific_suffix in the filename.
    # The path is determined by args.files_stored_by and the time components.
    path = build_output_path(args.container_name, args.prefix, year, month_abbr, day, hour, minute, args.files_stored_by)
    # The file name checked here is based on the job's unique suffix.
    file_path = os.path.join(path, f"{args.prefix}_{job_specific_suffix}.json.zst")
    return os.path.exists(file_path)

def range_suffix(base_suffix, dt, granularity):
    """Generates a time-based suffix (e.g., YYYYMonDD, YYYYMonDDHH, YYYYMonDDHHMM)."""
    suffix = base_suffix # Expected format like YYYYMonDD
    if granularity in ("hour", "minute"):
        suffix += f"H{dt.hour:02}"
    if granularity == "minute":
        suffix += f"M{dt.minute:02}"
    return suffix

# === Database Functions ===
def init_db(db_path=SQLITE_PATH):
    global DB_CONN
    DB_CONN = sqlite3.connect(db_path, timeout=10) # Added timeout
    try:
        DB_CONN.execute("""
            CREATE TABLE IF NOT EXISTS optimal_chunks (
                query TEXT NOT NULL,
                date TEXT NOT NULL, /* YYYY-MM-DD */
                hour INTEGER NOT NULL,
                chunk_minutes INTEGER NOT NULL,
                PRIMARY KEY (query, date, hour)
            )
        """)
        DB_CONN.commit()
        logging.info(f"🗃️ Database initialized at {db_path}")
    except sqlite3.Error as e:
        logging.error(f"🚨 Database initialization error: {e}")
        sys.exit(1)


def get_optimal_chunk_minutes_from_db(query, date_str, hour):
    if not DB_CONN:
        logging.error("🚨 DB connection not initialized for get_optimal_chunk_minutes_from_db")
        return None
    query = query[:255]  # Truncate to prevent oversized keys
    try:
        cursor = DB_CONN.cursor()
        cursor.execute("SELECT chunk_minutes FROM optimal_chunks WHERE query = ? AND date = ? AND hour = ?", (query, date_str, hour))
        row = cursor.fetchone()
        return row[0] if row else None
    except sqlite3.Error as e:
        logging.error(f"🚨 DB get_optimal_chunk_minutes_from_db error: {e}")
        return None

def store_optimal_chunk_minutes_in_db(query, date_str, hour, minutes):
    if not DB_CONN:
        logging.error("🚨 DB connection not initialized for store_optimal_chunk_minutes_in_db")
        return
    query = query[:255]  # Truncate to prevent oversized keys
    try:
        cursor = DB_CONN.cursor()
        cursor.execute("INSERT OR REPLACE INTO optimal_chunks (query, date, hour, chunk_minutes) VALUES (?, ?, ?, ?)", (query, date_str, hour, minutes))
        DB_CONN.commit()
        logging.debug(f"💾 Stored optimal chunk: Query hash (partial), Date {date_str}, Hour {hour} -> {minutes} min")
    except sqlite3.Error as e:
        logging.error(f"🚨 DB store_optimal_chunk_minutes_in_db error: {e}")

# === SumoExporter Class ===
class SumoExporter:
    def __init__(self, access_id, access_key, api_endpoint, rate_limit=4):
        self.session = requests.Session()
        self.session.auth = (access_id, access_key)
        self.session.headers.update({"Content-Type": "application/json", "Accept": "application/json"})
        self.api_endpoint = api_endpoint.rstrip('/')
        self.semaphore = Semaphore(rate_limit)
        # SAS URL and Azure container path are not used within this class in the original code.
        # If they were intended for direct upload from here, methods would need to be added.

    def _request_with_retry(self, method, url, **kwargs):
        with self.semaphore:
            while True:
                try:
                    resp = self.session.request(method, url, **kwargs)
                    if resp.status_code == 429: # Rate limit
                        logging.warning(f"🚦 Rate limit hit for {method} {url}. Backing off for {SUMO_HTTP_API_BACKOFF_SECONDS}s. Retry count: {kwargs.get('retry_count', 0)}")
                        time.sleep(SUMO_HTTP_API_BACKOFF_SECONDS * (kwargs.get('retry_count', 0) + 1)) # Exponential-ish backoff
                        kwargs['retry_count'] = kwargs.get('retry_count', 0) + 1
                        if kwargs['retry_count'] > 5: # Max retries
                           logging.error(f"🚨 Max retries exceeded for {method} {url}.")
                           resp.raise_for_status() # Will raise HTTPError for 429 after max retries
                        continue
                    resp.raise_for_status() # Raises HTTPError for bad responses (4xx or 5xx)
                    return resp
                except requests.exceptions.Timeout as e:
                    logging.warning(f"⏱️ Timeout for {method} {url}: {e}. Retrying in {SUMO_HTTP_API_BACKOFF_SECONDS}s...")
                    time.sleep(SUMO_HTTP_API_BACKOFF_SECONDS)
                except requests.RequestException as e:
                    logging.error(f"🚨 RequestException for {method} {url}: {e}. Retrying in 10s...")
                    time.sleep(10) # General retry for other request exceptions


    def create_job(self, query, time_from, time_to):
        payload = {"query": query, "from": time_from, "to": time_to, "timeZone": "UTC"}
        url = f"{self.api_endpoint}/api/v1/search/jobs"
        logging.debug(f"Creating job: {query} from {time_from} to {time_to}")
        resp = self._request_with_retry("POST", url, json=payload)
        return resp.json()["id"]

    def get_job_status(self, job_id):
        url = f"{self.api_endpoint}/api/v1/search/jobs/{job_id}"
        resp = self._request_with_retry("GET", url)
        return resp.json()

    def wait_for_completion(self, job_id, poll_interval=5, initial_delay=5):
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
                # Depending on severity, might want to raise an exception or handle differently
            time.sleep(poll_interval)


    def stream_messages(self, job_id, limit_per_request=10000, max_messages_to_fetch=None):
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

            if actual_limit == 0 and max_messages_to_fetch is not None : # ensure we try to fetch if max_messages_to_fetch is set.
                 break # if max_messages_to_fetch was 0 or already met.

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

            if len(messages) < actual_limit: # Fewer messages than limit means we got all remaining
                break
            if max_messages_to_fetch is not None and messages_fetched_count >= max_messages_to_fetch:
                break
        logging.info(f"Finished streaming for job {job_id}. Total messages yielded: {messages_fetched_count}")


def find_optimal_chunk_size(exp, args, query, search_start_time, max_minutes_for_search_window):
    """Determines optimal chunk size in minutes for a given query and start time."""
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
            job_id = exp.create_job(query, search_start_time.isoformat(), search_chunk_end_time.isoformat())
            status = exp.wait_for_completion(job_id, initial_delay=args.poll_initial_delay)
            message_count = status.get("messageCount", 0)
            logging.info(f"📊 Chunk size {current_test_chunk_size_minutes} minutes yielded {message_count} messages (Limit: {args.max_messages_per_file}).")

            if message_count < args.max_messages_per_file:
                store_optimal_chunk_minutes_in_db(query, date_str, hour, current_test_chunk_size_minutes)
                return current_test_chunk_size_minutes

            if current_test_chunk_size_minutes == 1 and message_count >= args.max_messages_per_file:
                logging.warning(f"⚠️ Optimal chunk size is 1 minute, but it still yields {message_count} messages (>= {args.max_messages_per_file}). Proceeding with 1 minute.")
                break # Will store 1 minute outside loop

        except Exception as e:
            logging.error(f"🚨 Error during optimal chunk size test (size {current_test_chunk_size_minutes} min): {e}")
            if job_id:
                logging.info(f"Job ID was {job_id}")
            # If API call fails, we can't determine; maybe try smaller? Or rely on a default.
            # For now, we'll just continue to the next smaller size.

        current_test_chunk_size_minutes //= 2 # Try half the size

    logging.info(f"ℹ️ Defaulting to 1 minute chunk size for query hash (partial) at {search_start_time.isoformat()} after search.")
    store_optimal_chunk_minutes_in_db(query, date_str, hour, 1)
    return 1


def write_file_grouped(data, args):
    """Writes data to files, grouping messages by timestamp according to args.files_stored_by."""
    if not data:
        logging.debug("No data to write for this chunk.")
        return

    grouped_by_file_target = defaultdict(list)
    for m in data:
        raw_ts = m.get("map", {}).get("_messagetime", 0)
        try:
            # Ensure timestamp is valid and handle potential errors
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
        if args.files_stored_by == "minute":
            file_group_key = (msg_year, msg_month_abbr, msg_day, msg_hour, msg_minute)
        elif args.files_stored_by == "hour":
            file_group_key = (msg_year, msg_month_abbr, msg_day, msg_hour)
        elif args.files_stored_by == "day":
            file_group_key = (msg_year, msg_month_abbr, msg_day)
        else:  # month
            file_group_key = (msg_year, msg_month_abbr)

        grouped_by_file_target[file_group_key].append(m)

    cctx = zstd.ZstdCompressor()
    for key_tuple, items_for_file in grouped_by_file_target.items():
        # Unpack key_tuple to determine path and filename components
        current_year = key_tuple[0]
        current_month_abbr = key_tuple[1]
        current_day = key_tuple[2] if len(key_tuple) > 2 else None
        current_hour = key_tuple[3] if len(key_tuple) > 3 else None
        current_minute = key_tuple[4] if len(key_tuple) > 4 else None

        output_dir_path = build_output_path(args.container_name, args.prefix,
                                         current_year, current_month_abbr,
                                         current_day, current_hour, current_minute,
                                         args.files_stored_by)
        os.makedirs(output_dir_path, exist_ok=True)

        # Generate filename suffix based on the group key (i.e., files_stored_by granularity)
        filename_parts = [f"{current_year:04}", current_month_abbr]
        if args.files_stored_by in ["day", "hour", "minute"] and current_day is not None:
            filename_parts.append(f"{current_day:02}")
        if args.files_stored_by in ["hour", "minute"] and current_hour is not None:
            filename_parts.append(f"H{current_hour:02}")
        if args.files_stored_by == "minute" and current_minute is not None:
            filename_parts.append(f"M{current_minute:02}")
        filename_time_suffix = "".join(filename_parts)

        final_file_path = os.path.join(output_dir_path, f"{args.prefix}_{filename_time_suffix}.json.zst")

        if args.no_file_if_zero_messages and not items_for_file: # Already checked 'data' but this is per-file
            logging.info(f"💨 Skipping empty file target: {final_file_path}")
            continue

        if args.skip_if_archive_exists and os.path.exists(final_file_path):
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


def query_and_export(exp, args, query, chunk_start_dt, chunk_end_dt, job_identifier_suffix, depth=0):
    indent = "  " * depth
    duration = chunk_end_dt - chunk_start_dt + timedelta(seconds=1) # Inclusive end

    # Check if a marker for this specific job chunk already exists
    # Uses the job_identifier_suffix, which is unique to the chunk's start time at minute level
    s_year, s_month_abbr, s_day, s_hour, s_minute = (
        chunk_start_dt.year, calendar.month_abbr[chunk_start_dt.month], chunk_start_dt.day,
        chunk_start_dt.hour, chunk_start_dt.minute
    )
    # The file_exists check here relies on a specific naming convention for "job marker files",
    # which includes the job_identifier_suffix. This differs from write_file_grouped's final output filename.
    if args.skip_if_archive_exists and file_exists(args, job_identifier_suffix, s_year, s_month_abbr, s_day, s_hour, s_minute):
        logging.info(f"{indent}⏭️ Skipping job {job_identifier_suffix} ({chunk_start_dt.isoformat()} to {chunk_end_dt.isoformat()}) as its marker/output file exists.")
        return

    logging.info(f"{indent}🔎 Querying for job {job_identifier_suffix}: {chunk_start_dt.isoformat()} → {chunk_end_dt.isoformat()}")

    job_id = None
    try:
        job_id = exp.create_job(query, chunk_start_dt.isoformat(), chunk_end_dt.isoformat())
        exp.wait_for_completion(job_id, initial_delay=args.poll_initial_delay)

        # Stream messages with max_messages_per_file + 1 to check if limit is exceeded
        data = list(exp.stream_messages(job_id, max_messages_to_fetch=args.max_messages_per_file + 1))
        logging.info(f"{indent}📬 Received {len(data)} messages for job {job_identifier_suffix}.")

        if len(data) > args.max_messages_per_file: # More messages than allowed in one file
            logging.info(f"{indent}Messages ({len(data)}) exceed max_messages_per_file ({args.max_messages_per_file}). Splitting chunk.")
            if duration > timedelta(minutes=1): # If current chunk is more than 1 minute, we can split it

                split_granularity_duration = timedelta(hours=1)
                if duration <= timedelta(hours=1):
                    split_granularity_duration = timedelta(minutes=1)

                num_sub_chunks = (duration.total_seconds() + split_granularity_duration.total_seconds() -1 ) // split_granularity_duration.total_seconds()
                num_sub_chunks = int(max(1, num_sub_chunks))


                for i in range(num_sub_chunks):
                    sub_chunk_start = chunk_start_dt + i * split_granularity_duration
                    if sub_chunk_start > chunk_end_dt:
                        break

                    sub_chunk_end = min(sub_chunk_start + split_granularity_duration - timedelta(seconds=1), chunk_end_dt)

                    # Suffix for the sub-job, based on its specific start time (minute precision for uniqueness)
                    sub_job_id_suffix_base = f"{sub_chunk_start.year}{calendar.month_abbr[sub_chunk_start.month]}{sub_chunk_start.day:02}"
                    sub_job_id_suffix = range_suffix(sub_job_id_suffix_base, sub_chunk_start, "minute")

                    query_and_export(exp, args, query, sub_chunk_start, sub_chunk_end,
                                     sub_job_id_suffix, depth + 1)
                return # Return after dispatching sub-tasks for the oversized chunk
            else: # duration is 1 minute or less, but still too many messages
                logging.warning(f"{indent}⚠️ Chunk {job_identifier_suffix} ({chunk_start_dt.isoformat()}) is at minimum 1-min granularity but has {len(data)} messages (limit {args.max_messages_per_file}). Writing first {args.max_messages_per_file} messages.")
                data = data[:args.max_messages_per_file] # Truncate to fit

        # Write data if any, or if not skipping empty files
        if data or not args.no_file_if_zero_messages:
            write_file_grouped(data, args)
        else:
            logging.info(f"{indent}💨 No messages for job {job_identifier_suffix} and no_file_if_zero_messages is True. Skipping file write.")

    except Exception as e:
        logging.error(f"{indent}🚨 Failed to process chunk {job_identifier_suffix} ({chunk_start_dt.isoformat()} to {chunk_end_dt.isoformat()}): {e}")
        if job_id: logging.error(f"{indent}Associated Job ID: {job_id}")


# === Main Execution ===
# === Main Execution ===
def main():
    parser = argparse.ArgumentParser(description="Extract data from SumoLogic to compressed JSON files.")
    # ... (all existing arguments) ...
    parser.add_argument("--query", required=True, help="SumoLogic query string.")
    parser.add_argument("--years", nargs="+", type=int, required=True, help="Year(s) to process.")
    parser.add_argument("--months", nargs="+", help="Month abbreviation(s) e.g., Jan Feb. Processes all if omitted.")
    parser.add_argument("--days", nargs="+", type=int, help="Day(s) of the month. Processes all if omitted.")

    parser.add_argument("--prefix", default="sumo_export", help="Prefix for output filenames and directories.")
    parser.add_argument("--container-name", default="sumo-archive", help="Root directory for archives.")
    parser.add_argument("--files-stored-by", choices=["month", "day", "hour", "minute"], default="month",
                        help="Granularity of subdirectories and file names for storing data.")

    parser.add_argument("--rate-limit", type=int, default=4, help="Max concurrent API calls to SumoLogic.")
    parser.add_argument("--max-messages-per-file", type=int, default=100000,
                        help="Maximum messages per output file. If a query chunk exceeds this, it may be split or truncated.")
    parser.add_argument("--initial-query-by-minutes", type=int, default=60,
                        help="Max duration in minutes for the initial search window when determining optimal chunk size (e.g., 60 for 1hr).")
    parser.add_argument("--poll-initial-delay", type=int, default=10, help="Initial delay (seconds) before polling job status.")

    parser.add_argument("--skip-if-archive-exists", action="store_true", help="Skip processing if output file/marker already exists.")
    parser.add_argument("--no-file-if-zero-messages", action="store_true", help="Do not create a file if a chunk query returns zero messages.")

    # --- New Arguments ---
    parser.add_argument("--get-prefix-optimal-chunk-size-to-db", action="store_true",
                        help="If set, only find and store optimal chunk sizes for the specified "
                             "query and time ranges to the DB. Does not export data.")
    parser.add_argument("--default-chunk-minutes-if-not-in-db", type=int, default=60,
                        help="Default chunk size in minutes to use for export if an optimal size "
                             "is not found in the database (and not in optimal chunk discovery mode).")
    # --- End New Arguments ---

    args = parser.parse_args()

    init_db() # Initialize database connection and tables

    sumo_access_id = must_env("SUMO_ACCESS_ID")
    sumo_access_key = must_env("SUMO_ACCESS_KEY")
    sumo_api_endpoint = must_env("SUMO_API_ENDPOINT")

    exporter = SumoExporter(sumo_access_id, sumo_access_key, sumo_api_endpoint, rate_limit=args.rate_limit)

    tasks = [] # Initialize tasks list for export mode

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

                    optimal_minutes_for_hour = None

                    if args.get_prefix_optimal_chunk_size_to_db:
                        # In discovery mode, always find and store.
                        logging.info(f"🔬 Discovery Mode: Finding optimal chunk size for query (truncated), date {db_date_str}, hour {hour_num:02}.")
                        # Cap the search window for optimal chunk size.
                        max_search_window_minutes = min(args.initial_query_by_minutes, 60)
                        # find_optimal_chunk_size now stores to DB internally
                        optimal_minutes_for_hour = find_optimal_chunk_size(exporter, args, args.query,
                                                                         current_hour_start_dt, max_search_window_minutes)
                        logging.info(f"🔬 Discovery for {db_date_str} H{hour_num:02} resulted in {optimal_minutes_for_hour} minutes (stored).")
                    else:
                        # In export mode, try to get from DB or use default.
                        optimal_minutes_for_hour = get_optimal_chunk_minutes_from_db(args.query, db_date_str, hour_num)
                        if not optimal_minutes_for_hour:
                            optimal_minutes_for_hour = args.default_chunk_minutes_if_not_in_db
                            logging.warning(f"⚠️ Optimal chunk size not in DB for query (truncated) / {db_date_str} H{hour_num:02}. "
                                            f"Using default: {optimal_minutes_for_hour} minutes.")
                        else:
                            logging.info(f"💾 DB: Found optimal chunk size for query (truncated) / {db_date_str} H{hour_num:02}: "
                                         f"{optimal_minutes_for_hour} minutes.")

                        # Prepare tasks for export only if not in discovery mode
                        actual_chunk_delta = timedelta(minutes=optimal_minutes_for_hour)
                        loop_chunk_start_dt = current_hour_start_dt
                        # Ensure end_of_hour_dt is precisely the last moment of the hour for comparison
                        end_of_hour_dt = current_hour_start_dt.replace(minute=59, second=59, microsecond=999999)


                        while loop_chunk_start_dt <= end_of_hour_dt:
                            # Calculate end of the current chunk, ensuring it doesn't exceed the hour boundary
                            loop_chunk_end_dt = min(loop_chunk_start_dt + actual_chunk_delta - timedelta(seconds=1), end_of_hour_dt)

                            job_id_suffix_base = f"{loop_chunk_start_dt.year}{calendar.month_abbr[loop_chunk_start_dt.month]}{loop_chunk_start_dt.day:02}"
                            job_id_suffix = range_suffix(job_id_suffix_base, loop_chunk_start_dt, "minute")

                            tasks.append((exporter, args, args.query, loop_chunk_start_dt, loop_chunk_end_dt, job_id_suffix))
                            loop_chunk_start_dt += actual_chunk_delta

    if args.get_prefix_optimal_chunk_size_to_db:
        logging.info("✅ Optimal chunk size discovery mode complete. Results stored in database.")
    else:
        logging.info(f"Prepared {len(tasks)} tasks for data export.")
        if tasks:
            with ThreadPoolExecutor(max_workers=args.rate_limit) as executor:
                futures = [executor.submit(query_and_export, *task_args) for task_args in tasks]
                for i, future in enumerate(as_completed(futures)):
                    try:
                        future.result()
                        logging.info(f"Task {i+1}/{len(tasks)} completed.")
                    except Exception as e:
                        logging.error(f"🚨 Task {i+1}/{len(tasks)} failed: {e}")
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
