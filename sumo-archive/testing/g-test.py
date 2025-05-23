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
GLOBAL_OPTIMAL_CHUNK_MINUTES = None
GLOBAL_CONSECUTIVE_GROW_COUNT = 0
FILES_STORED_BY = "minute"

def must_env(key):
    val = os.getenv(key)
    if not val:
        logging.critical(f"Missing required environment variable: {key}")
        sys.exit(1)
    return val

def build_output_path(container, prefix, year, month_abbr, day, hour, minute):
    path_parts = [container, prefix, str(year), month_abbr, f"{day:02}", f"H{hour:02}", f"M{minute:02}"]
    return os.path.join(*path_parts)

def check_file_exists(args, year, month, day, hour, minute):
    month_abbr = calendar.month_abbr[month]
    output_dir = build_output_path(args.container_name, args.prefix, year, month_abbr, day, hour, minute)
    filename = f"{args.prefix}_{year:04}{month_abbr}{day:02}H{hour:02}M{minute:02}.json.zst"
    full_path = os.path.join(output_dir, filename)
    return os.path.exists(full_path)

class SumoExporter:
    def __init__(self, access_id, access_key, api_endpoint, rate_limit, backoff_seconds):
        self.session = requests.Session()
        self.session.auth = (access_id, access_key)
        self.session.headers.update({"Content-Type": "application/json", "Accept": "application/json"})
        self.api_endpoint = api_endpoint.rstrip('/')
        self.semaphore = Semaphore(rate_limit)
        self.backoff_seconds = backoff_seconds

    def _request_with_retry(self, method, url, **kwargs):
        with self.semaphore:
            retry_count = 0
            while retry_count <= 5:
                try:
                    resp = self.session.request(method, url, **kwargs)
                    if resp.status_code == 429:
                        logging.warning(f"Rate limit hit for {method} {url}. Backing off for {self.backoff_seconds}s. Retry count: {retry_count}")
                        time.sleep(self.backoff_seconds * (retry_count + 1))
                        retry_count += 1
                        continue
                    resp.raise_for_status()
                    return resp
                except requests.exceptions.Timeout as e:
                    logging.warning(f"Timeout for {method} {url}: {e}. Retrying in {self.backoff_seconds}s...")
                    time.sleep(self.backoff_seconds)
                except requests.RequestException as e:
                    logging.error(f"RequestException for {method} {url}: {e}. Retrying in 10s...")
                    time.sleep(10)
                retry_count += 1

            logging.error(f"Max retries exceeded for {method} {url}.")
            raise requests.RequestException(f"Max retries exceeded for {method} {url}")

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
                logging.error(f"Job {job_id} {state}. Error: {error_msg}. Trace: {status.get('trace', 'N/A')}")
                raise Exception(f"Job {job_id} {state}. Error: {error_msg}")
            if state == "GATHERING RESULTS" and status.get("pendingWarnings"):
                logging.warning(f"Job {job_id} has pending warnings: {status.get('pendingWarnings')}")
            if state == "GATHERING RESULTS" and status.get("pendingErrors"):
                logging.error(f"Job {job_id} has pending errors: {status.get('pendingErrors')}")
            time.sleep(poll_interval)

    def stream_messages(self, job_id, limit_per_request=10000):
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
    global GLOBAL_OPTIMAL_CHUNK_MINUTES, GLOBAL_CONSECUTIVE_GROW_COUNT

    query_key_for_adaptive = f"{query[:50]}_adaptive_check"

    if query_key_for_adaptive not in adaptive_chunk_state:
        adaptive_chunk_state[query_key_for_adaptive] = {
            'last_chunk_minutes': current_chunk_minutes,
            'last_message_count': -1,
            'consecutive_shrink_count': 0,
            'last_adaptive_check_time': datetime.now(timezone.utc)
        }

    state = adaptive_chunk_state[query_key_for_adaptive]

    test_chunk_minutes = current_chunk_minutes
    if test_chunk_minutes == 0:
        test_chunk_minutes = 1

    test_chunk_end_time = current_start_dt + timedelta(minutes=test_chunk_minutes) - timedelta(seconds=1)

    job_id = None
    message_count = 0
    try:
        job_id = exporter.create_job(query, current_start_dt.isoformat(), test_chunk_end_time.isoformat())
        status = exporter.wait_for_completion(job_id, initial_delay=args.poll_initial_delay)
        message_count = status.get("messageCount", 0)
        logging.info(f"Adaptive test: {current_start_dt.strftime('%Y-%m-%d %H:%M')} - {test_chunk_end_time.strftime('%H:%M')} (Test Chunk {test_chunk_minutes} min) yielded {message_count} messages (Limit: {args.api_max_allowed_messages_for_query}).")

    except Exception as e:
        logging.error(f"Error during adaptive chunk size test (size {test_chunk_minutes} min) for {current_start_dt.isoformat()}: {e}")
        if job_id: logging.error(f"Associated Job ID: {job_id}")

        proposed_chunk_minutes = max(1, test_chunk_minutes // 4)
        if proposed_chunk_minutes == test_chunk_minutes:
            proposed_chunk_minutes = max(1, test_chunk_minutes - 1)

        logging.info(f"Adaptive test failed. Shrinking test chunk from {test_chunk_minutes} to {proposed_chunk_minutes} min.")
        GLOBAL_OPTIMAL_CHUNK_MINUTES = proposed_chunk_minutes
        GLOBAL_CONSECUTIVE_GROW_COUNT = 0
        state['consecutive_shrink_count'] += 1
        state['last_chunk_minutes'] = proposed_chunk_minutes
        state['last_message_count'] = message_count
        state['last_adaptive_check_time'] = datetime.now(timezone.utc)
        return proposed_chunk_minutes

    proposed_chunk_minutes = test_chunk_minutes

    if message_count >= args.api_max_allowed_messages_for_query:
        logging.info(f"Messages ({message_count}) at {test_chunk_minutes} min exceed API limit. Shrinking.")
        proposed_chunk_minutes = max(1, test_chunk_minutes // 2)
        if proposed_chunk_minutes == test_chunk_minutes:
            proposed_chunk_minutes = 1

        state['consecutive_shrink_count'] += 1
        GLOBAL_CONSECUTIVE_GROW_COUNT = 0
        state['last_adaptive_check_time'] = datetime.now(timezone.utc)

        if state['consecutive_shrink_count'] >= args.consecutive_max_message_shrink_count:
            logging.info(f"Consecutive shrink limit reached ({args.consecutive_max_message_shrink_count}). Setting global optimal to {proposed_chunk_minutes} minutes.")
            GLOBAL_OPTIMAL_CHUNK_MINUTES = proposed_chunk_minutes
            return proposed_chunk_minutes

    elif message_count > 0 and message_count < args.api_max_allowed_messages_for_query * (1 - args.consecutive_max_message_shrink_percent / 100.0):
        logging.info(f"Messages ({message_count}) are well below API limit. Trying to grow chunk size.")

        new_size_candidate = test_chunk_minutes * 2
        proposed_chunk_minutes = min(args.initial_query_by_minutes, new_size_candidate)
        proposed_chunk_minutes = max(1, proposed_chunk_minutes)

        GLOBAL_CONSECUTIVE_GROW_COUNT += 1
        state['consecutive_shrink_count'] = 0
        state['last_adaptive_check_time'] = datetime.now(timezone.utc)

        if GLOBAL_CONSECUTIVE_GROW_COUNT >= args.consecutive_non_max_message_grow_count:
            logging.info(f"Consecutive grow limit reached ({args.consecutive_non_max_message_grow_count}). Setting global optimal to {proposed_chunk_minutes} minutes.")
            GLOBAL_OPTIMAL_CHUNK_MINUTES = proposed_chunk_minutes
            GLOBAL_CONSECUTIVE_GROW_COUNT = 0
            return proposed_chunk_minutes
    else:
        logging.debug(f"Messages ({message_count}) at {test_chunk_minutes} min are in acceptable range. Maintaining chunk size.")
        state['consecutive_shrink_count'] = 0
        GLOBAL_CONSECUTIVE_GROW_COUNT = 0
        proposed_chunk_minutes = test_chunk_minutes
        state['last_adaptive_check_time'] = datetime.now(timezone.utc)

    state['last_chunk_minutes'] = proposed_chunk_minutes
    state['last_message_count'] = message_count

    return proposed_chunk_minutes

def write_file_for_minute(data, args, dt_minute):
    if not data:
        logging.debug(f"No data to write for minute {dt_minute.isoformat()}.")
        if args.no_file_if_zero_messages:
            logging.info("Skipping file write due to no-file-if-zero-messages set and zero messages.")
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
    logging.info(f"Processing chunk {job_identifier_suffix}: {chunk_start_dt.isoformat()} -> {chunk_end_dt.isoformat()}")

    job_id = None
    try:
        job_id = exp.create_job(query, chunk_start_dt.isoformat(), chunk_end_dt.isoformat())
        status = exp.wait_for_completion(job_id, initial_delay=args.poll_initial_delay)
        total_messages_for_job = status.get("messageCount", 0)

        logging.info(f"Sumo Logic job {job_identifier_suffix} ({chunk_start_dt.strftime('%H:%M')} - {chunk_end_dt.strftime('%H:%M')}) completed with {total_messages_for_job} messages.")

        if total_messages_for_job > args.api_max_allowed_messages_for_query:
            logging.warning(f"Messages ({total_messages_for_job}) for job {job_identifier_suffix} exceed api-max-allowed-messages-for-query ({args.api_max_allowed_messages_for_query}). Splitting this chunk.")

            duration_seconds = (chunk_end_dt - chunk_start_dt).total_seconds()
            duration_minutes = int(duration_seconds / 60) + (1 if duration_seconds % 60 > 0 else 0)

            if duration_minutes <= 1:
                logging.critical(f"A 1-minute chunk ({chunk_start_dt.isoformat()}) still exceeds {args.api_max_allowed_messages_for_query} messages. Data WILL BE TRUNCATED for this minute unless api_max_allowed_messages_for_query is increased. Fetched {total_messages_for_job} messages.")
                all_messages = list(exp.stream_messages(job_id, limit_per_request=10000))
                logging.info(f"Fetched {len(all_messages)} available messages for critically large 1-minute chunk {job_identifier_suffix}.")
            else:
                new_chunk_minutes = max(1, duration_minutes // 2)

                mid_point = chunk_start_dt + timedelta(minutes=new_chunk_minutes)

                process_query_chunk_recursive(exp, args, query, chunk_start_dt, mid_point - timedelta(seconds=1), f"{job_identifier_suffix}-P1", new_chunk_minutes, adaptive_chunk_state)
                process_query_chunk_recursive(exp, args, query, mid_point, chunk_end_dt, f"{job_identifier_suffix}-P2", duration_minutes - new_chunk_minutes, adaptive_chunk_state)
                return

        all_messages = list(exp.stream_messages(job_id, limit_per_request=10000))
        logging.info(f"Received {len(all_messages)} messages for job {job_identifier_suffix}.")

        grouped_by_minute = defaultdict(list)
        for m in all_messages:
            raw_ts = m.get("map", {}).get("_messagetime", 0)
            try:
                ts = int(raw_ts) // 1000
                dt_utc = datetime.fromtimestamp(ts, timezone.utc)
                minute_start_dt = dt_utc.replace(second=0, microsecond=0)
                grouped_by_minute[minute_start_dt].append(m)
            except (ValueError, TypeError):
                logging.warning(f"Invalid timestamp '{raw_ts}' in message. Skipping message for file grouping.")
                continue

        current_minute_dt = chunk_start_dt.replace(second=0, microsecond=0)
        chunk_end_minute_dt = chunk_end_dt.replace(second=0, microsecond=0)
        # Ensure we iterate through every minute within the chunk's boundaries
        # This handles cases where some minutes might have no messages but we still want to "check" them.
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
    parser.add_argument("--query", required=True, help="SumoLogic query string.")
    parser.add_argument("--years", nargs="+", type=int, required=True, help="Year(s) to process.")
    parser.add_argument("--months", nargs="+", help="Month abbreviation(s) e.g., Jan Feb. Processes all if omitted.")
    parser.add_argument("--days", nargs="+", type=int, help="Day(s) of the month. Processes all if omitted.")

    parser.add_argument("--prefix", default="sumo_export", help="Prefix for output filenames and directories.")
    parser.add_argument("--container-name", default="sumo-archive", help="Root directory for archives.")

    parser.add_argument("--rate-limit", type=int, default=4, help="Max concurrent API calls to SumoLogic.")
    parser.add_argument("--api-max-allowed-messages-for-query", type=int, default=100000,
                        help="Maximum messages allowed per single Sumo Logic API query. If a query chunk exceeds this, it will be split recursively.")
    parser.add_argument("--initial-query-by-minutes", type=int, default=60,
                        help="Initial duration in minutes for a query chunk. This is the starting point for dynamic adjustments and the maximum allowed chunk size.")
    parser.add_argument("--poll-initial-delay", type=int, default=10, help="Initial delay (seconds) before polling job status.")

    parser.add_argument("--overwrite-if-archive-file-exists", action="store_true", help="Overwrite the output file if it already exists.")
    parser.add_argument("--no-file-if_zero_messages", action="store_true", help="Do not create a file if a chunk query returns zero messages.")

    parser.add_argument("--consecutive-max-message-shrink-count", type=int, default=1,
                        help="Number of consecutive query chunk size reductions due to exceeding max messages before trying to settle.")
    parser.add_argument("--consecutive-max-message-shrink-percent", type=int, default=50,
                        help="Percentage threshold for message count drop to trigger growth (e.g., if current count < 50% of max).")
    parser.add_argument("--consecutive-non-max-message-grow-count", type=int, default=2,
                        help="Number of consecutive query chunk size increases where message count is well below max before trying to settle.")
    parser.add_argument("--consecutive-non-max-message-grow-percent", type=int, default=50,
                        help="Percentage threshold for message count increase to trigger growth (e.g., if current count > 50% of max).")

    parser.add_argument("--adaptive-check-interval-hours", type=int, default=12,
                        help="Interval in hours to periodically re-evaluate the global optimal chunk size for growth, even if consecutive grow conditions aren't met.")


    parser.add_argument("--log-file", type=str, default="sumo-query-to-files.log",
                        help="Path to the log file.")
    parser.add_argument("--api-backoff-seconds", type=int, default=8,
                        help="Seconds to back off when Sumo Logic API rate limit is hit.")

    args = parser.parse_args()

    LOG_FILE = args.log_file
    SUMO_HTTP_API_BACKOFF_SECONDS = args.api_backoff_seconds

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler(sys.stdout)],
    )

    sumo_access_id = must_env("SUMO_ACCESS_ID")
    sumo_access_key = must_env("SUMO_ACCESS_KEY")
    sumo_api_endpoint = must_env("SUMO_API_ENDPOINT")

    exporter = SumoExporter(sumo_access_id, sumo_access_key, sumo_api_endpoint,
                            rate_limit=args.rate_limit, backoff_seconds=args.api_backoff_seconds)

    tasks_to_queue = []
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

                    current_block_start_dt = None

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

                        is_last_minute_of_hour = (minute_num == 59)
                        is_end_of_block = file_exists_for_minute or is_last_minute_of_hour

                        if is_end_of_block and current_block_start_dt is not None:
                            block_end_dt = current_minute_to_check_dt.replace(second=59, microsecond=999999) if not file_exists_for_minute else (current_minute_to_check_dt - timedelta(minutes=1)).replace(second=59, microsecond=999999)

                            current_adaptive_test_size = GLOBAL_OPTIMAL_CHUNK_MINUTES if GLOBAL_OPTIMAL_CHUNK_MINUTES is not None else args.initial_query_by_minutes

                            query_adaptive_state = adaptive_chunk_state.get(f"{args.query[:50]}_adaptive_check", {})
                            last_adaptive_check_time = query_adaptive_state.get('last_adaptive_check_time', datetime.min.replace(tzinfo=timezone.utc))

                            if (datetime.now(timezone.utc) - last_adaptive_check_time) >= timedelta(hours=args.adaptive_check_interval_hours) or GLOBAL_OPTIMAL_CHUNK_MINUTES is None:
                                logging.info(f"Triggering adaptive chunk size re-evaluation for query beginning at {current_block_start_dt}.")
                                current_adaptive_test_size = get_optimal_query_minutes(
                                    exporter, args, args.query, current_block_start_dt,
                                    current_adaptive_test_size, adaptive_chunk_state
                                )
                                GLOBAL_OPTIMAL_CHUNK_MINUTES = current_adaptive_test_size
                            else:
                                logging.debug(f"Using current GLOBAL_OPTIMAL_CHUNK_MINUTES: {GLOBAL_OPTIMAL_CHUNK_MINUTES}")
                                current_adaptive_test_size = GLOBAL_OPTIMAL_CHUNK_MINUTES

                            chunk_start_for_api = current_block_start_dt
                            while chunk_start_for_api <= block_end_dt:
                                chunk_end_for_api = min(
                                    chunk_start_for_api + timedelta(minutes=current_adaptive_test_size) - timedelta(seconds=1),
                                    block_end_dt
                                )
                                job_id_suffix_base = f"{chunk_start_for_api.year}{calendar.month_abbr[chunk_start_for_api.month]}{chunk_start_for_api.day:02}"
                                job_id_suffix = f"{job_id_suffix_base}H{chunk_start_for_api.hour:02}M{chunk_start_for_api.minute:02}"

                                tasks_to_queue.append((exporter, args, args.query, chunk_start_for_api, chunk_end_for_api, job_id_suffix, current_adaptive_test_size, adaptive_chunk_state))

                                chunk_start_for_api += timedelta(minutes=current_adaptive_test_size)

                            current_block_start_dt = None

                        elif file_exists_for_minute:
                            current_block_start_dt = None

    logging.info(f"Prepared {len(tasks_to_queue)} initial tasks for data export.")
    if tasks_to_queue:
        with ThreadPoolExecutor(max_workers=args.rate_limit) as executor:
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
