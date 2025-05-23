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
from threading import Semaphore
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import argparse

# === Configuration ===
SUMO_HTTP_API_BACKOFF_SECONDS = 15
LOG_FILE = "sumo-query-to-files.log"
SQLITE_PATH = "query_chunk_sizes.db"
DB_CONN = None

# === Logging ===
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
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
    base_path = Path(container) / prefix / str(year) / month_abbr
    if files_stored_by in ["day", "hour", "minute"] and day is not None:
        base_path /= f"{day:02}"
    if files_stored_by in ["hour", "minute"] and hour is not None:
        base_path /= f"H{hour:02}"
    if files_stored_by == "minute" and minute is not None:
        base_path /= f"M{minute:02}"
    return base_path

def range_suffix(base_suffix, dt, granularity):
    suffix = base_suffix
    if granularity in ("hour", "minute"):
        suffix += f"H{dt.hour:02}"
    if granularity == "minute":
        suffix += f"M{dt.minute:02}"
    return suffix

def file_exists(args, job_specific_suffix, year, month_abbr, day=None, hour=None, minute=None):
    path = build_output_path(args.container_name, args.prefix, year, month_abbr, day, hour, minute, args.files_stored_by)
    file_path = path / f"{args.prefix}_{job_specific_suffix}.json.zst"
    return file_path.exists()

# === Exporter ===
class SumoExporter:
    def __init__(self, access_id, access_key, api_endpoint, rate_limit=4):
        self.session = requests.Session()
        self.session.auth = (access_id, access_key)
        self.session.headers.update({"Content-Type": "application/json", "Accept": "application/json"})
        self.api_endpoint = api_endpoint.rstrip('/')
        self.semaphore = Semaphore(rate_limit)

    def _request_with_retry(self, method, url, **kwargs):
        with self.semaphore:
            retries = 0
            while True:
                try:
                    resp = self.session.request(method, url, **kwargs)
                    if resp.status_code == 429:
                        retries += 1
                        if retries > 5:
                            resp.raise_for_status()
                        time.sleep(SUMO_HTTP_API_BACKOFF_SECONDS * retries)
                        continue
                    resp.raise_for_status()
                    return resp
                except requests.exceptions.RequestException as e:
                    logging.error(f"Retrying {url} after error: {e}")
                    time.sleep(10)

    def create_job(self, query, time_from, time_to):
        url = f"{self.api_endpoint}/api/v1/search/jobs"
        payload = {"query": query, "from": time_from, "to": time_to, "timeZone": "UTC"}
        return self._request_with_retry("POST", url, json=payload).json()["id"]

    def get_job_status(self, job_id):
        url = f"{self.api_endpoint}/api/v1/search/jobs/{job_id}"
        return self._request_with_retry("GET", url).json()

    def wait_for_completion(self, job_id, poll_interval=5, initial_delay=5):
        time.sleep(initial_delay)
        while True:
            status = self.get_job_status(job_id)
            state = status.get("state")
            if state == "DONE GATHERING RESULTS":
                return status
            if state in ["CANCELLED", "FAILED"]:
                raise Exception(f"Job {job_id} {state}: {status.get('error', 'Unknown')}")
            time.sleep(poll_interval)

    def stream_messages(self, job_id, limit_per_request=10000, max_messages_to_fetch=None):
        offset = 0
        fetched = 0
        while True:
            limit = min(limit_per_request, max_messages_to_fetch - fetched if max_messages_to_fetch else limit_per_request)
            url = f"{self.api_endpoint}/api/v1/search/jobs/{job_id}/messages"
            messages = self._request_with_retry("GET", url, params={"limit": limit, "offset": offset}).json().get("messages", [])
            if not messages:
                break
            for msg in messages:
                yield msg
                fetched += 1
            offset += len(messages)
            if len(messages) < limit:
                break

# === File Writer ===
def write_compressed_json(file_path, messages):
    os.makedirs(file_path.parent, exist_ok=True)
    cctx = zstd.ZstdCompressor()
    try:
        with open(file_path, "wb") as f:
            data = json.dumps(messages, indent=2).encode("utf-8")
            f.write(cctx.compress(data))
        logging.info(f"✅ Written: {file_path} ({len(messages)} messages)")
    except Exception as e:
        logging.error(f"🚨 Failed to write {file_path}: {e}")

# === Chunk Processor ===
def process_chunk(exporter, args, query, chunk_start_dt, chunk_end_dt, job_suffix, min_duration=timedelta(minutes=1), depth=0):
    indent = "  " * depth

    if args.dry_run:
        logging.info(f"{indent}🧪 DRY RUN: Would process chunk {job_suffix} from {chunk_start_dt} to {chunk_end_dt}")
        return

    path = build_output_path(args.container_name, args.prefix,
                             chunk_start_dt.year, calendar.month_abbr[chunk_start_dt.month],
                             chunk_start_dt.day, chunk_start_dt.hour, chunk_start_dt.minute,
                             args.files_stored_by)
    output_file = path / f"{args.prefix}_{job_suffix}.json.zst"

    if args.skip_if_archive_exists and output_file.exists():
        logging.info(f"{indent}⏭️ Skipping existing file: {output_file}")
        return

    logging.info(f"{indent}🔍 Querying from {chunk_start_dt} to {chunk_end_dt} for {job_suffix}")

    try:
        job_id = exporter.create_job(query, chunk_start_dt.isoformat(), chunk_end_dt.isoformat())
        exporter.wait_for_completion(job_id)
        messages = list(exporter.stream_messages(job_id, max_messages_to_fetch=args.max_messages_per_file + 1))

        date_str = chunk_start_dt.strftime("%Y-%m-%d")
        hour = chunk_start_dt.hour
        duration = chunk_end_dt - chunk_start_dt

        if len(messages) > args.max_messages_per_file:
            logging.warning(f"{indent}⚠️ Max messages exceeded for chunk {job_suffix}. Considering split.")
            # simulate adaptive chunk update logic here
            if duration <= min_duration:
                logging.warning(f"{indent}⚠️ Cannot split further. Writing first {args.max_messages_per_file} messages")
                messages = messages[:args.max_messages_per_file]
                write_compressed_json(output_file, messages)
                return

            midpoint = chunk_start_dt + (duration / 2)
            process_chunk(exporter, args, query, chunk_start_dt, midpoint, range_suffix(job_suffix, chunk_start_dt, "minute"), min_duration, depth + 1)
            process_chunk(exporter, args, query, midpoint + timedelta(seconds=1), chunk_end_dt, range_suffix(job_suffix, chunk_end_dt, "minute"), min_duration, depth + 1)
            return

        if messages or not args.no_file_if_zero_messages:
            write_compressed_json(output_file, messages)
        else:
            logging.info(f"{indent}💨 No messages to write for chunk {job_suffix}")

    except Exception as e:
        logging.error(f"{indent}🚨 Error processing chunk {job_suffix}: {e}")

# === Adaptive Chunking Support ===
def init_db():
    global DB_CONN
    DB_CONN = sqlite3.connect(SQLITE_PATH)
    DB_CONN.execute("""
        CREATE TABLE IF NOT EXISTS optimal_chunks (
            query TEXT,
            date TEXT,
            hour INTEGER,
            chunk_minutes INTEGER,
            hit_limit_count INTEGER DEFAULT 0,
            PRIMARY KEY (query, date, hour)
        )""")
    DB_CONN.commit()

def get_initial_chunk_size(query, date_str, hour):
    if not args.use_db_optimal_chunk_size:
        return args.initial_query_by_minutes
    cur = DB_CONN.execute(
        "SELECT chunk_minutes FROM optimal_chunks WHERE query = ? AND date = ? AND hour = ?",
        (query, date_str, hour))
    row = cur.fetchone()
    return row[0] if row else args.initial_query_by_minutes

def update_chunk_stats(query, date_str, hour, current_chunk, hit_limit):
    if not args.use_db_optimal_chunk_size:
        return

    cur = DB_CONN.execute(
        "SELECT chunk_minutes, hit_limit_count FROM optimal_chunks WHERE query = ? AND date = ? AND hour = ?",
        (query, date_str, hour))
    row = cur.fetchone()

    chunk = current_chunk
    hit_count = 0

    if row:
        chunk, hit_count = row

    if hit_limit:
        hit_count += 1
        logging.info(f"📉 Auto-chunk shrink logic: hit max_messages again (count={hit_count})")
        if hit_count >= args.consecutive_max_message_shrink_count:
            new_chunk = max(1, int(chunk * (args.consecutive_max_message_shrink_percent / 100.0)))
            hit_count = 0
            chunk = new_chunk
            logging.info(f"🔻 Reducing chunk size to {chunk} min")
    else:
        hit_count -= 1
        logging.info(f"📈 Auto-chunk grow logic: did not hit max_messages (count={hit_count})")
        if hit_count <= -args.consecutive_non_max_message_grow_count:
            new_chunk = int(chunk * (1 + args.consecutive_non_max_message_grow_percent / 100.0))
            new_chunk = min(new_chunk, args.initial_query_by_minutes)
            hit_count = 0
            chunk = new_chunk
            logging.info(f"🔺 Increasing chunk size to {chunk} min")

    DB_CONN.execute("""
        INSERT INTO optimal_chunks (query, date, hour, chunk_minutes, hit_limit_count)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(query, date, hour) DO UPDATE SET
            chunk_minutes = excluded.chunk_minutes,
            hit_limit_count = excluded.hit_limit_count
    """, (query, date_str, hour, chunk, hit_count))
    DB_CONN.commit()

def periodic_probe_larger_chunk(query, date_str, hour, current_chunk):
    if not args.use_db_optimal_chunk_size:
        return
    if current_chunk < args.initial_query_by_minutes:
        proposed = min(args.initial_query_by_minutes, current_chunk * 2)
        logging.info(f"🔍 Probing with larger chunk size {proposed} min (from {current_chunk} min)")
        DB_CONN.execute("""
            INSERT OR REPLACE INTO optimal_chunks (query, date, hour, chunk_minutes, hit_limit_count)
            VALUES (?, ?, ?, ?, 0)
        """, (query, date_str, hour, proposed))
        DB_CONN.commit()

# === CLI Parser ===
def parse_args():
    parser = argparse.ArgumentParser(description="Export Sumo Logic data into compressed files.")
    parser.add_argument("--query", required=True)
    parser.add_argument("--prefix", default="sumo_export")
    parser.add_argument("--container-name", default="output")
    parser.add_argument("--files-stored-by", choices=["month", "day", "hour", "minute"], default="month")
    parser.add_argument("--max-messages-per-file", type=int, default=100000)
    parser.add_argument("--initial-query-by-minutes", type=int, default=60)
    parser.add_argument("--skip-if-archive-exists", action="store_true")
    parser.add_argument("--no-file-if-zero-messages", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--rate-limit", type=int, default=4)
    parser.add_argument("--max-workers", type=int, default=4)
    parser.add_argument("--max-retries", type=int, default=1)

    parser.add_argument("--years", nargs="+", type=int)
    parser.add_argument("--months", nargs="+")
    parser.add_argument("--days", nargs="+", type=int)

    # Adaptive chunking
    parser.add_argument("--use-db-optimal-chunk-size", action="store_true")
    parser.add_argument("--consecutive-max-message-shrink-count", type=int, default=2)
    parser.add_argument("--consecutive-max-message-shrink-percent", type=int, default=50)
    parser.add_argument("--consecutive-non-max-message-grow-count", type=int, default=5)
    parser.add_argument("--consecutive-non-max-message-grow-percent", type=int, default=50)

    return parser.parse_args()

# === Main ===
def main():
    global args
    args = parse_args()

    if args.use_db_optimal_chunk_size:
        init_db()

    access_id = must_env("SUMO_ACCESS_ID")
    access_key = must_env("SUMO_ACCESS_KEY")
    endpoint = must_env("SUMO_API_ENDPOINT")

    exporter = SumoExporter(access_id, access_key, endpoint, rate_limit=args.rate_limit)

    for year in args.years or [datetime.now().year]:
        for month_num in range(1, 13):
            month_abbr = calendar.month_abbr[month_num]
            if args.months and month_abbr not in args.months:
                continue

            days_in_month = calendar.monthrange(year, month_num)[1]
            selected_days = args.days if args.days else range(1, days_in_month + 1)

            for day in selected_days:
                tasks = []
                for hour in range(24):
                    dt = datetime(year, month_num, day, hour, 0, 0, tzinfo=timezone.utc)
                    date_str = dt.strftime("%Y-%m-%d")
                    chunk_minutes = get_initial_chunk_size(args.query, date_str, hour)
                    start = dt
                    end = start + timedelta(minutes=chunk_minutes) - timedelta(seconds=1)
                    suffix = range_suffix(f"{start.year}{calendar.month_abbr[start.month]}{start.day:02}", start, "minute")
                    tasks.append((exporter, args, args.query, start, end, suffix))

                with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
                    futures = {executor.submit(process_chunk, *t): t for t in tasks}
                    retries = []
                    for i, f in enumerate(as_completed(futures)):
                        try:
                            f.result()
                            logging.info(f"✅ Hour {i+1}/24 completed for {year}-{month_num:02}-{day:02}")
                        except Exception as e:
                            logging.error(f"❌ Task {i+1}/24 failed: {e}")
                            retries.append(futures[f])

                    if retries:
                        logging.info(f"🔁 Retrying {len(retries)} failed tasks")
                        for t in retries:
                            for attempt in range(1, args.max_retries + 1):
                                try:
                                    process_chunk(*t)
                                    logging.info(f"✅ Retry succeeded on attempt {attempt} for {t[3]} → {t[4]}")
                                    break
                                except Exception as e:
                                    logging.error(f"❌ Retry {attempt}/{args.max_retries} failed: {e}")
                                    if attempt == args.max_retries:
                                        logging.error(f"🚨 Giving up on task: {t}")

if __name__ == "__main__":
    main()

