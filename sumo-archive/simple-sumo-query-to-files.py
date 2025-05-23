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

