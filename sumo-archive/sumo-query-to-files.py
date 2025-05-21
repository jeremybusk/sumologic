#!/usr/bin/env python3
# Full updated script continues here...
# Existing imports and SumoExporter class remain unchanged above

#!/usr/bin/env python3
import os
import sys
import json
import time
import logging
import calendar
import requests
import zstandard as zstd
from datetime import datetime, timedelta, timezone, UTC
from collections import defaultdict
from threading import Semaphore

# === Configuration ===
sumo_http_api_backoff_seconds = 15
log_file = "sumo-query-to-files.log"

# === Logging Setup ===
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    handlers=[logging.FileHandler(log_file), logging.StreamHandler(sys.stdout)],
)

# === Utility Functions ===
def must_env(key):
    val = os.getenv(key)
    if not val:
        print(f"Missing required env var: {key}")
        sys.exit(1)
    return val

def build_output_path(container, prefix, year, month_abbr, day=None, hour=None, minute=None, files_stored_by="month"):
    path = os.path.join(container, prefix, str(year), month_abbr)
    if files_stored_by in ["day", "hour", "minute"] and day is not None:
        path = os.path.join(path, f"{day:02}")
    if files_stored_by in ["hour", "minute"] and hour is not None:
        path = os.path.join(path, f"H{hour:02}")
    if files_stored_by == "minute" and minute is not None:
        path = os.path.join(path, f"M{minute:02}")
    return path

def file_exists(args, suffix, year, month_abbr, day=None, hour=None, minute=None):
    path = build_output_path(args.container_name, args.prefix, year, month_abbr, day, hour, minute, args.files_stored_by)
    file_path = os.path.join(path, f"{args.prefix}_{suffix}.json.zst")
    logging.info(f"🔍 Checking for existing archive: {file_path} → {os.path.exists(file_path)}")
    return os.path.exists(file_path)

def range_suffix(base_suffix, dt, granularity):
    parts = [base_suffix]
    if granularity in ("hour", "minute"):
        parts.append(f"H{dt.hour:02}")
    if granularity == "minute":
        parts.append(f"M{dt.minute:02}")
    return ''.join(parts)

def should_skip_chunk(args, suffix, year, month_abbr, day=None, hour=None, minute=None):
    return args.skip_if_archive_exists and file_exists(args, suffix, year, month_abbr, day, hour, minute)

# === Core Class ===
class SumoExporter:
    def __init__(self, access_id, access_key, api_endpoint, sas_url=None, azure_container_path=None, verbose=None, rate_limit=4):
        self.session = requests.Session()
        self.session.auth = (access_id, access_key)
        self.session.headers.update({"Content-Type": "application/json", "Accept": "application/json"})
        self.api_endpoint = api_endpoint.rstrip('/')
        self.sas_url = sas_url
        self.azure_container_path = azure_container_path or ""
        self.semaphore = Semaphore(rate_limit)

    def upload_blob_from_memory(self, data_bytes: bytes, blob_name: str):
        from urllib.parse import urlparse
        import posixpath
        if not self.sas_url:
            raise ValueError("AZURE_BLOB_SAS must be set for upload.")
        cctx = zstd.ZstdCompressor()
        compressed_bytes = cctx.compress(data_bytes)
        parsed = urlparse(self.sas_url)
        upload_url = f"{parsed.scheme}://{parsed.netloc}/{posixpath.join(self.azure_container_path, blob_name).lstrip('/')}?{parsed.query}"
        headers = {"x-ms-blob-type": "BlockBlob", "Content-Type": "application/zstd"}
        resp = requests.put(upload_url, headers=headers, data=compressed_bytes)
        resp.raise_for_status()
        logging.info(f"✅ Uploaded to Azure Blob: {upload_url.split('?')[0]}")

    def create_job(self, query, time_from, time_to):
        with self.semaphore:
            payload = {"query": query, "from": time_from, "to": time_to, "timeZone": "UTC"}
            url = f"{self.api_endpoint}/api/v1/search/jobs"
            while True:
                try:
                    resp = self.session.post(url, json=payload)
                    if resp.status_code == 429:
                        logging.warning("Rate limit on create_job, backing off")
                        time.sleep(sumo_http_api_backoff_seconds)
                        continue
                    resp.raise_for_status()
                    return resp.json()["id"]
                except requests.RequestException as e:
                    logging.error(f"create_job error: {e}")
                    time.sleep(5)

    def wait_for_completion(self, job_id, poll_interval=5, initial_delay=5):
        url = f"{self.api_endpoint}/api/v1/search/jobs/{job_id}"
        time.sleep(initial_delay)
        while True:
            try:
                resp = self.session.get(url)
                if resp.status_code == 429:
                    logging.warning("Rate limit on wait_for_completion")
                    time.sleep(sumo_http_api_backoff_seconds)
                    continue
                resp.raise_for_status()
                if resp.json().get("state") == "DONE GATHERING RESULTS":
                    return
                if resp.json().get("state") in ["CANCELLED", "FAILED"]:
                    raise Exception(f"Job {job_id} failed")
            except requests.RequestException as e:
                logging.error(f"wait_for_completion error: {e}")
            time.sleep(poll_interval)

    def stream_messages(self, job_id, limit_per_request=10000, max_messages=100000):
        offset = 0
        while True:
            params = {"limit": limit_per_request, "offset": offset}
            url = f"{self.api_endpoint}/api/v1/search/jobs/{job_id}/messages"
            try:
                resp = self.session.get(url, params=params)
                if resp.status_code == 429:
                    logging.warning("Rate limit on stream_messages")
                    time.sleep(sumo_http_api_backoff_seconds)
                    continue
                resp.raise_for_status()
                messages = resp.json().get("messages", [])
                if not messages:
                    break
                for m in messages:
                    yield m
                offset += len(messages)
                if len(messages) < limit_per_request or offset >= max_messages:
                    break
            except requests.RequestException as e:
                logging.error(f"stream_messages error: {e}")
                break

# Remaining: query_and_export, write_file_grouped, and main function


# === New Functions ===
def write_file_grouped(data, args, suffix, year, month_abbr, day=None):
    grouped = defaultdict(list)
    for m in data:
        ts = int(m.get("map", {}).get("_messagetime", 0)) // 1000
        dt = datetime.fromtimestamp(ts, UTC)
        if args.files_stored_by == "minute":
            key = (dt.day, dt.hour, dt.minute)
        elif args.files_stored_by == "hour":
            key = (dt.day, dt.hour)
        else:
            key = (dt.day,)
        grouped[key].append(m)

    for key, items in grouped.items():
        d, h, m = (key + (None,) * (3 - len(key)))
        path = build_output_path(args.container_name, args.prefix, year, month_abbr, d, h, m, args.files_stored_by)
        os.makedirs(path, exist_ok=True)
        final_path = os.path.join(path, f"{args.prefix}_{suffix}.json.zst")
        if not items and args.no_file_if_contains_zero_messages:
            continue
        with open(final_path, "wb") as f:
            cctx = zstd.ZstdCompressor()
            f.write(cctx.compress(json.dumps(items, indent=2).encode("utf-8")))
        logging.info(f"💾 Saved: {final_path}")

def query_and_export(exp, args, query, start, end, suffix, year, month_abbr, day=None, depth=0):
    indent = "  " * depth
    duration = end - start
    hour = start.hour if args.files_stored_by in ["hour", "minute"] else None
    minute = start.minute if args.files_stored_by == "minute" else None
    if should_skip_chunk(args, suffix, year, month_abbr, day, hour, minute):
        logging.info(f"{indent}⏩ Skipping {suffix} (already archived)")
        return

    logging.info(f"{indent}🔍 Querying: {start.isoformat()} → {end.isoformat()}")
    job_id = exp.create_job(query, start.isoformat(), end.isoformat())
    exp.wait_for_completion(job_id, initial_delay=args.poll_initial_delay)
    data = list(exp.stream_messages(job_id, max_messages=args.max_messages))
    logging.info(f"{indent}📥 {len(data)} messages for {suffix}")

    if len(data) >= args.max_messages:
        if duration > timedelta(hours=1):
            for h in range(int(duration.total_seconds() // 3600)):
                chunk_start = start + timedelta(hours=h)
                chunk_end = min(chunk_start + timedelta(hours=1) - timedelta(seconds=1), end)
                chunk_suffix = range_suffix(suffix, chunk_start, "hour")
                query_and_export(exp, args, query, chunk_start, chunk_end, chunk_suffix, year, month_abbr, chunk_start.day, depth + 1)
            return
        elif duration > timedelta(minutes=1):
            for m in range(int(duration.total_seconds() // 60)):
                chunk_start = start + timedelta(minutes=m)
                chunk_end = min(chunk_start + timedelta(minutes=1) - timedelta(seconds=1), end)
                chunk_suffix = range_suffix(suffix, chunk_start, "minute")
                query_and_export(exp, args, query, chunk_start, chunk_end, chunk_suffix, year, month_abbr, chunk_start.day, depth + 1)
            return
        else:
            logging.error(f"{indent}⚠️ Cannot split below 1-minute granularity.")
            return

    if args.no_file_if_contains_zero_messages and not data:
        return
    write_file_grouped(data, args, suffix, year, month_abbr, day)


from concurrent.futures import ThreadPoolExecutor, as_completed

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", nargs="+", type=int, help="Limit to specific days of month (1-31)")
    parser.add_argument("--no-file-if-contains_zero-messages", action="store_true")
    parser.add_argument("--query", required=True)
    parser.add_argument("--years", nargs="+", type=int, required=True)
    parser.add_argument("--months", nargs="+", help="Limit to specific months, e.g. Jan Feb")
    parser.add_argument("--prefix", default="sumo_export")
    parser.add_argument("--container-name", default="sumo-archive")
    parser.add_argument("--rate-limit", type=int, default=4)
    parser.add_argument("--max-messages", type=int, default=100000)
    parser.add_argument("--upload", action="store_true")
    parser.add_argument("--query-by", choices=["month", "day", "hour", "minute"], default="month")
    parser.add_argument("--files-stored-by", choices=["month", "day", "hour", "minute"], default="month")
    parser.add_argument("--poll-initial-delay", type=int, default=5)
    parser.add_argument("--skip-if-archive-exists", action="store_true")
    args = parser.parse_args()

    exp = SumoExporter(
        must_env("SUMO_ACCESS_ID"),
        must_env("SUMO_ACCESS_KEY"),
        must_env("SUMO_API_ENDPOINT"),
        os.getenv("AZURE_BLOB_SAS"),
        args.container_name,
        rate_limit=args.rate_limit,
    )

    with ThreadPoolExecutor(max_workers=args.rate_limit) as executor:
        futures = []
        for year in args.years:
            for month in range(1, 13):
                month_abbr = calendar.month_abbr[month]
                if args.months and month_abbr not in args.months:
                    continue
                days_in_month = calendar.monthrange(year, month)[1]
                selected_days = args.days if args.days else range(1, days_in_month + 1)

                for day in selected_days:
                    if args.query_by == "month":
                        start = datetime(year, month, 1, tzinfo=timezone.utc)
                        end = datetime(year, month, days_in_month, 23, 59, 59, tzinfo=timezone.utc)
                        suffix = f"{year}{month_abbr}"
                        futures.append(executor.submit(query_and_export, exp, args, args.query, start, end, suffix, year, month_abbr))
                        break

                    for hour in range(24):
                        if args.query_by == "day":
                            start = datetime(year, month, day, 0, 0, tzinfo=timezone.utc)
                            end = datetime(year, month, day, 23, 59, 59, tzinfo=timezone.utc)
                            suffix = f"{year}{month_abbr}{day:02}"
                            futures.append(executor.submit(query_and_export, exp, args, args.query, start, end, suffix, year, month_abbr, day))
                            break

                        for minute in range(60):
                            if args.query_by == "hour":
                                start = datetime(year, month, day, hour, 0, tzinfo=timezone.utc)
                                end = datetime(year, month, day, hour, 59, 59, tzinfo=timezone.utc)
                                suffix = f"{year}{month_abbr}{day:02}H{hour:02}"
                                futures.append(executor.submit(query_and_export, exp, args, args.query, start, end, suffix, year, month_abbr, day))
                                break

                            if args.query_by == "minute":
                                start = datetime(year, month, day, hour, minute, tzinfo=timezone.utc)
                                end = datetime(year, month, day, hour, minute, 59, tzinfo=timezone.utc)
                                suffix = f"{year}{month_abbr}{day:02}H{hour:02}M{minute:02}"
                                futures.append(executor.submit(query_and_export, exp, args, args.query, start, end, suffix, year, month_abbr, day))

        for future in as_completed(futures):
            future.result()

if __name__ == "__main__":
    main()
