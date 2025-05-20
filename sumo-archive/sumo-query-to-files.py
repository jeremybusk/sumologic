#!/usr/bin/env python3
import os
import sys
import json
import time
import logging
import calendar
import random
import requests
import zstandard as zstd
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Semaphore

log_file = "sumo-query-to-files.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(message)s',
    handlers=[logging.FileHandler(log_file), logging.StreamHandler(sys.stdout)]
)

class SumoExporter:
    def __init__(self, access_id, access_key, api_endpoint, sas_url=None, azure_container_path=None, verbose=None, rate_limit=4):
        self.access_id = access_id
        self.access_key = access_key
        self.api_endpoint = api_endpoint.rstrip('/')
        self.sas_url = sas_url
        self.session = requests.Session()
        self.session.auth = (access_id, access_key)
        self.session.headers.update({"Content-Type": "application/json", "Accept": "application/json"})
        self.azure_container_path = azure_container_path or ""
        self.verbose = verbose
        self.semaphore = Semaphore(rate_limit)

    def upload_blob_from_memory(self, data_bytes: bytes, blob_name: str):
        if not self.sas_url:
            raise ValueError("AZURE_BLOB_SAS must be set for upload.")
        from urllib.parse import urlparse
        import posixpath

        cctx = zstd.ZstdCompressor()
        compressed_bytes = cctx.compress(data_bytes)

        parsed = urlparse(self.sas_url)
        upload_url = f"{parsed.scheme}://{parsed.netloc}/{posixpath.join(self.azure_container_path, blob_name).lstrip('/')}?{parsed.query}"
        headers = {
            "x-ms-blob-type": "BlockBlob",
            "Content-Type": "application/zstd"
        }

        resp = requests.put(upload_url, headers=headers, data=compressed_bytes)
        resp.raise_for_status()
        logging.info(f"✅ Uploaded to Azure Blob: {upload_url.split('?')[0]}")

    def create_job(self, query: str, time_from: str, time_to: str) -> str:
        with self.semaphore:
            payload = {"query": query, "from": time_from, "to": time_to, "timeZone": "UTC"}
            url = f"{self.api_endpoint}/api/v1/search/jobs"
            try:
                resp = self.session.post(url, json=payload)
                if resp.status_code == 429:
                    logging.warning(f"Rate limit hit on create_job: {url}")
                    time.sleep(random.randint(5, 10))
                    # random.randint(5, 10)
                    return self.create_job(query, time_from, time_to)
                resp.raise_for_status()
                return resp.json()["id"]
            except requests.exceptions.RequestException as e:
                logging.error(f"create_job error: {e}")
                raise

    def wait_for_completion(self, job_id: str, poll_interval: int = 5):
        url = f"{self.api_endpoint}/api/v1/search/jobs/{job_id}"
        while True:
            try:
                resp = self.session.get(url)
                if resp.status_code == 429:
                    logging.warning(f"Rate limit hit on wait_for_completion: {url}")
                    time.sleep(random.randint(5, 10))
                    continue
                resp.raise_for_status()
                data = resp.json()
                if data.get("state") == "DONE GATHERING RESULTS":
                    return
                elif data.get("state") in ["CANCELLED", "FAILED"]:
                    raise Exception(f"Job {job_id} failed with state: {data['state']}")
            except requests.exceptions.RequestException as e:
                logging.error(f"wait_for_completion error: {e}")
                raise
            time.sleep(poll_interval)

    def stream_messages(self, job_id: str, limit_per_request: int = 10000, max_messages: int = 100000):
        offset = 0
        total = 0
        while True:
            params = {"limit": limit_per_request, "offset": offset}
            url = f"{self.api_endpoint}/api/v1/search/jobs/{job_id}/messages"
            try:
                resp = self.session.get(url, params=params)
                if resp.status_code == 429:
                    logging.warning(f"Rate limit hit on stream_messages: {url}")
                    time.sleep(random.randint(5, 10))
                    continue
                resp.raise_for_status()
                data = resp.json()
                items = data.get("messages", [])
                if not items:
                    break
                for item in items:
                    yield item
                offset += len(items)
                total += len(items)
                if len(items) < limit_per_request or total >= max_messages:
                    break
            except requests.exceptions.RequestException as e:
                logging.error(f"stream_messages error: {e}")
                raise

def must_env(key):
    val = os.getenv(key)
    if not val:
        print(f"Missing required env var: {key}")
        sys.exit(1)
    return val

def generate_ranges(start, end, step):
    current = start
    while current < end:
        next_time = current + step
        yield current, min(next_time, end)
        current = next_time

def process_and_return(exp, query, start, end, args, depth=0):
    indent = "  " * depth
    logging.info(f"{indent}🔄 Querying: {start} → {end}")
    job_id = exp.create_job(query, start.isoformat(), end.isoformat())
    exp.wait_for_completion(job_id)
    data = list(exp.stream_messages(job_id, max_messages=args.max_messages))
    logging.info(f"{indent}📥 Retrieved: {len(data)} records")

    if len(data) >= args.max_messages and (end - start) > timedelta(minutes=1):
        logging.warning(f"{indent}⚠️ Hit message cap in range {start}–{end}. Auto-splitting smaller.")
        substep = timedelta(minutes=1)
        subranges = list(generate_ranges(start, end, substep))
        subdata = []
        with ThreadPoolExecutor(max_workers=args.concurrent_workers) as executor:
            futures = [executor.submit(process_and_return, exp, query, s, e, args, depth + 1) for s, e in subranges]
            for f in as_completed(futures):
                subdata.extend(f.result())
        return subdata

    return data

def process_parallel_merge(exp, query, start, end, suffix, args, step):
    ranges = list(generate_ranges(start, end, step))
    all_data = []
    with ThreadPoolExecutor(max_workers=args.concurrent_workers) as executor:
        futures = [executor.submit(process_and_return, exp, query, s, e, args) for s, e in ranges]
        for f in as_completed(futures):
            all_data.extend(f.result())

    all_data.sort(key=lambda m: int(m.get("map", {}).get("_messagetime", 0)))
    json_bytes = json.dumps(all_data, indent=2).encode("utf-8")

    if args.local_file_output:
        local_file = f"{args.prefix}_{suffix}.json.zst"
        with open(local_file, "wb") as f:
            cctx = zstd.ZstdCompressor()
            f.write(cctx.compress(json_bytes))
        logging.info(f"💾 Saved: {local_file}")

    if args.upload:
        blob_name = f"{args.prefix}_{suffix}.json.zst"
        exp.upload_blob_from_memory(json_bytes, blob_name)

def ensure_directory_exists(file_path):
    directory_path = os.path.dirname(file_path)
    if directory_path:
        os.makedirs(directory_path, exist_ok=True)
        print(f"📁 Directory ensured: {directory_path}")
    return directory_path

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--query", required=True)
    parser.add_argument("--years", nargs="+", type=int, required=True)
    parser.add_argument("--months", nargs="+", help="3-letter month abbreviations like Jan Feb Mar")
    parser.add_argument("--local-file-output", action="store_true")
    parser.add_argument("--prefix", default="sumo_export")
    parser.add_argument("--concurrent-workers", type=int, default=4)
    parser.add_argument("--rate-limit", type=int, default=4)
    parser.add_argument("--max-messages", type=int, default=100000)
    parser.add_argument("--upload", action="store_true", help="Upload to Azure Blob using AZURE_BLOB_SAS")
    parser.add_argument("--breakup-blobs-by", choices=["month", "day", "hour"], default="month")
    parser.add_argument("--hourly-initial-chunk-minutes", type=int, default=5, help="Initial chunk size in minutes for hour-level exports")
    args = parser.parse_args()

    access_id = must_env("SUMO_ACCESS_ID")
    access_key = must_env("SUMO_ACCESS_KEY")
    endpoint = must_env("SUMO_API_ENDPOINT")

    local_directory_path = ensure_directory_exists(args.prefix)

    exp = SumoExporter(
        access_id, access_key, endpoint,
        os.getenv("AZURE_BLOB_SAS"),
        verbose=True,
        rate_limit=args.rate_limit
    )

    for year in args.years:
        for month in range(1, 13):
            month_abbr = calendar.month_abbr[month]
            if args.months and month_abbr not in args.months:
                continue
            month_start = datetime(year, month, 1, tzinfo=timezone.utc)
            days_in_month = calendar.monthrange(year, month)[1]
            month_end = datetime(year, month, days_in_month, 23, 59, 59, tzinfo=timezone.utc)

            if args.breakup_blobs_by == "month":
                suffix = f"{year}{month_abbr}"
                process_parallel_merge(exp, args.query, month_start, month_end, suffix, args, step=timedelta(days=1))

            elif args.breakup_blobs_by == "day":
                for day_start, day_end in generate_ranges(month_start, month_end + timedelta(seconds=1), timedelta(days=1)):
                    suffix = f"{year}{month_abbr}{day_start.day:02}"
                    process_parallel_merge(exp, args.query, day_start, day_end, suffix, args, step=timedelta(hours=1))

            elif args.breakup_blobs_by == "hour":
                for day_start, day_end in generate_ranges(month_start, month_end + timedelta(seconds=1), timedelta(days=1)):
                    for hour_start, hour_end in generate_ranges(day_start, day_end, timedelta(hours=1)):
                        suffix = f"{year}{month_abbr}{hour_start.day:02}H{hour_start.hour:02}"
                        step = timedelta(minutes=args.hourly_initial_chunk_minutes)
                        process_parallel_merge(exp, args.query, hour_start, hour_end, suffix, args, step=step)

if __name__ == "__main__":
    main()
