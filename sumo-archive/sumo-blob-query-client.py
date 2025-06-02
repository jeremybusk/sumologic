#!/usr/bin/env python3
import os
import time
import sys
import json
import argparse
import posixpath
import gzip
import logging
from urllib.parse import urlparse
from io import BytesIO
from datetime import timedelta
from dateutil import parser as dtparser
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

def get_blob_client(container, blob_name):
    account_url = os.environ.get("AZURE_STORAGE_ACCOUNT_URL")
    if not account_url:
        logger.critical("AZURE_STORAGE_ACCOUNT_URL environment variable not set")
        sys.exit(1)

    credential = DefaultAzureCredential()
    blob_service_client = BlobServiceClient(account_url=account_url, credential=credential)
    return blob_service_client.get_blob_client(container=container, blob=blob_name)

def download_blob_to_bytes(container, blob_name):
    try:
        client = get_blob_client(container, blob_name)
        return client.download_blob().readall()
    except Exception as e:
        if "404" in str(e):
            return None
        raise

def parse_gzip_json(data_bytes):
    with gzip.open(BytesIO(data_bytes), 'rt', encoding='utf-8') as f:
        return json.load(f)

def process_blob(args, container, blob_name, from_time_ms=None, to_time_ms=None):
    logger.info(f"📦 Fetching: {blob_name}")

    try:
        raw_bytes = download_blob_to_bytes(container, blob_name)
        if not raw_bytes:
            logger.warning(f"Blob not found: {blob_name}")
            return False
        records = parse_gzip_json(raw_bytes)
    except Exception as e:
        logger.error(f"Failed to process {blob_name}: {e}")
        return False

    match_count = 0
    for entry in records:
        m = entry.get("map", {})
        try:
            raw_ts = m.get("_messagetime", 0)
            if isinstance(raw_ts, str) and 'T' in raw_ts:
                message_time = int(dtparser.isoparse(raw_ts).timestamp() * 1000)
            else:
                message_time = int(raw_ts)
        except (ValueError, TypeError):
            message_time = 0

        if (from_time_ms and message_time < from_time_ms) or (to_time_ms and message_time > to_time_ms):
            continue

        matched = False

        if args.search_key and str(m.get(args.search_key, "")).lower() == args.search_value.lower():
            matched = True
        elif args.search_key_values:
            pairs = [(kv.split(":", 1)[0], kv.split(":", 1)[1].lower()) for kv in args.search_key_values if ":" in kv]
            if not args.match_any_kv:
                matched = all(val in str(m.get(key, "")).lower() for key, val in pairs)
            else:
                matched = any(val in str(m.get(key, "")).lower() for key, val in pairs)
        else:
            matched = True

        if matched:
            if args.display_values:
                for key, val in m.items():
                    print(f"{key}: {val}")
                print("---")
                time.sleep(args.display_values_wait)
            else:
                # logger.debug(f"🟢 Match: id={m.get('_messageid')}")
                logger.debug(f"🟢 Match: id={m.get('_messageid')} raw={m.get('_raw', '')}")
            match_count += 1
        elif args.show_non_match:
            print(f"📝 Non-match: id={m.get('_messageid')} raw={m.get('_raw', '')}")

    print(f"✅ Processed {blob_name}: {len(records)} entries, {match_count} matched.\n")
    logger.info(f"✅ Processed {blob_name}: {len(records)} entries, {match_count} matched.\n")
    return True

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--display-values-wait", type=int,default=1)
    parser.add_argument("--container-name", required=True)
    parser.add_argument("--blob-prefix-base", required=True)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date")
    parser.add_argument("--search-key")
    parser.add_argument("--search-value")
    parser.add_argument("--search-key-values", nargs="+")
    parser.add_argument("--match-any-kv", action="store_true")
    parser.add_argument("--show-non-match", action="store_true")
    parser.add_argument("--process-entries-without-timestamp", action="store_true")
    parser.add_argument("--display-values", action="store_true")
    parser.add_argument("--log-level", default="WARNING", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"])
    args = parser.parse_args()

    logging.getLogger().setLevel(getattr(logging, args.log_level.upper()))

    from_dt = dtparser.isoparse(args.start_date)
    to_dt = dtparser.isoparse(args.end_date) if args.end_date else from_dt.replace(minute=59, second=59, microsecond=999999)
    from_ms = int(from_dt.timestamp() * 1000)
    to_ms = int(to_dt.timestamp() * 1000)

    current = from_dt.replace(minute=0, second=0, microsecond=0)
    while current <= to_dt:
        prefix = posixpath.join(
            args.blob_prefix_base,
            current.strftime("%Y"), current.strftime("%m"), current.strftime("%d"), current.strftime("%H")
        )
        for minute in range(60):
            ts = current.replace(minute=minute)
            if ts < from_dt or ts > to_dt:
                continue
            blob_name = posixpath.join(prefix, f"{minute:02d}.json.gz")
            found = process_blob(args, args.container_name, blob_name, from_ms, to_ms)
            if not found:
                break
        current = current.replace(minute=0) + timedelta(hours=1)

if __name__ == "__main__":
    main()

