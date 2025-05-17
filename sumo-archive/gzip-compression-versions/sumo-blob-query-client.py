#!/usr/bin/env python3

import os
import sys
import gzip
import json
import argparse
import requests
import posixpath
from urllib.parse import urlparse
from io import BytesIO
from dateutil import parser as dtparser



def build_blob_url(sas_base, container, blob_name):
    parsed = urlparse(sas_base)
    base_url = f"{parsed.scheme}://{parsed.netloc}"
    query = parsed.query
    blob_path = posixpath.join(container, blob_name)
    return f"{base_url}/{blob_path}?{query}"


def download_blob_to_bytes(url):
    resp = requests.get(url)
    if resp.status_code != 200:
        raise Exception(f"Download failed: {resp.status_code} {url}")
    return resp.content


def parse_gz_json(data_bytes):
    with gzip.GzipFile(fileobj=BytesIO(data_bytes)) as gz:
        return json.load(gz)


# def process_blob(index, args):
def process_blob(index, args, from_time_ms=None, to_time_ms=None):

    blob_name = f"{args.blob_name_base}_part{index}.json.gz"
    url = build_blob_url(os.environ["AZURE_BLOB_SAS"], args.container_name, blob_name)
    print(f"📦 Fetching: {blob_name}")

    try:
        raw_bytes = download_blob_to_bytes(url)
        records = parse_gz_json(raw_bytes)
    except Exception as e:
        print(f"❌ Failed to process {blob_name}: {e}")
        print(f"This is probably expected as it's due to the end of parts for {args.blob_name_base} so blob doesn't exist so you can ignore.")
        return False

    match_count = 0
    for entry in records:
        m = entry.get("map", {})
        try:
            message_time = int(m.get("_messagetime", 0))
        except ValueError:
            message_time = 0

        if (from_time_ms and message_time < from_time_ms) or (to_time_ms and message_time > to_time_ms):
            continue  # Skip outside time range

        raw_text = m.get("_raw", "")
        matched = False

        # Match on --search-key and --search-value
        if args.search_key and str(m.get(args.search_key)) == args.search_value:
            matched = True

        # Match on --search-key-values (substring match)
        elif args.search_key_values:
            if not args.match_any_kv:
                matched = True
                for kv in args.search_key_values:
                    if ":" not in kv:
                        matched = False
                        break
                    key, substr = kv.split(":", 1)
                    val = str(m.get(key, ""))
                    if substr not in val:
                        matched = False
                        break
            else:
                for kv in args.search_key_values:
                    if ":" in kv:
                        key, substr = kv.split(":", 1)
                        val = str(m.get(key, ""))
                        if substr in val:
                            matched = True
                            break

        if matched:
            print(f"🟢 Match: id={m.get('_messageid')} raw={raw_text}")
            match_count += 1
        elif args.show_non_match:
            print(f"📝 id={m.get('_messageid')} raw={raw_text}")
    print(f"✅ Processed {blob_name}: {len(records)} entries, {match_count} matched.\n")
    return True



def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--container-name", required=True, help="Blob container name")
    parser.add_argument("--blob-name-base", required=True, help="Prefix of blob files (excluding _partN.json.gz)")
    parser.add_argument("--search-key", help="Optional key to filter within map")
    parser.add_argument("--search-value", help="Optional value to filter on")
    parser.add_argument("--from-time", help="Start time in RFC3339 format (e.g. 2025-05-14T16:09:46+00:00)")
    parser.add_argument("--to-time", help="End time in RFC3339 format (e.g. 2025-05-14T17:00:00+00:00)")
    parser.add_argument("--show-non-match", action="store_true", help="Print non matching entries")
    parser.add_argument(
        "--search-key-values",
        nargs="+",
        help='Match all key:substring filter (e.g. --search-key-values "_sourcehost:192.16" "_raw:changed state to down")'
    )
    parser.add_argument(
        "--match-any-kv",
        action="store_true",
        help="If set, any key:value pairs in --search-key-values will cause a match (instead of all)"
    )
    args = parser.parse_args()

    if "AZURE_BLOB_SAS" not in os.environ:
        print("❌ AZURE_BLOB_SAS environment variable not set")
        sys.exit(1)

    index = 1
    from_time_ms = None
    to_time_ms = None
    if args.from_time:
        from_time_ms = int(dtparser.isoparse(args.from_time).timestamp() * 1000)
    if args.to_time:
        to_time_ms = int(dtparser.isoparse(args.to_time).timestamp() * 1000)
    while True:
        # success = process_blob(index, args)
        success = process_blob(index, args, from_time_ms, to_time_ms)
        if not success:
            break
        index += 1


if __name__ == "__main__":
    main()

