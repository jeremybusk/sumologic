#!/usr/bin/env python3
import os
import sys
import json
import argparse
import requests
import posixpath
import zstandard as zstd
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

def parse_zstd_json(data_bytes):
    dctx = zstd.ZstdDecompressor()
    decompressed = dctx.decompress(data_bytes)
    return json.loads(decompressed)

def process_blob(index, args, from_time_ms=None, to_time_ms=None):
    blob_name = f"{args.blob_name_base}_part{index}.json.zst"
    url = build_blob_url(os.environ["AZURE_BLOB_SAS"], args.container_name, blob_name)
    print(f"📦 Fetching: {blob_name}")

    try:
        raw_bytes = download_blob_to_bytes(url)
        records = parse_zstd_json(raw_bytes)
    except Exception as e:
        print(f"❌ Failed to process {blob_name}: {e}")
        print("This may be expected if no more blob parts are available.")
        return False

    match_count = 0
    for entry in records:
        m = entry.get("map", {})
        try:
            message_time = int(m.get("_messagetime", 0))
        except ValueError:
            message_time = 0

        if (from_time_ms and message_time < from_time_ms) or (to_time_ms and message_time > to_time_ms):
            continue

        raw_text = m.get("_raw", "")
        matched = False

        if args.search_key and str(m.get(args.search_key)) == args.search_value:
            matched = True
        elif args.search_key_values:
            if not args.match_any_kv:
                matched = all(
                    ":" in kv and kv.split(":", 1)[1] in str(m.get(kv.split(":", 1)[0], ""))
                    for kv in args.search_key_values
                )
            else:
                matched = any(
                    ":" in kv and kv.split(":", 1)[1] in str(m.get(kv.split(":", 1)[0], ""))
                    for kv in args.search_key_values
                )

        if matched:
            print(f"🟢 Match: id={m.get('_messageid')} raw={raw_text}")
            match_count += 1
        elif args.show_non_match:
            print(f"📝 id={m.get('_messageid')} raw={raw_text}")

    print(f"✅ Processed {blob_name}: {len(records)} entries, {match_count} matched.\n")
    return True

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--container-name", required=True)
    parser.add_argument("--blob-name-base", required=True)
    parser.add_argument("--search-key")
    parser.add_argument("--search-value")
    parser.add_argument("--from-time")
    parser.add_argument("--to-time")
    parser.add_argument("--show-non-match", action="store_true")
    parser.add_argument("--search-key-values", nargs="+")
    parser.add_argument("--match-any-kv", action="store_true")
    args = parser.parse_args()

    if "AZURE_BLOB_SAS" not in os.environ:
        print("❌ AZURE_BLOB_SAS environment variable not set")
        sys.exit(1)

    index = 1
    from_time_ms = int(dtparser.isoparse(args.from_time).timestamp() * 1000) if args.from_time else None
    to_time_ms = int(dtparser.isoparse(args.to_time).timestamp() * 1000) if args.to_time else None

    while True:
        success = process_blob(index, args, from_time_ms, to_time_ms)
        if not success:
            break
        index += 1

if __name__ == "__main__":
    main()

