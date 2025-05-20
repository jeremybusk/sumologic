#!/usr/bin/env python3
import os
import sys
import json
import argparse
import calendar
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

def process_blob(blob_name, args, from_time_ms=None, to_time_ms=None):
    url = build_blob_url(os.environ["AZURE_BLOB_SAS"], args.container_name, blob_name)
    print(f"📦 Fetching: {blob_name}")

    try:
        raw_bytes = download_blob_to_bytes(url)
        records = parse_zstd_json(raw_bytes)
    except Exception as e:
        print(f"❌ Failed to process {blob_name}: {e}")
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

        if args.search_key_values:
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
    parser.add_argument("--prefix", required=True, help="Blob path prefix, e.g. '2025/May/test1'")
    parser.add_argument("--from-time")
    parser.add_argument("--to-time")
    parser.add_argument("--show-non-match", action="store_true")
    parser.add_argument("--search-key-values", nargs="+", help="\"_raw:Interface GigabitEthernet6/0/40 _collector:MyCollector\"")
    parser.add_argument("--match-any-kv", action="store_true")
    parser.add_argument("--years", nargs="+", type=int)
    parser.add_argument("--months", nargs="+", help="3-letter month abbreviations like Jan Feb Mar")
    parser.add_argument("--days", nargs="+", type=int)
    parser.add_argument("--blob-is-parted", action="store_true", help="If set will incrementally search partN of each blob")
    args = parser.parse_args()

    if "AZURE_BLOB_SAS" not in os.environ:
        print("❌ AZURE_BLOB_SAS environment variable not set")
        sys.exit(1)

    from_time_ms = int(dtparser.isoparse(args.from_time).timestamp() * 1000) if args.from_time else None
    to_time_ms = int(dtparser.isoparse(args.to_time).timestamp() * 1000) if args.to_time else None

    def month_abbrs():
        return {m.lower(): i for i, m in enumerate(calendar.month_abbr) if m}

    if args.years:
        for year in args.years:
            for m_abbr in args.months or month_abbrs().keys():
                month_idx = month_abbrs()[m_abbr.lower()]
                month_str = calendar.month_abbr[month_idx]
                if args.days:
                    for day in args.days:
                        suffix = f"{year}{month_str}{day:02}"
                        if args.blob_is_parted:
                            index = 1
                            while True:
                                # blob_name = posixpath.join(args.prefix, f"{suffix}_part{index}.json.zst")
                                blob_name = posixpath.join(f"{args.prefix}_{suffix}_part{index}.json.zst")
                                # blob_name = f"{args.prefix}_{suffix}_part{index}.json.zst"
                                if not process_blob(blob_name, args, from_time_ms, to_time_ms):
                                    break
                                index += 1
                        else:
                            blob_name = posixpath.join(f"{args.prefix}_{suffix}.json.zst")
                            # blob_name = posixpath.join(args.prefix, f"{suffix}.json.zst")
                            # blob_name = f"{args.prefix}_{suffix}.json.zst"
                            process_blob(blob_name, args, from_time_ms, to_time_ms)
                else:
                    suffix = f"{year}{month_str}"
                    if args.blob_is_parted:
                        index = 1
                        while True:
                            # blob_name = posixpath.join(args.prefix, f"{suffix}_part{index}.json.zst")
                            blob_name = posixpath.join(f"{args.prefix}_{suffix}_part{index}.json.zst")
                            if not process_blob(blob_name, args, from_time_ms, to_time_ms):
                                break
                            index += 1
                    else:
                        blob_name = posixpath.join(f"{args.prefix}_{suffix}.json.zst")
                        process_blob(blob_name, args, from_time_ms, to_time_ms)
    else:
        # fallback to part-based search with prefix only
        index = 1
        while True:
            blob_name = posixpath.join(f"{args.prefix}_part{index}.json.zst")
            if not process_blob(blob_name, args, from_time_ms, to_time_ms):
                break
            index += 1

if __name__ == "__main__":
    main()
