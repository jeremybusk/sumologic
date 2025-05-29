#!/usr/bin/env python3
import argparse
import gzip
import json
from datetime import datetime, timedelta
from pathlib import Path
import sys


def parse_args():
    parser = argparse.ArgumentParser(
        description="Query archived Sumo Logic minute files.")
    parser.add_argument("--archive-dir", required=True,
                        help="Path to archive base directory")
    parser.add_argument("--start", required=True,
                        help="Start time (UTC) in YYYY-MM-DDTHH:MM format")
    parser.add_argument("--end", required=True,
                        help="End time (UTC) in YYYY-MM-DDTHH:MM format")
    parser.add_argument("--match", action="append",
                        help='Match filter in key=substring format. Can be used multiple times.')
    parser.add_argument(
        "--output", help="Optional output file path. Defaults to stdout.")
    return parser.parse_args()


def generate_minute_range(start, end):
    current = start
    while current < end:
        yield current
        current += timedelta(minutes=1)


def build_file_path(base_dir, dt):
    return Path(base_dir) / f"{dt.year}/{dt.month:02}/{dt.day:02}/{dt.hour:02}/{dt.minute:02}.json.gz"


def matches_all_filters(msg, filters):
    if not filters:
        return True
    m = msg.get("map", {})
    for key, substr in filters:
        val = m.get(key, "")
        if substr not in str(val):
            return False
    return True


def load_messages(file_path):
    try:
        with gzip.open(file_path, 'rt') as f:
            return json.load(f)
    except Exception as e:
        print(f"⚠️ Failed to read {file_path}: {e}", file=sys.stderr)
        return []


def main():
    args = parse_args()

    try:
        start = datetime.fromisoformat(args.start)
        end = datetime.fromisoformat(args.end)
    except Exception as e:
        print(f"❌ Invalid date format: {e}", file=sys.stderr)
        sys.exit(1)

    filters = []
    if args.match:
        for m in args.match:
            if '=' not in m:
                print(
                    f"❌ Invalid --match value: {m}. Must be in key=substring format.", file=sys.stderr)
                sys.exit(1)
            key, substr = m.split("=", 1)
            filters.append((key, substr))

    output_file = open(args.output, 'w') if args.output else None

    for minute in generate_minute_range(start, end):
        path = build_file_path(args.archive_dir, minute)
        if not path.exists():
            continue
        messages = load_messages(path)
        for msg in messages:
            if matches_all_filters(msg, filters):
                line = json.dumps(msg)
                if output_file:
                    output_file.write(line + "\n")
                else:
                    print(line)

    if output_file:
        output_file.close()


if __name__ == "__main__":
    main()
