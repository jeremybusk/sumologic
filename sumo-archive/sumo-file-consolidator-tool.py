#!/usr/bin/env python3
import os
import argparse
import calendar
from datetime import datetime
from collections import defaultdict
import zstandard as zstd
import json

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--container-name", required=True)
    parser.add_argument("--prefix", required=True)
    parser.add_argument("--years", nargs="+", type=int, required=True)
    parser.add_argument("--months", nargs="+", help="Limit to specific months (Jan, Feb, etc.)")
    parser.add_argument("--mode", choices=["report", "consolidate"], default="report")
    parser.add_argument("--consolidate-granularity", choices=["hour", "day"], help="Consolidate from minute to hour, or hour to day")
    return parser.parse_args()

def walk_files(container, prefix, year, month_abbr):
    base_path = os.path.join(container, prefix, str(year), month_abbr)
    existing = defaultdict(list)
    for root, _, files in os.walk(base_path):
        for f in files:
            if f.endswith(".json.zst"):
                parts = f.split("_")[-1].replace(".json.zst", "")
                existing[parts].append(os.path.join(root, f))
    return existing

def report_missing(container, prefix, years, months):
    all_stats = []
    for year in years:
        for month in range(1, 13):
            month_abbr = calendar.month_abbr[month]
            if months and month_abbr not in months:
                continue
            expected_days = calendar.monthrange(year, month)[1]
            base_path = os.path.join(container, prefix, str(year), month_abbr)
            found_days = set()
            found_hours = defaultdict(set)
            found_minutes = defaultdict(lambda: defaultdict(set))

            for root, _, files in os.walk(base_path):
                for file in files:
                    if not file.endswith(".json.zst"):
                        continue
                    parts = file.split("_")[-1].replace(".json.zst", "")
                    if "H" in parts:
                        day_str, hour_str = parts.split("H")
                        day = int(day_str[-2:])
                        hour = int(hour_str[:2])
                        found_days.add(day)
                        found_hours[day].add(hour)
                        if "M" in hour_str:
                            minute = int(hour_str[3:5])
                            found_minutes[day][hour].add(minute)
                    else:
                        day = int(parts[-2:])
                        found_days.add(day)

            # Print missing items
            print(f"\n📅 {year}-{month_abbr}:")
            for day in range(1, expected_days + 1):
                if day not in found_days:
                    print(f"  ❌ Missing day: {day:02}")
                for hour in range(24):
                    if hour not in found_hours[day]:
                        print(f"    ❌ Missing hour: {day:02}H{hour:02}")
                    for minute in range(60):
                        if minute not in found_minutes[day][hour]:
                            print(f"      ❌ Missing minute: {day:02}H{hour:02}M{minute:02}")


def consolidate(args):
    for year in args.years:
        for month in range(1, 13):
            month_abbr = calendar.month_abbr[month]
            if args.months and month_abbr not in args.months:
                continue
            base_path = os.path.join(args.container_name, args.prefix, str(year), month_abbr)
            for day in range(1, 32):
                for hour in range(24):
                    minute_files = []
                    for minute in range(60):
                        minute_path = os.path.join(base_path, f"{day:02}", f"H{hour:02}", f"M{minute:02}")
                        file_name = f"{args.prefix}_{year}{month_abbr}{day:02}H{hour:02}M{minute:02}.json.zst"
                        full_path = os.path.join(minute_path, file_name)
                        if os.path.exists(full_path):
                            minute_files.append(full_path)

                    if len(minute_files) == 60:
                        combined = []
                        for f in minute_files:
                            with open(f, 'rb') as zf:
                                dctx = zstd.ZstdDecompressor()
                                decompressed = dctx.decompress(zf.read())
                                combined.extend(json.loads(decompressed))

                        # Write hour file
                        output_path = os.path.join(base_path, f"{day:02}", f"H{hour:02}")
                        os.makedirs(output_path, exist_ok=True)
                        out_file = os.path.join(output_path, f"{args.prefix}_{year}{month_abbr}{day:02}H{hour:02}.json.zst")
                        with open(out_file, 'wb') as f:
                            cctx = zstd.ZstdCompressor()
                            f.write(cctx.compress(json.dumps(combined, indent=2).encode('utf-8')))
                        print(f"🌀 Consolidated hour file: {out_file}")

def main():
    args = parse_args()
    if args.mode == "report":
        report_missing(args.container_name, args.prefix, args.years, args.months)
    elif args.mode == "consolidate":
        if not args.consolidate_granularity:
            print("--consolidate-granularity is required when mode is consolidate")
            return
        consolidate(args)

if __name__ == "__main__":
    main()

