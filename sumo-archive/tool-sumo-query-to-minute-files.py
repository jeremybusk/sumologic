#!/usr/bin/env python3
import os
import json
import gzip
from collections import defaultdict
from datetime import datetime
import argparse

# Ensure output directories exist
def ensure_directory_exists(path):
    os.makedirs(path, exist_ok=True)

# Load messages from a gzip-compressed file
def load_messages_from_gzip(file_path):
    with gzip.open(file_path, "rt") as f:
        return json.load(f)

# Save messages to a gzip-compressed file
def save_messages_to_gzip(messages, file_path):
    ensure_directory_exists(os.path.dirname(file_path))
    with gzip.open(file_path, "wt") as f:
        json.dump(messages, f, indent=2)
    print(f"✅ Saved {len(messages)} messages to {file_path}")

# Generate a summary report for a year
def generate_summary_report(input_dir, year):
    summary = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    missing = {"months": [], "days": [], "hours": [], "minutes": []}

    # Traverse the directory structure
    for month in range(1, 13):
        month_dir = os.path.join(input_dir, str(year), f"{month:02}")
        if not os.path.exists(month_dir):
            missing["months"].append(month)
            continue

        for day in range(1, 32):
            day_dir = os.path.join(month_dir, f"{day:02}")
            if not os.path.exists(day_dir):
                missing["days"].append(f"{year}/{month:02}/{day:02}")
                continue

            for hour in range(24):
                hour_dir = os.path.join(day_dir, f"{hour:02}")
                if not os.path.exists(hour_dir):
                    missing["hours"].append(f"{year}/{month:02}/{day:02}/{hour:02}")
                    continue

                for minute in range(60):
                    minute_file = os.path.join(hour_dir, f"{minute:02}.json.gz")
                    if not os.path.exists(minute_file):
                        missing["minutes"].append(f"{year}/{month:02}/{day:02}/{hour:02}/{minute:02}")
                    else:
                        messages = load_messages_from_gzip(minute_file)
                        summary[month][day][hour].append(len(messages))

    # Print summary
    print(f"📊 Summary Report for {year}")
    print(f"Missing Months: {missing['months']}")
    print(f"Missing Days: {missing['days']}")
    print(f"Missing Hours: {missing['hours']}")
    print(f"Missing Minutes: {missing['minutes']}")
    return summary

# Aggregate messages into larger files
def aggregate_messages(input_dir, output_dir, aggregate_to):
    levels = {"year": 1, "month": 2, "day": 3, "hour": 4}
    if aggregate_to not in levels:
        raise ValueError(f"Invalid aggregation level: {aggregate_to}. Must be one of {list(levels.keys())}.")

    aggregate_level = levels[aggregate_to]

    # Traverse the directory structure
    for root, _, files in os.walk(input_dir):
        path_parts = root.split(os.sep)
        if len(path_parts) < aggregate_level + 1:
            continue

        # Determine the aggregation key
        key = os.path.join(*path_parts[:aggregate_level + 1])
        aggregated_messages = []

        for file in files:
            if file.endswith(".json.gz"):
                file_path = os.path.join(root, file)
                messages = load_messages_from_gzip(file_path)
                aggregated_messages.extend(messages)

        if aggregated_messages:
            output_file = os.path.join(output_dir, key + ".json.gz")
            save_messages_to_gzip(aggregated_messages, output_file)

# Main function
def main():
    parser = argparse.ArgumentParser(description="Sumo Logic Tool for managing and analyzing message files.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Summary report command
    summary_parser = subparsers.add_parser("summary", help="Generate a summary report for a year.")
    summary_parser.add_argument("--input-dir", required=True, help="Directory containing the input files.")
    summary_parser.add_argument("--year", type=int, required=True, help="Year to generate the summary for.")

    # Aggregate command
    aggregate_parser = subparsers.add_parser("aggregate", help="Aggregate messages into larger files.")
    aggregate_parser.add_argument("--input-dir", required=True, help="Directory containing the input files.")
    aggregate_parser.add_argument("--output-dir", required=True, help="Directory to save the aggregated files.")
    aggregate_parser.add_argument("--aggregate-to", required=True, choices=["year", "month", "day", "hour"],
                                   help="Level to aggregate messages to (year, month, day, hour).")

    args = parser.parse_args()

    if args.command == "summary":
        generate_summary_report(args.input_dir, args.year)
    elif args.command == "aggregate":
        aggregate_messages(args.input_dir, args.output_dir, args.aggregate_to)

if __name__ == "__main__":
    main()
