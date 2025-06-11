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
from dateutil.relativedelta import relativedelta
from azure.identity import DefaultAzureCredential
from azure.storage.blob import ContainerClient

# --- Import Optional Dependencies ---
try:
    import zstandard as zstd
except ImportError:
    zstd = None
try:
    import lz4.frame
except ImportError:
    lz4 = None
try:
    from tqdm import tqdm
except ImportError:
    tqdm = None
try:
    import ijson
except ImportError:
    ijson = None

# --- Basic Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)


def get_container_client(container_name, account_url=None, access_key=None):
    if not account_url:
        logger.critical("Storage account URL not provided. Set AZURE_STORAGE_ACCOUNT_URL or use --storage-account-url.")
        sys.exit(1)
    credential = access_key or DefaultAzureCredential()
    return ContainerClient(account_url=account_url, container_name=container_name, credential=credential)


def get_streaming_decompressor(blob_stream, compression):
    """Returns a stream wrapped in the appropriate decompressor."""
    if compression == 'gzip':
        return gzip.GzipFile(fileobj=blob_stream)
    if compression == 'zstd':
        if not zstd: raise ImportError("zstandard library is not installed. Please run 'pip install zstandard'")
        dctx = zstd.ZstdDecompressor()
        return dctx.stream_reader(blob_stream)
    if compression == 'lz4':
        if not lz4: raise ImportError("lz4 library is not installed. Please run 'pip install lz4'")
        return lz4.frame.LZ4FrameFile(blob_stream)
    if compression == 'none':
        return blob_stream
    raise ValueError(f"Unsupported compression type for streaming: {compression}")


def decompress_in_memory(data_bytes, compression):
    """Decompresses a byte object entirely in memory."""
    if compression == 'gzip':
        with gzip.open(BytesIO(data_bytes), 'rt', encoding='utf-8') as f:
            return json.load(f)
    if compression == 'zstd':
        if not zstd: raise ImportError("zstandard library is not installed. Please run 'pip install zstandard'")
        return json.loads(zstd.decompress(data_bytes))
    if compression == 'lz4':
        if not lz4: raise ImportError("lz4 library is not installed. Please run 'pip install lz4'")
        return json.loads(lz4.frame.decompress(data_bytes))
    if compression == 'none':
        return json.loads(data_bytes)
    raise ValueError(f"Unsupported compression type: {compression}")


def is_record_matched(record_map, args):
    """Helper function to determine if a record matches search criteria."""
    if args.search_key:
        return str(record_map.get(args.search_key, "")).lower() == args.search_value.lower()
    if args.search_key_values:
        pairs = [(kv.split(":", 1)[0], kv.split(":", 1)[1].lower()) for kv in args.search_key_values if ":" in kv]
        if not args.match_any_kv:
            return all(val in str(record_map.get(key, "")).lower() for key, val in pairs)
        else:
            return any(val in str(record_map.get(key, "")).lower() for key, val in pairs)
    # If no search keys are provided, every record is a match.
    return True


def process_blob_stream(args, container_client, blob_name, from_time_ms, to_time_ms):
    """Processes a blob using a memory-efficient streaming pipeline."""
    if not ijson:
        raise ImportError("ijson library is not installed. Please run 'pip install ijson' for streaming mode.")
    
    logger.info(f"📦 Starting stream search in: {blob_name}")
    blob_client = container_client.get_blob_client(blob_name)
    match_count = 0
    record_count = 0

    try:
        # **FIX:** Removed the 'with' statement here
        blob_stream = blob_client.download_blob()
        
        decompress_stream = get_streaming_decompressor(blob_stream, args.compression)
        # Assuming the JSON is an array of objects: 'item' corresponds to each element.
        parser = ijson.items(decompress_stream, 'item')
        
        for record in parser:
            record_count += 1
            m = record.get("map", {})
            try:
                raw_ts = m.get("_messagetime", 0)
                message_time = int(dtparser.isoparse(raw_ts).timestamp() * 1000) if isinstance(raw_ts, str) and 'T' in raw_ts else int(raw_ts)
            except (ValueError, TypeError):
                message_time = 0

            if (from_time_ms and message_time < from_time_ms) or (to_time_ms and message_time > to_time_ms):
                continue
            
            if is_record_matched(m, args):
                match_count += 1
                if args.display_values:
                    print(json.dumps(m, indent=2))
                    print("---")
        
        logger.info(f"✅ Finished stream search in {blob_name}: {record_count} entries, {match_count} matched.\n")
    except ijson.JSONError as e:
        logger.error(f"Invalid JSON in {blob_name}. Could not complete streaming parse. Error: {e}")
    except Exception as e:
        logger.error(f"An error occurred during streaming for {blob_name}: {e}")
        # To aid debugging, you might want to see the full error
        # import traceback
        # traceback.print_exc()

def process_blob_in_memory(args, container_client, blob_name, from_time_ms, to_time_ms):
    """(Original method) Fetches, parses, and processes a blob entirely in memory."""
    logger.info(f"📦 Starting in-memory search in: {blob_name}")
    try:
        blob_client = container_client.get_blob_client(blob_name)
        
        # Download with progress bar
        blob_properties = blob_client.get_blob_properties()
        blob_size = blob_properties.size
        progress_bar = tqdm(total=blob_size, unit='B', unit_scale=True, desc=f"Downloading {os.path.basename(blob_name)}", leave=False) if tqdm else None
        
        output_stream = BytesIO()
        with blob_client.download_blob() as stream:
            for chunk in stream.chunks():
                output_stream.write(chunk)
                if progress_bar: progress_bar.update(len(chunk))
        if progress_bar: progress_bar.close()
        
        raw_bytes = output_stream.getvalue()
        if not raw_bytes:
            logger.warning(f"Blob is empty: {blob_name}"); return
        
        logger.info(f"Decompressing and parsing {os.path.basename(blob_name)}...")
        records = decompress_in_memory(raw_bytes, args.compression)

        match_count = 0
        for entry in records:
            m = entry.get("map", {})
            try:
                raw_ts = m.get("_messagetime", 0)
                message_time = int(dtparser.isoparse(raw_ts).timestamp() * 1000) if isinstance(raw_ts, str) and 'T' in raw_ts else int(raw_ts)
            except (ValueError, TypeError): message_time = 0
            
            if (from_time_ms and message_time < from_time_ms) or (to_time_ms and message_time > to_time_ms):
                continue

            if is_record_matched(m, args):
                match_count += 1
                if args.display_values:
                    print(json.dumps(m, indent=2))
                    print("---")
        logger.info(f"✅ Finished in-memory search in {blob_name}: {len(records)} entries, {match_count} matched.\n")
    except Exception as e:
        logger.error(f"Failed to process {blob_name} in-memory: {e}")

def main():
    parser = argparse.ArgumentParser(description="Search Azure Blob Storage for log data.", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    
    parser.add_argument("--in-memory", action="store_true", help="Load entire file into memory before processing. Faster for small files, but can crash on large files.")
    parser.add_argument("--storage-account-url", help="URL of the Azure Storage account (e.g., https://<account_name>.blob.core.windows.net). Can also be set via AZURE_STORAGE_ACCOUNT_URL env var.")
    parser.add_argument("--storage-access-key", help="Access key for the Azure Storage account. If not provided, DefaultAzureCredential will be used.")
    parser.add_argument("--container-name", required=True, help="Name of the blob container.")
    parser.add_argument("--blob-prefix-base", default="", help="Base prefix for blobs, prepended to the date-based prefix.")
    parser.add_argument("--query-by", default="day", choices=["year", "month", "day", "hour", "minute"], help="Time unit to query blobs by.")
    parser.add_argument("--start-date", required=True, help="Start date/time in ISO format (e.g., 2025-06-11T10:00:00Z).")
    parser.add_argument("--end-date", help="End date/time in ISO format. Defaults to the end of the period specified by --query-by and --start-date.")
    parser.add_argument("--compression", default="gzip", choices=["gzip", "zstd", "lz4", "none"], help="Compression type of the source blobs.")
    parser.add_argument("--search-key", help="Single key to search for.")
    parser.add_argument("--search-value", help="Value for the single search key.")
    parser.add_argument("--search-key-values", nargs="+", help="List of key:value pairs to search for.")
    parser.add_argument("--match-any-kv", action="store_true", help="Match any key-value pair if --search-key-values is used (default is to match all).")
    parser.add_argument("--display-values", action="store_true", help="Display all key-value pairs for matched records.")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], help="Set the logging level.")
    args = parser.parse_args()

    # --- Setup Logging ---
    logging.getLogger().setLevel(getattr(logging, args.log_level.upper()))
    azure_logger = logging.getLogger("azure.core.pipeline.policies")
    azure_logger.setLevel(logging.WARNING if args.log_level.upper() != "DEBUG" else logging.DEBUG)
    
    if not args.storage_account_url: args.storage_account_url = os.environ.get("AZURE_STORAGE_ACCOUNT_URL")

    from_dt = dtparser.isoparse(args.start_date)
    if not args.end_date:
        _, increment = get_prefix_and_increment(args.query_by, from_dt)
        to_dt = from_dt + increment - timedelta(microseconds=1)
        logger.info(f"End date not specified, defaulting to {to_dt.isoformat()}")
    else:
        to_dt = dtparser.isoparse(args.end_date)
    
    from_ms = int(from_dt.timestamp() * 1000)
    to_ms = int(to_dt.timestamp() * 1000)

    # --- Blob Processing Loop ---
    container_client = get_container_client(args.container_name, args.storage_account_url, args.storage_access_key)
    current_dt = from_dt
    while current_dt <= to_dt:
        date_prefix, increment = get_prefix_and_increment(args.query_by, current_dt)
        full_prefix = posixpath.join(args.blob_prefix_base, date_prefix) if args.blob_prefix_base else date_prefix
        logger.debug(f"Querying for blobs with prefix: '{full_prefix}'")
        try:
            blob_list = list(container_client.list_blobs(name_starts_with=full_prefix))
            if not blob_list:
                logger.debug(f"No blobs found for prefix: '{full_prefix}'")
            else:
                for blob in blob_list:
                    if args.in_memory:
                        process_blob_in_memory(args, container_client, blob.name, from_ms, to_ms)
                    else:
                        process_blob_stream(args, container_client, blob.name, from_ms, to_ms)
        except Exception as e:
            logger.error(f"Failed to list or process blobs for prefix '{full_prefix}': {e}")
        current_dt += increment

def get_prefix_and_increment(query_by, current_time):
    if query_by == "year": return current_time.strftime("%Y"), relativedelta(years=1)
    if query_by == "month": return posixpath.join(current_time.strftime("%Y"), current_time.strftime("%m")), relativedelta(months=1)
    if query_by == "day": return posixpath.join(current_time.strftime("%Y"), current_time.strftime("%m"), current_time.strftime("%d")), timedelta(days=1)
    if query_by == "hour": return posixpath.join(current_time.strftime("%Y"), current_time.strftime("%m"), current_time.strftime("%d"), current_time.strftime("%H")), timedelta(hours=1)
    if query_by == "minute": return posixpath.join(current_time.strftime("%Y"), current_time.strftime("%m"), current_time.strftime("%d"), current_time.strftime("%H")), timedelta(minutes=1)
    raise ValueError(f"Invalid query-by value: {query_by}")

if __name__ == "__main__":
    main()
