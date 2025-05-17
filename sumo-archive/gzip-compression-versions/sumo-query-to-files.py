#!/usr/bin/env python3

import os
import sys
import json
import gzip
import time
import requests
from io import BytesIO
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse
import posixpath

class SumoExporter:
    def __init__(self, access_id, access_key, api_endpoint, sas_url=None, azure_container_path=None, verbose=None):
        self.access_id = access_id
        self.access_key = access_key
        self.api_endpoint = api_endpoint.rstrip('/') # Normalize
        self.sas_url = sas_url
        self.session = requests.Session()
        self.session.auth = (access_id, access_key)
        self.session.headers.update({"Content-Type": "application/json", "Accept": "application/json"})
        self.azure_container_path = azure_container_path or "" # Path within Azure, e.g. "mycontainer/subfolder"
        self.verbose = verbose

    @staticmethod
    def sanitize(s: str) -> str:
        s = str(s) # Ensure it's a string
        # More comprehensive sanitization for filenames/blob names
        # Replace common problematic characters with underscore
        # Remove characters that are outright invalid in many filesystems/blob names
        # Valid Azure Blob Chars: letters, numbers, and punctuation symbols: / . - _
        # Path separators '/' are okay if creating hierarchy.
        # For a single part of a name, '/' should be replaced.
        # This simple version replaces several known problematic chars for flat names.
        # If 's' is intended to be a path, '/' should be preserved or handled differently.
        # Assuming 's' will be part of a flat filename component here.
        invalid_chars_map = str.maketrans({
            " ": "_", "\"": "", "'": "", "=": "_", "\\": "_", "?": "_",
            "<": "_", ">": "_", ":": "_", "*": "_", "|": "_",
            # Keep '/' if query can contain it and it's desired in filename.
            # For now, replacing it as per original intention for query sanitization.
            "/": "_"
        })
        return s.translate(invalid_chars_map)

    def _upload_to_azure(self, upload_url: str, data_source: bytes or BytesIO, content_type: str):
        """Helper for Azure uploads without Sumo auth."""
        if self.verbose:
            # Limit URL length in print for very long SAS tokens
            max_url_len = 150
            display_url = upload_url[:max_url_len] + '...' if len(upload_url) > max_url_len else upload_url
            print(f"Attempting Azure upload to: {display_url}")
        
        try:
            resp = requests.put(upload_url, headers={
                "x-ms-blob-type": "BlockBlob",
                "Content-Type": content_type 
            }, data=data_source)
            resp.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)

            print(f"✅ Uploaded to Azure: {upload_url.split('?')[0]}") # Print URL without SAS token

        except requests.exceptions.RequestException as e:
            error_message = f"Azure upload failed: {e}"
            if hasattr(e, 'response') and e.response is not None:
                error_message += f"\nStatus Code: {e.response.status_code}"
                if self.verbose or (e.response.status_code >= 400 and e.response.status_code < 500) : # Show client error details
                    error_message += f"\nAzure Response: {e.response.text[:500]}" # Limit error text length
            raise Exception(error_message) from e


    def _build_azure_blob_url(self, blob_name_in_container: str) -> str:
        if not self.sas_url:
            raise ValueError("AZURE_BLOB_SAS (sas_url) is not configured for Azure operation.")

        parsed_sas = urlparse(self.sas_url)
        # self.azure_container_path is "actual_azure_container/optional_subfolder"
        # blob_name_in_container is "query_part1.json.gz"
        # target_blob_full_path becomes "actual_azure_container/optional_subfolder/query_part1.json.gz"
        target_blob_full_path = posixpath.join(self.azure_container_path, blob_name_in_container)
        
        # Ensure the path for the URL is relative to the account (no leading slash if netloc is present)
        final_url_path_part = target_blob_full_path.lstrip('/')
        
        # Rebuild URL: scheme://netloc/path_to_blob?query_from_sas
        upload_url = f"{parsed_sas.scheme}://{parsed_sas.netloc}/{final_url_path_part}?{parsed_sas.query}"
        return upload_url

    def upload_blob_from_memory(self, data_bytes: bytes, blob_name: str):
        upload_url = self._build_azure_blob_url(blob_name)
        if self.verbose:
             print(f"Constructed Azure upload URL (from memory): {upload_url.split('?')[0]}")
        self._upload_to_azure(upload_url, data_bytes, "application/gzip")

    def upload_blob_from_file(self, local_file_path: str):
        blob_final_name = os.path.basename(local_file_path)
        upload_url = self._build_azure_blob_url(blob_final_name)
        
        if self.verbose:
            print(f"Constructed Azure upload URL (from file {local_file_path}): {upload_url.split('?')[0]}")
        
        with open(local_file_path, "rb") as f_data:
            self._upload_to_azure(upload_url, f_data, "application/gzip")

    def create_job(self, query: str, time_from: str, time_to: str) -> str:
        payload = {
            "query": query,
            "from": time_from,
            "to": time_to,
            "timeZone": "UTC"
        }
        if self.verbose:
            print("Creating Sumo Logic search job with payload:")
            print(json.dumps(payload, indent=2))
        
        jobs_url = f"{self.api_endpoint}/api/v1/search/jobs"
        if self.verbose:
            print(f"POSTing to {jobs_url}")

        response = self.session.post(jobs_url, json=payload)
        
        if self.verbose:
            print(f"Sumo Job Create Response {response.status_code}: {response.text[:500]}")
        response.raise_for_status() 
        
        data = response.json()
        job_id = data.get("id")
        if not job_id:
            raise ValueError(f"No job ID ('id') in Sumo Logic response: {data}")
        return job_id

    def wait_for_completion(self, job_id: str, poll_interval: int = 5):
        status_url = f"{self.api_endpoint}/api/v1/search/jobs/{job_id}"
        if self.verbose:
            print(f"Polling job status: {status_url}")
        
        while True:
            try:
                response = self.session.get(status_url)
                response.raise_for_status()
                data = response.json()
                state = data.get("state")
                # Sumo uses recordCount for aggregates, messageCount for messages
                count = data.get("messageCount", data.get("recordCount", 0))

                if self.verbose:
                    print(f"Job state: {state}, Count: {count}, Pending warnings: {data.get('pendingWarnings')}, Pending errors: {data.get('pendingErrors')}")

                if state == "DONE GATHERING RESULTS":
                    print(f"✅ Job {job_id} completed. Total records/messages: {count}")
                    if data.get("pendingErrors") or data.get("pendingWarnings"):
                         print(f"⚠️ Job {job_id} finished with warnings/errors. Check Sumo UI for details. Errors: {data.get('pendingErrors')}, Warnings: {data.get('pendingWarnings')}")
                    return data 
                elif state in ["CANCELLED", "FAILED"]:
                    error_details = data.get('statusMessage', str(data.get('pendingErrors', 'Unknown error')))
                    raise Exception(f"❌ Sumo Logic job {job_id} ended with state: {state}. Details: {error_details}")
                
                time.sleep(poll_interval)
            except requests.exceptions.RequestException as e:
                print(f"⚠️ Error polling job status for {job_id}: {e}. Retrying in {poll_interval}s...")
                time.sleep(poll_interval) # Wait before retrying

    def fetch_messages(self, job_id: str, limit_per_request: int = 10000) -> list:
        all_items = [] # Can be messages or records (for aggregate queries)
        offset = 0
        
        # Determine if fetching messages or records
        job_status_url = f"{self.api_endpoint}/api/v1/search/jobs/{job_id}"
        job_status_resp = self.session.get(job_status_url)
        job_status_resp.raise_for_status()
        job_type_field = "messages" if job_status_resp.json().get("messageCount", 0) > 0 else "records"
        data_key = "messages" if job_type_field == "messages" else "records"


        if self.verbose:
            print(f"Fetching {job_type_field} for job {job_id}...")

        while True:
            url = f"{self.api_endpoint}/api/v1/search/jobs/{job_id}/{job_type_field}?limit={limit_per_request}&offset={offset}"
            if self.verbose:
                print(f"Fetching from: {url}")
            
            resp = self.session.get(url)
            resp.raise_for_status()
            
            try:
                data = resp.json()
            except requests.exceptions.JSONDecodeError as e:
                raise Exception(f"Failed to decode JSON from {url}. Status: {resp.status_code}, Response: {resp.text[:500]}") from e

            items = data.get(data_key, [])
            
            if not items:
                if self.verbose:
                    print(f"No more {job_type_field} to fetch.")
                break
            
            all_items.extend(items)
            offset += len(items)
            if self.verbose:
                print(f"Fetched {len(items)} {job_type_field}. Total so far: {len(all_items)}.")

        print(f"Total {job_type_field} fetched for job {job_id}: {len(all_items)}")
        return all_items
    
    def _message_chunk_generator(self, messages: list, max_mb_per_chunk: int):
        chunk_size_limit_bytes = max_mb_per_chunk * 1024 * 1024
        current_chunk_data = []
        estimated_current_chunk_size_bytes = 0 
        chunk_num = 1

        for msg_index, msg_data in enumerate(messages):
            try:
                # Estimate size of this single message when serialized to JSON.
                # Add a small overhead for list structure (commas, newlines if indenting).
                msg_bytes_len = len(json.dumps(msg_data).encode('utf-8')) + 2 
            except TypeError as e:
                print(f"⚠️ Warning: Could not serialize message at index {msg_index} to estimate size: {e}. Msg: {str(msg_data)[:100]}. Assigning small default size.")
                msg_bytes_len = 100 # Assign a small default size

            if estimated_current_chunk_size_bytes + msg_bytes_len > chunk_size_limit_bytes and current_chunk_data:
                if self.verbose:
                    print(f"Yielding chunk {chunk_num} with {len(current_chunk_data)} messages, estimated uncompressed size ~{estimated_current_chunk_size_bytes / (1024*1024):.2f} MB")
                yield chunk_num, current_chunk_data
                chunk_num += 1
                current_chunk_data = []
                estimated_current_chunk_size_bytes = 0
            
            current_chunk_data.append(msg_data)
            estimated_current_chunk_size_bytes += msg_bytes_len

        if current_chunk_data:
            if self.verbose:
                print(f"Yielding final chunk {chunk_num} with {len(current_chunk_data)} messages, estimated uncompressed size ~{estimated_current_chunk_size_bytes / (1024*1024):.2f} MB")
            yield chunk_num, current_chunk_data

    def _write_gzipped_json_chunk(self, chunk_data_list: list, output_filename: str):
        try:
            with gzip.open(output_filename, "wt", encoding="utf-8") as f:
                json.dump(chunk_data_list, f, indent=2) 
            
            file_size_mb = os.path.getsize(output_filename) / (1024 * 1024)
            print(f"✅ Wrote chunk to {output_filename} ({file_size_mb:.2f} MB actual gzipped size)")
        except Exception as e:
            print(f"❌ Error writing gzipped chunk to {output_filename}: {e}")
            raise

    def write_messages_to_gzipped_files(self, messages: list, base_filename_prefix: str, max_mb_per_file: int) -> list:
        output_files = []
        if not messages:
            print("ℹ️ No messages to write to files.")
            return output_files

        for chunk_idx, message_list_for_chunk in self._message_chunk_generator(messages, max_mb_per_file):
            chunk_filename = f"{base_filename_prefix}_part{chunk_idx}.json.gz"
            self._write_gzipped_json_chunk(message_list_for_chunk, chunk_filename)
            output_files.append(chunk_filename)
        return output_files


def must_env(var_name: str) -> str:
    val = os.getenv(var_name)
    if not val:
        print(f"❌ Missing required environment variable: {var_name}")
        sys.exit(1)
    return val

def main():
    import argparse

    parser = argparse.ArgumentParser(description="Export Sumo Logic query results and optionally upload to Azure Blob Storage.")
    parser.add_argument("--query", default=os.getenv("SUMO_QUERY"), help="Sumo Logic query. Env: SUMO_QUERY")
    parser.add_argument("--from", dest="time_from", default=os.getenv("SUMO_FROM"), help="Start time (ISO 8601 or relative, e.g., -15m). Env: SUMO_FROM. Defaults to 15 mins ago.")
    parser.add_argument("--to", dest="time_to", default=os.getenv("SUMO_TO"), help="End time (ISO 8601 or relative). Env: SUMO_TO. Defaults to now.")
    parser.add_argument("--max-file-size-mb", type=int, default=os.getenv("SUMO_MAX_FILE_SIZE_MB", "1000"), help="Max uncompressed size (MB) per output file part. Env: SUMO_MAX_FILE_SIZE_MB (default: %(default)sMB).")
    parser.add_argument("--verbose", action="store_true", default=os.getenv("SUMO_VERBOSE", "false").lower() == "true", help="Enable verbose output. Env: SUMO_VERBOSE=true")
    parser.add_argument("--short-filename", action="store_true", default=os.getenv("SUMO_SHORT_FILENAME", "false").lower() == "true", help="Use only sanitized query for base filename. Env: SUMO_SHORT_FILENAME=true")
    
    # Upload options
    parser.add_argument("--upload-direct-to-blob", action="store_true", help="Stream gzipped data directly to Azure Blob from memory without writing temporary local files.")
    parser.add_argument("--upload-from-disk", action="store_true", help="Upload files from disk to Azure Blob (requires files to be written first).")

    parser.add_argument("--endpoint", default=os.getenv("SUMO_ENDPOINT", "https://api.us2.sumologic.com"), help="Sumo Logic API endpoint. Env: SUMO_ENDPOINT (default: %(default)s)")
    parser.add_argument("--azure-container-path", default=os.getenv("AZURE_CONTAINER_PATH", "sumo-archive"), help="Azure Blob container & path prefix (e.g., 'mycontainer/subfolder'). Env: AZURE_CONTAINER_PATH (default: %(default)s)")
    
    args = parser.parse_args()

    if args.upload_direct_to_blob and args.upload_from_disk:
        print("❌ Error: Cannot use --upload-direct-to-blob and --upload-from-disk simultaneously.")
        sys.exit(1)

    access_id = must_env("SUMO_ACCESS_ID")
    access_key = must_env("SUMO_ACCESS_KEY")
    
    sas_url = None
    if args.upload_direct_to_blob or args.upload_from_disk:
        sas_url = os.getenv("AZURE_BLOB_SAS")
        if not sas_url:
            print("❌ Error: --upload-direct-to-blob or --upload-from-disk requires AZURE_BLOB_SAS environment variable.")
            sys.exit(1)

    query = args.query
    if not query:
        print("❌ Error: Must provide --query or set SUMO_QUERY.")
        sys.exit(1)

    # Default time logic: if not provided, 'to' is now, 'from' is 15 minutes before 'to'.
    # Sumo API expects ISO 8601 format, e.g., "YYYY-MM-DDTHH:MM:SSZ" or relative like "-1h".
    # For job creation, explicit ISO range is generally more reliable.
    time_to_str = args.time_to
    if not time_to_str:
        time_to_str = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    
    time_from_str = args.time_from
    if not time_from_str:
        # Default 'from' is 15 minutes before 'to_str' if 'to_str' is absolute, else 15 min before now.
        # For simplicity, always make default 'from' 15 mins before the (default or specified) 'to_str' time.
        # However, Sumo API usually wants 'from' to be before 'to'.
        # The original code's default for 'from' was 15m before *current script execution time*.
        # This is simpler and often what's intended for "recent" data.
        try:
            to_datetime_obj = datetime.fromisoformat(time_to_str.replace('Z', '+00:00'))
            time_from_str = (to_datetime_obj - timedelta(minutes=15)).replace(microsecond=0).isoformat()
        except ValueError: # If to_str is not ISO (e.g. relative like "-1h") or invalid
             if args.verbose:
                print(f"⚠️ Could not parse --to '{time_to_str}' as ISO datetime for default --from. Defaulting --from to 15 mins before current time.")
             time_from_str = (datetime.now(timezone.utc) - timedelta(minutes=15)).replace(microsecond=0).isoformat()


    if args.verbose:
        print(f"Query Time Range: From='{time_from_str}' To='{time_to_str}'")

    exporter = SumoExporter(
        access_id, access_key, args.endpoint, 
        sas_url=sas_url, 
        azure_container_path=args.azure_container_path, 
        verbose=args.verbose
    )

    try:
        if args.verbose:
            print(f"Starting Sumo Logic export for query: {query[:200]}...") # Truncate long queries in log
        job_id = exporter.create_job(query, time_from_str, time_to_str)
        if args.verbose:
            print(f"Sumo Logic Job ID: {job_id}")
        
        exporter.wait_for_completion(job_id)
        items = exporter.fetch_messages(job_id)

        if not items:
            print("ℹ️ No messages/records found for the given query and time range.")
            sys.exit(0)

        # Generate base filename prefix
        sanitized_query_part = exporter.sanitize(query[:60]) # Limit length of query part
        if args.short_filename:
            base_filename_prefix = sanitized_query_part
        else:
            # Sanitize time strings for filename (replace common ISO chars)
            sanitized_from = time_from_str.replace(':', '-').replace('T', '_').replace('Z','').replace('+00-00','UTC')
            sanitized_to = time_to_str.replace(':', '-').replace('T', '_').replace('Z','').replace('+00-00','UTC')
            base_filename_prefix = f"{sanitized_query_part}__{sanitized_from}_to_{sanitized_to}"
        
        if args.verbose:
            print(f"Using base filename prefix: {base_filename_prefix}")

        if args.upload_direct_to_blob:
            print("🚀 Uploading directly to Azure Blob Storage...")
            uploaded_chunk_count = 0
            for chunk_idx, item_list_for_chunk in exporter._message_chunk_generator(items, args.max_file_size_mb):
                gzipped_data_buffer = BytesIO()
                # Use "wb" with GzipFile for BytesIO, and write encoded JSON string
                with gzip.GzipFile(fileobj=gzipped_data_buffer, mode="wb") as gz_file:
                    gz_file.write(json.dumps(item_list_for_chunk, indent=2).encode('utf-8'))
                
                blob_name_for_chunk = f"{base_filename_prefix}_part{chunk_idx}.json.gz"
                exporter.upload_blob_from_memory(gzipped_data_buffer.getvalue(), blob_name_for_chunk)
                uploaded_chunk_count +=1
            print(f"Total chunks uploaded directly to Azure: {uploaded_chunk_count}")

        else: # Write to disk first (default), then optionally upload from disk
            print("Writing items to local gzipped files...")
            written_files = exporter.write_messages_to_gzipped_files(items, base_filename_prefix, args.max_file_size_mb)
            
            if args.upload_from_disk:
                print("🚀 Uploading files from disk to Azure Blob Storage...")
                if not written_files:
                     print("No files were written to disk to upload.")
                for file_to_upload in written_files:
                    try:
                        exporter.upload_blob_from_file(file_to_upload)
                    except Exception as e_upload:
                        print(f"❌ Failed to upload {file_to_upload}: {e_upload}")
                    finally: # Attempt to remove local file even if upload failed
                        if args.verbose:
                            print(f"Cleaning up local file: {file_to_upload}")
                        try:
                            os.remove(file_to_upload)
                        except OSError as e_remove: # Catch permission errors etc.
                            print(f"⚠️ Warning: Could not remove local file {file_to_upload}: {e_remove}")
                print(f"Local file upload process completed. Check logs for individual success/failure.")
            elif written_files: # Not uploading, just confirm files written
                print("Skipping Azure upload. Files written locally:")
                for wf_path in written_files:
                    print(f"  -> {wf_path}")
        
        print("✅ Export process completed successfully.")

    except Exception as e:
        print(f"❌ An critical error occurred during the export process: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
