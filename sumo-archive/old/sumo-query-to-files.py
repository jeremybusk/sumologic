#!/usr/bin/env python3
import os
import sys
import json
import time
import requests
import zstandard as zstd
from io import BytesIO
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse
import posixpath

class SumoExporter:
    def __init__(self, access_id, access_key, api_endpoint, sas_url=None, azure_container_path=None, verbose=None):
        self.access_id = access_id
        self.access_key = access_key
        self.api_endpoint = api_endpoint.rstrip('/')
        self.sas_url = sas_url
        self.session = requests.Session()
        self.session.auth = (access_id, access_key)
        self.session.headers.update({"Content-Type": "application/json", "Accept": "application/json"})
        self.azure_container_path = azure_container_path or ""
        self.verbose = verbose

    @staticmethod
    def sanitize(s: str) -> str:
        invalid_chars_map = str.maketrans({
            " ": "_", "\"": "", "'": "", "=": "_", "\\": "_", "?": "_",
            "<": "_", ">": "_", ":": "_", "*": "_", "|": "_", "/": "_"
        })
        return str(s).translate(invalid_chars_map)

    def _upload_to_azure(self, upload_url: str, data_source: bytes or BytesIO, content_type: str):
        if self.verbose:
            max_url_len = 150
            display_url = upload_url[:max_url_len] + '...' if len(upload_url) > max_url_len else upload_url
            print(f"Uploading to: {display_url}")

        resp = requests.put(upload_url, headers={
            "x-ms-blob-type": "BlockBlob",
            "Content-Type": content_type
        }, data=data_source)
        resp.raise_for_status()
        print(f"✅ Uploaded to Azure: {upload_url.split('?')[0]}")

    def _build_azure_blob_url(self, blob_name_in_container: str) -> str:
        if not self.sas_url:
            raise ValueError("AZURE_BLOB_SAS is not set.")
        parsed_sas = urlparse(self.sas_url)
        target_blob_full_path = posixpath.join(self.azure_container_path, blob_name_in_container)
        return f"{parsed_sas.scheme}://{parsed_sas.netloc}/{target_blob_full_path.lstrip('/')}?{parsed_sas.query}"

    def upload_blob_from_memory(self, data_bytes: bytes, blob_name: str):
        upload_url = self._build_azure_blob_url(blob_name)
        if self.verbose:
            print(f"Uploading (memory): {upload_url.split('?')[0]}")
        self._upload_to_azure(upload_url, data_bytes, "application/zstd")

    def upload_blob_from_file(self, local_file_path: str):
        blob_final_name = os.path.basename(local_file_path)
        upload_url = self._build_azure_blob_url(blob_final_name)
        with open(local_file_path, "rb") as f_data:
            self._upload_to_azure(upload_url, f_data, "application/zstd")

    def create_job(self, query: str, time_from: str, time_to: str) -> str:
        payload = {"query": query, "from": time_from, "to": time_to, "timeZone": "UTC"}
        response = self.session.post(f"{self.api_endpoint}/api/v1/search/jobs", json=payload)
        response.raise_for_status()
        return response.json()["id"]

    def wait_for_completion(self, job_id: str, poll_interval: int = 5):
        status_url = f"{self.api_endpoint}/api/v1/search/jobs/{job_id}"
        while True:
            response = self.session.get(status_url)
            response.raise_for_status()
            data = response.json()
            state = data.get("state")
            if state == "DONE GATHERING RESULTS":
                return
            elif state in ["CANCELLED", "FAILED"]:
                raise Exception(f"Job {job_id} failed with state: {state}")
            time.sleep(poll_interval)

    def fetch_messages(self, job_id: str, limit_per_request: int = 10000) -> list:
        all_items = []
        offset = 0
        job_type = "messages"
        while True:
            url = f"{self.api_endpoint}/api/v1/search/jobs/{job_id}/{job_type}?limit={limit_per_request}&offset={offset}"
            resp = self.session.get(url)
            resp.raise_for_status()
            items = resp.json().get(job_type, [])
            if not items:
                break
            all_items.extend(items)
            offset += len(items)
        return all_items

    def _message_chunk_generator(self, messages: list, max_mb_per_chunk: int):
        chunk_size_limit_bytes = max_mb_per_chunk * 1024 * 1024
        current_chunk_data = []
        current_size = 0
        chunk_num = 1
        for msg in messages:
            msg_bytes = json.dumps(msg).encode('utf-8')
            if current_size + len(msg_bytes) > chunk_size_limit_bytes and current_chunk_data:
                yield chunk_num, current_chunk_data
                chunk_num += 1
                current_chunk_data = []
                current_size = 0
            current_chunk_data.append(msg)
            current_size += len(msg_bytes)
        if current_chunk_data:
            yield chunk_num, current_chunk_data

    def write_messages_to_zstd_files(self, messages: list, base_filename_prefix: str, max_mb_per_file: int) -> list:
        output_files = []
        for chunk_idx, chunk_data in self._message_chunk_generator(messages, max_mb_per_file):
            filename = f"{base_filename_prefix}_part{chunk_idx}.json.zst"
            json_bytes = json.dumps(chunk_data, indent=2).encode("utf-8")
            with open(filename, "wb") as f:
                cctx = zstd.ZstdCompressor()
                f.write(cctx.compress(json_bytes))
            output_files.append(filename)
            print(f"✅ Wrote {filename}")
        return output_files


def must_env(key):
    val = os.getenv(key)
    if not val:
        print(f"Missing required env var: {key}")
        sys.exit(1)
    return val

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--query", required=True)
    parser.add_argument("--from", dest="from_time")
    parser.add_argument("--to", dest="to_time")
    parser.add_argument("--upload", action="store_true")
    parser.add_argument("--max-size", type=int, default=1000)
    parser.add_argument("--prefix", default="sumo_export")
    args = parser.parse_args()

    access_id = must_env("SUMO_ACCESS_ID")
    access_key = must_env("SUMO_ACCESS_KEY")
    endpoint = must_env("SUMO_API_ENDPOINT")
    sas_url = os.getenv("AZURE_BLOB_SAS")

    to_time = args.to_time or datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    from_time = args.from_time or (datetime.now(timezone.utc) - timedelta(minutes=15)).replace(microsecond=0).isoformat()

    exp = SumoExporter(access_id, access_key, endpoint, sas_url, "sumo-archive", verbose=True)
    job_id = exp.create_job(args.query, from_time, to_time)
    exp.wait_for_completion(job_id)
    messages = exp.fetch_messages(job_id)

    if args.upload:
        for idx, chunk in exp._message_chunk_generator(messages, args.max_size):
            json_bytes = json.dumps(chunk, indent=2).encode("utf-8")
            cctx = zstd.ZstdCompressor()
            zstd_bytes = cctx.compress(json_bytes)
            blob_name = f"{args.prefix}_part{idx}.json.zst"
            exp.upload_blob_from_memory(zstd_bytes, blob_name)
    else:
        exp.write_messages_to_zstd_files(messages, args.prefix, args.max_size)

if __name__ == "__main__":
    main()

