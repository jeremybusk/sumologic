#!/usr/bin/env python3

import argparse
import concurrent.futures
import gzip
import json
import logging
import os
import sys
import threading
import time
from datetime import datetime, timedelta, timezone
import re
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import zstandard
import io

curl_lock = threading.Lock()

def setup_logging(level_name, log_file=None):
    level = getattr(logging, level_name.upper(), logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(levelname)s - %(threadName)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)]
    )
    if log_file:
        try:
            fh = logging.FileHandler(log_file)
            fh.setLevel(level)
            fh.setFormatter(logging.Formatter(
                "%(asctime)s - %(levelname)s - %(threadName)s - %(message)s"
            ))
            logging.getLogger().addHandler(fh)
            logging.info(f"Logging to file: {log_file}")
        except IOError as e:
            logging.error(f"Could not open log file {log_file}: {e}")
            sys.exit(1)

def get_env_or_arg(env_var, arg_val):
    return os.environ.get(env_var) if arg_val is None else arg_val

def parse_kv_list(kv_str):
    if not kv_str:
        return {}
    out = {}
    for pair in kv_str.split(","):
        if "=" not in pair:
            logging.error(f"Invalid pair '{pair}', expected key=value")
            sys.exit(1)
        k, v = pair.split("=", 1)
        out[k.strip()] = v.strip()
    return out

def parse_list(list_str, default=None):
    if list_str is None:
        return default or []
    return [item.strip() for item in list_str.split(",") if item.strip()]

def generate_file_paths(start_dt, end_dt, base_dir, granularity, compression):
    delta_map = {
        "minute": timedelta(minutes=1),
        "hour":   timedelta(hours=1),
        "day":    timedelta(days=1),
    }
    fmt_map = {
        "minute": ["%Y","%m","%d","%H","%M"],
        "hour":   ["%Y","%m","%d","%H"],
        "day":    ["%Y","%m","%d"],
    }
    if granularity not in delta_map:
        logging.error(f"Unsupported granularity: {granularity}")
        return []

    paths = []
    cur = start_dt
    delta = delta_map[granularity]
    fmt_elems = fmt_map[granularity]

    while cur <= end_dt:
        parts = [cur.strftime(fe) for fe in fmt_elems]
        candidate = os.path.join(base_dir, *parts) + f".json.{compression}"
        if os.path.exists(candidate):
            paths.append(candidate)
        else:
            logging.debug(f"Missing file, skipping: {candidate}")
        cur += delta

    if not paths:
        logging.warning("No files found in the specified range.")
    return paths

def create_session_with_retries():
    sess = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429,500,502,503,504],
        allowed_methods=["GET","POST"]
    )
    adapter = HTTPAdapter(max_retries=retry)
    sess.mount("http://", adapter)
    sess.mount("https://", adapter)
    return sess

def verify_stream(stream_labels, first_ts, last_ts, query_url, auth, verify_tls, session):
    selector = "{" + ",".join(f'{k}="{v}"' for k,v in stream_labels.items()) + "}"
    params = {"query": selector, "start": first_ts, "end": last_ts, "limit": 1}
    try:
        r = session.get(query_url, params=params, auth=auth, verify=verify_tls, timeout=10)
        r.raise_for_status()
        data = r.json()
        if data.get("data",{}).get("result"):
            logging.info(f"[verify] logs found for {selector}")
            return True
        else:
            logging.error(f"[verify] no logs found for {selector}")
            return False
    except requests.RequestException as e:
        logging.error(f"[verify] request failed for {selector}: {e}")
        return False

def process_file(
    file_path,
    compression_format,
    loki_push_url,
    tenant,
    auth,
    add_labels,
    remove_labels,
    keep_labels,
    label_limit,
    verify_tls,
    verify_push=False,
    verify_delay=5,
    verify_max_attempts=1,
    create_curl=False,
    session=None,
    batch_size=1000
):
    def sanitize_label_key(k, fallback_prefix="x", idx=0):
        k = k.replace('.', '_').replace('-', '_')
        # if not k or not k[0].isalpha():
        if not k or not re.match(r'^[a-zA-Z_]', k):
            k = f"{fallback_prefix}_{k}"
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', k):
            k = f"{fallback_prefix}_{idx}"
        return k

    logging.info(f"📄 Processing {file_path}")
    try:
        if compression_format == "gz":
            with gzip.open(file_path, "rt", encoding="utf-8") as f:
                entries = json.load(f)
        else:
            dctx = zstandard.ZstdDecompressor()
            with open(file_path, "rb") as f, dctx.stream_reader(f) as reader:
                text_stream = io.TextIOWrapper(reader, encoding="utf-8")
                entries = json.load(text_stream)
    except Exception as e:
        logging.error(f"❌ Failed to read/decompress {file_path}: {e}")
        return

    logging.debug(f"✅ Loaded {len(entries)} entries from {file_path}")

    valid = [e for e in entries if "map" in e and "_messagetime" in e["map"]]
    logging.debug(f"📝 Valid entries after filter: {len(valid)}")

    if not valid:
        logging.warning(f"⚠️ No valid entries in {file_path}")
        return

    valid.sort(key=lambda e: int(e["map"]["_messagetime"]))

    streams = {}
    for e_idx, e in enumerate(valid):
        original_map = e["map"]

        if keep_labels and remove_labels:
            logging.error("❌ Cannot use both --keep-labels and --remove-labels. Skipping.")
            return

        if keep_labels:
            raw_labels = {k: original_map[k] for k in keep_labels if k in original_map}
        else:
            raw_labels = {k: v for k, v in original_map.items() if k not in ("_raw", "_receipttime", "_messagetime")}
            for k in remove_labels:
                raw_labels.pop(k, None)

        labels = {}
        for i, (k, v) in enumerate(raw_labels.items()):
            clean_k = sanitize_label_key(k, idx=i)
            labels[clean_k] = str(v)

        labels.update(add_labels)

        if not labels:
            labels["job"] = "sumo"

        if len(labels) > label_limit:
            logging.error(f"❌ Stream has {len(labels)} labels (> {label_limit}): {labels}. Skipping.")
            continue

        raw = original_map.get("_raw", "")
        ts_ms = int(original_map["_messagetime"])
        ts_ns = str(ts_ms * 1_000_000)

        message = f"{raw} {' '.join(f'{k}={v}' for k,v in add_labels.items())}" if add_labels else raw

        key = tuple(sorted(labels.items()))
        if key not in streams:
            streams[key] = {"stream": labels, "values": []}
        streams[key]["values"].append([ts_ns, message])

    # Filter out empty streams
    valid_streams = []
    for s_idx, s in enumerate(streams.values()):
        if not s["values"]:
            logging.warning(f"🚫 Stream {s_idx+1} with labels={s['stream']} has no values, skipping")
            continue
        valid_streams.append(s)

    if not valid_streams:
        logging.warning(f"⚠️ All streams empty for {file_path}, nothing to push")
        return

    logging.debug(f"🔗 Generated {len(valid_streams)} valid streams from {file_path}")


    if create_curl:
        payload_str = json.dumps({"streams": list(streams.values())}, indent=2)
        curl_cmd = f'''curl -vk -u "${{LOKI_USER}}:${{LOKI_PASS}}" \\
  -H "X-Scope-OrgID: ${{ORG_ID}}" \\
  -H "Content-Type: application/json" \\
  -XPOST "${{LOKI_URL}}" \\
  --data-raw '{payload_str}'

'''
        with curl_lock:
            with open("curl.out", "a") as cf:
                cf.write(curl_cmd)
        logging.info(f"✍️ Wrote curl command for {file_path} to curl.out")
        return

    if not verify_tls:
        requests.packages.urllib3.disable_warnings()

    for s_idx, s in enumerate(streams.values()):
        all_values = s["values"]
        logging.debug(f"📦 Stream {s_idx+1}: {len(all_values)} entries, labels={s['stream']}")

        for start in range(0, len(all_values), batch_size):
            chunk = all_values[start:start + batch_size]
            payload = {"streams": [{"stream": s["stream"], "values": chunk}]}
            headers = {"Content-Type": "application/json"}
            if tenant:
                headers["X-Scope-OrgID"] = tenant

            logging.debug(f"🚀 Pushing batch [{start}:{start+len(chunk)}] of stream {s_idx+1} with {len(chunk)} entries")

            try:
                resp = session.post(
                    loki_push_url,
                    headers=headers,
                    json=payload,
                    auth=auth,
                    verify=verify_tls,
                    timeout=30
                )
                resp.raise_for_status()
                logging.info(f"✅ Pushed {len(chunk)} entries from {file_path} (stream labels={s['stream']})")
            except requests.RequestException as e:
                logging.error(f"❌ Chunk push failed for {file_path}: {e}")
                continue

def main():
    p = argparse.ArgumentParser(
        description="Reads Sumo Logic JSON logs and pushes them to Grafana Loki."
    )
    p.add_argument("--loki-push-url", help="Loki push API URL. Env: LOKI_PUSH_URL")
    p.add_argument("--tenant", help="Loki tenant ID. Env: LOKI_TENANT")
    p.add_argument("--basic-auth-user", help="Loki user. Env: LOKI_USER")
    p.add_argument("--basic-auth-pass", help="Loki pass. Env: LOKI_PASS")
    p.add_argument("--skip-tls-verify", action="store_true", help="Disable TLS verify.")
    p.add_argument("--message-files-dir", required=True, help="Base dir for log files.")
    p.add_argument("--compression-format", choices=["gz","zst"], required=True)
    p.add_argument("--message-files-format", choices=["minute","hour","day"], required=True)
    p.add_argument("--from-datetime", required=True)
    p.add_argument("--to-datetime", required=True)
    p.add_argument("--add-labels", help="e.g. env=prod,region=us-east-1")
    group = p.add_mutually_exclusive_group()
    group.add_argument("--remove-labels", help="e.g. _collectorid,_blockid")
    group.add_argument("--keep-labels", help="e.g. host,job")
    p.add_argument("--label-limit", type=int, default=15,
                   help="Maximum number of labels per stream (default: 15)")
    p.add_argument("--verify-push", action="store_true")
    p.add_argument("--verify-delay", type=int, default=5)
    p.add_argument("--verify-max-attempts", type=int, default=1)
    p.add_argument("--create-curl", action="store_true")
    p.add_argument("--log-file")
    p.add_argument("--log-level", default="INFO", choices=["DEBUG","INFO","WARNING","ERROR","CRITICAL"])
    p.add_argument("--concurrency", type=int, default=10)
    p.add_argument("--batch-size", type=int, default=1000)

    args = p.parse_args()
    setup_logging(args.log_level, args.log_file)

    if args.create_curl:
        with open("curl.out","w") as cf:
            cf.write("# Generated curl commands\n\n")
        logging.info("Initialized curl.out")

    loki_url = get_env_or_arg("LOKI_PUSH_URL", args.loki_push_url)
    tenant   = get_env_or_arg("LOKI_TENANT", args.tenant)
    user     = get_env_or_arg("LOKI_USER", args.basic_auth_user)
    passwd   = get_env_or_arg("LOKI_PASS", args.basic_auth_pass)
    if not loki_url:
        logging.error("Loki push URL is required.")
        sys.exit(1)
    auth = (user, passwd) if user and passwd else None

    add_labels = parse_kv_list(args.add_labels)
    remove_labels = parse_list(args.remove_labels, default=[])
    keep_labels = parse_list(args.keep_labels) if args.keep_labels else None

    start_dt = datetime.fromisoformat(args.from_datetime.replace("Z","+00:00")).astimezone(timezone.utc)
    end_dt = datetime.fromisoformat(args.to_datetime.replace("Z","+00:00")).astimezone(timezone.utc)

    paths = generate_file_paths(
        start_dt, end_dt,
        args.message_files_dir, args.message_files_format,
        args.compression_format
    )
    if not paths:
        sys.exit(0)

    session = create_session_with_retries()
    verify_tls = not args.skip_tls_verify

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=args.concurrency, thread_name_prefix="wrk"
    ) as ex:
        futures = [
            ex.submit(
                process_file,
                path,
                args.compression_format,
                loki_url,
                tenant,
                auth,
                add_labels,
                remove_labels,
                keep_labels,
                args.label_limit,
                verify_tls,
                args.verify_push,
                args.verify_delay,
                args.verify_max_attempts,
                args.create_curl,
                session,
                args.batch_size
            )
            for path in paths
        ]
        concurrent.futures.wait(futures)

    logging.info("✅ Done.")

if __name__ == "__main__":
    main()
