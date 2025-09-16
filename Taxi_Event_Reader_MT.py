"""
Events Reader Download
----------------------

Author: kavyasripunna2020
Date:   2025-09-15T21:32:10Z
Version: a030626-dirty
License: MIT
"""

import boto3

import snappy
import FileFormatDetection
from boto3 import session

import os
import io
import threading
import time

from botocore import UNSIGNED
from botocore.client import Config
from botocore.exceptions import NoCredentialsError


import TripEvent
import AdaptTimeOption
from Event import Event 

import logging
import Event
import TripEvent
from AdaptTimeOption import AdaptTimeOption
REGION = "us-east-1"
BUCKET_NAME = "aws-bigdata-blog"
OBJECT_PREFIX = "artifacts/flink-refarch/data/nyc-tlc-trips.snz/"
MAX_FILES = 3  # max files to process
OUTPUT_DIR = "./snappy_decompressed_events"

# ----------------------
# Globals for simple stats
# ----------------------
stats_lock = threading.Lock()
total_events = 0
total_processing_time = 0.0
earliest_time = None
latest_time = None

# ----------------------
# Helpers
# ----------------------
def safe_filename_from_key(key: str, ext: str = ".ndjson") -> str:
    """
    Turn an S3 key into a filesystem-friendly file name, preserving structure hints.
    Examples:
      artifacts/flink-refarch/data/file.snz/part-0000.snz  -> artifacts_flink-refarchdatafile.snz_part-0000.snz.ndjson
    """
    fname = key.replace("/", "")
    return fname + ext

def list_s3_objects(bucket_name: str, prefix: str, max_files: int):
    """Return up to max_files ObjectSummary entries (unsigned)."""
    s3_res = session.Session().resource("s3", config=Config(signature_version=UNSIGNED))
    bucket = s3_res.Bucket(bucket_name)
    out = []
    for i, obj in enumerate(bucket.objects.filter(Prefix=prefix)):
        #logger.info(f"Found: {obj.key}")
        print(f"Found: {obj.key}")
        out.append(obj)
        if i + 1 >= max_files:
            break
    return out

def download_and_decompress(s3_client, obj_summary):
    """Download and snappy-decompress to BytesIO; return (stream, read_time, size)."""
    start = time.time()
    resp = s3_client.get_object(Bucket=obj_summary.bucket_name, Key=obj_summary.key)
    payload = resp["Body"].read()
    size = resp["ContentLength"]
    src = io.BytesIO(payload)
    dst = io.BytesIO()
    snappy.stream_decompress(src=src, dst=dst)
    dst.seek(0)
    read_time = max(0.0, time.time() - start)
    return dst, read_time, size

def process_object(obj_summary, s3_client):
    """
    For a single S3 object:
    - download + decompress
    - parse each line to extract timestamp (ms) but DO NOT convert
    - write all raw lines to a dedicated output file
    - update global stats
    """
    global total_events, total_processing_time, earliest_time, latest_time

    key = obj_summary.key
    stream, read_time, size = download_and_decompress(s3_client, obj_summary)
    bps = (size / read_time) if read_time > 0 else float("inf")
    #logger.info(f"Read {key}: {size} bytes in {read_time:.2f}s ({bps:.2f} B/s)")
    print(f"Read {key}: {size} bytes in {read_time:.2f}s ({bps:.2f} B/s)")  
    # Prepare per-object output file
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    out_path = os.path.join(OUTPUT_DIR, safe_filename_from_key(key, ".ndjson"))

    events = 0
    start_proc = time.time()

    with open(out_path, "w", encoding="utf-8", buffering=1024 * 1024) as fh:
        for raw in stream:
            try:
                line = raw.decode("unicode_escape")
                ev = TripEvent(line)
                ts_ms = ev.timestamp  # keep raw millis

                # update bounds
                if ts_ms is not None:
                    with stats_lock:
                        if earliest_time is None or ts_ms < earliest_time:
                            earliest_time = ts_ms
                        if latest_time is None or ts_ms > latest_time:
                            latest_time = ts_ms

                # write the original line
                if not line.endswith("\n"):
                    line += "\n"
                fh.write(line)

                events += 1
                if events % 50000 == 0:
                    fh.flush()
            except ValueError:
                #logger.warning(f"{key}: Ignoring malformed line.")
                print(f"{key}: Ignoring malformed line.")
            except Exception as e:
                #logger.error(f"{key}: Error processing line: {e}")
                print(f"{key}: Error processing line: {e}")

    proc_time = max(0.0, time.time() - start_proc)
    with stats_lock:
        total_events += events
        total_processing_time += proc_time

    thr = (events / proc_time) if proc_time > 0 else 0.0
    #logger.info(f"Wrote {events} events to {out_path} | ProcTime {proc_time:.2f}s | {thr:.2f} ev/s")
    print(f"Wrote {events} events to {out_path} | ProcTime {proc_time:.2f}s | {thr:.2f} ev/s")  
    return out_path, events, proc_time

session_ = session.Session()
s3_resource = session_.resource('s3', region_name=REGION)

bucket = s3_resource.Bucket(BUCKET_NAME)
print("Files in S3 bucket:")

for obj in bucket.objects.filter(Prefix=OBJECT_PREFIX):
    target = obj.key.split("/")[-1]
    if target:  # avoid downloading empty prefix
        print(f"Downloading {obj.key} to {target}")
        bucket.download_file(obj.key, "./"+target)
        print(f"Downloaded {obj.key} to {target}")
        res = FileFormatDetection.sniff_format("./"+target)
        print(f'File Format detection summary for {target} is {res.summary()}')
        print(f'File Format detection isColumar for {target} is {res.is_columnar()}')
        print(f'File Format detection isCompressed for {target} is {res.is_compressed()}')
        print(f'File Format detection metadata for {target} is {res.metadata()}')
        print(target)
        print(f"Size: {obj.size/(1024*1024)} MB")