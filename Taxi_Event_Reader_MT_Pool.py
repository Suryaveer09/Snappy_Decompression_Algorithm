#import boto3
from boto3 import session
from botocore import UNSIGNED
from botocore.client import Config
#from botocore.exceptions import NoCredentialsError, ClientError
from botocore.exceptions import ClientError
import logging

import os
import io
import threading
import time
import signal
import sys

import snappy
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError



import FileFormatDetection

from TripEvent import TripEvent
#import AdaptTimeOption

REGION = "us-east-1"
BUCKET_NAME = "aws-bigdata-blog"
OBJECT_PREFIX = "artifacts/flink-refarch/data/nyc-tlc-trips.snz/"
MAX_FILES = 20
OUTPUT_DIR = "./snappy_decompressed_events"
MAX_WORKERS = min(10, MAX_FILES)  # Reduced from 20, don't exceed file count
DOWNLOAD_TIMEOUT = 300  # 5 minutes timeout for downloads

# ----------------------
# Globals for simple stats
# ----------------------
stats_lock = threading.Lock()
total_events = 0
total_processing_time = 0.0
earliest_time = None
latest_time = None

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(threadName)s - %(message)s')
logger = logging.getLogger(__name__)


def signal_handler(sig, frame):
    """Handle Ctrl+C gracefully"""
    logger.info("Received interrupt signal, shutting down...")
    sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)


def safe_filename_from_key(key: str, ext: str = ".ndjson") -> str:
    """Turn an S3 key into a filesystem-friendly file name."""
    fname = key.replace("/", "").replace("\\", "")
    return fname + ext


def list_s3_objects(bucket_name: str, prefix: str, max_files: int):
    """Return up to max_files ObjectSummary entries (unsigned)."""
    try:
        s3_res = session.Session().resource("s3", config=Config(signature_version=UNSIGNED))
        bucket = s3_res.Bucket(bucket_name)
        out = []
        for i, obj in enumerate(bucket.objects.filter(Prefix=prefix)):
            logger.info(f"Found: {obj.key}")
            out.append(obj)
            if i + 1 >= max_files:
                break
        return out
    except Exception as e:
        logger.error(f"Error listing S3 objects: {e}")
        return []


def download_and_decompress(obj_summary):
    """Download and snappy-decompress; return (stream, read_time, size)."""
    # Create a fresh S3 client for this thread to avoid contention
    sess = session.Session()
    s3_client = sess.client("s3", region_name=REGION,
                            config=Config(signature_version=UNSIGNED,
                                          read_timeout=DOWNLOAD_TIMEOUT,
                                          retries={'max_attempts': 3}))

    start = time.time()
    try:
        resp = s3_client.get_object(Bucket=obj_summary.bucket_name, Key=obj_summary.key)
        payload = resp["Body"].read()
        size = resp["ContentLength"]

        # Decompress using smaller memory footprint
        src = io.BytesIO(payload)
        dst = io.BytesIO()

        # Clear payload from memory immediately
        del payload

        snappy.stream_decompress(src=src, dst=dst)
        dst.seek(0)

        read_time = max(0.0, time.time() - start)
        return dst, read_time, size

    except ClientError as e:
        logger.error(f"S3 error downloading {obj_summary.key}: {e}")
        raise
    except Exception as e:
        logger.error(f"Error downloading {obj_summary.key}: {e}")
        raise


def process_object(obj_summary):
    """Process a single S3 object with improved error handling and resource management."""
    thread_name = threading.current_thread().name
    logger.info(f"Starting processing for {obj_summary.key}")

    global total_events, total_processing_time, earliest_time, latest_time

    try:
        key = obj_summary.key
        stream, read_time, size = download_and_decompress(obj_summary)

        bps = (size / read_time) if read_time > 0 else float("inf")
        logger.info(f"Downloaded {key}: {size} bytes in {read_time:.2f}s ({bps:.2f} B/s)")

        file_format = FileFormatDetection.sniff_stream(stream)
        logger.info(f"Detected file format for {key}: {file_format}")
        stream.seek(0)

        # Prepare output
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        out_path = os.path.join(OUTPUT_DIR, safe_filename_from_key(key, ".ndjson"))

        events = 0
        start_proc = time.time()

        # Use smaller buffer and ensure proper cleanup
        with open(out_path, "w", encoding="utf-8", buffering=8192) as fh:  # Reduced buffer
            try:
                for raw in stream:
                    try:
                        line = raw.decode("unicode_escape")
                        ev = TripEvent(line)
                        ts_ms = ev.timestamp

                        # Update bounds with lock
                        if ts_ms is not None:
                            with stats_lock:
                                if earliest_time is None or ts_ms < earliest_time:
                                    earliest_time = ts_ms
                                if latest_time is None or ts_ms > latest_time:
                                    latest_time = ts_ms

                        # Write line
                        if not line.endswith("\n"):
                            line += "\n"
                        fh.write(line)

                        events += 1
                        if events % 10000 == 0:  # More frequent flushes
                            fh.flush()
                            logger.debug(f"{key}: Processed {events} events")

                    except ValueError as e:
                        logger.warning(f"{key}: Ignoring malformed line: {e}")
                    except Exception as e:
                        logger.error(f"{key}: Error processing line: {e}")

            finally:
                fh.flush()

        # Cleanup stream
        stream.close()

        proc_time = max(0.0, time.time() - start_proc)

        # Update global stats
        with stats_lock:
            total_events += events
            total_processing_time += proc_time

        thr = (events / proc_time) if proc_time > 0 else 0.0
        logger.info(f"Completed {key}: {events} events in {proc_time:.2f}s ({thr:.2f} ev/s)")

        return out_path, events, proc_time, thread_name

    except Exception as e:
        logger.error(f"Failed processing {obj_summary.key}: {e}")
        raise


def main():
    logger.info(f"Starting with MAX_WORKERS={MAX_WORKERS}")

    # List objects first
    objs = list_s3_objects(BUCKET_NAME, OBJECT_PREFIX, MAX_FILES)
    if not objs:
        logger.error("No S3 objects found for given prefix.")
        return

    # Adjust worker count based on actual files found
    actual_workers = min(MAX_WORKERS, len(objs))
    logger.info(f"Processing {len(objs)} files with {actual_workers} workers")

    t0 = time.time()
    outputs = []

    try:
        # Use context manager with timeout
        with ThreadPoolExecutor(max_workers=actual_workers,
                                thread_name_prefix="S3Worker") as pool:
            # Submit all tasks
            futures = {pool.submit(process_object, obj): obj.key for obj in objs}

            # Process results with timeout
            for fut in as_completed(futures, timeout=DOWNLOAD_TIMEOUT * len(objs)):
                key = futures[fut]
                try:
                    out_path, events, proc_time, thread_name = fut.result(timeout=30)
                    logger.info(f"Thread {thread_name} completed {key}: {events} events")
                    outputs.append(out_path)

                except TimeoutError:
                    logger.error(f"Timeout processing {key}")
                    fut.cancel()
                except Exception as e:
                    logger.error(f"Failed processing {key}: {e}")

    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        return
    except Exception as e:
        logger.error(f"ThreadPool error: {e}")
        return

    total_time = max(0.0, time.time() - t0)

    # Final stats
    if total_events > 0:
        overall_thr = (total_events / total_processing_time) if total_processing_time > 0 else 0.0
        logger.info("----- SUMMARY -----")
        logger.info(f"Objects processed: {len(outputs)}")
        logger.info(f"Total events: {total_events}")
        logger.info(f"Overall processing throughput: {overall_thr:.2f} ev/s")
        if earliest_time is not None:
            logger.info(f"Earliest Event Time (ms): {earliest_time}")
        if latest_time is not None:
            logger.info(f"Latest Event Time (ms): {latest_time}")
        logger.info(f"Output directory: {os.path.abspath(OUTPUT_DIR)}")
        logger.info(f"Total wall time: {total_time:.2f}s")


if __name__ == "__main__":
    main()