"""
Snappy Decompression Script
---------------------------

$AUTHOR$
$DATE$
$VERSION$
$LICENSE$
$DESCRIPTION$
"""


import uuid
import snappy
import boto3
import io
from botocore.exceptions import ClientError

# --- Source bucket (NYC) ---
SRC_REGION = "us-east-1"
SRC_BUCKET = "aws-bigdata-blog"
SRC_PREFIX = "artifacts/flink-refarch/data/nyc-tlc-trips.snz/"

src_s3 = boto3.client("s3", region_name=SRC_REGION)

# --- Destination bucket (your MAC-prefixed bucket) ---
DST_REGION = "us-east-2"
session_dst = boto3.session.Session(region_name=DST_REGION)
dst_s3 = session_dst.client("s3")
effective_region = dst_s3.meta.region_name

def get_mac_prefix() -> str:
    return f"{uuid.getnode():012x}"

DST_BUCKET = f"{get_mac_prefix()}-nyc-taxi".lower()
DST_PREFIX = "decompressed"

# ---- helpers for dest bucket (same as you used before) ----
def bucket_exists_and_region(bucket: str):
    try:
        dst_s3.head_bucket(Bucket=bucket)
        loc = dst_s3.get_bucket_location(Bucket=bucket).get("LocationConstraint")
        return True, (loc or "us-east-1")
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("404", "NoSuchBucket", "NotFound"):
            return False, None
        if code in ("301", "PermanentRedirect"):
            try:
                loc = dst_s3.get_bucket_location(Bucket=bucket).get("LocationConstraint")
                return True, (loc or "us-east-1")
            except ClientError:
                return True, None
        if code in ("403",):
            return True, None
        raise

def ensure_bucket(bucket: str, client_region: str):
    exists, bucket_region = bucket_exists_and_region(bucket)
    if exists:
        if bucket_region and bucket_region != client_region:
            raise RuntimeError(
                f"Bucket '{bucket}' exists in '{bucket_region}', "
                f"but client region is '{client_region}'."
            )
        print(f"[INFO] Bucket {bucket} already exists (region: {bucket_region or client_region})")
        return
    if client_region == "us-east-1":
        dst_s3.create_bucket(Bucket=bucket)
    else:
        dst_s3.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={"LocationConstraint": client_region},
        )
    print(f"[INFO] Created bucket: {bucket} in {client_region}")

print(f"[DEBUG] Destination client region: {effective_region}")
ensure_bucket(DST_BUCKET, effective_region)

# ---- List & process objects from source prefix ----
paginator = src_s3.get_paginator("list_objects_v2")
pages = paginator.paginate(Bucket=SRC_BUCKET, Prefix=SRC_PREFIX)

for page in pages:
    for item in page.get("Contents", []):
        key = item["Key"]
        # Skip folder placeholders and non-.snz files
        if key.endswith("/") or not key.endswith(".snz"):
            continue

        # Get object (client) -> response dict
        resp = src_s3.get_object(Bucket=SRC_BUCKET, Key=key)
        body = resp["Body"]  # StreamingBody (file-like)
        size = resp.get("ContentLength", 0)

        # Prepare destination key (strip .snz)
        filename = key.rsplit("/", 1)[-1]
        # out_name = filename[:-4] + ".txt" # remove ".snz"
        # replace snz with txt
        out_name = filename.replace(".snz", ".txt")
        dst_key = f"{DST_PREFIX}/{filename}" if DST_PREFIX else filename

        # Stream-decompress from S3 -> memory buffer -> upload
        buf = io.BytesIO()
        snappy.stream_decompress(src=body, dst=buf)   # ✅ correct streaming API
        buf.seek(0)
        dst_s3.upload_fileobj(buf, DST_BUCKET, dst_key)

        print(f"[INFO] {key} ({size} bytes) -> s3://{DST_BUCKET}/{dst_key}")