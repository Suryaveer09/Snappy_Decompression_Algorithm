"""
Snappy Decompression Script
---------------------------

$AUTHOR$
$DATE$
$VERSION$
$LICENSE$
$DESCRIPTION$
"""


import os
import uuid
import snappy
import boto3
from io import BytesIO
from botocore.exceptions import ClientError

def get_mac_prefix() -> str:
    """Return MAC address as a lowercase string without colons."""
    return f"{uuid.getnode():012x}"

LOCAL_DIR = "./"
BASE_BUCKET = "nyc-taxi"
PREFIX = "decompressed"
PREFERRED_REGION = "us-east-2"  # your target region

# Create a session/client pinned to your preferred region
session = boto3.session.Session(region_name=PREFERRED_REGION)
s3 = session.client("s3")
effective_region = s3.meta.region_name  # what this client will actually use

BUCKET = f"{get_mac_prefix()}-{BASE_BUCKET}".lower()

def bucket_exists_and_region(bucket: str):
    """Return (exists: bool, region: str|None)."""
    try:
        s3.head_bucket(Bucket=bucket)
        loc = s3.get_bucket_location(Bucket=bucket).get("LocationConstraint")
        return True, (loc or "us-east-1")
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("404", "NoSuchBucket", "NotFound"):
            return False, None
        if code in ("301", "PermanentRedirect"):
            # Exists but different region; try to fetch it
            try:
                loc = s3.get_bucket_location(Bucket=bucket).get("LocationConstraint")
                return True, (loc or "us-east-1")
            except ClientError:
                return True, None
        if code in ("403",):
            # Exists but you don’t have permissions to head it
            return True, None
        raise  # unexpected

def ensure_bucket(bucket: str, client_region: str):
    exists, bucket_region = bucket_exists_and_region(bucket)
    if exists:
        if bucket_region and bucket_region != client_region:
            raise RuntimeError(
                f"Bucket '{bucket}' exists in region '{bucket_region}', "
                f"but your client is using '{client_region}'. Use another name or region."
            )
        print(f"[INFO] Bucket {bucket} already exists (region: {bucket_region or client_region})")
        return

    # Create with correct LocationConstraint (omit ONLY for us-east-1)
    if client_region == "us-east-1":
        s3.create_bucket(Bucket=bucket)
    else:
        s3.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={"LocationConstraint": client_region},
        )
    print(f"[INFO] Created bucket: {bucket} in {client_region}")

print(f"[DEBUG] Effective client region: {effective_region}")
ensure_bucket(BUCKET, effective_region)

# Process and upload files
for filename in os.listdir(LOCAL_DIR):
    if not filename.endswith(".snz"):
        continue

    in_path = os.path.join(LOCAL_DIR, filename)
    key = f"{PREFIX}/{filename[:-4]}" if PREFIX else filename[:-4]

    with open(in_path, "rb") as src:
        buf = BytesIO()
        snappy.stream_decompress(src=src, dst=buf)
        buf.seek(0)
        s3.upload_fileobj(buf, BUCKET, key)

    print(f"[INFO] Uploaded to s3://{BUCKET}/{key}")
