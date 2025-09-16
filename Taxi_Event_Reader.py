"""
Events Reader Download
----------------------

Author: kavyasripunna2020
Date:   2025-09-15T21:32:10Z
Version: a030626-dirty
License: MIT
"""

import boto3
import FileFormatDetection
from boto3 import session

region = "us-east-1"
bucket_name = "aws-bigdata-blog"
object_prefix = "artifacts/flink-refarch/data/nyc-tlc-trips.snz/"

session_ = session.Session()
s3_resource = session_.resource('s3', region_name=region)

bucket = s3_resource.Bucket(bucket_name)
print("Files in S3 bucket:")
# for obj in bucket.objects.filter(Prefix=object_prefix):
#     # get_obj = s3_resource.Object(bucket_name, obj.key)
#     print(obj.key)

for obj in bucket.objects.filter(Prefix=object_prefix):
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