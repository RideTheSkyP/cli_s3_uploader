This utility uploads files from a zip archive to an S3 bucket with defined concurrency.

Usage:

s3_uploader [zip_url] [s3_bucket_name] --concurrency [number_of_concurrent_uploads] --verbose

Example:

s3_uploader https://getsamplefiles.com/download/zip/sample-1.zip my_s3_bucket --concurrency 3 --verbose

