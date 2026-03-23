import boto3
import os

def upload_file(bucket: str, local_path: str, s3_key: str, region: str = None):
    session_args = {}
    if region:
        session_args['region_name'] = region
    s3 = boto3.client('s3', **session_args)
    s3.upload_file(local_path, bucket, s3_key)
    return f"s3://{bucket}/{s3_key}"

if __name__ == '__main__':
    # Example usage for local testing
    bucket = os.environ.get('SALES_BUCKET') or 'personalbucket20645'
    local_path = "jobs/fetch_sales_spark.py"
    s3_key = "scripts/fetch_sales_spark.py"
    print(upload_file(bucket, local_path, s3_key, os.environ.get('AWS_REGION')))
