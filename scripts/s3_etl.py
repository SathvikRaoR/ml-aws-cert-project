import os
import boto3
import pandas as pd
from dotenv import load_dotenv
import logging

# Load environment variables from .env
load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
S3_BUCKET = os.getenv("S3_BUCKET")

DATA_DIR = os.path.join(os.getcwd(), 'data')
LOG_DIR = os.path.join(os.getcwd(), 'logs')
LOG_FILE = os.path.join(LOG_DIR, "s3_etl.log")

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# Set up the S3 client
s3 = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_DEFAULT_REGION
)

def upload_file(local_path, s3_key):
    try:
        s3.upload_file(local_path, S3_BUCKET, s3_key)
        logging.info(f'Uploaded {local_path} to s3://{S3_BUCKET}/{s3_key}')
        print(f"[+] Uploaded {local_path} to s3://{S3_BUCKET}/{s3_key}")
    except Exception as e:
        logging.error(f'Failed to upload {local_path}: {e}')
        print(f"[!] Upload error: {e}")

def download_file(s3_key, local_path):
    try:
        s3.download_file(S3_BUCKET, s3_key, local_path)
        logging.info(f'Downloaded s3://{S3_BUCKET}/{s3_key} to {local_path}')
        print(f"[+] Downloaded s3://{S3_BUCKET}/{s3_key} to {local_path}")
    except Exception as e:
        logging.error(f'Failed to download {s3_key}: {e}')
        print(f"[!] Download error: {e}")

def list_files(prefix=''):
    try:
        resp = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
        files = [obj['Key'] for obj in resp.get('Contents', [])]
        logging.info(f'Listed files under {prefix}: {files}')
        print(f"[+] Files in s3://{S3_BUCKET}/{prefix}: {files}")
        return files
    except Exception as e:
        logging.error(f'Failed to list files in {prefix}: {e}')
        print(f"[!] List error: {e}")
        return []

def validate_csv(local_path):
    try:
        df = pd.read_csv(local_path)
        required_cols = {'date', 'desc', 'amount'}
        if not required_cols.issubset(set(df.columns)):
            raise ValueError(f"Missing columns: {required_cols - set(df.columns)}")
        print("[+] CSV validation passed")
        return True
    except Exception as e:
        logging.error(f"CSV validation error: {e}")
        print(f"[!] CSV validation error: {e}")
        return False

def run_demo_etl():
    test_csv = os.path.join(DATA_DIR, "test_upload.csv")
    # Create dummy data
    df = pd.DataFrame({
        'date': ["2025-07-06"],
        'desc': ["Sample Transaction"],
        'amount': [123.45]
    })
    df.to_csv(test_csv, index=False)
    print(f"[+] Created test CSV: {test_csv}")
    logging.info(f"Created test CSV: {test_csv}")

    if not validate_csv(test_csv):
        print("[!] Validation failed—aborting upload.")
        return

    upload_file(test_csv, "test/test_upload.csv")
    list_files("test/")

    # Download and validate
    dl_csv = os.path.join(DATA_DIR, "test_download.csv")
    download_file("test/test_upload.csv", dl_csv)
    validate_csv(dl_csv)

if __name__ == '__main__':
    run_demo_etl()