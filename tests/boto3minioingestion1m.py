from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import boto3
from boto3.s3.transfer import TransferConfig
import os
import time

# --- Configuration ---
MINIO_ENDPOINT = "100.94.70.9:31677"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"

BUCKET_NAME = "airflow-test"
SOURCE_OBJECT_NAME = "TrafficData.csv"
TARGET_OBJECT_NAME = "botoingestion/TrafficData.csv"

LOCAL_FILE_PATH = "/tmp/TrafficData.csv"

def download_and_upload_minio():
    s3_client = boto3.client(
        "s3",
        endpoint_url=f"http://{MINIO_ENDPOINT}",
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )
    
    # Download the file locally
    print(f"Downloading s3://{BUCKET_NAME}/{SOURCE_OBJECT_NAME} to {LOCAL_FILE_PATH}")
    dstart_time = time.time()
    s3_client.download_file(BUCKET_NAME, SOURCE_OBJECT_NAME, LOCAL_FILE_PATH)
    dend_time = time.time()
    print("Timetaken to download: ", dend_time - dstart_time, "seconds")
    
    # Upload the file to new folder in same bucket
    config = TransferConfig(
        multipart_threshold=10 * 1024 * 1024,
        max_concurrency=10,
        multipart_chunksize=10 * 1024 * 1024,
        use_threads=True
    )
    ustart_time = time.time()
    s3_client.upload_file(
        LOCAL_FILE_PATH,
        BUCKET_NAME,
        TARGET_OBJECT_NAME,
        Config=config
    )
    uend_time = time.time()
    print(f"Uploaded {LOCAL_FILE_PATH} to s3://{BUCKET_NAME}/{TARGET_OBJECT_NAME}")
    print("Timetaken to upload: ", uend_time - ustart_time, "seconds")

# --- Airflow DAG definition ---
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 7, 4),
    'retries': 1,
}

with DAG(
    dag_id='botocore_minio_ingestion_1m',
    default_args=default_args,
    description='Download and upload file within MinIO bucket in the same task',
    schedule_interval=None,
    catchup=False,
    tags=['minio', 'csv', 'upload'],
) as dag:

    download_upload_task = PythonOperator(
        task_id='download_and_upload_file',
        python_callable=download_and_upload_minio
    )

    download_upload_task
