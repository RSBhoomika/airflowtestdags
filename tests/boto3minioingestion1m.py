from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import boto3
from boto3.s3.transfer import TransferConfig
import os

# --- Configuration ---
MINIO_ENDPOINT = "100.94.70.9:31677"  # Replace with your MinIO host
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
#CSV_FILE_PATH = "/opt/airflow/dags/TrafficData.csv"  # Replace with actual CSV file path
CSV_FILE_PATH = "/opt/airflow/dags/repo/tests/one-million-data.csv"
BUCKET_NAME = "airflow-test"  # Replace with target bucket name
OBJECT_NAME = "botoingestion/one-million-data.csv"



def upload_to_minio():
    s3_client = boto3.client(
        "s3",
        endpoint_url=f"http://{MINIO_ENDPOINT}",
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )

    try:
        s3_client.head_bucket(Bucket=BUCKET_NAME)
    except:
        s3_client.create_bucket(Bucket=BUCKET_NAME)

    # Configure multipart upload threshold and concurrency
    config = TransferConfig(
        multipart_threshold=5 * 1024 * 1024,  # 5 MB threshold for multipart upload
        max_concurrency=10,  # Number of threads for multipart upload
        multipart_chunksize=5 * 1024 * 1024,  # Chunk size 5 MB
        use_threads=True
    )

    s3_client.upload_file(
        CSV_FILE_PATH,
        BUCKET_NAME,
        OBJECT_NAME,
        Config=config
    )
    print(f"Uploaded {CSV_FILE_PATH} to {BUCKET_NAME}/{OBJECT_NAME} in MinIO")

# --- Airflow DAG definition ---
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 7, 4),
    'retries': 1,
}

with DAG(
    dag_id='botocore_minio_ingestion_1m',
    default_args=default_args,
    description='Upload a CSV file to MinIO bucket',
    schedule_interval=None,  # Trigger manually
    #schedule_interval='*/5 * * * *', 
    start_date=datetime(2025, 7, 16),
    #end_date=datetime(2025, 7, 12), 
    catchup=False,
    tags=['minio', 'csv', 'upload'],
) as dag:

    upload_task = PythonOperator(
        task_id='upload_csv_to_minio',
        python_callable=upload_to_minio
    )

    upload_task 
