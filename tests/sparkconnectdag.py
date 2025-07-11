from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
import logging
import os

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 7, 11),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

S3_PATH = "s3a://airflow-test/sparkconnect/"
SPARK_CONNECT_ENDPOINT = "sc://100.94.70.9:30816"  

def create_spark_session():
    os.environ['JAVA_HOME'] = '/opt/java/openjdk'
    os.environ['PATH'] = f"{os.environ['JAVA_HOME']}/bin:" + os.environ['PATH']
    logging.info("Creating Spark Connect session...")
    
    spark = SparkSession.builder \
        .remote(SPARK_CONNECT_ENDPOINT) \
        .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.4') \
        .config("spark.hadoop.fs.s3a.access.key", "minio") \
        .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://100.94.70.9:31677") \
        .config("spark.network.timeout", "60s") \
        .config("spark.hadoop.fs.s3a.connection.timeout", "60000") \
        .config("spark.hadoop.fs.s3a.attempts.maximum", "3") \
        .config("spark.hadoop.fs.s3a.retry.interval", "1000ms") \
        .config("mapreduce.fileoutputcommitter.algorithm.version", "2") \
        .config("fs.s3a.committer.staging.conflict-mode", "replace") \
        .getOrCreate()

    return spark

def upload_to_s3():
    logging.basicConfig(level=logging.INFO)
    log = logging.getLogger(__name__)
    log.info("Starting Spark Connect upload job...")

    try:
        spark = create_spark_session()

        log.info("Reading CSV file from local filesystem...")
        df = spark.read.option("header", "true").csv("s3a://airflow-test/10k_data.csv")
        df.show(5)
        row_count = df.count()
        log.info(f"Number of rows: {row_count}")

        log.info(f"Writing dataframe to S3 location {S3_PATH} ...")
        df.write.csv(S3_PATH, mode="overwrite")
        log.info("Write to S3 completed.")

        spark.stop()

    except Exception as e:
        log.error(f"Error running Spark Connect upload job: {e}", exc_info=True)
        raise

def delete_s3_path():
    logging.basicConfig(level=logging.INFO)
    log = logging.getLogger(__name__)
    log.info(f"Deleting S3 path: {S3_PATH}")

    try:
        spark = create_spark_session()

        # Delete path using S3 API or Hadoop FS command outside JVM
        # Since Spark Connect does not support JVM access,
        # use an alternative method like boto3 or minio client here
        import boto3
        from botocore.exceptions import ClientError

        # Configure boto3 client for MinIO
        s3_client = boto3.client(
            's3',
            endpoint_url="http://100.94.70.9:31677",
            aws_access_key_id="minio",
            aws_secret_access_key="minio123",
            region_name='us-east-1',
            config=boto3.session.Config(signature_version='s3v4'),
            use_ssl=False
        )

        # Extract bucket and key prefix from S3_PATH
        # S3_PATH example: "s3a://airflow-test/sparkconnect/"
        path_without_scheme = S3_PATH.replace("s3a://", "")
        bucket, prefix = path_without_scheme.split('/', 1)

        # List and delete all objects under the prefix
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

        objects_to_delete = []
        for page in pages:
            if "Contents" in page:
                for obj in page["Contents"]:
                    objects_to_delete.append({'Key': obj['Key']})

        if objects_to_delete:
            delete_response = s3_client.delete_objects(
                Bucket=bucket,
                Delete={'Objects': objects_to_delete}
            )
            log.info(f"Deleted objects under {S3_PATH}: {delete_response}")
        else:
            log.info(f"No objects found to delete at {S3_PATH}")

        spark.stop()

    except Exception as e:
        log.error(f"Error deleting S3 path: {e}", exc_info=True)
        raise


with DAG(
    dag_id='spark_connect_minio_test',
    default_args=default_args,
    description='Upload CSV to S3, sleep 20s, then delete using Spark Connect',
    schedule_interval=None,
    start_date=datetime(2025, 7, 11),
    catchup=False,
    tags=['pyspark', 's3', 'spark_connect'],
) as dag:

    upload_task = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3,
    )

    sleep_task = BashOperator(
        task_id='sleep_20_seconds',
        bash_command='sleep 20',
    )

    delete_task = PythonOperator(
        task_id='delete_s3_path',
        python_callable=delete_s3_path,
    )

    upload_task >> sleep_task >> delete_task
