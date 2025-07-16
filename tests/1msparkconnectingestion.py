from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
import logging
import os
import time

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
    .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.4') \
    .config("spark.remote","sc://100.94.70.9:30816")\
    .config("spark.hadoop.fs.s3a.access.key","minio") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://100.94.70.9:31677") \
    .config("mapreduce.fileoutputcommitter.algorithm.version", "2") \
    .config("fs.s3a.committer.staging.conflict-mode", "replace") \
    .config("fs.s3a.fast.upload", "true") \
    .config("fs.s3a.fast.upload.buffer", "disk") \
    .config("fs.s3a.threads.max", "20") \
    .config("fs.s3a.multipart.size", "512M") \
    .config("fs.s3a.committer.threads", "20") \
    .config("spark.sql.shuffle.partitions", "400") \
    .config("spark.default.parallelism", "400") \
    .getOrCreate()

    return spark

def upload_to_s3():
    logging.basicConfig(level=logging.INFO)
    log = logging.getLogger(__name__)
    log.info("Starting Spark Connect upload job...")

    try:
        spark = create_spark_session()

        log.info("Reading CSV file from local filesystem...")
        #rstart_time = time.time()
        df = spark.read.option("header", "true").csv("s3a://airflow-test/TrafficData.csv")
        #rend_time = time.time()
        #print("Timetaken to read: ", rend_time - rstart_time, "seconds")
        #df = spark.read.option("header", "true").csv("/tmp/10k-data.csv")
        #wstart_time = time.time()
        df.write.mode("overwrite").format("parquet").save(S3_PATH)
        #wend_time = time.time()
        #df.write.csv(S3_PATH, mode="overwrite")
        log.info("Write to S3 completed.")
        #print("Timetaken to write: ", wend_time - wstart_time, "seconds")

        spark.stop()

    except Exception as e:
        log.error(f"Error running Spark Connect upload job: {e}", exc_info=True)
        raise

def count_rows():
    logging.basicConfig(level=logging.INFO)
    log = logging.getLogger(__name__)
    log.info("Starting Spark Connect count rows job...")

    spark = create_spark_session()
    try:
        #log.info(f"Reading parquet data from {S3_PATH}...")
        df = spark.read.parquet(S3_PATH)
        #log.info(f"Read parquet completed in {read_time:.2f} seconds")

        row_count = df.count()
        log.info(f"Row count: {row_count}")

    finally:
        spark.stop()

with DAG(
    dag_id='spark_connect_minio_ingestion_1mil',
    default_args=default_args,
    description='Upload CSV to S3, sleep 20s, then delete using Spark Connect',
    schedule_interval=None,
    #schedule_interval='*/5 * * * *',
    start_date=datetime(2025, 7, 16),
    catchup=False,
    tags=['pyspark', 's3', 'spark_connect'],
) as dag:

    upload_task = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3,
    )

    count_rows_task = PythonOperator(
        task_id='count_rows',
        python_callable=count_rows,
    )
    

    upload_task >> count_rows_task
