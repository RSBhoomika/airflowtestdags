from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark import SparkConf
import os
import logging
import uuid
import time

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 7, 11),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

S3_SOURCE_PATH = "s3a://vaz-source-data/nspire/input_files/"
S3_DESTINATION_PATH = "s3a://vaz/nspire/sessions/"


def create_spark_session():
    os.environ['JAVA_HOME'] = '/opt/java/openjdk'
    os.environ['PATH'] = f"{os.environ['JAVA_HOME']}/bin:" + os.environ['PATH']

    conf = SparkConf().setAppName("AirflowPySparkJob")
    spark = SparkSession.builder.config(conf=conf) \
        .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.4') \
        .config("spark.hadoop.fs.s3a.access.key", "minio") \
        .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://100.94.70.9:31677") \
        .config("spark.network.timeout", "60s") \
        .config("spark.hadoop.fs.s3a.connection.timeout", "60000") \
        .config("spark.hadoop.fs.s3a.attempts.maximum", "3") \
        .config("spark.hadoop.fs.s3a.retry.interval", "1000ms") \
        .getOrCreate()

    hadoop_conf = spark._jsc.hadoopConfiguration()
    hadoop_conf.set("mapreduce.fileoutputcommitter.algorithm.version", "2")
    hadoop_conf.set("fs.s3a.committer.staging.conflict-mode", "replace")

    return spark

def upload_to_s3():
    logging.basicConfig(level=logging.INFO)
    log = logging.getLogger(__name__)
    log.info("Starting Spark upload job...")

    try:
        spark = create_spark_session()

        log.info(f"Reading all gzipped CSV files from S3 path: {S3_SOURCE_PATH}*.gz ...")
        df = spark.read.option("header", "true").csv(f"{S3_SOURCE_PATH}*.gz")
        df.show(5)
        row_count = df.count()
        log.info(f"Number of rows: {row_count}")

        
        log.info(f"Writing dataframe to S3 location {S3_DESTINATION_PATH} as Parquet ...")
        df['id'] = [str(uuid.uuid4()) for _ in range(len(df))]
        #df['file'] = filename
        df['createTime'] = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        start_time = time.time()
        df.write.mode("overwrite").format("parquet").save(S3_DESTINATION_PATH)
        end_time = time.time()
        print("Timetaken to write: ", end_time - start_time, "seconds")
        # df.write.parquet(S3_DESTINATION_PATH, mode="overwrite")
        log.info("Write to S3 completed.")

        spark.stop()

    except Exception as e:
        log.error(f"Error running Spark upload job: {e}", exc_info=True)
        raise

with DAG(
    dag_id='nspire-session-ingestion-dag',
    default_args=default_args,
    description='Upload CSV to S3, sleep 20s, then delete',
    schedule_interval=None,
    start_date=datetime(2025, 7, 11),
    catchup=False,
    tags=['pyspark', 's3'],
) as dag:

    upload_nspire_sessions_task = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3,
    )

    upload_nspire_sessions_task
