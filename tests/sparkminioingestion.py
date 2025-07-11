from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark import SparkConf
import os
import logging

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 7, 11),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

S3_PATH = "s3a://airflow-test/1mdata/"

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

        log.info("Reading CSV file from local filesystem...")
        df = spark.read.option("header", "true").csv("/opt/airflow/dags/repo/tests/10k_data.csv")
        df.show(5)
        row_count = df.count()
        log.info(f"Number of rows: {row_count}")
        log.info(f"Writing dataframe to S3 location {S3_PATH} ...")
        df.write.csv(S3_PATH, mode="overwrite")
        log.info("Write to S3 completed.")

        spark.stop()

    except Exception as e:
        log.error(f"Error running Spark upload job: {e}", exc_info=True)
        raise

def delete_s3_path():
    logging.basicConfig(level=logging.INFO)
    log = logging.getLogger(__name__)
    log.info(f"Deleting S3 path: {S3_PATH}")

    try:
        spark = create_spark_session()

        hadoop_conf = spark._jsc.hadoopConfiguration()
        uri = spark._jvm.java.net.URI(S3_PATH)
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(uri, hadoop_conf)
        path = spark._jvm.org.apache.hadoop.fs.Path(S3_PATH)

        if fs.exists(path):
            fs.delete(path, True)  # recursive delete
            log.info(f"Deleted S3 path: {S3_PATH}")
        else:
            log.info(f"Path does not exist: {S3_PATH}")

        spark.stop()

    except Exception as e:
        log.error(f"Error deleting S3 path: {e}", exc_info=True)
        raise


with DAG(
    dag_id='pyspark_s3_upload_sleep_delete',
    default_args=default_args,
    description='Upload CSV to S3, sleep 20s, then delete',
    schedule_interval='*/5 * * * *',
    start_date=datetime(2025, 7, 11),
    catchup=False,
    tags=['pyspark', 's3'],
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
