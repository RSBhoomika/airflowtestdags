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
    'retry_delay': timedelta(seconds=30),  # Reduced from 1 minute
}

def upload_to_s3():
    """Upload data to S3"""
    logging.basicConfig(level=logging.INFO)
    log = logging.getLogger(__name__)
    log.info("Starting Spark upload job...")

    # Set JAVA_HOME environment for PySpark
    os.environ['JAVA_HOME'] = '/opt/java/openjdk'
    os.environ['PATH'] = f"{os.environ['JAVA_HOME']}/bin:" + os.environ['PATH']

    try:
        conf = SparkConf()
        log.info("Initializing SparkSession...")
        spark = SparkSession.builder.config(conf=conf) \
        .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.4') \
        .config("spark.hadoop.fs.s3a.access.key","minio") \
        .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://100.94.70.9:31677") \
        .config("spark.network.timeout", "30s") \
        .config("spark.hadoop.fs.s3a.connection.timeout", "30000") \
        .config("spark.hadoop.fs.s3a.attempts.maximum", "2") \
        .config("spark.hadoop.fs.s3a.retry.interval", "500ms") \
        .getOrCreate()
    
        hadoop_conf = spark._jsc.hadoopConfiguration()
        hadoop_conf.set("mapreduce.fileoutputcommitter.algorithm.version", "2")
        hadoop_conf.set("fs.s3a.committer.staging.conflict-mode", "replace")

        # Read CSV from local filesystem
        log.info("Reading and writing CSV to S3...")
        df = spark.read.option("header", "true").csv("/opt/airflow/dags/repo/tests/10k_data.csv")
        
        # Write to S3 (removed show() to save time)
        output_path = "s3a://airflow-test/1mdata/"
        df.write.csv(output_path, mode="overwrite")
        log.info("Upload completed.")
        
        spark.stop()

    except Exception as e:
        log.error(f"Error running Spark upload job: {e}", exc_info=True)
        raise

def delete_from_s3():
    """Delete data from S3"""
    logging.basicConfig(level=logging.INFO)
    log = logging.getLogger(__name__)
    log.info("Starting S3 deletion...")

    # Set JAVA_HOME environment for PySpark
    os.environ['JAVA_HOME'] = '/opt/java/openjdk'
    os.environ['PATH'] = f"{os.environ['JAVA_HOME']}/bin:" + os.environ['PATH']

    try:
        conf = SparkConf()
        spark = SparkSession.builder.config(conf=conf) \
        .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.4') \
        .config("spark.hadoop.fs.s3a.access.key","minio") \
        .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://100.94.70.9:31677") \
        .config("spark.network.timeout", "30s") \
        .config("spark.hadoop.fs.s3a.connection.timeout", "30000") \
        .config("spark.hadoop.fs.s3a.attempts.maximum", "2") \
        .config("spark.hadoop.fs.s3a.retry.interval", "500ms") \
        .getOrCreate()

        # Delete from S3
        output_path = "s3a://airflow-test/1mdata/"
        hadoop_conf = spark._jsc.hadoopConfiguration()
        uri = spark._jvm.java.net.URI(output_path)
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(uri, hadoop_conf)
        path = spark._jvm.org.apache.hadoop.fs.Path(output_path)
        
        if fs.exists(path):
            fs.delete(path, True)
            log.info(f"Deleted: {output_path}")
        else:
            log.info("Path not found")
            
        spark.stop()

    except Exception as e:
        log.error(f"Error in deletion: {e}", exc_info=True)
        raise

with DAG(
    dag_id='pyspark_s3_inline_dag',
    default_args=default_args,
    description='Run PySpark job inline in Airflow and write to S3',
    schedule_interval=None,
    start_date=datetime(2025, 7, 11),
    end_date=datetime(2025, 7, 14), 
    catchup=False,
    tags=['pyspark', 's3', 'inline'],
) as dag:

    upload_task = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3
    )

    sleep_task = BashOperator(
        task_id='sleep_5_seconds',
        bash_command='sleep 5',
    )

    delete_task = PythonOperator(
        task_id='delete_from_s3',
        python_callable=delete_from_s3
    )

    upload_task >> sleep_task >> delete_task
