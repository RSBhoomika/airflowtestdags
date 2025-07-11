from airflow import DAG
from airflow.operators.python import PythonOperator
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

def run_pyspark_job():
    logging.basicConfig(level=logging.INFO)
    log = logging.getLogger(__name__)
    log.info("Starting Spark job...")

    # Set JAVA_HOME environment for PySpark
    os.environ['JAVA_HOME'] = '/opt/java/openjdk'
    os.environ['PATH'] = f"{os.environ['JAVA_HOME']}/bin:" + os.environ['PATH']
    log.info(f"JAVA_HOME set to: {os.environ['JAVA_HOME']}")

    try:
        conf = SparkConf()
        log.info("Initializing SparkSession...")
        spark = SparkSession.builder.config(conf=conf) \
        .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.4') \
        .config("spark.hadoop.fs.s3a.access.key","root") \
        .config("spark.hadoop.fs.s3a.secret.key", "tatacomm") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://100.94.70.9:30978") \
        .config("spark.network.timeout", "60s") \
        .config("spark.hadoop.fs.s3a.connection.timeout", "60000") \
        .config("spark.hadoop.fs.s3a.attempts.maximum", "3") \
        .config("spark.hadoop.fs.s3a.retry.interval", "1000ms") \
        .getOrCreate()
    
        hadoop_conf = spark._jsc.hadoopConfiguration()
        hadoop_conf.set("mapreduce.fileoutputcommitter.algorithm.version", "2")
        hadoop_conf.set("fs.s3a.committer.staging.conflict-mode", "replace")

        # Read CSV from local filesystem
        log.info("Reading CSV file from local filesystem...")
        #df = spark.read.option("header", "true").csv("/opt/airflow/dags/TrafficData.csv")
        df = spark.read.option("header", "true").csv("/opt/airflow/dags/repo/tests/10k_data.csv")
        #df.show()

        # Write to S3
        log.info("Writing dataframe to S3 location s3a://airflow-test/1mdata/ ...")
        df.write.csv("s3a://airflow-test/1mdata/", mode="overwrite")
        log.info("Write to S3 completed.")
        spark.stop()
        #log.info("Spark job completed successfully.")

    except Exception as e:
        log.error(f"Error running Spark job: {e}", exc_info=True)
        raise

with DAG(
    dag_id='pyspark_s3_inline_dag',
    default_args=default_args,
    description='Run PySpark job inline in Airflow and write to S3',
    schedule_interval=None,
    #schedule_interval='*/5 * * * *', 
    start_date=datetime(2025, 7, 11),
    end_date=datetime(2025, 7, 14), 
    catchup=False,
    tags=['pyspark', 's3', 'inline'],
) as dag:

    spark_task = PythonOperator(
        task_id='run_spark_s3_job',
        python_callable=run_pyspark_job
    )

    spark_task
