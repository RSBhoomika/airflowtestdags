from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
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

S3_SOURCE_PATH = "s3a://vaz-source-data/nspire/repository/feed_aggs/"
S3_DESTINATION_PATH = "s3a://vaz/nspire/feed_aggs/"


def create_spark_session():
    os.environ['JAVA_HOME'] = '/opt/java/openjdk'
    os.environ['PATH'] = f"{os.environ['JAVA_HOME']}/bin:" + os.environ['PATH']

    conf = SparkConf().setAppName("AirflowPySparkJob")
    spark = SparkSession.builder.config(conf=conf) \
        .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.4,org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.3.0') \
        .config("spark.hadoop.fs.s3a.access.key", "minio") \
        .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://100.94.70.9:31677") \
        .config("spark.network.timeout", "60s") \
        .config("spark.hadoop.fs.s3a.connection.timeout", "60000") \
        .config("spark.hadoop.fs.s3a.attempts.maximum", "3") \
        .config("spark.hadoop.fs.s3a.retry.interval", "1000ms") \
        .config("spark.sql.catalog.nspire_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.nspire_catalog.type", "hadoop") \
        .config("spark.sql.catalog.nspire_catalog.warehouse", "s3a://vaz/nspire/") \
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

        

        log.info(f"Writing dataframe to Iceberg table nspire_catalog.feed_aggs ...")
        from pyspark.sql.types import DoubleType, IntegerType
        # Add missing columns from table schema if not present in DataFrame
        table_columns = [
            "datetime", "global_cell_id", "cell_latitude", "cell_longitude", "rx_qual", "rx_lev", "serving_rsrp", "serving_rsrq", "latency", "qos_rating", "id", "createTime"
        ]
        for col in table_columns:
            if col not in df.columns:
                df = df.withColumn(col, F.lit(None))
        # Only keep columns in table schema, in order
        df = df.select(*table_columns)
        # Add id and createTime
        df = df.withColumn('id', F.expr("uuid()"))
        df = df.withColumn('createTime', F.lit(datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')))
        # Cast columns to match Iceberg schema
        df = df.withColumn("cell_latitude", df["cell_latitude"].cast(DoubleType()))
        df = df.withColumn("cell_longitude", df["cell_longitude"].cast(DoubleType()))
        df = df.withColumn("rx_qual", df["rx_qual"].cast(IntegerType()))
        df = df.withColumn("rx_lev", df["rx_lev"].cast(IntegerType()))
        df = df.withColumn("serving_rsrp", df["serving_rsrp"].cast(IntegerType()))
        df = df.withColumn("serving_rsrq", df["serving_rsrq"].cast(IntegerType()))
        df = df.withColumn("latency", df["latency"].cast(IntegerType()))
        df = df.withColumn("qos_rating", df["qos_rating"].cast(IntegerType()))
        start_time = time.time()
        df.writeTo("nspire_catalog.feed_aggs").overwritePartitions()
        end_time = time.time()
        print("Timetaken to write: ", end_time - start_time, "seconds")
        log.info("Write to Iceberg table completed.")

        spark.stop()

    except Exception as e:
        log.error(f"Error running Spark upload job: {e}", exc_info=True)
        raise

with DAG(
    dag_id='nspire-feed-aggs-ingestion-dag',
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
