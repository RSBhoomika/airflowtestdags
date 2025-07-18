from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
import os
import logging
import uuid
import tempfile
import requests
from requests.auth import HTTPBasicAuth
import pandas as pd
from pyspark.sql import SparkSession
from pyspark import SparkConf

doris_user = "test_user"
doris_password = "password"
doris_host = "100.94.70.9"
doris_port = "31161"
doris_db = "nspire"

def get_doris_conn():
    return {
        "fe_host": doris_host,
        "fe_port": doris_port,
        "user": doris_user,
        "password": doris_password,
        "database": doris_db
    }

# Doris schema columns (from dag1.py)
DORIS_COLUMNS = [
    "id", "startDateTime", "date", "hour", "endDateTime", "technology", "global_cell_id", "mcc", "mnc",
    "lac", "cell_id", "tac", "eci", "cell_latitude", "cell_longitude",
    "ecno", "rscp", "rsrp", "rsrq", "rx_qual", "rx_lev",
    "ecno_max", "ecno_min", "ecno_stdev",
    "rscp_max", "rscp_min", "rscp_stdev",
    "rsrp_max", "rsrp_min", "rsrp_stdev",
    "rsrq_max", "rsrq_min", "rsrq_stdev",
    "rx_qual_max", "rx_qual_min", "rx_qual_stdev",
    "rx_lev_max", "rx_lev_min", "rx_lev_stdev",
    "average_latency", "download_speed", "upload_speed",
    "Number_of_Rows_Aggregated", "feed_qos_rating", "createTime", "file"
]

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 7, 11),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

FEED_INDIVIDUAL_PATH = "s3a://vaz/nspire/feed_individual/"
FEED_AGGS_PATH = "s3a://vaz/nspire/feed_aggs/"
DORIS_TABLE = "sessions"
DORIS_CONN_ID = "doris_default"

# def get_doris_conn():
#     conn = BaseHook.get_connection(DORIS_CONN_ID)
#     host = conn.host
#     port = conn.port or 8030
#     user = conn.login
#     password = conn.password
#     schema = conn.schema or "nspire"
#     return {
#         "fe_host": host,
#         "fe_port": port,
#         "user": user,
#         "password": password,
#         "database": schema
#     }

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
        .getOrCreate()
    return spark

def stream_load_to_doris(conn_info, table_name, csv_file_path):
    load_url = f"http://{conn_info['fe_host']}:{conn_info['fe_port']}/api/{conn_info['database']}/{table_name}/_stream_load"
    headers = {
        "label": f"airflow_load_{uuid.uuid4()}",
        "format": "csv",
        "strip_outer_array": "false",
        "column_separator": ",",
        "columns": ','.join(DORIS_COLUMNS),
        "Expect": "100-continue"
    }
    logging.info(f"conn_info: {conn_info}")
    logging.info(f"csv_file_path: {csv_file_path}")
    logging.info(f"🔄 Starting stream load to Doris: {load_url} for table {table_name}")
    logging.info(f"headers: {headers}")
    auth=HTTPBasicAuth(conn_info["user"], conn_info["password"])
    with open(csv_file_path, 'rb') as f:
        response = requests.put(
            load_url,
            headers=headers,
            data=f,
            auth=auth
        )
    logging.info(f"Response status: {response.status_code}")
    logging.info(f"Response body: {response.text}")
    if response.status_code == 401:
        raise Exception("Unauthorized: Check your Doris credentials or authentication method.")
    if response.status_code != 200 or 'Success' not in response.text:
        raise Exception(f"Stream load failed: {response.text}")

def join_and_ingest_to_doris():
    logging.basicConfig(level=logging.INFO)
    log = logging.getLogger(__name__)
    log.info("Starting Spark join and Doris ingestion job...")
    try:
        spark = create_spark_session()
        feed_individual = spark.read.format("iceberg").load(FEED_INDIVIDUAL_PATH)
        feed_aggs = spark.read.format("iceberg").load(FEED_AGGS_PATH)
        log.info("Read feed_individual and feed_aggs tables.")
        joined = feed_individual.join(feed_aggs, on="global_cell_id", how="inner")
        log.info(f"Joined DataFrame count: {joined.count()}")
        # Convert to Pandas DataFrame for stream load
        pandas_df = joined.toPandas()
        # Add/format columns as per DORIS_COLUMNS
        pandas_df['id'] = [str(uuid.uuid4()) for _ in range(len(pandas_df))]
        pandas_df['createTime'] = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        pandas_df['file'] = 'spark_joined_ingest.csv'
        out_df = pandas_df.reindex(columns=DORIS_COLUMNS)
        temp_csv_path = tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix="_mod.csv").name
        out_df.to_csv(temp_csv_path, index=False)
        conn_info = get_doris_conn()
        stream_load_to_doris(conn_info, DORIS_TABLE, temp_csv_path)
        os.remove(temp_csv_path)
        log.info("Write to Doris completed.")
        spark.stop()
    except Exception as e:
        log.error(f"Error in Spark Doris ingestion job: {e}", exc_info=True)
        raise

with DAG(
    dag_id='nspire-sessions-dag',
    default_args=default_args,
    description='Join feed_individual and feed_aggs on global_cell_id and ingest to Doris',
    schedule_interval=None,
    start_date=datetime(2025, 7, 11),
    catchup=False,
    tags=['pyspark', 's3', 'doris'],
) as dag:
    join_and_ingest_task = PythonOperator(
        task_id='join_and_ingest_to_doris',
        python_callable=join_and_ingest_to_doris,
    )
    join_and_ingest_task
