from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark import SparkConf
import os
import logging
import time

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 7, 18),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

S3_PATH = "s3a://regression-dump/one_million/"

def create_spark_session():
    os.environ['JAVA_HOME'] = '/opt/java/openjdk'
    os.environ['PATH'] = f"{os.environ['JAVA_HOME']}/bin:" + os.environ['PATH']

    conf = SparkConf().setAppName("AirflowPySparkJob")
    spark = SparkSession.builder.config(conf=conf) \
    .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.4,org.apache.iceberg:iceberg-spark:1.7.1,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.1')\
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog") \
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
    .config("spark.sql.catalog.spark_catalog.type", "hadoop") \
    .config("spark.sql.catalog.spark_catalog.warehouse", "s3a://regression-dump/") \
    .getOrCreate()

    hadoop_conf = spark._jsc.hadoopConfiguration()
    hadoop_conf.set("mapreduce.fileoutputcommitter.algorithm.version", "2")
    hadoop_conf.set("fs.s3a.committer.staging.conflict-mode", "replace")

    return spark

def upload_to_s3():
    logging.basicConfig(level=logging.INFO)
    log = logging.getLogger(__name__)
    log.info("Starting Spark Iceberg write job...")

    try:
        spark = create_spark_session()

        log.info("Creating Iceberg table if not exists...")
        spark.sql("""
        CREATE TABLE IF NOT EXISTS spark_catalog.one_million.one_million_data (
            CDRId STRING,
            CDRVersion INT,
            CompanyIntID INT,
            CompanyName STRING,
            InvoiceNumber STRING,
            BusinessUnitLevel INT,
            BusinessUnit STRING,
            BusinessUnitTAG STRING,
            SharedBalanceUsed DOUBLE,
            DepartmentID STRING,
            DepartmentName STRING,
            CostCenterID STRING,
            CostCenterName STRING,
            AccountNumber DECIMAL(38,0),
            CustomerNumber DECIMAL(38,0),
            InvoicePeriod STRING,
            TadigCode STRING,
            GlobalTitle STRING,
            MCC INT,
            MNC INT,
            Country STRING,
            Operator STRING,
            ProductId STRING,
            MSISDN DECIMAL(38,0),
            IMSI DECIMAL(38,0),
            SIM DECIMAL(38,0),
            eUICCID DECIMAL(38,0),
            CallType STRING,
            TrafficType STRING,
            CallForwarding INT,
            DestinationName STRING,
            DestinationType STRING,
            CallingParty DECIMAL(38,0),
            CalledParty STRING,
            APN STRING,
            IPAddress STRING,
            CallDate DATE,
            CallTime STRING,
            Duration INT,
            BillableDuration INT,
            Bytes BIGINT,
            BalanceTypeID INT,
            ZoneID INT,
            Zone STRING,
            TotalRetailCharge DECIMAL(38,20),
            WholesaleTAG STRING,
            MappedIMSI BIGINT,
            PropositionAssociated STRING,
            CommercialOfferPropositionUsed STRING,
            ChargeNumber INT,
            Threshold INT,
            ActualUsage DECIMAL(38,0),
            ZoneNameTo STRING,
            RetailDuration INT,
            UsedId STRING,
            UsedFrom STRING,
            CELLID STRING,
            UEIP STRING,
            UsedType STRING,
            BillCycleDay INT,
            UsedNumber DECIMAL(38,0),
            Device STRING,
            IMEI STRING,
            RatingGroupId INT,
            PlanName STRING
        )
        USING iceberg
        """).show()

        log.info("Reading CSV files from S3...")
        df = spark.read.option("header", "true").csv("s3a://airflow-test/TrafficData.csv")
        df.show(5)

        log.info("Writing DataFrame to Iceberg table...")
        start_time = time.time()
        df.writeTo("spark_catalog.one_million.one_million_data").using("iceberg").createOrReplace()
        end_time = time.time()
        log.info(f"Time taken to write Iceberg table: {end_time - start_time:.2f} seconds")

        log.info("Reading data back from Iceberg table...")
        hudi_table = spark.read \
            .format("iceberg") \
            .load("s3a://regression-dump/one_million/one_million_data/")
        hudi_table.show()

        count_df = spark.sql("SELECT count(*) FROM spark_catalog.one_million.one_million_data")
        count_df.show()

        spark.stop()

    except Exception as e:
        log.error(f"Error running Spark Iceberg job: {e}", exc_info=True)
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
    dag_id='pyspark_minio_upload_sleep_delete_500k',
    default_args=default_args,
    description='Upload CSV to S3, sleep 20s, then delete',
    schedule_interval='*/5 * * * *',
    start_date=datetime(2025, 7, 18),
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
