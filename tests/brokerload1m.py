from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import pymysql
import time

def load_data():
    conn = pymysql.connect(
        host="100.94.70.9",
        port=31115,
        user="test_user",
        password="password",
        database="test"
    )
    cursor = conn.cursor()
    query = """
    LOAD LABEL test.table7777 (
        DATA INFILE('s3://airflow-test/TrafficData.csv')
        INTO TABLE table2
        COLUMNS TERMINATED BY ','
        (CDRId, CDRVersion, CompanyIntID, CompanyName, InvoiceNumber, BusinessUnitLevel, BusinessUnit, BusinessUnitTAG, SharedBalanceUsed,
        DepartmentID, DepartmentName, CostCenterID, CostCenterName, AccountNumber, CustomerNumber, InvoicePeriod, TadigCode, GlobalTitle,
        MCC, MNC, Country, Operator, ProductId, MSISDN, IMSI, SIM, eUICCID, CallType, TrafficType, CallForwarding, DestinationName,
        DestinationType, CallingParty, CalledParty, APN, IPAddress, CallDate, CallTime, Duration, BillableDuration, Bytes, BalanceTypeID,
        ZoneID, Zone, TotalRetailCharge, WholesaleTAG, MappedIMSI, PropositionAssociated, CommercialOfferPropositionUsed, ChargeNumber,
        Threshold, ActualUsage, ZoneNameTo, RetailDuration, UsedId, UsedFrom, CELLID, UEIP, UsedType, BillCycleDay, UsedNumber, Device,
        IMEI, RatingGroupId, PlanName)
    )
    WITH S3 (
        "s3.endpoint" = "http://100.94.70.9:31677",
        "s3.access_key" = "minio",
        "s3.secret_key" = "minio123",
        "format" = "csv",
        "s3.use_aws_sdk_default_behavior" = "false",
        "s3.region" = "us-east-1",
        "enable_ssl" = "false",
        "use_instance_profile" = "false",
        "use_path_style_access" = "true",
        "s3.multi_part_size" = "512MB",
        "s3.request_timeout_ms" = "120000"
    )
    PROPERTIES (
        "timeout" = "21600",
        "max_filter_ratio" = "0.1",
        "strict_mode" = "false",
        "load_parallelism" = "16",
        "send_batch_parallelism" = "4",
        "skip_lines" = "0",
        "priority" = "HIGH"
    );
    """
    print(f"Executing query: {query}")
    start_time = time.time()
    cursor.execute(query)
    result = cursor.fetchall()
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Execution time: {execution_time:.6f} seconds")
    time.sleep(20)  # Wait for 20 seconds
    cursor.close()
    conn.close()

def check_load():
    conn = pymysql.connect(
        host="100.94.70.9",
        port=31115,
        user="test_user",
        password="password",
        database="test"
    )
    cursor = conn.cursor()
    query_delete = """show load from test where LABEL ='table7777';;"""
    print(f"Executing query: {query_delete}")
    cursor.execute(query_delete)
    result_load = cursor.fetchall()
    print(f"Query Result: {result_load}")
    cursor.close()
    conn.close()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'doris_broker_load_1m',
    default_args=default_args,
    description='Load data into StarRocks and check status',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag,
)

check_task = PythonOperator(
    task_id='check_load',
    python_callable=check_load,
    dag=dag,
)

sleep_task = BashOperator(
      task_id='sleep_20_seconds',
      bash_command='sleep 7',
    )

load_task >> sleep_task >> check_task 
