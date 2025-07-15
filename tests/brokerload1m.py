from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import time
import pymysql

def load_data(label, **kwargs):
    conn = pymysql.connect(
        host="100.94.70.9",
        port=31115,
        user="test_user",
        password="password",
        database="test"
    )
    cursor = conn.cursor()
    query = f"""
    LOAD LABEL test.{label} (
        DATA INFILE('s3://one-million-data/TrafficData.csv')
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
    try:
        result = cursor.fetchall()
    except Exception:
        result = None
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Execution time: {execution_time:.6f} seconds")
    cursor.close()
    conn.close()

def wait_20_seconds():
    print("Waiting for 20 seconds...")
    time.sleep(20)

def check_load_status(label, **kwargs):
    conn = pymysql.connect(
        host="100.94.70.9",
        port=31115,
        user="test_user",
        password="password",
        database="test"
    )
    cursor = conn.cursor()
    query = f"show load from test where LABEL ='{label}';"
    print(f"Executing query: {query}")
    cursor.execute(query)
    result = cursor.fetchall()
    print(f"Query Result: {result}")
    cursor.close()
    conn.close()

default_args = {
    'start_date': datetime(2024, 1, 1),
}

dag = DAG(
    'doris_broker_load_1m',
    default_args=default_args,
    schedule_interval=None,  # Run on demand
    catchup=False,
    description='Load data into StarRocks from S3 and check status',
)

unique_label = '{{ ts_nodash }}'

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    op_kwargs={'label': unique_label},
    dag=dag,
)

wait_task = PythonOperator(
    task_id='wait_20_seconds',
    python_callable=wait_20_seconds,
    dag=dag,
)

check_status_task = PythonOperator(
    task_id='check_load_status',
    python_callable=check_load_status,
    op_kwargs={'label': unique_label},
    dag=dag,
)

load_task >> wait_task >> check_status_task 
