from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime
import boto3
import requests
import time

# Constants
minio_endpoint = "http://100.94.70.9:31677"
minio_access_key = "minio"
minio_secret_key = "minio123"
s3_bucket = "airflow-test"
s3_key = "TrafficData.csv.gz"  # GZIP file

# Doris credentials from Airflow connection
conn = BaseHook.get_connection("doris_conn")
doris_user = conn.login
doris_password = conn.password
doris_host = conn.host
doris_port = conn.port

def stream_load_gz_file():
    start = time.time()

    # S3 (MinIO) client
    s3 = boto3.client(
        's3',
        endpoint_url=minio_endpoint,
        aws_access_key_id=minio_access_key,
        aws_secret_access_key=minio_secret_key,
    )

    # Confirm file exists
    try:
        s3.head_object(Bucket=s3_bucket, Key=s3_key)
    except s3.exceptions.ClientError as e:
        raise FileNotFoundError(f"{s3_key} not found in MinIO bucket '{s3_bucket}'")

    # Stream the file directly
    obj = s3.get_object(Bucket=s3_bucket, Key=s3_key)
    stream = obj['Body']

    # Doris stream load API endpoint
    url = f"http://{doris_host}:{doris_port}/api/test/table1/_stream_load"
    headers = {
        "Expect": "100-continue",
        "Content-Encoding": "gzip",  # Tell Doris it's gzipped
        "Content-Type": "application/octet-stream",
        "Transfer-Encoding": "chunked",  # Enable streaming
        "max_filter_ratio": "0.5",
        "label": f"stream_load_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}",
        "column_separator": ",",
        "columns": "CDRId,CDRVersion,CompanyIntID,CompanyName,InvoiceNumber,BusinessUnitLevel,BusinessUnit,BusinessUnitTAG,SharedBalanceUsed,DepartmentID,DepartmentName,CostCenterID,CostCenterName,AccountNumber,CustomerNumber,InvoicePeriod,TadigCode,GlobalTitle,MCC,MNC,Country,Operator,ProductId,MSISDN,IMSI,SIM,eUICCID,CallType,TrafficType,CallForwarding,DestinationName,DestinationType,CallingParty,CalledParty,APN,IPAddress,CallDate,CallTime,Duration,BillableDuration,Bytes,BalanceTypeID,ZoneID,Zone,TotalRetailCharge,WholesaleTAG,MappedIMSI,PropositionAssociated,CommercialOfferPropositionUsed,ChargeNumber,Threshold,ActualUsage,ZoneNameTo,RetailDuration,UsedId,UsedFrom,CELLID,UEIP,UsedType,BillCycleDay,UsedNumber,Device,IMEI,RatingGroupId,PlanName",
        "skip_header": "0",
    }

    response = requests.put(
        url,
        headers=headers,
        auth=(doris_user, doris_password),
        data=stream,  # Stream from S3 directly
        timeout=300
    )

    duration = time.time() - start
    print(f"Stream load completed in {duration:.2f} seconds.")

    if response.status_code != 200:
        raise Exception(f"Stream load failed: {response.status_code} - {response.text}")
    print("Stream load response:", response.text)

# Define the DAG
with DAG(
    dag_id='doris_streamload_gzip_from_minio',
    default_args={'owner': 'airflow', 'depends_on_past': False, 'retries': 0},
    schedule_interval=None,
    start_date=datetime(2025, 7, 14),
    catchup=False,
    tags=['doris', 'minio', 'gzip', 'stream_load']
) as dag:

    stream_load_task = PythonOperator(
        task_id='stream_load_gz_file_to_doris',
        python_callable=stream_load_gz_file,
    )
