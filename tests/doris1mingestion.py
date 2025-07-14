import boto3
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime

minio_endpoint = "http://100.94.70.9:31677"
minio_access_key = "minio"
minio_secret_key = "minio123"
s3_bucket = "airflow-test"
s3_key = "TrafficData.csv"

conn = BaseHook.get_connection("doris_conn")
doris_user = conn.login
doris_password = conn.password
doris_host = conn.host
doris_port = conn.port

def stream_load_minio_to_doris():
    s3 = boto3.client(
        's3',
        endpoint_url=minio_endpoint,
        aws_access_key_id=minio_access_key,
        aws_secret_access_key=minio_secret_key,
    )
    
    # Get streaming body of the object (do NOT read fully)
    s3_obj = s3.get_object(Bucket=s3_bucket, Key=s3_key)
    stream_body = s3_obj['Body']
    
    url = f"http://{doris_host}:{doris_port}/api/test/table1/_stream_load"
    headers = {
        "Expect": "100-continue",
        "max_filter_ratio": "0.1",
        "label": f"stream_load_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}",
        "column_separator": ",",
        "columns": "CDRId,CDRVersion,CompanyIntID,CompanyName,InvoiceNumber,BusinessUnitLevel,BusinessUnit,BusinessUnitTAG,SharedBalanceUsed,DepartmentID,DepartmentName,CostCenterID,CostCenterName,AccountNumber,CustomerNumber,InvoicePeriod,TadigCode,GlobalTitle,MCC,MNC,Country,Operator,ProductId,MSISDN,IMSI,SIM,eUICCID,CallType,TrafficType,CallForwarding,DestinationName,DestinationType,CallingParty,CalledParty,APN,IPAddress,CallDate,CallTime,Duration,BillableDuration,Bytes,BalanceTypeID,ZoneID,Zone,TotalRetailCharge,WholesaleTAG,MappedIMSI,PropositionAssociated,CommercialOfferPropositionUsed,ChargeNumber,Threshold,ActualUsage,ZoneNameTo,RetailDuration,UsedId,UsedFrom,CELLID,UEIP,UsedType,BillCycleDay,UsedNumber,Device,IMEI,RatingGroupId,PlanName",
        "skip_header": "0",
    }
    
    # Use streaming upload, passing the boto3 stream as data
    response = requests.put(
        url,
        headers=headers,
        auth=(doris_user, doris_password),
        data=stream_body,   # stream_body supports .read()
        timeout=300,
        stream=True
    )
    
    if response.status_code != 200:
        raise Exception(f"Stream load failed: {response.text}")
    print("Stream load successful.")

with DAG(
    dag_id='doris_streamload_direct_stream',
    default_args={'owner': 'airflow', 'depends_on_past': False, 'retries': 0},
    schedule_interval=None,
    start_date=datetime(2025, 7, 11),
    catchup=False,
    tags=['doris', 'minio', 'stream_load']
) as dag:

    task = PythonOperator(
        task_id='stream_load_direct_from_minio',
        python_callable=stream_load_minio_to_doris,
    )
