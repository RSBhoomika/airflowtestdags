from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.hooks.base import BaseHook
from datetime import datetime
import os
import boto3

# Default DAG args
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0,
}

# Doris connection details
conn = BaseHook.get_connection("doris_conn")
doris_user = conn.login
doris_password = conn.password
doris_host = conn.host
doris_port = conn.port

minio_endpoint = "http://100.94.70.9:31677"   
minio_access_key = "minio"
minio_secret_key = "minio123"

# S3/MinIO bucket and object key
s3_bucket = "airflow-test"
s3_key = "TrafficData.csv"
local_file_path = "/opt/airflow/dags/repo/tests/TrafficData.csv"

# Download from MinIO if not exists
def download_if_not_exists():
    if os.path.exists(local_file_path):
        print(f"File {local_file_path} already exists, skipping download.")
        return
    
    print(f"Downloading s3://{s3_bucket}/{s3_key} from MinIO endpoint {minio_endpoint}")
    
    s3 = boto3.client(
        's3',
        endpoint_url=minio_endpoint,
        aws_access_key_id=minio_access_key,
        aws_secret_access_key=minio_secret_key
    )
    
    s3.download_file(s3_bucket, s3_key, local_file_path)
    print("Download complete.")

with DAG(
    dag_id='doris_streamload_one_million',
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2025, 7, 11),
    catchup=False,
    description='Download from MinIO once and stream load to Doris ',
    tags=['doris', 'minio', 'stream_load'],
) as dag:

    download_task = PythonOperator(
        task_id='download_from_minio_if_missing',
        python_callable=download_if_not_exists
    )

    stream_load_task = BashOperator(
        task_id='stream_load_to_doris',
        bash_command=f"""
        curl --location-trusted -u {doris_user}:{doris_password} -T {local_file_path} \\
          -H "Expect: 100-continue" \\
          -H "max_filter_ratio: 0.1" \\
          -H "label: stream_load_{{{{ ts_nodash }}}}" \\
          -H "column_separator: ," \\
          -H "columns: CDRId,CDRVersion,CompanyIntID,CompanyName,InvoiceNumber,BusinessUnitLevel,BusinessUnit,BusinessUnitTAG,SharedBalanceUsed,DepartmentID,DepartmentName,CostCenterID,CostCenterName,AccountNumber,CustomerNumber,InvoicePeriod,TadigCode,GlobalTitle,MCC,MNC,Country,Operator,ProductId,MSISDN,IMSI,SIM,eUICCID,CallType,TrafficType,CallForwarding,DestinationName,DestinationType,CallingParty,CalledParty,APN,IPAddress,CallDate,CallTime,Duration,BillableDuration,Bytes,BalanceTypeID,ZoneID,Zone,TotalRetailCharge,WholesaleTAG,MappedIMSI,PropositionAssociated,CommercialOfferPropositionUsed,ChargeNumber,Threshold,ActualUsage,ZoneNameTo,RetailDuration,UsedId,UsedFrom,CELLID,UEIP,UsedType,BillCycleDay,UsedNumber,Device,IMEI,RatingGroupId,PlanName" \\
          -H "skip_header: 0" \\
          http://{doris_host}:{doris_port}/api/test/table1/_stream_load
        """
    )

    download_task >> stream_load_task
