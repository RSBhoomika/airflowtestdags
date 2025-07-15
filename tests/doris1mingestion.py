from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.hooks.base import BaseHook
from datetime import datetime

import mysql.connector

# Default DAG arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0,
}

# Get Doris (StarRocks) connection details from Airflow connection
# conn = BaseHook.get_connection("doris_default")
# doris_user = conn.login
# doris_password = conn.password
# doris_host = conn.host
# doris_port = conn.port
doris_user = "test_user"
doris_password = "password"
doris_host = "100.94.70.9"
doris_port = "31161"

# Target table and file details
db_name = "test"
table_name = "table2"
#file_path = "/opt/airflow/dags/repo/tests/test-one-million-data.csv.gz"
file_path = "/opt/airflow/dags/repo/tests/one-million-data.csv"


# Define truncate function using mysql-connector-python
# def truncate_table():
#     print("Connecting to Doris MySQL-compatible endpoint...")
#     connection = mysql.connector.connect(
#         host=doris_host,
#         port="31115",
#         user=doris_user,
#         password=doris_password,
#         database=db_name
#     )
    
#     cursor = connection.cursor()
#     print(f"Truncating {db_name}.{table_name}")
#     cursor.execute(f"TRUNCATE TABLE {db_name}.{table_name};")
#     connection.commit()
#     print("Table truncated successfully.")
#     cursor.close()
#     connection.close()

# Define DAG
with DAG(
    dag_id='doris_streamload_1m',
    default_args=default_args,
    #schedule_interval='*/5 * * * *', 
    schedule_interval=None,
    start_date=datetime(2025, 7, 11),
    #end_date=datetime(2025, 7, 14),    # Stop after 3 days
    catchup=False,
    description='Truncate Doris table and stream load every 5 mins for 3 days',
    tags=['doris', 'stream_load'],
) as dag:

    # Truncate task
    # truncate_task = PythonOperator(
    #     task_id='truncate_table',
    #     python_callable=truncate_table
    # )

    # Stream load task using curl
    stream_load_task = BashOperator(
        task_id='stream_load_to_doris',
        bash_command=f"""
        curl --location-trusted -u {doris_user}:{doris_password} -T {file_path} \\
          -H "Expect: 100-continue" \\
          -H "max_filter_ratio: 0.1" \\
          -H "column_separator: ," \\
          -H "columns: CDRId,CDRVersion,CompanyIntID,CompanyName,InvoiceNumber,BusinessUnitLevel,BusinessUnit,BusinessUnitTAG,SharedBalanceUsed,DepartmentID,DepartmentName,CostCenterID,CostCenterName,AccountNumber,CustomerNumber,InvoicePeriod,TadigCode,GlobalTitle,MCC,MNC,Country,Operator,ProductId,MSISDN,IMSI,SIM,eUICCID,CallType,TrafficType,CallForwarding,DestinationName,DestinationType,CallingParty,CalledParty,APN,IPAddress,CallDate,CallTime,Duration,BillableDuration,Bytes,BalanceTypeID,ZoneID,Zone,TotalRetailCharge,WholesaleTAG,MappedIMSI,PropositionAssociated,CommercialOfferPropositionUsed,ChargeNumber,Threshold,ActualUsage,ZoneNameTo,RetailDuration,UsedId,UsedFrom,CELLID,UEIP,UsedType,BillCycleDay,UsedNumber,Device,IMEI,RatingGroupId,PlanName" \\
          -H "skip_header: 0" \\
          http://{doris_host}:{doris_port}/api/{db_name}/{table_name}/_stream_load
        """
    )

    # sleep_task = BashOperator(
    #   task_id='sleep_20_seconds',
    #   bash_command='sleep 10',
    # )

    stream_load_task 
