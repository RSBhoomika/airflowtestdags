from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import logging

# Default DAG arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0,
}

doris_user = "test_user"
doris_password = "password"
doris_host = "100.94.70.9"
doris_port = 31115  # Doris FE MySQL-compatible port

db_name = "test"
table_name = "table2"
file_path = "s3://airflow-test/TrafficData.csv"

column_list = "CDRId,CDRVersion,CompanyIntID,CompanyName,InvoiceNumber,BusinessUnitLevel,BusinessUnit,BusinessUnitTAG,SharedBalanceUsed,DepartmentID,DepartmentName,CostCenterID,CostCenterName,AccountNumber,CustomerNumber,InvoicePeriod,TadigCode,GlobalTitle,MCC,MNC,Country,Operator,ProductId,MSISDN,IMSI,SIM,eUICCID,CallType,TrafficType,CallForwarding,DestinationName,DestinationType,CallingParty,CalledParty,APN,IPAddress,CallDate,CallTime,Duration,BillableDuration,Bytes,BalanceTypeID,ZoneID,Zone,TotalRetailCharge,WholesaleTAG,MappedIMSI,PropositionAssociated,CommercialOfferPropositionUsed,ChargeNumber,Threshold,ActualUsage,ZoneNameTo,RetailDuration,UsedId,UsedFrom,CELLID,UEIP,UsedType,BillCycleDay,UsedNumber,Device,IMEI,RatingGroupId,PlanName"

# Broker Load SQL for MinIO (S3-compatible)
broker_name = "s3_broker"
broker_props = {
    "aws_access_key": "minio",
    "aws_secret_key": "minio123",
    "endpoint": "http://100.94.70.9:31677"
}
broker_props_sql = ", ".join([f'"{k}"="{v}"' for k, v in broker_props.items()])

def run_broker_load(**context):
    import mysql.connector
    ts_nodash = context['ts_nodash'] if 'ts_nodash' in context else datetime.now().strftime('%Y%m%dT%H%M%S')
    label = f"{db_name}.airflow_broker_load_{ts_nodash}"
    load_sql = f'''
    LOAD LABEL {label}
    (
        DATA INFILE(\"{file_path}\")
        INTO TABLE {table_name}
        FORMAT AS \"csv\"
        COLUMNS TERMINATED BY ","
        ({column_list})
    )
    WITH BROKER \"{broker_name}\"
    (
        {broker_props_sql}
    )
    PROPERTIES
    (
        \"timeout\" = \"600\",
        \"max_filter_ratio\" = \"0.1\"
        -- \"compress_type\" = \"GZ\"
    );
    '''
    logging.info(f"Submitting Broker Load SQL to Doris: {load_sql}")
    try:
        connection = mysql.connector.connect(
            host=doris_host,
            port=doris_port,
            user=doris_user,
            password=doris_password,
            database=db_name
        )
        cursor = connection.cursor()
        cursor.execute(load_sql)
        connection.commit()
        cursor.close()
        connection.close()
        logging.info("Broker Load submitted successfully.")
    except Exception as e:
        logging.error(f"Error during Broker Load: {e}")
        raise

with DAG(
    dag_id='doris_broker_load_1m',
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2025, 7, 11),
    catchup=False,
    description='Broker Load to Doris from MinIO',
    tags=['doris', 'broker_load'],
) as dag:

    broker_load_task = PythonOperator(
        task_id='broker_load_to_doris',
        python_callable=run_broker_load
    )

    broker_load_task 
