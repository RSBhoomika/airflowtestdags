from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Default DAG arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0,
}

doris_user = "test_user"
doris_password = "password"
doris_host = "100.94.70.9"
doris_port = "31161"

db_name = "test"
table_name = "table2"
file_path = "s3://airflow-test/TrafficData.csv"

# Broker Load SQL for MinIO (S3-compatible)
broker_name = "s3_broker"
broker_props = {
    "aws_access_key": "minio",
    "aws_secret_key": "minio123",
    "endpoint": "http://100.94.70.9:31677"
}
broker_props_sql = ", ".join([f'"{k}"="{v}"' for k, v in broker_props.items()])

load_sql = f'''
LOAD LABEL {db_name}.airflow_broker_load_{{{{ ts_nodash }}}}
(
    DATA INFILE("{file_path}")
    INTO TABLE {table_name}
    FORMAT AS "csv"
    (COLUMNS TERMINATED BY ",")
)
WITH BROKER "{broker_name}"
(
    {broker_props_sql}
)
PROPERTIES
(
    "timeout" = "600",
    "max_filter_ratio" = "0.1"
    -- "compress_type" = "GZ"  # Remove or comment out for plain CSV
);
'''

def run_broker_load():
    import mysql.connector
    connection = mysql.connector.connect(
        host=doris_host,
        port=31115,  # Doris FE MySQL-compatible port
        user=doris_user,
        password=doris_password,
        database=db_name
    )
    cursor = connection.cursor()
    cursor.execute(load_sql)
    connection.commit()
    cursor.close()
    connection.close()

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
