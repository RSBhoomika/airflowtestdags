from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from scripts.emit_openmetadata_lineage import emit_column_lineage_to_om
from scripts.fact_lineage import emit_lineage_by_query
from scripts.audit_logger import failure_callback, success_callback
from connectors.Doris_hook_connector import DorisHook
from airflow.utils.task_group import TaskGroup
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import pandas as pd
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import boto3

# ── S3-compatible GCP settings ────────────────────────────────────
AWS_ACCESS_KEY_ID = "minio"
AWS_SECRET_ACCESS_KEY = "minio123"
REGION_NAME = "us-east-1"
ENDPOINT_URL = "http://100.94.70.9:31677"
BUCKET_NAME = "openmetadata"
OBJECT_KEY = "minio-source/sales/sales.csv"
LOCAL_PATH = "/tmp/raw_sales.csv"

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
    "retries": 0,
    "depends_on_past": False,
    # "on_failure_callback": failure_callback,
    #"on_success_callback": success_callback,
}

def download_csv_from_s3():
    session = boto3.Session(
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=REGION_NAME,
    )
    s3_client = session.client(
        service_name="s3",
        endpoint_url=ENDPOINT_URL,
    )
    s3_client.download_file(BUCKET_NAME, OBJECT_KEY, LOCAL_PATH)

def load_csv_to_doris(**context):
    csv_path = '/tmp/raw_sales.csv'
    table_name = 'raw_sales'
    doris = DorisHook(conn_id='doris_default')

    doris.run_sql(f"TRUNCATE TABLE demo_database.{table_name}")
    df = pd.read_csv(csv_path)
    rows_loaded, rows_rejected = doris.stream_load(df,table_name)  # Unpack result from stream_load

    return {
        "rows_loaded": rows_loaded,
        "rows_rejected": rows_rejected
    }

with DAG(
    dag_id="csv_to_doris_pipeline_OM",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["doris", "openmetadata"],
) as dag:
    
    with TaskGroup(group_id="load_csv_to_doris") as load_csv_to_doris_group:

        Download_file = PythonOperator(
            task_id='download_csv_from_s3',
            python_callable=download_csv_from_s3,
            provide_context=True
        )

        load_file = PythonOperator(
            task_id='load_csv_to_doris',
            python_callable=load_csv_to_doris,
            provide_context=True
        )

        emit_lineage = PythonOperator(
            task_id="emit_openmetadata_lineage",
            python_callable=emit_column_lineage_to_om,
        )
        Download_file >> load_file >> emit_lineage

    with TaskGroup(group_id="dimension_refresh") as dimension_refresh_group:
        dim_product_catalog = SQLExecuteQueryOperator(
            task_id="dim_product_catalog",
            conn_id="doris_default",
            sql="scripts/dim_product_catalog.sql"
        )
        dim_product_lineage = PythonOperator(
            task_id="dim_product_lineage",
            python_callable=emit_lineage_by_query,
            op_args=["dim_product_catalog"],
            provide_context=True
        )

        dim_customer_profile = SQLExecuteQueryOperator(
            task_id="dim_customer_catalog",
            conn_id="doris_default",
            sql="scripts/dim_customer_profile.sql"
        )
        
        dim_customer_lineage = PythonOperator(
            task_id="dim_customer_lineage",
            python_callable=emit_lineage_by_query,
            op_args=["dim_customer_profile"],
            provide_context=True
        )

        dim_product_catalog >> dim_product_lineage
        dim_customer_profile >> dim_customer_lineage


    with TaskGroup(group_id="fact_sales_order") as fact_sales_order:
        fact_sales_order_task = SQLExecuteQueryOperator(
            task_id="fact_sales_order",
            conn_id="doris_default",
            sql="scripts/fact_sales_order.sql"
        )
        fact_lineage = PythonOperator(
            task_id="fact_lineage",
            python_callable=emit_lineage_by_query,
            op_args=["fact_sales_order"],
            provide_context=True
        )
        fact_sales_order_task >> fact_lineage

    with TaskGroup(group_id="Reporting_views") as reporting_views_group:
        sales_summary = SQLExecuteQueryOperator(
            task_id="sales_summary",
            conn_id="doris_default",
            sql="scripts/vw_sales_summary.sql"
        )
        sales_summary_lineage = PythonOperator(
            task_id="sales_summary_lineage",
            python_callable=emit_lineage_by_query,
            op_args=["vw_sales_summary"],
            provide_context=True
        )

        product_channel_summary = SQLExecuteQueryOperator(
            task_id="product_channel_summary",
            conn_id="doris_default",
            sql="scripts/vw_product_channel_summary.sql"
        )

        product_channel_summary_lineage = PythonOperator(
            task_id="product_channel_summary_lineage",
            python_callable=emit_lineage_by_query,
            op_args=["vw_product_channel_summary"],
            provide_context=True
        )
        sales_summary >> sales_summary_lineage
        product_channel_summary >> product_channel_summary_lineage
        

load_csv_to_doris_group >> dimension_refresh_group >> fact_sales_order >> reporting_views_group
