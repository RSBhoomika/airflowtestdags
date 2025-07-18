from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.utils.task_group import TaskGroup
from connectors.Doris_hook_connector import DorisHook
from scripts.lineage_emitter_om import emit_lineage_to_om


default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
    "retries": 0,
    "depends_on_past": False,
}
columns = """CDRId, CDRVersion, CompanyIntID, CompanyName, InvoiceNumber, BusinessUnitLevel, BusinessUnit, BusinessUnitTAG, SharedBalanceUsed,
            DepartmentID, DepartmentName, CostCenterID, CostCenterName, AccountNumber, CustomerNumber, InvoicePeriod, TadigCode, GlobalTitle,
            MCC, MNC, Country, Operator, ProductId, MSISDN, IMSI, SIM, eUICCID, CallType, TrafficType, CallForwarding, DestinationName,
            DestinationType, CallingParty, CalledParty, APN, IPAddress, CallDate, CallTime, Duration, BillableDuration, Bytes, BalanceTypeID,
            ZoneID, Zone, TotalRetailCharge, WholesaleTAG, MappedIMSI, PropositionAssociated, CommercialOfferPropositionUsed, ChargeNumber,
            Threshold, ActualUsage, ZoneNameTo, RetailDuration, UsedId, UsedFrom, CELLID, UEIP, UsedType, BillCycleDay, UsedNumber, Device,
            IMEI, RatingGroupId, PlanName """
def load_trafficdata_to_doris(**context):
        doris = DorisHook(conn_id="doris_demo")
        tablename = "traffic_data"
        doris.run_sql(f"TRUNCATE TABLE test.{tablename}")
        doris.brokerload_data(
            table_name=tablename,
            s3_path="s3://openmetadata/minio-source/TrafficData/TrafficData.csv",
            columns=columns,
            s3endpoint="http://100.94.70.9:31677",
            s3access_key="minio",
            s3secret_key="minio123",
            context=context,
            database="test"
        )

def emit_trafficdata_lineage(**context):
    column_list = [col.strip() for col in columns.split(",") if col.strip()]
    print(f"Emitting lineage for columns: {column_list}")
    emit_lineage_to_om(
        source_fqn="openmetadata-minio.openmetadata.minio-source/TrafficData",
        target_fqn="doris.default.demo_database.traffic_data",
        source_type="container",
        target_type="table",
        column_mappings=column_list
    )

with DAG(
    dag_id='load_trafficdata_to_doris',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description='Load trafficdata from S3 into Doris using Broker Load'
) as dag:
        
        with TaskGroup(group_id="load_traffic_data_to_Doris") as load_traffic_data_to_doris_group:

            load_task = PythonOperator(
            task_id='load_data_to_doris',
            python_callable=load_trafficdata_to_doris,
            provide_context=True
        )
            lineage_task = PythonOperator(
            task_id='emit_column_lineage',
            python_callable=emit_trafficdata_lineage,
            provide_context=True
        )
            load_task >> lineage_task
        
        load_traffic_data_to_doris_group
