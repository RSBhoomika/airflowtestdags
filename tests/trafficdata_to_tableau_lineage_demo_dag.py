from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from connectors.Doris_hook_connector import DorisHook


default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
    "retries": 0,
    "depends_on_past": False,
}

def load_trafficdata_to_doris(**context):
        doris = DorisHook(conn_id="doris_demo")
        tablename = "trafficdata"
        doris.run_sql(f"TRUNCATE TABLE demo_database.{tablename}")
        doris.brokerload_data(
            table_name=tablename,
            s3_path="s3://airflow-test/TrafficData.csv",
            columns= """CDRId, CDRVersion, CompanyIntID, CompanyName, InvoiceNumber, BusinessUnitLevel, BusinessUnit, BusinessUnitTAG, SharedBalanceUsed,
            DepartmentID, DepartmentName, CostCenterID, CostCenterName, AccountNumber, CustomerNumber, InvoicePeriod, TadigCode, GlobalTitle,
            MCC, MNC, Country, Operator, ProductId, MSISDN, IMSI, SIM, eUICCID, CallType, TrafficType, CallForwarding, DestinationName,
            DestinationType, CallingParty, CalledParty, APN, IPAddress, CallDate, CallTime, Duration, BillableDuration, Bytes, BalanceTypeID,
            ZoneID, Zone, TotalRetailCharge, WholesaleTAG, MappedIMSI, PropositionAssociated, CommercialOfferPropositionUsed, ChargeNumber,
            Threshold, ActualUsage, ZoneNameTo, RetailDuration, UsedId, UsedFrom, CELLID, UEIP, UsedType, BillCycleDay, UsedNumber, Device,
            IMEI, RatingGroupId, PlanName """,
            s3endpoint="http://100.94.70.9:31677",
            s3access_key="minio",
            s3secret_key="minio123",
            context=context
        )

with DAG(
    dag_id='load_trafficdata_to_doris',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description='Load trafficdata from S3 into Doris using Broker Load'
) as dag:

        load_task = PythonOperator(
        task_id='load_data_to_doris',
        python_callable=load_trafficdata_to_doris,
        provide_context=True
    )