from airflow import DAG
from datetime import timedelta, datetime
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from airflow.models import Variable
from kubernetes.client import models as k8s
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 7, 18),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}


with DAG(
   'spark-application-dag',
   default_args=default_args,
   description='Read in csv format Write in iceberg format',
   schedule_interval=None,
   start_date=datetime(2025, 7, 18),
   catchup=False,
   tags=['example']
) as dag:
   t1 = SparkKubernetesOperator(
       task_id='spark-kubernetes-job',
       trigger_rule="all_success",
       depends_on_past=False,
       retries=3,
       application_file="spark.yaml",
       namespace="default"
   )
