import datetime

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


from Data_mart_dag.dm_main import transfet_to_data_mart

with DAG(dag_id='ETL_dag_Data_Mart',
         schedule_interval='@once',
         start_date=datetime.datetime(2023, 7, 17),
         catchup=False) as dag:

    trigger_ETL_dag_DDS = TriggerDagRunOperator(
        task_id='trigger_ETL_dag_DDS',
        trigger_dag_id='ETL_dag_DDS',
        reset_dag_run=True,
        wait_for_completion=True
    )

    start_step = DummyOperator(task_id='start_step')
    transfer_data = PythonOperator(
        task_id='transfer_data', python_callable=transfet_to_data_mart)

trigger_ETL_dag_DDS >> start_step >> transfer_data

