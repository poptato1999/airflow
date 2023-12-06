import datetime

import pendulum

from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.python import PythonOperator
from airflow.operators.branch import BaseBranchOperator
from airflow.exceptions import AirflowException
from airflow.decorators import task_group
from airflow.utils.task_group import TaskGroup
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from airflow.providers.http.operators.http import SimpleHttpOperator

with DAG(
    dag_id="1_dags",
    schedule="30 9 * * *",
    start_date=pendulum.datetime(2023,12,6,tz='Asia/Seoul'),
    catchup=False,
    schedule=None
) as dag:
    
    tb_cycle_station_info = SimpleHttpOperator(
        task_id='tb_cycle_station_info',
        http_conn_id='openapi.seoul.go.kr',
        endpoint='{{var.value.apikey_openapi_seoul_go_kr}}/json/tbCycleStationInfo/1/10/',
        method='GET',
        header={'Content-Type': 'application/json',
                'charset': 'utf-8',
                'Accept': '*/*'
                }
    )
    
    @task(task_id='python_2')
    def python_2(**kwargs):
        ti = kwargs['ti']
        rslt = ti.xcom_pull(task_ids='tb_cycle_station_info')
        import json
        from pprint import pprint
        
        pprint(json.load(rslt))
        
    tb_cycle_station_info >> python_2()    
  
        
  