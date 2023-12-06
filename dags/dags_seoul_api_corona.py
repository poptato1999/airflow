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
from operators.seoul_api_to_csv_operator import SeoulApiToCsvOperator

with DAG(
    dag_id="dags_seoul_api_corona",
    schedule='0 7 * * *',
    start_date=pendulum.datetime(2023,12,6,tz='Asia/Seoul'),
    catchup=False,
) as dag:
    '''서울시 코로나19 확진자 발생동향'''
    tb_corona19_count_status = SeoulApiToCsvOperator(
        task_id='tb_corona19_count_status',
        dataset_nm = 'TbCorona19CountStatus',
        path='/opt/airflow/files/TbCorona19CountStatus/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash}}',
        file_name='TbCorona19CountStatus.csv'
    )
    
    '''서울시 코로나19 백신 예방접종 현황'''
    tb_corona19_vaccine_stat_new = SeoulApiToCsvOperator(
        task_id='tb_corona19_vaccine_stat_new',
        dataset_nm = 'TbCorona19VaccinestatNew',
        path='/opt/airflow/files/TbCorona19VaccinestatNew/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash}}',
        file_name='TbCorona19VaccinestatNew.csv'
    )
    

        
    tb_corona19_count_status >> tb_corona19_vaccine_stat_new    
  
        
  