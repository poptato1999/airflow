import datetime

import pendulum

from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="1_dag_bash_python_with_xcom",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
) as dag:
    
    @task(task_id='python_push')
    def python_push_xcom():
        result_dict = {'status': 'Good', 'data': [1,2,3], 'options_cnt':100}
        return result_dict
    
    bash_pull = BashOperator(
        task_id='bash_pull',
        env={
            'STATUS': '{{ti.xcom_pull(task_ids="python_push")["status"]}}',
            'DATA': '{{ti.xcom_pull(task_ids="python_push")["data"]}}',
            'OPTIONS_CNT': '{{ti.xcom_pull(task_ids="python_push")["options_cnt"]}}',
        },
        
        bash_command='echo $STATUS && echo $DATA && echo $OPTIONS_CNT'
    )
    
    python_push_xcom() >> bash_pull