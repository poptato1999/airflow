from airflow import DAG
import pendulum
import datetime

from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="dags_python_operator",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2023, 11, 18, tz="Asia/Seoul"),
    catchup=False,
) as dag:
    bash_push = BashOperator(
        task_id='bash_push',
        bash_command = "echo START && "
                        "echo XCOM_PUSHED"
                        "{{ ti.xcom_push(key='bash_pushed', value='first_bash_message')}} &&"
                        "echo COMPLETE"
    )
    bash_pull = BashOperator(
        task_id='bash_pull',
        env={'PUSHED_VALUE': "{{ti.xcom_pull(key='bash_pushed')}}",
             'RETURN_VALUE': "{{ti.xcom_pull(task_ids='bash_push')}}"}
        bash_command = "echo $PUSHED_VALUE && echo $RETURN_VALUE ",
        do_xcom_push=False
    )
    bash_push >> bash_pull


