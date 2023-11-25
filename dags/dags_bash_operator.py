import datetime

import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="dag_bash_operator",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
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