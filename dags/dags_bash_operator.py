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


with DAG(
    dag_id="1_dag",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2023, 12, 5, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
) as dag:
    def inner_func(**kwargs):
        msg = kwargs.get('msg') or ''
        print(msg)
    
    @task_group(group_id='first_group')
    def group_1():
        ''' task_group 데커레이터를 이용한 첫 번ㅉ 그룹입니다. '''
        
        @task(task_id='inner_function1')
        def inner_func1(**kwargs):
            print('첫 번째 TaskGroup 내 첫 번째 task입니다.')
            
        inner_function2 = PythonOperator(
            task_id='inner_function2',
            python_callable=inner_func,
            op_kwargs={'msg': '첫 번째 TaskGroup내 두번째 task 입니다.'}
        )
        
        inner_func1() >> inner_function2
        
    with TaskGroup(group_id='second_group', tooltip='두 번째 그룹입니다.') as group_2:
        @task(task_id='inner_function1')
        def inner_func1(**kwargs):
            print('두 번째 TaskGroup 첫 번째 task 입니다.')
            
        inner_function2 = PythonOperator(
            task_id='inner_function2',
            python_callable=inner_func,
            op_kwargs={'msg': '두 번째 TaskGroup 내 두 번째 task입니다.'}
        )
        inner_func1() >> inner_function2
        
    group_1() >> group_2