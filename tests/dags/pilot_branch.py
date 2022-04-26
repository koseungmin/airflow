from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from datetime import datetime


def hello_world_py(*args, **kwargs):
    from pprint import pprint
    print('Hello World')
    pprint(args)
    pprint(kwargs)


def bye_world_py(*args, **kwargs):
    from pprint import pprint
    print('Goodbye')
    pprint(args)
    pprint(kwargs)


dag_id = 'poc_hello_branch'

default_args = {'owner': 'airflow',
                'start_date': datetime(2021, 1, 1)
                }

schedule = None


def which_path(*args, **kwargs):
    return 'b1'


dag = DAG(
    dag_id,
    schedule_interval=schedule,
    default_args=default_args)

with dag:
    start = PythonOperator(
        task_id='hello_world',
        python_callable=hello_world_py)
    b0 = PythonOperator(
        task_id='b0',
        python_callable=hello_world_py)
    b1 = PythonOperator(
        task_id='b1',
        python_callable=hello_world_py)
    b2 = PythonOperator(
        task_id='b2',
        python_callable=hello_world_py)

    branch = BranchPythonOperator(task_id='branch', python_callable=which_path)
    end = PythonOperator(
        task_id='bye_world',
        trigger_rule='one_success',
        python_callable=bye_world_py)

    start >> branch >> [b0, b1, b2] >> end
