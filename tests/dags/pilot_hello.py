from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime


def hello_world_py(*args, **kwargs):
    from pprint import pprint
    print('Hello World')
    pprint(args)
    pprint(kwargs)


dag_id = 'poc_hello'


default_args = {'owner': 'airflow',
                'start_date': datetime(2021, 1, 1)
                }

schedule = None

dag = DAG(
    dag_id,
    schedule_interval=schedule,
    default_args=default_args)

with dag:
    t1 = PythonOperator(
        task_id='hello_world',
        python_callable=hello_world_py)
