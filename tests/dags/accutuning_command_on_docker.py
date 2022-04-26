from airflow import DAG

from datetime import datetime, timedelta

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from custom_operator import DockerExOperator

# from airflow.operators.bash_operator import BashOperator
# from airflow.operators.python_operator import PythonOperator
# from airflow.providers.docker.operators.docker import DockerOperator


default_args = {
    'start_date': datetime(2022, 1, 1),
    'owner': 'airflow',
    'retries': 0,
    'retry_delay': timedelta(seconds=60),
    'provide_context': True,
    # 'render_template_as_native_obj': True,
    # 'on_failure_callback': on_failure_task,
    # 'on_success_callback': on_success_task,
    # 'execution_timeout': timedelta(seconds=60)
}


def make_parameters(**kwargs):
    cmd = kwargs['dag_run'].conf['cmd']
    cmd_args = kwargs['dag_run'].conf['cmd_args']

    command = f'''python /code/manage.py {cmd} '''
    command += ' '.join([f'--{k} {v}' for (k, v) in cmd_args.items() if v])

    print(command)

    kwargs['task_instance'].xcom_push(key='command', value=command)


with DAG(dag_id='accutuning_command_on_docker', concurrency=2, schedule_interval=None, default_args=default_args) as dag:
    start = DummyOperator(task_id='start')

    parameters = PythonOperator(task_id='make_parameters', python_callable=make_parameters)

    command_worker = DockerExOperator(
        task_id='command_worker',
        image='{{dag_run.conf.accutuning_image}}',
        command='{{ti.xcom_pull(key="command")}}',
        api_version='auto',
        auto_remove=True,
        volume_mount='{{dag_run.conf.accutuning_volume_mount}}',
        environment_str='{{dag_run.conf.accutuning_env_vars}}',
        do_xcom_push=True,
        docker_url='unix://var/run/docker-ext.sock',
        network_mode='accutuning_default',
        mount_tmp_dir=False,
    )

    end = DummyOperator(task_id='end')

    start >> parameters >> command_worker >> end
