from airflow import DAG

from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from custom_operator import DockerExOperator


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

with DAG(dag_id='ml_run_cmd', concurrency=2, schedule_interval=None, default_args=default_args) as dag:
    start = DummyOperator(task_id='start')
    django_command = DockerExOperator(
        task_id='before_worker',
        image='{{dag_run.conf.accutuning_image}}',
        command='{{dag_run.conf.command}}',
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

    start >> django_command >> end
