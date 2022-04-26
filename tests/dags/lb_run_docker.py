from airflow import DAG

from datetime import datetime, timedelta
import json

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


def make_uuid():
    import uuid
    return str(uuid.uuid4()).replace('-', '')


def get_command_name(experiment_process_type):
    command_dict = {  # TODO 이 참에 이거 전부 통일하면 안될까? ml_ + process_type
        'parse': 'ml_parse',
        'labeling': 'lb_tagtext',
        'lb_predict': 'lb_predict',
        'preprocess': 'ml_preprocess',
        'optuna': 'ml_optuna',
        'modelstat': 'ml_modelstat',
        'deploy': 'ml_deploy',
        'predict': 'ml_predict',
        'ensemble': 'ml_ensemble',
        'cluster': 'cl_run',
        'cl_predict': 'cl_predict',
        'dataset_eda': 'ml_dataset_eda',
    }
    return command_dict[experiment_process_type]


def make_accutuning_docker_command(django_command, experiment_id, container_uuid, execute_range, experiment_process_type, experiment_target, proceed_next):
    return f"python /code/manage.py {django_command} "\
        + f"--experiment {experiment_id} --uuid {container_uuid} --execute_range {execute_range} "\
        + f"--experiment_process_type {experiment_process_type} --experiment_target {experiment_target} --proceed_next {proceed_next}"


def make_accutuning_docker_command_lb(experiment_process_type, conf_path, model_path, output_fp):
    return f"python /code/accutuning_lb/ {experiment_process_type}.py "\
        + f"--conf_path {conf_path} --model_path {model_path} --output_fp {output_fp} "


def make_parameters(**kwargs):
    experiment_id = kwargs['dag_run'].conf['experiment_id']
    experiment_process_type = kwargs['dag_run'].conf['experiment_process_type']
    experiment_target = kwargs['dag_run'].conf['experiment_target']
    proceed_next = kwargs['dag_run'].conf['proceed_next']

    container_uuid = make_uuid()
    django_command = get_command_name(experiment_process_type)
    docker_command_before = make_accutuning_docker_command(django_command, experiment_id, container_uuid, 'before', experiment_process_type, experiment_target, proceed_next)
    docker_command_after = make_accutuning_docker_command(django_command, experiment_id, container_uuid, 'after', experiment_process_type, experiment_target, proceed_next)

    kwargs['task_instance'].xcom_push(key='uuid', value=container_uuid)
    kwargs['task_instance'].xcom_push(key='before_command', value=docker_command_before)
    kwargs['task_instance'].xcom_push(key='after_command', value=docker_command_after)


def make_worker_env(**kwargs):
    workspace_path = kwargs['task_instance'].xcom_pull(task_ids='before_worker')
    worker_env_vars_str = kwargs['dag_run'].conf['worker_env_vars']

    print(f'workspace_path:{workspace_path}')
    print(f'worker_env_vars:{worker_env_vars_str}')

    env_dict = json.loads(worker_env_vars_str)
    env_dict['ACCUTUNING_WORKSPACE'] = workspace_path
    worker_env_vars = json.dumps(env_dict)

    print(f'worker_env_vars:{worker_env_vars}')

    kwargs['task_instance'].xcom_push(key='worker_env_vars', value=worker_env_vars)


with DAG(dag_id='lb_run_docker', concurrency=2, schedule_interval=None, default_args=default_args) as dag:
    start = DummyOperator(task_id='start', dag=dag)

    parameters = PythonOperator(task_id='make_parameters', python_callable=make_parameters, dag=dag)

    before_worker = DockerExOperator(
        task_id='before_worker',
        image='{{dag_run.conf.accutuning_image}}',
        command='{{ti.xcom_pull(key="before_command")}}',
        api_version='auto',
        auto_remove=True,
        volume_mount='{{dag_run.conf.accutuning_volume_mount}}',
        environment_str='{{dag_run.conf.accutuning_env_vars}}',
        do_xcom_push=True,
        docker_url='unix://var/run/docker-ext.sock',
        network_mode='accutuning_default',
        mount_tmp_dir=False,
        dag=dag,
    )

    worker_env = PythonOperator(task_id='make_worker_env', python_callable=make_worker_env, dag=dag)

    worker = DockerExOperator(
        task_id='worker',
        image='{{dag_run.conf.worker_image}}',
        command=None,
        api_version='auto',
        auto_remove=True,
        volume_mount='{{dag_run.conf.worker_volume_mount}}',
        environment_str='{{ti.xcom_pull(key="worker_env_vars")}}',
        docker_url='unix://var/run/docker-ext.sock',
        network_mode='accutuning_default',
        mount_tmp_dir=False,
        dag=dag,
    )
    after_worker = DockerExOperator(
        task_id='after_worker',
        image='{{dag_run.conf.accutuning_image}}',
        command='{{ti.xcom_pull(key="after_command")}}',
        api_version='auto',
        auto_remove=True,
        volume_mount='{{dag_run.conf.accutuning_volume_mount}}',
        environment_str='{{dag_run.conf.accutuning_env_vars}}',
        docker_url='unix://var/run/docker-ext.sock',
        network_mode='accutuning_default',
        mount_tmp_dir=False,
        dag=dag,
    )
    end = DummyOperator(task_id='end', dag=dag)

    start >> parameters >> before_worker >> worker_env >> worker >> after_worker >> end
