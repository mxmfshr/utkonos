from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import os

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'mxmfshr',
    'depends_on_past': False,
    'email': ['mxmfshr@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'start_date': datetime(2018, 2, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success',
    'airflow_home': os.environ["AIRFLOW_HOME"]
}

dag = DAG(
    'monthly',
    default_args=default_args,
    description='Monthly DAG',
    schedule_interval='@monthly',
    user_defined_macros=default_args
)

t1 = BashOperator(
    task_id='load_data',
    bash_command='papermill {{ airflow_home }}/scripts/get_data.ipynb - -p date {{ ds }} -p period monthly > /dev/null',
    dag=dag,
)

t2 = BashOperator(
    task_id='report_notebook',
    bash_command='papermill {{ airflow_home }}/scripts/eda.ipynb {{ airflow_home }}/reports/monthly/notebooks/report_{{ ds }}.ipynb -p data_path {{ airflow_home }}/data/raw/monthly/output_{{ ds }}.csv',
    dag=dag,
)

t3 = BashOperator(
    task_id='report_html',
    bash_command='jupyter nbconvert {{ airflow_home }}/reports/monthly/notebooks/report_{{ ds }}.ipynb --output {{ airflow_home }}/reports/monthly/html/report_{{ ds }} --to html --no-input',
    dag=dag,
)

t4 = BashOperator(
    task_id='preprocess',
    bash_command='papermill {{ airflow_home }}/scripts/preprocessing.ipynb - -p date {{ ds }} -p period monthly > /dev/null',
    dag=dag,
)

t5 = BashOperator(
    task_id='fit_model',
    bash_command='papermill {{ airflow_home }}/model/model.ipynb - -p date {{ ds }} -p period monthly > /dev/null',
    dag=dag,
)

t1.set_downstream(t2)
t1.set_downstream(t4)

t2.set_downstream(t3)

t4.set_downstream(t5)
