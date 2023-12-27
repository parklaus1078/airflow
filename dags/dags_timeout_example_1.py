from airflow import DAG
import pendulum
from datetime import timedelta
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.decorators import task
from airflow.exceptions import AirflowException

email_str = Variable.get("emails")
email_list = [email.strip() for email in email_str.split(",")]

with DAG(
    dag_id="dags_timeout_example_1",
    start_date=pendulum.datetime(2023, 12, 25, tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
    dagrun_timeout=timedelta(minutes=1),                            # DAG의 시간 제한
    default_args={
        "execution_timeout": timedelta(seconds=20),                 # 각 Task의 시간 제한
        "email_on_failure": True,
        "email": email_list
    }
) as dag:
    bash_sleep_30 = BashOperator(
        task_id="bash_sleep_30",
        bash_command="sleep 30"
    )

    bash_sleep_10 = BashOperator(
        trigger_rule="all_done",
        task_id="bash_sleep_10",
        bash_command="sleep 10"
    )

    bash_sleep_30 >> bash_sleep_10