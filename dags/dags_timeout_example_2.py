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
    dag_id="dags_timeout_example_2",
    start_date=pendulum.datetime(2023, 12, 25, tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
    dagrun_timeout=timedelta(minutes=1),
    default_args={
        "execution_timeout": timedelta(seconds=40),
        "email_on_failure": True,
        "email": email_list
    }
) as dag:
    bash_sleep_35 = BashOperator(
        task_id="bash_sleep_35",
        bash_command="sleep 35"
    )

    bash_sleep_36 = BashOperator(
        trigger_rule="all_done",
        task_id="bash_sleep_36",
        bash_command="sleep 36"
    )

    bash_go = BashOperator(
        task_id="bash_go",
        bash_command="exit 0"
    )

    bash_sleep_35 >> bash_sleep_36 >> bash_go