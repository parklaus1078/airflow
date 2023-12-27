from airflow import DAG
import pendulum
from datetime import timedelta
from airflow.operators.bash import BashOperator
from airflow.models import Variable

email_str = Variable.get("emails")
email_list = [email.strip() for email in email_str.split(",")]

with DAG(
    dag_id="dags_sla_email_example",
    start_date=pendulum.datetime(2023, 12, 25, tz="Asia/Seoul"),
    schedule="*/10 * * * *",
    catchup=False,
    default_args={
        "sla": timedelta(seconds=70),
        "email": email_list
    }
) as dag:
    task_slp_30s_sla_70s = BashOperator(
        task_id="task_slp_30s_sla_70s",
        bash_command="sleep 30"
    )

    task_slp_60s_sla_70s = BashOperator(
        task_id="task_slp_60s_sla_70s",
        bash_command="sleep 60"
    )

    task_slp_10s_sla_70s = BashOperator(
        task_id="task_slp_10s_sla_70s",
        bash_command="sleep 10"
    )

    task_slp_10s_sla_30s = BashOperator(
        task_id="task_slp_10s_sla_30s",
        bash_command="sleep 10",
        sla=timedelta(seconds=30)
    )

    task_slp_30s_sla_70s >> task_slp_60s_sla_70s >> task_slp_10s_sla_70s >> task_slp_10s_sla_30s