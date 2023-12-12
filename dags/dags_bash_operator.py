from airflow import DAG
import datetime
import pendulum
from airflow.operators.bash_operator import BashOperator

with DAG(
    dag_id="dags_bash_operator",                                    # name of DAG. Usually same with file name
    schedule="0 0 * * *",                                           # cron scheduler
    start_date=pendulum.datetime(2023, 1, 1, tz="Asia/Seoul"),      # From when will this DAG run
    catchup=False,                                                  # If today is 230301, and the start_date is 230101, setting catchup true will catch up the missing period's DAG at once
    # dagrun_timeout=datetime.timedelta(minutes=60),                # threshold of holding the DAG if it fails
    # tags=["chapter 1"],                                             # Tags to sort in DAG UI
    # params={"example_key": "example_value"},                        # Parameters to be passed to all tasks in the dag
) as dag:
    bash_t1 = BashOperator(
        task_id="bash_t1",                                          # task id = task variable name
        bash_command="echo whoami",
    )

    bash_t2 = BashOperator(
        task_id="bash_t2",
        bash_command = "kay park"
    )

    bash_t1 >> bash_t2