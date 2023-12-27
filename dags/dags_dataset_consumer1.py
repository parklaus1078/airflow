from airflow import Dataset
from airflow import DAG
from airflow.operators.bash import BashOperator
import pendulum

dataset_dags_dataset_producer_1 = Dataset("dags_dataset_producer_1")

with DAG(
    dag_id="dags_dataset_consumer1",
    schedule=[dataset_dags_dataset_producer_1],                     # producer_1 DAG의 수행 완료 시간이 schedule
    start_date=pendulum.datetime(2023, 12, 25, tz="Asia/Seoul"),
    catchup=False
) as dag:
    bash_task = BashOperator(
        task_id="bash_task",
        bash_command="echo {{ti.run_id}} && echo 'producer_1 이 완료되면 수행'"
    )