from airflow import DAG
import pendulum
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor

with DAG(
    dag_id="dags_file_sensor",
    start_date=pendulum.datetime(2023, 12, 18, tz="Asia/Seoul"),
    schedule="0 7 * * *",
    catchup=True
) as dag:
    tpssSubwayPassenger_sensor = FileSensor(
        task_id="tpssSubwayPassenger_sensor",
        fs_conn_id="conn_file_opt_airflow_files",
        filepath="tpssSubwayPassenger/{{data_interval_end.in_timezone('Asia/Seoul') | ds_nodash}}/tbPublicTransitPopulationStatus.csv",
        recursive=False,
        poke_interval=60,
        timeout=60*60*24,
        mode="reschedule"
    )