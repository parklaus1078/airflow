from airflow import DAG
import pendulum
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.decorators import task

with DAG(
    dag_id="dags_http_operator",
    schedule=None,               
    start_date=pendulum.datetime(2023, 12, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    tb_cycle_station_info = SimpleHttpOperator(
        task_id="tb_cycle_station_info",
        http_conn_id="seoul_public_data",
        endpoint='{{var.value.apikey_openapi_seoul_go_kr}}/json/CardSubwayStatsNew/1/5/20231001',
        method="GET",
        headers={
            "Content-Type": "application/json",
            "charset": "utf-8",
            "Accept": "*/*"
        }
    )

    @task(task_id="python_2")
    def python_2(**kwargs):
        ti = kwargs["ti"]
        result = ti.xcom_pull(task_ids="tb_cycle_station_info")
        import json
        from pprint import pprint

        pprint(json.loads(result))

    tb_cycle_station_info >> python_2()