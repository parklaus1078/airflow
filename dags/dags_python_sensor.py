from airflow import DAG
import pendulum
from airflow.operators.bash import BashOperator
from airflow.sensors.python import PythonSensor
from airflow.hooks.base import BaseHook

with DAG(
    dag_id="dags_python_sensor",
    start_date=pendulum.datetime(2023, 12, 18, tz="Asia/Seoul"),
    schedule="10 1 * * *",
    catchup=False
) as dag:
    # Objective : 
    # 서울시 공공데이터에서 당일 날짜로 데이터가 생성되었는지 센싱하기(날짜가 있는 경우)
    def check_api_update(http_conn_id, endpoint, base_date_col, **kwargs):
        import requests
        import json
        from dateutil import relativedelta

        connection = BaseHook.get_connection(http_conn_id)
        url = f"{connection.host}:{connection.port}/{endpoint}/1/100"
        print(url)
        response = requests.get(url)

        contents = json.loads(response.text)
        key_name = list(contents.keys())[0]                 # 코로나 확진자 동향 파악 데이터의 첫번째 칼럼이 날짜임.
        row_data = contents.get(key_name).get("row")
        last_dt = row_data[0].get(base_date_col)
        last_date = last_dt[:10]
        last_date = last_date.replace(".", "-").replace("/", "-")

        try:
            pendulum.from_format(last_date, "YYYY-MM-DD")
        except:
            from airflow.exceptions import AirflowException
            AirflowException(f"!!!!!!!!!!!!! 경고 : {base_date_col} 칼럽은 YYYY.MM.DD 또는 YYYY/MM/DD 형태가 아닙니다. !!!!!!!!!!!!!")

        today_ymd = kwargs.get("data_interval_end").in_timezone("Asia/Seoul").strftime("%y-%m-%d")
        if last_date >= today_ymd:
            print(f"생성 확인 (배치 날짜: {today_ymd} / API Last 날짜: {last_date})")
            return True
        else:
            print(f"업데이트 미완료 (배치 날짜: {today_ymd} / API Last 날짜: {last_date})")
            return False
        
    sensor_task = PythonSensor(
        task_id="sensor_task",
        python_callable=check_api_update,
        op_kwargs={
            "http_conn_id": "seoul_public_data",
            "endpoint": "{{var.value.apikey_openapi_seoul_go_kr}}/json/TbCorona19CountStatus",
            "base_date_col": "S_DT"         # 데이터 기준일 칼럼명
        },
        poke_interval=600,
        mode="reschedule"
    )