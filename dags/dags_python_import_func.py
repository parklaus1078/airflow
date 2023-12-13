from airflow import DAG
import pendulum
import datetime
from airflow.operators.python import PythonOperator
from common.common_func import get_sftp
# python interpreter는 common_func 파일의 위치를 인식하려면 plugins.common.common_func로 해줘야함.
# 그러나 airflow 컨테이너는 PATH를 plugins까지 잡아놓았기 때문에 airflow가 인식하지 못함. 그래서 .env 파일에 따로 PYTHONPATH를 특정해 놓아야 함.
# env 파일을 저장하면 바로 인식하기 시작함.

with DAG(
    dag_id = "dags_python_import_func",
    schedule = "30 6 1 * *",  # 30분 6시 1일 매월 모든_요일
    start_date = pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup = False
) as dag: 
    
    task_get_sftp = PythonOperator(
        task_id="task_fet_sftp",
        python_callable=get_sftp
    )