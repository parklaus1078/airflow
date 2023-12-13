from airflow import DAG
import pendulum
import datetime
from airflow.operators.bash import BashOperator

with DAG(
    dag_id = "dags_bash_select_fruit",
    schedule = "10 0 * * 6#1",  # 10분 0시 매일 매월 첫번째_토요일
    start_date = pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup = False
) as dag: 

    t1_orange = BashOperator(
        task_id = "t1_orange",
        bash_command="/opt/airflow/plugins/shell/select_fruit.sh ORANGE"   # 실행하기 이전에 chmod +x 파일명으로 실행 권한 부여하기!
    )

    t1_avocado = BashOperator(
        task_id = "t1_avocado",
        bash_command="/opt/airflow/plugins/shell/select_fruit.sh AVOCADO"   # 실행하기 이전에 chmod +x 파일명으로 실행 권한 부여하기!
    )

    t1_orange >> t1_avocado