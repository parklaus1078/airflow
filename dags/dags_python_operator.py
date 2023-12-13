from airflow import DAG
import pendulum
import datetime
from airflow.operators.python import PythonOperator
import random

with DAG(
    dag_id = "dags_python_operator",
    schedule = "30 6 1 * *",  # 0분 8시 1일 매월 모든_요일
    start_date = pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup = False
) as dag: 
    def select_fruit():
        fruit = ["APPLE", "ORANGE", "BANANA", "AVOCADO"]
        rand_int = random.randint(0,3)
        print(fruit[rand_int])

    py_t1 = PythonOperator(
        task_id = "py_t1",
        python_callable=select_fruit
    )

    py_t1