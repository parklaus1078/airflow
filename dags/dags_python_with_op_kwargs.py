from airflow import DAG
import pendulum

from airflow.operators.python import PythonOperator
from common.common_func import regist2

with DAG(
    dag_id="dags_python_with_op_kwargs",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    regist2_t1 = PythonOperator(
        task_id="regist_t1",
        python_callable=regist2,
        op_args=["Kay Park", "Male", "Kr", " Yong-In"],
        op_args={
            "email": "gon2312@gmail.com",
            "phone": "010-1111-1111"
        }
    )

    regist2_t1