from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator
from airflow.decorators import task
from airflow.decorators import task_group           # 방법 1 : 데코레이터
from airflow.utils.task_group import TaskGroup      # 방법 2 : 클래스

with DAG(
    dag_id="dags_python_with_task_group",
    schedule=None,
    start_date=pendulum.datetime(2023, 12, 10, tz="Asia/Seoul"),
    catchup=False
) as dag:
    def inner_func(**kwargs):
        msg = kwargs.get("msg") or ""
        print(msg)

    @task_group(group_id = "first_group")
    def group_1():
        ''' task_group decorator를 이용한 첫번째 그룹입니다. '''

        @task(task_id = "inner_function_1")
        def inner_func1(**kwargs):
            print("첫번째 TaskGroup 내 첫번째 task 입니다.")

        inner_func2 = PythonOperator(
            task_id="inner_func2",
            python_callable=inner_func,
            op_kwargs={"msg" : "첫번째 TaskGroup 내 두번째 task 입니다."}
        )

        inner_func1() >> inner_func2

    with TaskGroup(group_id="second_group", tooltip="두번째 그룹입니다.") as group_2:
        ''' Docstring does not appear. '''
        @task(task_id="inner_func1")
        def inner_func1(**kwargs):
            print("두번째 TaskGroup 내 첫번째 task입니다.")

        inner_func2 = PythonOperator(
            task_id="inner_func2",
            python_callable=inner_func,
            op_kwargs={"msg" : "두번째 TaskGroup 내 두번째 task 입니다."}        
        )
        
        inner_func1() >> inner_func2

    group_1() >> group_2