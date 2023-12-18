from airflow import DAG
import pendulum
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.operators.branch import BaseBranchOperator

with DAG(
    dag_id="dags_base_branch_operator",
    schedule="30 6 * * *",               # 30분 6시 매일 매월 매요일
    start_date=pendulum.datetime(2023, 12, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    def common_func(**kwargs):
        print(kwargs["selected"])

    task_a = PythonOperator(
        task_id="task_a",
        python_callable=common_func,
        op_kwargs={'selected':"A"}
    )

    task_b = PythonOperator(
        task_id="task_b",
        python_callable=common_func,
        op_kwargs={'selected':"B"}
    )

    task_c = PythonOperator(
        task_id="task_c",
        python_callable=common_func,
        op_kwargs={'selected':"C"}
    )

    class CustomBranchOperator(BaseBranchOperator): # 클래스 상속할 때 chose branch 함수 만들어서 어떻게 분기 칠 지 정의해야함.
        def choose_branch(self, context):
            import random
            
            item_list = ["A", "B", "C"]
            selected_item = random.choice(item_list)
            if selected_item == "A":
                return "task_a"
            elif selected_item in ["B", "C"]:
                return ["task_b", "task_c"]
            
    custom_branch_operator = CustomBranchOperator(task_id="python_branch_task")
    custom_branch_operator >> [task_a, task_b, task_c]