from airflow import DAG
import pendulum
from airflow.decorators import task

with DAG(
    dag_id = "dags_python_with_macro",
    schedule = "10 0 * * *",
    start_date = pendulum.datetime(2023, 3, 10, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    @task(
        task_id="task_using_macros",
        templates_dict = {
            "start_date":'{{ (data_interval_end.in_timezone("Asia/Seoul") + macros.dateutil.relativedelta.relativedelta(months=-1, day=1)) | ds }}',
            "end_date":'{{ (data_interval_end.in_timezone("Asia/Seoul").replace(day=1) + macros.dateutil.relativedelta.relativedelta(days=-1)) | ds }}'
            }
    )
    def get_datetime_macro(**kwargs):
        templates_dict = kwargs.get("templates_dict") or {}
        if templates_dict:
            start_date = templates_dict.get("start_date") or "NO start_date INDICATED"
            end_date = templates_dict.get("end_date") or "NO end_date INDICATED"
            print("start_date " + start_date)
            print("end_date" + end_date)

    @task(task_id="task_direct_calc")
    def get_datetime_calc(**kwargs):
        from dateutil.relativedelta import relativedelta        # DAG 부하를 줄이기 위해 Task 하나에서만 쓰는 모듈은 해당 task에서 importing하는 편이 좋음.
        data_interval_end = kwargs["data_interval_end"]

        prev_month_day_first = data_interval_end.in_timezone("Asia/Seoul") + relativedelta(months=-1, day=1)
        prev_month_day_last = data_interval_end.in_timezone("Asia/Seoul").replace(day=1) + relativedelta(days=-1)
        print(prev_month_day_first.strftime("%Y-%m-%d"))
        print(prev_month_day_last.strftime("%Y-%m-%d"))

    get_datetime_macro() >> get_datetime_calc()