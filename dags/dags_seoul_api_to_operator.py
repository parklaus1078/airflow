from airflow import DAG
from operators.seoul_api_to_csv_operator import SeoulApiToCsvOperator
import pendulum

with DAG(
    dag_id="dags_seoul_api_to_operator",
    schedule="0 7 * * *",
    start_date=pendulum.datetime(2023, 12, 10, tz="Asia/Seoul"),
    catchup=False
) as dag:
    '''서울시 분실물 습득물 정보 목록'''
    tb_lost_and_found_list = SeoulApiToCsvOperator(
        task_id="tb_lost_and_found_list",
        dataset_name="lostArticleInfo",
        path="/opt/airflow/files/lostArticleInfo/{{data_interval_end.in_timezone('Asia/Seoul') | ds_nodash}}",
        file_name="tbLostAndFoundList.csv"
    )

    '''서울시 대중교통 이용량'''
    tb_public_transit_population_status = SeoulApiToCsvOperator(
        task_id="tb_public_transit_population_status",
        dataset_name="tpssSubwayPassenger",
        path="/opt/airflow/files/tpssSubwayPassenger/{{data_interval_end.in_timezone('Asia/Seoul') | ds_nodash}}",
        file_name="tbPublicTransitPopulationStatus.csv"
    )

    tb_lost_and_found_list >> tb_public_transit_population_status
