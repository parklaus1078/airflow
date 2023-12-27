from sensors.seoul_api_date_sensor import SeoulApiDateSensor
from airflow import DAG
import pendulum

with DAG(
    dag_id="dags_custom_sensors",
    start_date=pendulum.datetime(2023, 12, 25, tz="Asia/Seoul"),
    schedule=None,
    catchup=False
) as dag:
    tb_corona_19_count_status_sensor = SeoulApiDateSensor(
        task_id = "tb_corona_19_count_status_sensor",
        dataset_name="TbCorona19CountStatus",
        base_date_col="S_DT",
        day_off=0,
        poke_interval=600,
        mode='reschedule'
    )

    tb_corona_19_vaccine_stat_new_sensor = SeoulApiDateSensor(
        task_id = "tb_corona_19_vaccine_stat_new_sensor",
        dataset_name="TbCorona19VaccinestatNew",
        base_date_col="S_VC_DT",
        day_off=-1,
        poke_interval=600,
        mode='reschedule'
    )