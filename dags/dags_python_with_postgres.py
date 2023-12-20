from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator

with DAG(
    dag_id="dags_python_with_postgres",
    start_date=pendulum.datetime(2023, 12, 10, tz="Asia/Seoul"),
    schedule=None,
    catchup=False
) as dag:
    
    def insert_postgres(postgres_conn_id, **kwargs):
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        from contextlib import closing 
        
        postgres_hook = PostgresHook(postgres_conn_id)
        with closing(postgres_hook.get_conn()) as conn:
            with closing(conn.cursor()) as cursor:
                dag_id = kwargs.get("ti").dag_id
                task_id = kwargs.get("ti").task_id
                run_id = kwargs.get("ti").run_id
                msg = "Insert 수행"
                sql = "insert into py_operator_direct_insert values (%s,%s,%s,%s);"
                cursor.execute(sql, (dag_id, task_id, run_id, msg))
                conn.commit()

    insert_postgres = PythonOperator(
        task_id="insert_postgres",
        python_callable=insert_postgres,
        op_kwargs={"postgres_conn_id": "conn-db-postgres-custom"}
    )

    insert_postgres