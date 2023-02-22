from airflow import DAG
import logging as log
import pendulum
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

def load_data_task_function():
    log.info("ENTERED: LOAD DATA TASK FUNCTION")
    
    return

def call_api_task_function():
    log.info("ENTERED: CALL API TASK FUNCTION")
    
    return

def transform_data_task_function():
    log.info("ENTERED: TRANSFORM DATA TASK FUNCTION")
    
    return

def write_data_task_function():
    log.info("ENTERED: WRITE DATA TASK FUNCTION")
    
    return

with DAG(
    dag_id="Twitter_DAG",
    schedule_interval="0 9 * * *",
    start_date=pendulum.datetime(2023, 2, 26, tz="US/Pacific"),
    catchup=False,
) as dag:
    
    load_data_task = PythonOperator(task_id="load_data_task", python_callable=load_data_task_function)
    call_api_task = PythonOperator(task_id="call_api_task", python_callable=call_api_task_function)
    transform_data_task = PythonOperator(task_id="transform_data_task", python_callable=transform_data_task_function)
    write_data_task = PythonOperator(task_id="write_data_task", python_callable=write_data_task_function)
    

load_data_task >> call_api_task >> transform_data_task >> write_data_task