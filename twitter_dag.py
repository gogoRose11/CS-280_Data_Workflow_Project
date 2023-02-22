from airflow import DAG
import logging as log
import pendulum
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from models.config import Session #You would import this from your config file
from models.user import User


def load_data_task_function():
    log.info("ENTERED: LOAD DATA TASK FUNCTION")

    session = Session()
    # This will retrieve all of the users from the database 
    # (It'll be a list, so you may have 100 users or 0 users)
    users_list = session.query(User).all() 
    log.info(f"USER LIST: {users_list}")
    session.close()

    # PULL USER INFORMATION AND PASS IT TO THE NEXT TASK
    
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