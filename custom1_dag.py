from airflow import DAG
import logging as log
import pendulum
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

def first_task_function():
    log.info("This is the first task - NAME")
    name = "Eric Rose"
    log.info(f"My name is {name}")
    lowercase_task_function(name)
    uppercase_task_function(name)
    return

def lowercase_task_function(name):
    log.info("This is your second task - LOWERCASE")
    log.info(f"Lowercase Name: {name.lowercase()}")
    

def uppercase_task_function(name):
    log.info("This is your third task - UPPERCASE")
    log.info(f"Uppercase Name: {name.Uppercase()}")

def count_task_function():
    log.info("This is your fourth task - COUNT")
    name = "Eric Rose"
    log.info(f"There are {len(name)} characters in the name Eric Rose")
    return


with DAG(
    dag_id="My_Custom_CS_280_DAG",
    schedule_interval="0 10 * * *",
    start_date=pendulum.datetime(2023, 9, 1, tz="US/Pacific"),
    catchup=False,
) as dag:
    start_task = DummyOperator(task_id="start_task")
    first_task = PythonOperator(task_id="first_task", python_callable=first_task_function)
    second_task = PythonOperator(task_id="second_task", python_callable=count_task_function)
    end_task = DummyOperator(task_id="end_task")

start_task >> first_task >> second_task >> end_task