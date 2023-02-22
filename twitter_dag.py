from airflow import DAG
import logging as log
import pendulum
import requests
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from models.config import Session #You would import this from your config file
from models.user import User
from airflow.models import Variable
from airflow.models import TaskInstance


def get_auth_header():
  my_bearer_token = Variable.get("TWITTER_BEARER_TOKEN")
  return {"Authorization": f"Bearer {my_bearer_token}"}


def twitter_request_task_function(user_id, request_type_url):
    #user_id = "44196397"
    api_url = f"https://api.twitter.com/2/users/{user_id}{request_type_url}"
    request = requests.get(api_url, headers=get_auth_header())
    #print(request.json())
    return request


def load_data_task_function(ti: TaskInstance, **kwargs):
    log.info("ENTERED: LOAD DATA TASK FUNCTION")

    session = Session()
    # This will retrieve all of the users from the database 
    # (It'll be a list, so you may have 100 users or 0 users)
    users_list = session.query(User).all() 
    log.info(f"USER LIST: {users_list}")
    session.close()


    # last 5 tweets
    tweets_url = f'https://api.twitter.com/1.1/statuses/user_timeline.json'
    tweets_list = []
    for user in users_list:
        log.info(f"Getting TWEETS for: {user.name}")
        response = requests.get(tweets_url, headers=get_auth_header(), params={"user_id": user.user_id, "count": 5})
        tweets_list.append(response.json())
    
    log.info(f"TWEETS LIST: {tweets_list}")

    # PULL USER INFORMATION AND PASS IT TO THE NEXT TASK
    # PUSH TO NEXT TASK
    ti.xcom_push("users_list", users_list)
    ti.xcom_push("tweet_list", tweets_list)
    return

def call_api_task_function(ti: TaskInstance, **kwargs):
    log.info("ENTERED: CALL API TASK FUNCTION")
    
    return

def transform_data_task_function(ti: TaskInstance, **kwargs):
    log.info("ENTERED: TRANSFORM DATA TASK FUNCTION")
    
    return

def write_data_task_function(ti: TaskInstance, **kwargs):
    log.info("ENTERED: WRITE DATA TASK FUNCTION")
    
    return

with DAG(
    dag_id="Twitter_DAG",
    schedule_interval="0 9 * * *",
    start_date=pendulum.datetime(2023, 2, 10, tz="US/Pacific"),
    catchup=False,
) as dag:
    
    load_data_task = PythonOperator(task_id="load_data_task", python_callable=load_data_task_function)
    call_api_task = PythonOperator(task_id="call_api_task", python_callable=call_api_task_function)
    transform_data_task = PythonOperator(task_id="transform_data_task", python_callable=transform_data_task_function)
    write_data_task = PythonOperator(task_id="write_data_task", python_callable=write_data_task_function)
    

load_data_task >> call_api_task >> transform_data_task >> write_data_task