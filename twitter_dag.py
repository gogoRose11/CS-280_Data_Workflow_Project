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
import pandas as pd


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
    tweet_fields = "public_metrics,author_id,text,created_at"
    for user in users_list:
        log.info(f"Getting TWEETS for: {user.name}")
        response = requests.get(tweets_url, headers=get_auth_header(), params={"user_id": user.user_id, "tweet.fields": tweet_fields, "count": 5})
        tweets_list.append(response.json())
    
    log.info(f"TWEETS LIST: {tweets_list}")

    # PULL USER INFORMATION AND PASS IT TO THE NEXT TASK
    # PUSH TO NEXT TASK
    ti.xcom_push("users_list", users_list)
    ti.xcom_push("tweet_list", tweets_list)
    return



def call_api_task_function(ti: TaskInstance, **kwargs):
    log.info("ENTERED: CALL API TASK FUNCTION")
    users_list = ti.xcom_pull(key='users_list', task_ids='load_data_task')
    tweets_list = ti.xcom_pull(key='tweet_list', task_ids='load_data_task')


    # GET UPDATED STATISTICS FOR EVERY USER
    user_requests = []
    #user_ids = Variable.get(f"TWITTER_USER_IDS", [], deserialize_json=True)
    user_fields = "public_metrics,username,id,name,created_at"
    for user in users_list:
        user_url = f"https://api.twitter.com/2/users/{user.user_id}"
        request = requests.get(user_url, headers=get_auth_header(), params={"user.fields": user_fields})
        log.info(f"USER REQUEST: {user.name}")
        log.info(request.json())
        user_requests.append(request.json())
    

    ti.xcom_push("users_list", users_list)
    ti.xcom_push("tweet_list", tweets_list)
    ti.xcom_push("users_requests", user_requests)
    return

def transform_data_task_function(ti: TaskInstance, **kwargs):
    log.info("ENTERED: TRANSFORM DATA TASK FUNCTION")
    users_list = ti.xcom_pull(key='users_list', task_ids='call_api_task')
    tweets_requests = ti.xcom_pull(key='tweet_list', task_ids='call_api_task')
    user_requests = ti.xcom_pull(key='users_requests', task_ids='call_api_task')

    user_df = get_user_pd(user_requests)
    return


def get_tweet_pd(tweet_requests):
    tweet_df = pd.DataFrame(columns=['id','text','retweet_count','reply_count','like_count','quote_count','impression_count'])

    # LOOP THROUGH TWEET RESPONSES
    for tweet in tweet_requests:
        #resp = tweet.json()
        resp = tweet
        data = resp.get('data')
        del data['edit_history_tweet_ids']
        del data['author_id']
        pub = data['public_metrics']
        del data['public_metrics']
        data.update(pub)
        tweet_df = tweet_df.append(data, ignore_index=True)

    return tweet_df


def get_user_pd(user_requests):
    user_df = pd.DataFrame(columns=['id','username','name','followers_count','following_count','tweet_count','listed_count','created_at'])

    # LOOP THROUGH USER RESPONSES
    for user in user_requests:
        #log.info("ENTERED USER REQUESTS FOR LOOP - GET USER PD")
        #resp = user.json()
        resp = user
        data = resp.get('data')
        #del data['profile_image_url']
        #del data['description']
        pub = data['public_metrics']
        del data['public_metrics']
        data.update(pub)
        user_df = user_df.append(data, ignore_index=True)

    log.info("USER DATAFRAME AT THE END OF GET USER PD")
    log.info(user_df)
    return user_df

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