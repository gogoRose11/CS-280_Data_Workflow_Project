from airflow import DAG
import logging as log
import pendulum
import requests
import pandas as pd
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from google.cloud import storage
from airflow.models import Variable
from airflow.models import TaskInstance
from databox import Client



def get_auth_header():
  my_bearer_token = Variable.get("TWITTER_BEARER_TOKEN")
  #my_bearer_token = "AAAAAAAAAAAAAAAAAAAAACwllgEAAAAA2pCuFW3x5ABAGkB5%2F%2F5n1CyTCNs%3DleybJQXEwrhTAuIPr20a49NhY5R2ii0SsvMoVxn4Beg3Zt7oL9"
  return {"Authorization": f"Bearer {my_bearer_token}"}


def twitter_request_task_function(user_id, request_type_url):
    #user_id = "44196397"
    api_url = f"https://api.twitter.com/2/users/{user_id}{request_type_url}"
    request = requests.get(api_url, headers=get_auth_header())
    #print(request.json())
    return request


def get_twitter_api_data_func(ti: TaskInstance, **kwargs):
    # USER REQUESTS
    user_requests = []
    user_ids = Variable.get(f"TWITTER_USER_IDS", [], deserialize_json=True)
    user_fields = "public_metrics,profile_image_url,username,description,id"
    for user in user_ids:
        user_url = f"https://api.twitter.com/2/users/{user}"
        request = requests.get(user_url, headers=get_auth_header(), params={"user.fields": user_fields})
        log.info(f"USER REQUEST: {user}")
        log.info(request.json())
        user_requests.append(request.json())
    

    # TWEET REQUESTS
    tweet_requests = []
    tweet_ids = Variable.get(f"TWITTER_TWEET_IDS", [], deserialize_json=True)
    tweet_fields = "public_metrics,author_id,text"
    for tweet in tweet_ids:
        tweet_url = f"https://api.twitter.com/2/tweets/{tweet}"
        request = requests.get(tweet_url, headers=get_auth_header(), params={"tweet.fields": tweet_fields})
        log.info(f"TWEET REQUEST: {tweet}")
        log.info(request.json())
        tweet_requests.append(request.json())

    # PUSH TO NEXT TASK
    ti.xcom_push("user_requests", user_requests)
    ti.xcom_push("tweet_requests", tweet_requests)



def transform_twitter_api_data_func(ti: TaskInstance, **kwargs):
    user_requests = ti.xcom_pull(key="user_requests", task_ids="twitter_extract_task")
    user_df = get_user_pd(user_requests)

    user_client = storage.Client()
    user_bucket = user_client.get_bucket("e-r-apache-airflow-cs280")
    log.info("MADE IT PAST FIRST USER_DF")
    user_bucket.blob("data/users.csv").upload_from_string(user_df.to_csv(index=False), "text/csv")

    
    tweet_requests = ti.xcom_pull(key="tweet_requests", task_ids="twitter_extract_task")
    tweet_df = get_tweet_pd(tweet_requests)

    client = storage.Client()
    bucket = client.get_bucket("e-r-apache-airflow-cs280")
    bucket.blob("data/tweets.csv").upload_from_string(tweet_df.to_csv(index=False), "text/csv")

    log.info("ABOUT TO PUSH TO NEXT TASK")

    # PUSH TO NEXT TASK
    user_json = user_df.to_json()
    tweet_json = tweet_df.to_json()
    ti.xcom_push("user_df", user_json)
    ti.xcom_push("tweet_df", tweet_json)



def databox_helper_users(user_df, client):

    for index in user_df.index:
        name = user_df['username'][index]
        followers_count = user_df['followers_count'][index]
        following_count = user_df['following_count'][index]
        tweet_count = user_df['tweet_count'][index]
        listed_count = user_df['listed_count'][index]
        name_followers_count = name + '_followers_count'
        name_following_count = name + '_following_count'
        name_tweet_count = name + '_tweet_count'
        name_listed_count = name + '_listed_count'
        #print(f"{full_name}: {followers_count}")
        client.push(name_followers_count, followers_count)
        client.push(name_following_count, following_count)
        client.push(name_tweet_count, tweet_count)
        client.push(name_listed_count, listed_count)
        log.info(f"PUSHED: {name}")


def databox_helper_tweets(tweet_df, client):

    for index in tweet_df.index:
        name = tweet_df['id'][index]
        reply_count = tweet_df['reply_count'][index]
        like_count = tweet_df['like_count'][index]
        impression_count = tweet_df['impression_count'][index]
        retweet_count = tweet_df['retweet_count'][index]
        name_reply_count = name + '_reply_count'
        name_like_count = name + '_like_count'
        name_impression_count = name + '_impression_count'
        name_retweet_count = name + '_retweet_count'
        #print(f"{full_name}: {reply_count}")
        client.push(name_reply_count, reply_count)
        client.push(name_like_count, like_count)
        client.push(name_impression_count, impression_count)
        client.push(name_retweet_count, retweet_count)
        log.info(f"PUSHED: {name}")

def load_data_func(ti:TaskInstance, **kwargs):

    log.info("ENTERED LOAD DATA FUNCTION")
    client = Client("lfshpao6g48kls6t0nav0p")
    user_json = ti.xcom_pull(key="user_df", task_ids="transform_twitter_api_data_task")
    user_df = pd.read_json(user_json)
    databox_helper_users(user_df, client)

    tweet_json = ti.xcom_pull(key="tweet_df", task_ids="transform_twitter_api_data_task")
    tweet_df = pd.read_json(tweet_json)
    databox_helper_tweets(tweet_df, client)






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
    user_df = pd.DataFrame(columns=['id','username','name','followers_count','following_count','tweet_count','listed_count'])

    # LOOP THROUGH USER RESPONSES
    for user in user_requests:
        #resp = user.json()
        resp = user
        data = resp.get('data')
        del data['profile_image_url']
        del data['description']
        pub = data['public_metrics']
        del data['public_metrics']
        data.update(pub)
        user_df = user_df.append(data, ignore_index=True)

    return user_df




with DAG(
    dag_id="project_lab_1_etl",
    schedule_interval="0 9 * * *",
    start_date=pendulum.datetime(2023, 1, 1, tz="US/Pacific"),
    catchup=False,
) as dag:
    get_twitter_api_data_task = PythonOperator(
        task_id="twitter_extract_task",
        python_callable=get_twitter_api_data_func,
        provide_context=True,
    )
    transform_twitter_api_data_task = PythonOperator(
        task_id="twitter_transform_task",
        python_callable=transform_twitter_api_data_func,
        provide_context=True,
    )
    load_data_task = PythonOperator(
        task_id="databox_load_task",
        python_callable=load_data_func,
        provide_context=True,
    )
    

get_twitter_api_data_task >> transform_twitter_api_data_task >> load_data_task