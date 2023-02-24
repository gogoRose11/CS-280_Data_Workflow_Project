from airflow import DAG
import logging as log
import pendulum
import requests
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from models.config import Session #You would import this from your config file
from models.user import User
from models.user_timeseries import User_Timeseries
from models.tweet import Tweet
from models.tweet_timeseries import Tweet_Timeseries
from airflow.models import Variable
from airflow.models import TaskInstance
import pandas as pd
from datetime import datetime
from google.cloud import storage
import io

def get_auth_header():
  my_bearer_token = Variable.get("TWITTER_BEARER_TOKEN")
  return {"Authorization": f"Bearer {my_bearer_token}"}


def load_data_task_function(ti: TaskInstance, **kwargs):
    log.info("ENTERED: LOAD DATA TASK FUNCTION")

    # RETRIEVE THE 10 USERS IN THE DATABASE
    session = Session()
    users_list = session.query(User).all() 
    log.info(f"USER LIST: {users_list}")
    session.close()

    # RETRIEVE ALL TWEETS IN THE DATABASE
    session = Session()
    tweets_list = session.query(Tweet).all()
    #log.info(f"Size of TWEETS LIST: {len(tweets_list)}")
    session.close()


    # RETRIEVE LAST 5 TWEETS FOR EVERY USER IN THE DATABASE
    tweets_url = f'https://api.twitter.com/1.1/statuses/user_timeline.json'
    last_five_tweets = []
    tweet_fields = "public_metrics,author_id,text,created_at"
    for user in users_list:
        #log.info(f"Getting TWEETS for: {user.name}")
        response = requests.get(tweets_url, headers=get_auth_header(), params={"user_id": user.user_id,"count": 5, 'include_rts':False,
                                                                               'exclude_replies':True})
        #log.info(f"{user.name} Tweets: {response.json()}")
        last_five_tweets.append(response.json())
    
    

    # PUSH USERS LIST, TWEET LIST, AND LAST 5 TWEETS PER USER TO CALL API TASK
    ti.xcom_push("users_list", users_list)
    ti.xcom_push("tweet_list", tweets_list)
    ti.xcom_push('last_five_tweets', last_five_tweets) # last_5_tweets
    return



def call_api_task_function(ti: TaskInstance, **kwargs):
    log.info("ENTERED: CALL API TASK FUNCTION")

    # PULL USERS LIST, TWEET LIST, AND LAST FIVE TWEETS FROM PREVIOUS LOAD DATA TASK
    users_list = ti.xcom_pull(key='users_list', task_ids='load_data_task')
    tweet_list = ti.xcom_pull(key='tweet_list', task_ids='load_data_task')
    last_five_tweets = ti.xcom_pull(key='last_five_tweets', task_ids='load_data_task') # last_5_tweets

    # GET UPDATED STATISTICS FOR EVERY USER
    updated_users = []
    user_fields = "public_metrics,username,id,name,created_at"
    for user in users_list:
        user_url = f"https://api.twitter.com/2/users/{user.user_id}"
        request = requests.get(user_url, headers=get_auth_header(), params={"user.fields": user_fields})
        #log.info(f"USER REQUEST: {user.name}")
        #log.info(request.json())
        updated_users.append(request.json())


    # GET UPDATED STATISTICS FOR EVERY TWEET
    updated_tweets = []
    tweet_fields = "public_metrics,author_id,text,created_at"
    for tweet in tweet_list:
        tweet_url = f"https://api.twitter.com/2/tweets/{tweet.tweet_id}"
        request = requests.get(tweet_url, headers=get_auth_header(), params={"tweet.fields": tweet_fields})
        #log.info(f"TWEET REQUEST: {tweet}")
        #log.info(request.json())
        updated_tweets.append(request.json())
        # WHEN DO I ADD THE DATE COLUMN??? WHEN I ADD IT TO THE DATABASE?

    

    # PUSH LAST FIVE TWEETS, UPDATED TWEETS, AND UPDATED USERS TO TRANSFORM DATA TASK
    ti.xcom_push("last_five_tweets", last_five_tweets)
    ti.xcom_push('updated_tweets', updated_tweets)
    ti.xcom_push("updated_users", updated_users)
    ti.xcom_push('users_list', users_list)
    return

def transform_data_task_function(ti: TaskInstance, **kwargs):
    log.info("ENTERED: TRANSFORM DATA TASK FUNCTION")

    # PULL LAST FIVE TWEETS, UPDATED TWEETS, AND UPDATED USERS FROM CALL API TASK
    last_five_tweets = ti.xcom_pull(key='last_five_tweets', task_ids='call_api_task')
    updated_tweets = ti.xcom_pull(key='updated_tweets', task_ids='call_api_task')
    updated_users = ti.xcom_pull(key='updated_users', task_ids='call_api_task')

    # CREATE USERS DATAFRAME
    user_df = get_user_pd(updated_users)

    # SEND USERS DATAFRAME TO GOOGLE CLOUD BUCKET
    user_client = storage.Client()
    user_bucket = user_client.get_bucket("e-r-apache-airflow-cs280")
    user_bucket.blob("data/users.csv").upload_from_string(user_df.to_csv(index=False), "text/csv")

    # CREATE TWEET DATAFRAME
    tweet_df = get_tweet_pd(last_five_tweets, updated_tweets)
    
    # SEND TWEETS DATAFRAME TO GOOGLE CLOUD BUCKET
    tweet_client = storage.Client()
    tweet_bucket = tweet_client.get_bucket("e-r-apache-airflow-cs280")
    tweet_bucket.blob("data/tweets.csv").upload_from_string(tweet_df.to_csv(index=False), "text/csv")


    return

def get_user_pd(user_requests):
    user_df = pd.DataFrame(columns=['user_id', 'username','name','created_at','followers_count','following_count','tweet_count','listed_count','date'])
    # user_id, username, name, created_at .... followers_count, following_count, tweet_count, listed_count, date


    # LOOP THROUGH USER RESPONSES
    for user in user_requests:
        #log.info("ENTERED USER REQUESTS FOR LOOP - GET USER PD")
        #resp = user.json()
        resp = user
        data = resp.get('data')
        data['user_id'] = data['id']
        data['date'] = datetime.now()
        del data['id']
        #del data['profile_image_url']
        #del data['description']
        pub = data['public_metrics']
        del data['public_metrics']
        data.update(pub)


        user_df = user_df.append(data, ignore_index=True)
        

    #log.info("USER DATAFRAME AT THE END OF GET USER PD")
    #log.info(user_df)
    return user_df


def get_tweet_pd(last_five_tweets, tweet_requests):
    tweet_df = pd.DataFrame(columns=['tweet_id', 'user_id', 'text', 'created_at', 'retweet_count', 'favorite_count', 'date'])
    #log.info(f"TWEET DF AT BEGINNING")
    #log.info(tweet_df)

   # log.info(f"LAST FIVE TWEETS")
    #log.info(last_five_tweets)
    # PARSE THROUGH LAST FIVE TWEETS
    for t in last_five_tweets:
        for tweet in t:
            user = tweet['user']
            data = {}
            data['tweet_id'] = tweet['id']
            data['user_id'] = user['id']
            data['text'] = tweet['text']
            data['created_at'] = tweet['created_at']
            data['retweet_count'] = tweet['retweet_count']
            data['favorite_count'] = tweet['favorite_count']
            data['date'] = datetime.now()
            tweet_df = tweet_df.append(data, ignore_index=True)


    # PARSE THROUGH ALL TWEETS
    for req in tweet_requests:
        tweet = req['data']
        pub = tweet['public_metrics']
        data = {}
        data['tweet_id'] = tweet['id']
        data['user_id'] = tweet['author_id']
        data['text'] = tweet['text']
        data['created_at'] = tweet['created_at']
        data['retweet_count'] = pub['retweet_count']
        data['favorite_count'] = pub['like_count']
        data['date'] = datetime.now()
        tweet_df = tweet_df.append(data, ignore_index=True)

    
    #log.info(f"TWEET DF AT END")
    #log.info(tweet_df)

    return tweet_df




def write_data_task_function(ti: TaskInstance, **kwargs):
    log.info("ENTERED: WRITE DATA TASK FUNCTION")
    
    user_df = pd.DataFrame(columns=['user_id', 'username','name','created_at','followers_count','following_count','tweet_count','listed_count','date'])
    #log.info(f"USER DATAFRAME BEFORE GOOGLE CLOUD CALL")
    #log.info(user_df)

    # GET USER.CSV FROM GOOGLE BUCKET
    user_client = storage.Client()
    user_bucket = user_client.get_bucket("e-r-apache-airflow-cs280")
    blob = user_bucket.get_blob('data/users.csv')

    # CONVERT USERS CSV TO PANDAS DATAFRAME
    users_csv = blob.download_as_string()
    user_df = pd.read_csv(io.BytesIO(users_csv))

    log.info(f"USER DATAFRAME AFTER GOOGLE CLOUD CALL")
    log.info(user_df)

    update_user_timeseries(user_df)


    tweet_df = pd.DataFrame(columns=['tweet_id', 'user_id', 'text', 'created_at', 'retweet_count', 'favorite_count', 'date'])

    # GET TWEET.CSV FROM GOOGLE CLIENT
    tweet_client = storage.Client()
    tweet_bucket = tweet_client.get_bucket("e-r-apache-airflow-cs280")
    blob = tweet_bucket.get_blob('data/tweets.csv')

    # CONVERT TWEETS CSV TO PANDAS DATAFRAME
    tweet_csv = blob.download_as_string()
    tweet_df = pd.read_csv(io.BytesIO(tweet_csv))

    update_tweet_timeseries(tweet_df)

    return

def update_user_timeseries(user_df):

    # RETRIEVE THE 10 USERS IN THE DATABASE
    session = Session()
    users_timeseries_list = session.query(User_Timeseries).all() 
    users_list = session.query(User).all()
    #log.info(f"USER LIST: {users_list}")

    
    for index, row in user_df.iterrows():
        curr_user = session.query(User).filter(User.user_id == row['user_id']).first()
        #log.info(f"CURR_USER: {curr_user}")
        user = User_Timeseries(user_id=row['user_id'], followers_count=row['followers_count'],
                               following_count=row['following_count'], tweet_count=row['tweet_count'],
                               listed_count=row['listed_count'], date=row['date'])
        session.add(user)
    session.commit()
    session.close()

def update_tweet_timeseries(tweet_df):

    # IF TWEET IS NOT IN DATABASE
        # ADD TWEET TO DATABASE
    # UPDATE TIMESERIES

    log.info("TWEET DATAFRAME")
    log.info(tweet_df)
    tweet_df = pd.DataFrame(columns=['tweet_id', 'user_id', 'text', 'created_at', 'retweet_count', 'favorite_count', 'date'])

    session = Session()
    tweet_timeseries_list = session.query(Tweet_Timeseries).all() 
    tweet_list = session.query(Tweet).all()

    curr_tweet_ids = []
    for tweet in tweet_list:
        curr_tweet_ids.append(tweet.tweet_id)
    
    for index, row in tweet_df.iterrows():
        add = True
        if row['tweet_id'] in curr_tweet_ids:
            add = False
        
        if add == True:
            tweet = Tweet(tweet_id=row['tweet_id'], user_id=row['user_id'], text=row['text'], created_at=row['created_at'])
            session.add(tweet)
        

        tweet_timeseries = Tweet_Timeseries(tweet_id=row['tweet_id'], retweet_count=row['retweet_count'], 
                                            favorite_count=row['favorite_count'], date=row['date'])
        session.add(tweet_timeseries)
    
    session.commit()
    session.close()



    


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