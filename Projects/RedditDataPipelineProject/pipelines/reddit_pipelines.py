import pandas as pd
from utils.constants import CLIENT_ID, SECRET , OUTPUT_PATH
from etls.reddit_etls import extract_posts, connect_reddit, transform_data,  load_data_to_csv
import praw
import json
from datetime import datetime



SECRET ='mvBSuYPm6LSa3wFglDZgxLH2FiE3Xg'
CLIENT_ID = 'tZQCml2RCGddrEUYZnaTvQ'
output_path='/opt/airflow/data/output'

def reddit_pipeline(file_name: str, subreddit_name:str , time_filter:str , limit=None):
    # connecting to reddit instance
    reddit_instance = connect_reddit(CLIENT_ID, SECRET, 'Project_user_agent')
    # extraction
    posts = extract_posts(reddit_instance, subreddit_name,time_filter , limit)
#   for post in posts:
#      print(f"Title: {post['title']}, Score: {post['score']}, URL: {post['url']}")
    post_df = pd.DataFrame(posts)
    # transformation
    post_df = transform_data(post_df)
    # loading to csv
    #file_path = f'{OUTPUT_PATH}/{file_name}.csv'
    
    file_path = f'{OUTPUT_PATH}/{file_name}.csv'
    load_data_to_csv(post_df, file_path)
    return file_path