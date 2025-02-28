import sys

import numpy as np
import pandas as pd
import praw
from praw import Reddit
from praw.exceptions import PRAWException
from prawcore.exceptions import ResponseException
from typing import List, Dict
from praw.models import Submission
from utils.constants import POST_FIELDS
from pathlib import Path


# Define credentials
'''
CLIENT_ID = 'tZQCml2RCGddrEUYZnaTvQ'
SECRET = 'mvBSuYPm6LSa3wFglDZgxLH2FiE3Xg'
USER_AGENT = 'your_user_agent'
limit=10
time_filter='day'
subreddit_name='dataengineering'
'''
# Function to connect to Reddit

def connect_reddit(client_id, client_secret, user_agent) -> Reddit:
    try:
        reddit = praw.Reddit(client_id=client_id,
                             client_secret=client_secret,
                             user_agent=user_agent)
        print("connected to reddit!")
        return reddit
    except Exception as e:
        print(f" Reddit connection failed: {e}")
        sys.exit(1)

# Establish connection
#reddit_instance = connect_reddit(CLIENT_ID, SECRET, USER_AGENT)

# Function to extract posts

def extract_posts(reddit_instance: praw.Reddit, subreddit_name: str, time_filter: str,limit: int ) -> List[Dict]:
    
    POST_FIELDS = (
        'id', 'title', 'score', 'num_comments', 'author', 'created_utc',
        'url', 'over_18', 'edited', 'spoiler', 'stickied'
    )
    
    try:
        subreddit = reddit_instance.subreddit(subreddit_name)
        posts = subreddit.top(time_filter=time_filter, limit=limit)
        post_list = []

        for post in posts:
            post_dict = vars(post)
            filtered_post = {key: post_dict[key] for key in POST_FIELDS if key in post_dict}
            post_list.append(filtered_post)

        return post_list
    except (PRAWException, ResponseException) as e:
        print(f"An error occurred while extracting posts: {e}")
        return []



#Transform posts

def transform_data(post_df: pd.DataFrame):
    post_df['created_utc'] = pd.to_datetime(post_df['created_utc'], unit='s')
    post_df['over_18'] = np.where((post_df['over_18'] == True), True, False)
    post_df['author'] = post_df['author'].astype(str)
    edited_mode = post_df['edited'].mode()
    post_df['edited'] = np.where(post_df['edited'].isin([True, False]), post_df['edited'], edited_mode).astype(bool) 
    post_df['num_comments'] = post_df['num_comments'].astype(int)
    post_df['score'] = post_df['score'].astype(int)
    post_df['title'] = post_df['title'].astype(str)

    return post_df



def load_data_to_csv(data: pd.DataFrame, path: str):
    data.to_csv(path, index=False)