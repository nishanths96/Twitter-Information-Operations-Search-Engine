#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
from elasticsearch import Elasticsearch
import json
import pandas as pd
from elasticsearch import helpers
from tqdm import tqdm
import ast


# In[2]:


csv_file_list = ["../Data/iran_201901_1_tweets_csv_hashed_1.csv", 
#                  "../Data/iran_201901_1_tweets_csv_hashed_2.csv",
#                 "../Data/iran_201901_1_tweets_csv_hashed_3.csv", 
#                  "../Data/iran_201901_1_tweets_csv_hashed_4.csv",
                "../Data/russian_linked_tweets_csv_hashed.csv",
                "../Data/venezuela_201901_1_tweets_csv_hashed_1.csv"]
#                 "../Data/venezuela_linked_tweets_csv_hashed.csv"]


# In[3]:


all_keys = ['tweetid', 'userid', 'user_display_name', 'user_screen_name', 'user_reported_location', 'user_profile_description',
          'user_profile_url', 'follower_count', 'following_count', 'account_creation_date', 'account_language', 'tweet_language',
          'tweet_text', 'tweet_time', 'tweet_client_name', 'in_reply_to_userid', 'in_reply_to_tweetid', 'quoted_tweet_tweetid',
           'is_retweet', 'retweet_userid', 'retweet_tweetid', 'quote_count', 
          'reply_count', 'like_count', 'retweet_count', 'hashtags', 'urls', 'user_mentions']


# In[4]:


null_values = {
    'tweetid':"", 'userid': "", 'user_display_name': "", 'user_screen_name': "", 
    'user_reported_location': "", 'user_profile_description': "", 'user_profile_url': "", 
    'follower_count':0, 'following_count':0, 'account_creation_date': "0000-00-00",     
    'account_language': "NULL", 'tweet_language': "NULL", 'in_reply_to_userid': "", 
    'tweet_text': "", 'tweet_time': "0000-00-00 00:00", 'tweet_client_name': "", 
    'in_reply_to_tweetid': "", 'in_reply_to_tweetid': "", 'quoted_tweet_tweetid': "", 
    'is_retweet': 2, 'retweet_userid': "", 'retweet_tweetid': "", 
    'quote_count': 0, 'reply_count': 0, 'like_count': 0, 'retweet_count': 0,
#     'user_mentions': []
    }


# In[5]:


error_count = 0
idx = 0
syntax_error_count = 0


# In[6]:


# function to connect to the elastic-search server
# @args: provide the "hostname" and the "port" of the server
def connect_elasticsearch(hostname, port):
    _es = None
    _es = Elasticsearch([{'host': hostname, 'port': port}])
    if _es.ping():
        print('Connected to the server')
    else:
        print('Error in connecting...')
    return _es


# In[9]:


def filterKeys(document):
    return {key: document[key] for key in all_keys}


# In[10]:


def doc_generator_for_doc(df):
    global idx
    prev_idx = idx
    df_iter = df.iterrows()
    for index, document in df_iter:
        yield {
                "_index": "twitter_index",
                "_type": "_doc",
                "_id" : idx,
                "_source": filterKeys(document)
            }
        idx += 1

#     raise StopIteration


# In[11]:


def index_chunk(chunk):
    global error_count
    try:
        helpers.bulk(es, doc_generator_for_doc(chunk))
    except:
        error_count = error_count + 1
        print("Bulk-index error occurred: ", error_count)
        pass


# In[13]:


def filterKeys(document):
    return {key: document[key] for key in all_keys}


# In[14]:


def doc_generator_for_doc(df):
    global idx
    prev_idx = idx
    df_iter = df.iterrows()
    for index, document in df_iter:
        yield {
                "_index": "twitter_index",
                "_type": "_doc",
                "_id" : idx,
                "_source": filterKeys(document)
            }
        idx += 1

#     raise StopIteration


# In[15]:


es = connect_elasticsearch("localhost", 9200)


# In[16]:


for file in tqdm(csv_file_list):
    # read the file
    for df in pd.read_csv(file, encoding='utf-8', chunksize = 1000):
    
        # convert it into relevant data-types
        df['is_retweet'] = df['is_retweet'].astype('int')
        df['account_creation_date'] = pd.to_datetime(df['account_creation_date'])
        df['tweet_time'] = pd.to_datetime(df['tweet_time'])

        # fill_na values with corresponding values
        df.fillna(value=null_values, inplace=True)

        # take care of Array's within the pandas dataframe - columns:
        for row in df.loc[df.user_mentions.isnull(), 'user_mentions'].index:
            df.at[row, 'user_mentions'] = '[]'

        for row in df.loc[df.urls.isnull(), 'urls'].index:
            df.at[row, 'urls'] = '[]'

        for row in df.loc[df.hashtags.isnull(), 'hashtags'].index:
            df.at[row, 'hashtags'] = '[]'
        try:    
            for index, row in df.iterrows():
                df.at[index, 'urls'] = ast.literal_eval( row['urls'] )
                df.at[index, 'user_mentions'] = ast.literal_eval( row['user_mentions'] )
                df.at[index, 'hashtags'] = ast.literal_eval( row['hashtags'] )

        except SyntaxError:
            list_of_strings = str(row[1:-1]).split(',')
            syntax_error_count += 1
            if syntax_error_count % 50 == 0:
                print("Syntax Error Count thus far: ", syntax_error_count)

            continue

        # INDEXING: 

        # get the data for indexing
        df = df[all_keys]

        # index the data 
        index_chunk(df)


# In[ ]:




