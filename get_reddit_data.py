# Databricks notebook source
client_id = dbutils.secrets.get(scope="my-secrets", key="reddit-client-id")
client_secret = dbutils.secrets.get(scope="my-secrets", key="reddit-client-secret")
user_agent = dbutils.sbecrets.get(scope="my-secrets", key="reddit-user-agent")

# COMMAND ----------

import praw
# Initialize Reddit API with credentials
reddit = praw.Reddit(client_id=client_id, 
                     client_secret=client_secret,
                     user_agent = user_agent)

# COMMAND ----------

# Specify the subreddit and time period
from datetime import datetime

subreddit_name = 'wallstreetbets'  # Example: penny stocks subreddit
subreddit = reddit.subreddit(subreddit_name)

post_data = []
for submission in subreddit.top(time_filter = 'all', limit=None):  # Fetch top posts for the past year
    post_info = {
        'title': submission.title,
        'score': submission.score,
        'id': submission.id,
        'url': submission.url,
        'created_utc': datetime.utcfromtimestamp(submission.created_utc).strftime('%Y-%m-%d %H:%M:%S'),
        'num_comments': submission.num_comments,
        'author': str(submission.author),
        'selftext': submission.selftext  # The content of the post
    }
    post_data.append(post_info)

# COMMAND ----------

post_data = spark.createDataFrame(post_data)

# COMMAND ----------

post_data.count()

# COMMAND ----------

post_data.write.format("delta").option("mergeSchema", "true").mode("overwrite").saveAsTable("hive_metastore.bronze.wallstreetbets_posts")

# COMMAND ----------

subreddit_name = 'pennystocks'  # Example: penny stocks subreddit
subreddit = reddit.subreddit(subreddit_name)

post_data = []
for submission in subreddit.top(time_filter = 'all', limit=None):  # Fetch top posts for the past year
    post_info = {
        'title': submission.title,
        'score': submission.score,
        'id': submission.id,
        'url': submission.url,
        'created_utc': datetime.utcfromtimestamp(submission.created_utc).strftime('%Y-%m-%d %H:%M:%S'),
        'num_comments': submission.num_comments,
        'author': str(submission.author),
        'selftext': submission.selftext  # The content of the post
    }
    post_data.append(post_info)

# COMMAND ----------

post_data = spark.createDataFrame(post_data)
post_data.write.format("delta").option("mergeSchema", "true").mode("overwrite").saveAsTable("hive_metastore.bronze.pennystocks_posts")
