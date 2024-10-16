# Databricks notebook source
# MAGIC %md
# MAGIC load needed libraries and authenticate with the alpaca API and import needed libraries

# COMMAND ----------

from tqdm import tqdm
import time
from tqdm import tqdm
import pandas as pd
from datetime import datetime
import alpaca_trade_api as tradeapi
from datetime import datetime, timedelta


from pyspark.sql.functions import col, max
from pyspark.sql.utils import AnalysisException
from pyspark.sql.types import StructType, StructField, StringType, DateType, DoubleType, IntegerType, LongType


# get secrets needed for connection with alpaca API
base_url = dbutils.secrets.get(scope="my-secrets", key="alpaca-api-base-url")
api_key = dbutils.secrets.get(scope="my-secrets", key="alpaca-api-key")
secret_key = dbutils.secrets.get(scope="my-secrets", key="alpaca-api-secret-key")
api = tradeapi.REST(api_key, secret_key, base_url, api_version='v2')

# this is the default start time if we have no prior stored stock date
default_start_date = "2010-01-01"

# COMMAND ----------

# MAGIC %md
# MAGIC - The `most_recent_date` variable should be passed from the update_stock_history notebook.

# COMMAND ----------

try:
    # Load the table into a DataFrame
    df = spark.table("hive_metastore.bronze.stock_history")

    # Retrieve the most recent date value
    most_recent_date = df.select(max(col("time"))).collect()[0][0]
    # Add exactly one day to the most recent date
    most_recent_date += timedelta(days=1)
    most_recent_date_str = most_recent_date.strftime('%Y-%m-%d')
    most_recent_date_str
except AnalysisException:
    # If the table does not exist, return an empty datetime value
    most_recent_date_str = default_start_date

most_recent_date_str

# COMMAND ----------

# MAGIC %md
# MAGIC The following query will help us identify which ticker symbols to collect data for, we are currently commenting this out since we'll need to add nuances in the workflow. 

# COMMAND ----------

symbols = df.select(col("symbol")).distinct().collect()
symbol_list = [row['symbol'] for row in symbols]

# COMMAND ----------

timeframe = '1Day'
bar_data = []
start_date = f"{most_recent_date_str}T00:00:00Z"

# loop through each ticker symbol and retreive stock market data for all days from last time run
for symbol in tqdm(symbol_list, desc=f"Fetching daily stock prices"):
    bars = api.get_bars(symbol, timeframe, start=start_date, limit=10000)
    for bar in bars:
        bar_data.append({
            'symbol': symbol,
            'time': bar.t,
            'open': bar.o,
            'high': bar.h,
            'low': bar.l,
            'close': bar.c,
            'volume': bar.v
        })
        
    # alpaca API call limit is 200 per minute, so lets slow down to 120 per minute
    time.sleep(0.5)

# COMMAND ----------

# Convert pandas.Timestamp to datetime.date
for record in bar_data:
    if isinstance(record['time'], pd.Timestamp):
        record['time'] = record['time'].date()
    record['open'] = float(record['open'])
    record['high'] = float(record['high'])
    record['low'] = float(record['low'])
    record['close'] = float(record['close'])

# Define the schema with consistent data types
schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("time", DateType(), True),
    StructField("open", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("close", DoubleType(), True),
    StructField("volume", LongType(), True),
    
    # Add more fields as necessary
])

# Create DataFrame with the defined schema
bar_df = spark.createDataFrame(bar_data, schema)
display(bar_df)

# COMMAND ----------

bar_df.count()

# COMMAND ----------

if most_recent_date_str != default_start_date:
    bar_df.write.format("delta").option("mergeSchema", "true").mode("append").saveAsTable("hive_metastore.bronze.stock_history")
else:
    bar_df.write.format("delta").option("mergeSchema", "true").mode("overwrite").saveAsTable("hive_metastore.bronze.stock_history")
