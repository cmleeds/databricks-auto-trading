# Databricks notebook source
# get secrets needed for connection with alpaca API
base_url = dbutils.secrets.get(scope="my-secrets", key="alpaca-api-base-url")
api_key = dbutils.secrets.get(scope="my-secrets", key="alpaca-api-key")
secret_key = dbutils.secrets.get(scope="my-secrets", key="alpaca-api-secret-key")

# COMMAND ----------

import alpaca_trade_api as tradeapi
api = tradeapi.REST(api_key, secret_key, base_url, api_version='v2')


# COMMAND ----------

query = """
SELECT *
FROM hive_metastore.bronze.ticker_info
WHERE price < 5
"""
df = spark.sql(query)
display(df)

# COMMAND ----------

start_year = 2010
timeframe='1Day'
symbols = df.select("symbol").collect()
# Convert the list of symbols to a list of strings
symbol_list = [row['symbol'] for row in symbols]

# COMMAND ----------

from tqdm import tqdm
import time
bar_data = []

# Loop through each year
api_call_count = 0
for year in range(start_year, start_year + 1):
    start_date = f"{year}-01-01T00:00:00Z"

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
            
        api_call_count += 1
        if api_call_count % 200 == 0:
            time.sleep(60)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DateType, DoubleType, IntegerType, LongType
import pandas as pd
from datetime import datetime

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

bar_df.write.format("delta").option("mergeSchema", "true").mode("overwrite").saveAsTable("hive_metastore.bronze.stock_history")
