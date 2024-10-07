# Databricks notebook source
# get secrets needed for connection with alpaca API
base_url = dbutils.secrets.get(scope="my-secrets", key="alpaca-api-base-url")
api_key = dbutils.secrets.get(scope="my-secrets", key="alpaca-api-key")
secret_key = dbutils.secrets.get(scope="my-secrets", key="alpaca-api-secret-key")

# COMMAND ----------

import alpaca_trade_api as tradeapi

# COMMAND ----------

# MAGIC %md
# MAGIC We'll be looking to ingest data on the latest stock prices, then we can filter this data for stocks under a certain dollor amount for trading

# COMMAND ----------

# instantiate connection with the alpaca API
api = tradeapi.REST(api_key, secret_key, base_url, api_version='v2')
# Get all tradable assets
assets = api.list_assets(status='active')
len(assets)

# COMMAND ----------

assets = [asset for asset in assets if asset.tradable and asset.exchange in ['NASDAQ', 'NYSE']]
len(assets)

# COMMAND ----------

assets_data = [
    {
        'symbol': asset.symbol,
        'name': asset.name,
        'status': asset.status,
        'exchange': asset.exchange,
        'tradable': asset.tradable,
        'shortable': asset.shortable,
        'marginable': asset.marginable,
        'easy_to_borrow': asset.easy_to_borrow
    }
    for asset in assets
]

# COMMAND ----------

api_call_count = 0
import time
from alpaca_trade_api.rest import APIError
for i in range(len(assets_data)):
    x = assets_data[i]
    api_call_count += 1 
    try:
        price = api.get_latest_trade(x['symbol']).price
        x.update({'price': price})
    except APIError as e:
        print(f"No trade found for {x['symbol']}: {e}")
        x.update({'price': None})

    # delay to avoid limit issues with API
    if api_call_count % 200 == 0:
        time.sleep(60)
        print("sleeping for 60 seconds at " + str(i))

# COMMAND ----------

assets_data = [
    {**item, 'price': float(item['price'])} for item in assets_data if item['price'] is not None
]

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, DoubleType, FloatType, StringType
schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("price", FloatType(), True),
    # Add more fields as necessary
])

# COMMAND ----------

assets_df = spark.createDataFrame(assets_data)
display(assets_df)

# COMMAND ----------

assets_df.count()

# COMMAND ----------

# Write the DataFrame to the Delta table
assets_df.write.format("delta").option("mergeSchema", "true").mode("overwrite").saveAsTable("hive_metastore.bronze.ticker_info")
