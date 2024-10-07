# Databricks notebook source
import yfinance as yf

# COMMAND ----------

query = """
SELECT DISTINCT symbol
FROM hive_metastore.bronze.stock_history
"""
symbols = spark.sql(query).select('symbol').collect()
symbol_list = [row['symbol'] for row in symbols]

# COMMAND ----------

import time
import yfinance as yf
from tqdm import tqdm
from requests.exceptions import HTTPError

# list to loop through to extract financial information
financial_info_list = []

# Define a delay parameter to avoid rate-limiting issues
delay = 1  # Delay in seconds between each API call

# Loop through each stock and extract the required data
for ticker in tqdm(symbol_list, desc=f"Fetching stock financials"):
    try:
        stock = yf.Ticker(ticker)
        stock_info = stock.info
        
        
        # Extract the required information
        if all(key in stock_info and stock_info[key] is not None for key in ['trailingEps', 'debtToEquity', 'averageVolume']):
            info_dict = {
                'ticker': ticker,
                'industry': stock_info.get('industry'),
                'sector': stock_info.get('sector'),
                'full_time_employees': stock_info.get('fullTimeEmployees'),
                'beta': stock_info.get('beta'),
                'market_cap': stock_info.get('marketCap'),
                'trailing_eps': stock_info.get('trailingEps'),
                'forward_eps': stock_info.get('forwardEps'),
                'first_trade_date': stock_info.get('firstTradeDateEpochUtc'),
                'earnings_per_share': stock_info.get('trailingEps'),
                'debt_to_equity': stock_info.get('debtToEquity'),
                'avg_volume': stock_info.get('averageVolume')
            }
            financial_info_list.append(info_dict)
    
    except HTTPError as http_err:
        pass
    except Exception as e:
        pass

    # Sleep to avoid rate-limiting issues
    time.sleep(delay)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, LongType

# Define the schema
schema = StructType([
    StructField("ticker", StringType(), True),
    StructField("industry", StringType(), True),
    StructField("sector", StringType(), True),
    StructField("full_time_employees", IntegerType(), True),
    StructField("beta", FloatType(), True),
    StructField("market_cap", LongType(), True),
    StructField("trailing_eps", FloatType(), True),
    StructField("forward_eps", FloatType(), True),
    StructField("first_trade_date", LongType(), True),
    StructField("earnings_per_share", FloatType(), True),
    StructField("debt_to_equity", FloatType(), True),
    StructField("avg_volume", LongType(), True)
])

# Create DataFrame with the defined schema
stock_data_df = spark.createDataFrame(financial_info_list, schema)
display(stock_data_df)

# COMMAND ----------

stock_data_df.write.format("delta").option("mergeSchema", "true").mode("overwrite").saveAsTable("hive_metastore.bronze.financials")
