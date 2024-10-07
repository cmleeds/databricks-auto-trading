# Databricks notebook source
import yfinance as yf
import time
from tqdm import tqdm
from requests.exceptions import HTTPError

# COMMAND ----------

query = """
SELECT DISTINCT symbol
FROM hive_metastore.bronze.stock_history
"""
symbols = spark.sql(query).select('symbol').collect()
symbol_list = [row['symbol'] for row in symbols]

# COMMAND ----------

income_sheet_list = []
balance_sheet_list = []
cash_flow_list = []

for ticker in tqdm(symbol_list, desc=f"Fetching stock financials (QUARTERLY)"):
    try:
        # Fetch the ticker object
        stock = yf.Ticker(ticker)
        
        # Get quarterly financials (Income Statement, Balance Sheet, Cash Flow)
        income_statement = stock.quarterly_financials.T.reset_index()  
        balance_sheet = stock.quarterly_balance_sheet.T.reset_index()
        cash_flow = stock.quarterly_cashflow.T.reset_index()
        
        # Add a column for ticker symbol
        income_statement['ticker'] = ticker
        balance_sheet['ticker'] = ticker
        cash_flow['ticker'] = ticker
        
        
        # Append the combined data to the list
        income_sheet_list.append(income_statement)
        # Append the combined data to the list
        balance_sheet_list.append(balance_sheet)
        # Append the combined data to the list
        cash_flow_list.append(cash_flow)

    except Exception as e:
        logging.error(f"Error fetching data for {ticker}: {e}")

# COMMAND ----------

import pandas as pd
import re
# Helper function to convert column names to snake_case and condense multiple underscores
def to_snake_case(column_name):
    # Convert CamelCase to snake_case and replace spaces with underscores
    column_name = column_name.replace(' ', '_').lower()
    # Condense multiple underscores into one
    column_name = re.sub(r'_+', '_', column_name)  
    return column_name

    
income_sheet_df = pd.concat(income_sheet_list)
income_sheet_df.columns = [to_snake_case(col) for col in income_sheet_df.columns]

balance_sheet_df = pd.concat(balance_sheet_list)
balance_sheet_df.columns = [to_snake_case(col) for col in balance_sheet_df.columns]

cash_flow_df = pd.concat(cash_flow_list)
cash_flow_df.columns = [to_snake_case(col) for col in cash_flow_df.columns]

# Create DataFrame with the defined schema
income_sheet_df = spark.createDataFrame(income_sheet_df)
balance_sheet_df = spark.createDataFrame(balance_sheet_df)
cash_flow_df = spark.createDataFrame(cash_flow_df)

# COMMAND ----------

income_sheet_df.write.format("delta").option("mergeSchema", "true").mode("overwrite").saveAsTable("hive_metastore.bronze.quarterly_income_sheets")

# COMMAND ----------

balance_sheet_df.write.format("delta").option("mergeSchema", "true").mode("overwrite").saveAsTable("hive_metastore.bronze.quarterly_balance_sheets")

# COMMAND ----------

cash_flow_df.write.format("delta").option("mergeSchema", "true").mode("overwrite").saveAsTable("hive_metastore.bronze.quarterly_cash_flow")
