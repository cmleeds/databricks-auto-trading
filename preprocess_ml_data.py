# Databricks notebook source
query = """
SELECT *
FROM hive_metastore.bronze.stock_history
"""
stock_history_df = spark.sql(query)
stock_history_df =stock_history_df.withColumnRenamed('symbol', 'ticker')
stock_history_df = stock_history_df.withColumnRenamed('time', 'date')
dates = stock_history_df.select(['ticker', 'date']).distinct()

# COMMAND ----------

stock_history_df.count()

# COMMAND ----------

from pyspark.sql import functions as F

result = stock_history_df.groupBy('ticker').agg(
    F.countDistinct('date').alias('distinct_date_count'),
    F.min('date').alias('earliest_date'),
    F.max('date').alias('latest_date')
)

result = result.orderBy('distinct_date_count')

# COMMAND ----------

filtered_result = result.filter(result.distinct_date_count < 30)
ticker_to_remove = filtered_result.select('ticker')

stock_history_df = stock_history_df.join(ticker_to_remove, on='ticker', how='left_anti')

# COMMAND ----------

stock_history_df.count()

# COMMAND ----------

query = """
SELECT *
FROM hive_metastore.silver.quarterly_cash_flow
"""
cash_flow_df = spark.sql(query)

# COMMAND ----------

query = """
SELECT *
FROM hive_metastore.silver.quarterly_income_sheets
"""
income_sheets_df = spark.sql(query)

# COMMAND ----------

query = """
SELECT *
FROM hive_metastore.silver.quarterly_balance_sheets
"""
balance_sheets_df = spark.sql(query)

# COMMAND ----------

from pyspark.sql.functions import min

earliest_income_date = income_sheets_df.select(min("date")).collect()[0][0]
earliest_cash_date = cash_flow_df.select(min("date")).collect()[0][0]
earliest_balance_date = balance_sheets_df.select(min("date")).collect()[0][0]

# COMMAND ----------

financial_dates = dates.filter(dates.date > earliest_balance_date)

# COMMAND ----------

tickers_to_use = cash_flow_df.select('ticker').distinct()
cash_pre_merge_df = financial_dates.filter(financial_dates.ticker.isin([row.ticker for row in tickers_to_use.collect()]))

cash_flow_merge_df = cash_flow_df.join(cash_pre_merge_df, on = ['ticker', 'date'], how='outer')
cash_flow_merge_df = cash_flow_merge_df.orderBy('ticker', 'date')

from pyspark.sql.window import Window
from pyspark.sql.functions import last

window_spec = Window.partitionBy('ticker').orderBy('date').rowsBetween(Window.unboundedPreceding, 0)

cash_flow_merge_df = cash_flow_merge_df.select(
    'ticker', 
    'date', 
    *[
        last(col, ignorenulls=True).over(window_spec).alias(col) 
        for col in cash_flow_merge_df.columns if col not in ['ticker', 'date']
    ]
)
cash_flow_merge_df = cash_flow_merge_df.orderBy('ticker', 'date')

# COMMAND ----------

tickers_to_use = income_sheets_df.select('ticker').distinct()
income_pre_merge_df = financial_dates.filter(financial_dates.ticker.isin([row.ticker for row in tickers_to_use.collect()]))

income_flow_merge_df = income_sheets_df.join(income_pre_merge_df, on = ['ticker', 'date'], how='outer')
income_flow_merge_df = income_flow_merge_df.orderBy('ticker', 'date')

income_flow_merge_df = income_flow_merge_df.select(
    'ticker', 
    'date', 
    *[
        last(col, ignorenulls=True).over(window_spec).alias(col) 
        for col in income_flow_merge_df.columns if col not in ['ticker', 'date']
    ]
)
income_flow_merge_df = income_flow_merge_df.orderBy('ticker', 'date')

# COMMAND ----------

tickers_to_use = balance_sheets_df.select('ticker').distinct()
balance_pre_merge_df = financial_dates.filter(financial_dates.ticker.isin([row.ticker for row in tickers_to_use.collect()]))

balance_flow_merge_df = balance_sheets_df.join(income_pre_merge_df, on = ['ticker', 'date'], how='outer')
balance_flow_merge_df = balance_flow_merge_df.orderBy('ticker', 'date')

balance_flow_merge_df = balance_flow_merge_df.select(
    'ticker', 
    'date', 
    *[
        last(col, ignorenulls=True).over(window_spec).alias(col) 
        for col in balance_flow_merge_df.columns if col not in ['ticker', 'date']
    ]
)
balance_flow_merge_df = balance_flow_merge_df.orderBy('ticker', 'date')

# COMMAND ----------

query = """
SELECT *
FROM hive_metastore.bronze.fred
"""
fred_df = spark.sql(query)

# COMMAND ----------

from pyspark.sql.functions import date_format
# Convert the timestamp to date format YYYY-MM-DD
fred_df = fred_df.withColumn("date", date_format("date", "yyyy-MM-dd"))
display(fred_df)

# COMMAND ----------

all_stock_dates = dates.select('date').distinct().orderBy('date')
earliest_stock_date = all_stock_dates.agg({"date": "min"}).collect()[0][0]

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import last

# Merge all_stock_dates into fred_df
pre_merge_fred_df = fred_df.join(all_stock_dates, on="date", how="outer")

# Define window specification
window_spec = Window.orderBy("date").rowsBetween(Window.unboundedPreceding, 0)

# Forward fill the fields GDP, CPI, and unemployment
pre_merge_fred_df = pre_merge_fred_df.withColumn("GDP", last("GDP", ignorenulls=True).over(window_spec))
pre_merge_fred_df = pre_merge_fred_df.withColumn("CPI", last("CPI", ignorenulls=True).over(window_spec))
pre_merge_fred_df = pre_merge_fred_df.withColumn("unemployment", last("unemployment", ignorenulls=True).over(window_spec))

# Display the merged DataFrame
pre_merge_fred_df = pre_merge_fred_df.filter(pre_merge_fred_df.date > earliest_stock_date)
pre_merge_fred_df = pre_merge_fred_df.orderBy('date')
display(pre_merge_fred_df)

# COMMAND ----------

stock_with_macro_df = stock_history_df.join(pre_merge_fred_df, on='date', how='left')
stock_with_macro_df = stock_with_macro_df.orderBy('ticker', 'date')

# COMMAND ----------

from pyspark.sql.functions import col

# Merge stock_history_df with income_flow_merge_df
stock_income_merge_df = stock_with_macro_df.join(income_flow_merge_df, on=['ticker', 'date'], how='left')

# Merge the result with cash_flow_merge_df
stock_income_cash_merge_df = stock_income_merge_df.join(cash_flow_merge_df, on=['ticker', 'date'], how='left')

# Merge the result with balance_flow_merge_df
final_merged_df = stock_income_cash_merge_df.join(balance_flow_merge_df, on=['ticker', 'date'], how='left')

final_merged_df = final_merged_df.orderBy('ticker', 'date')
# Display the final merged DataFrame
display(final_merged_df)

# COMMAND ----------

final_merged_df.write.format("delta").option("mergeSchema", "true").mode("overwrite").saveAsTable("hive_metastore.gold.processed_ml_input")
