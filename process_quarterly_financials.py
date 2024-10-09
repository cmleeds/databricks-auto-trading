# Databricks notebook source
query = """
SELECT *
FROM hive_metastore.bronze.quarterly_cash_flow
where index IS NOT NULL
"""
cash_flow_df = spark.sql(query)

# COMMAND ----------

from pyspark.sql.functions import date_format
# Convert the timestamp to date format YYYY-MM-DD
cash_flow_df = cash_flow_df.withColumn("date", date_format("index", "yyyy-MM-dd"))
# Drop the original timestamp column if no longer needed
cash_flow_df = cash_flow_df.drop("index")

# COMMAND ----------

cash_flow_df.write.format("delta").option("mergeSchema", "true").mode("overwrite").saveAsTable("hive_metastore.silver.quarterly_cash_flow")

# COMMAND ----------

query = """
SELECT *
FROM hive_metastore.bronze.quarterly_income_sheets
"""
income_sheets_df = spark.sql(query)

# COMMAND ----------

# Convert the timestamp to date format YYYY-MM-DD
income_sheets_df = income_sheets_df.withColumn("date", date_format("index", "yyyy-MM-dd"))
# Drop the original timestamp column if no longer needed
income_sheets_df = income_sheets_df.drop("index")

# COMMAND ----------

income_sheets_df.write.format("delta").option("mergeSchema", "true").mode("overwrite").saveAsTable("hive_metastore.silver.quarterly_income_sheets")

# COMMAND ----------

query = """
SELECT *
FROM hive_metastore.bronze.quarterly_balance_sheets
"""
balance_sheets_df = spark.sql(query)

# COMMAND ----------

# Convert the timestamp to date format YYYY-MM-DD
balance_sheets_df = balance_sheets_df.withColumn("date", date_format("index", "yyyy-MM-dd"))
# Drop the original timestamp column if no longer needed
balance_sheets_df = balance_sheets_df.drop("index")

# COMMAND ----------

balance_sheets_df.write.format("delta").option("mergeSchema", "true").mode("overwrite").saveAsTable("hive_metastore.silver.quarterly_balance_sheets")
