# Databricks notebook source
# get secrets needed for connection with alpaca API
api_key = dbutils.secrets.get(scope="my-secrets", key="fred-api-key")

# COMMAND ----------

start_date = '2000-01-01'

# COMMAND ----------

from fredapi import Fred
fred = Fred(api_key=api_key)
gdp_data = fred.get_series('GDP', observation_start=start_date)
unemployment_data = fred.get_series('UNRATE', observation_start=start_date)
cpi_data = fred.get_series('CPIAUCSL', observation_start=start_date)

# COMMAND ----------

gdp_data = gdp_data.reset_index()
gdp_data.columns = ['date', 'gdp']

# COMMAND ----------

unemployment_data = unemployment_data.reset_index()
unemployment_data.columns = ['date', 'unemployment']

# COMMAND ----------

cpi_data = cpi_data.reset_index()
cpi_data.columns = ['date', 'CPI']

# COMMAND ----------

# DBTITLE 1,b
merged_df = gdp_data.merge(cpi_data, on = 'date')
merged_df = merged_df.merge(unemployment_data, on = 'date')

# COMMAND ----------

fred_df = spark.createDataFrame(merged_df)

# COMMAND ----------

fred_df.write.format("delta").option("mergeSchema", "true").mode("overwrite").saveAsTable("hive_metastore.bronze.fred")
