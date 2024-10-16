# Databricks notebook source
query = """
SELECT * 
FROM hive_metastore.gold.processed_ml_input
WHERE date > '2020-01-01'
"""
df = spark.sql(query)
df.count()

# COMMAND ----------

from pyspark.sql.window import Window
import pyspark.sql.functions as F

# Define the window specification by group for ticker
window_spec = Window.partitionBy('ticker').orderBy('date')

# Shift the 'close' field back by five trading days by group for ticker
df = df.withColumn('close_shifted', F.lag('close', 5).over(window_spec))

# COMMAND ----------

# split_date = '2024-09-01'
# train_df = df.filter(df.date < split_date)
# test_df = df.filter(df.date >= split_date)

df = df.filter(df.close_shifted.isNotNull())
df.count()

# COMMAND ----------

from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.sql.functions import year, month, dayofmonth, dayofweek, hour, when, col

# Extract relevant features from the date_column
df = df.withColumn("year", year(col("date")))
df = df.withColumn("month", month(col("date")))
df = df.withColumn("day", dayofmonth(col("date")))
df = df.withColumn("day_of_week", dayofweek(col("date")))

# COMMAND ----------

feature_list = df.columns
feature_list.append('ticker_index')
feature_list.remove('date')
feature_list.remove('close_shifted')
feature_list.remove('ticker')

# COMMAND ----------

# label encode ticker symbols
indexer = StringIndexer(inputCol="ticker", outputCol="ticker_index")
df = indexer.fit(df).transform(df)

feature_cols = ['ticker_index', 'year', 'month', 'day', 'day_of_week', 'open', 'high', 'low', 'close', 'volume']
assembler = VectorAssembler(
    inputCols=feature_cols,
    outputCol="features")
df_lgb_ready = assembler.transform(df).select("features", "close_shifted")

# COMMAND ----------

help(LightGBMRegressor)

# COMMAND ----------

# Train LightGBM model
from synapse.ml.lightgbm import LightGBMRegressor
lgbm_regressor = LightGBMRegressor(
    labelCol="close_shifted",
    featuresCol="features",
    featureFraction = 0.5,
    numIterations = 1000,
    baggingFraction = 0.5,
    boostingType = 'dart',
    numThreads = 3
)

# COMMAND ----------

lgbm_model = lgbm_regressor.fit(df_lgb_ready)

# COMMAND ----------

model_path = "dbfs:/databricks/mlflow-tracking/964505978567110/cc6f9b1e00604467aa6c151a7d9cfc3e/artifacts/model"
mlflow.register_model(model_path, "value_finder")
