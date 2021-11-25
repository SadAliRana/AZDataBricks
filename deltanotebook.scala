// Databricks notebook source
// Lab - Creating a Delta Lake Table

import org.apache.spark.sql.functions._

spark.conf.set(
    "fs.azure.account.key.datalakestoragedp203.dfs.core.windows.net",
    dbutils.secrets.get(scope="data-lake-key",key="datalake"))

val df = spark.read.format("json")
.options(Map("inferSchema"->"true","header"->"true"))
.load("abfss://data@datalakestoragedp203.dfs.core.windows.net/raw/PT1H.json")

// Create a delta lake table

df.write.format("delta").mode("overwrite").saveAsTable("metrics")


// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC SELECT * FROM metrics

// COMMAND ----------

// If you want to speed up queries that make use of predicates that involve partition columns
// If you let's say use the metric name in where conditions

df.write.partitionBy("metricName").format("delta").mode("overwrite").saveAsTable("partitionedmetrics")


// COMMAND ----------

// MAGIC %sql
// MAGIC DESCRIBE HISTORY metrics
