# Databricks notebook source
# MAGIC %fs ls /databricks-datasets/asa/airlines

# COMMAND ----------

df = spark.read.format("csv").option("header",True).load("/databricks-datasets/asa/airlines")
df.display()

# COMMAND ----------

df.write.format("delta").mode("overwrite").saveAsTable("flightdta1")

# COMMAND ----------

# MAGIC %sql
# MAGIC show create table flightdata1
