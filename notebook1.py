# Databricks notebook source


# COMMAND ----------

# MAGIC %fs ls /databricks-datasets/asa/airlines

# COMMAND ----------

df = spark.read.format("csv").option("header",True).load("/databricks-datasets/asa/airlines")
df.display()

# COMMAND ----------

df.rdd.getNumPartitions()

# COMMAND ----------


