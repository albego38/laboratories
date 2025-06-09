# Databricks notebook source
# MAGIC %md
# MAGIC ## Notebook de ejemplo en Python

# COMMAND ----------

# Un pequeño DataFrame
data = [("Alice", 34), ("Bob", 45), ("Carol", 29)]
df = spark.createDataFrame(data, ["Nombre", "Edad"])

# Mostrarlo
df.show()
