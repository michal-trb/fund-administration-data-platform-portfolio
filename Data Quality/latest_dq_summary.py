# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window

dq_metrics = spark.table("fund_admin.ops.dq_metrics")

window = Window.partitionBy("dataset").orderBy(F.col("execution_date").desc())

dq_latest_summary = (
    dq_metrics
    .withColumn("rn", F.row_number().over(window))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

dq_latest_summary.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("fund_admin.ops.dq_latest_summary")

display(dq_latest_summary)