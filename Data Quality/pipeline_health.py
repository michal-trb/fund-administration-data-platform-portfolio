# Databricks notebook source
from pyspark.sql import functions as F

pipeline_health = (
    spark.table("fund_admin.ops.dq_latest_summary")
    .withColumn(
        "health_status",
        F.when(F.col("dq_score") >= 99, "Healthy")
         .when(F.col("dq_score") >= 95, "Warning")
         .otherwise("Critical")
    )
)

pipeline_health.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("fund_admin.ops.pipeline_health")

display(pipeline_health)