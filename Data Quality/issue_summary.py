# Databricks notebook source
from pyspark.sql import functions as F

quarantine_tables = [
    ("funds", "fund_admin.ops.dq_funds_quarantine"),
    ("investors", "fund_admin.ops.dq_investors_quarantine"),
    ("transactions", "fund_admin.ops.dq_transactions_quarantine"),
    ("positions", "fund_admin.ops.dq_positions_quarantine"),
    ("market_prices", "fund_admin.ops.dq_market_prices_quarantine"),
]

issue_dfs = []


# COMMAND ----------


for dataset, table_name in quarantine_tables:
    df = (
        spark.table(table_name)
        .withColumn("dataset", F.lit(dataset))
        .withColumn("dq_issue", F.explode("dq_issues"))
        .groupBy("dataset", "dq_issue")
        .agg(F.count("*").alias("issue_count"))
    )
    issue_dfs.append(df)

dq_issue_summary = issue_dfs[0]

for df in issue_dfs[1:]:
    dq_issue_summary = dq_issue_summary.unionByName(df)

dq_issue_summary.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("fund_admin.ops.dq_issue_summary")

display(dq_issue_summary)