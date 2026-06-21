# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql import Row
from datetime import datetime

VALID_ASSET_CLASSES = [
    "Equity",
    "Bond",
    "Cash",
    "ETF",
    "Alternative"
]

VALID_CURRENCIES = [
    "EUR",
    "USD",
    "SEK"
]

# COMMAND ----------


bronze_positions = spark.table("fund_admin.bronze.positions")

silver_funds_ref = spark.table("fund_admin.silver.funds").select(F.col("id").alias("valid_fund_id"))
silver_investors_ref = spark.table("fund_admin.silver.investors").select(F.col("id").alias("valid_investor_id"))

positions_base = (
    bronze_positions
    .select(
        F.trim("position_id").alias("id"),
        F.trim("fund_id").alias("fund_id"),
        F.trim("investor_id").alias("investor_id"),
        F.trim("valuation_date").alias("valuation_date_raw"),
        F.trim("asset_class").alias("asset_class"),
        F.trim("units").alias("units_raw"),
        F.trim("market_value").alias("market_value_raw"),
        F.trim("currency").alias("currency")
    )
    .withColumn("parsed_valuation_date", F.try_to_timestamp(F.col("valuation_date_raw"), F.lit("yyyy-MM-dd")))
    .withColumn("units", F.regexp_replace("units_raw", ",", ".").cast("decimal(18,4)"))
    .withColumn("market_value", F.regexp_replace("market_value_raw", ",", ".").cast("decimal(18,2)"))
)

# COMMAND ----------


id_counts = positions_base.groupBy("id").agg(F.count("*").alias("id_count"))

positions_validated = (
    positions_base
    .join(id_counts, on="id", how="left")
    .join(silver_funds_ref, positions_base.fund_id == silver_funds_ref.valid_fund_id, "left")
    .join(silver_investors_ref, positions_base.investor_id == silver_investors_ref.valid_investor_id, "left")
    .withColumn(
        "dq_issues",
        F.expr("""
            filter(array(
                CASE WHEN id IS NULL OR id = '' THEN 'MISSING_POSITION_ID' END,
                CASE WHEN id_count > 1 THEN 'DUPLICATE_POSITION_ID' END,
                CASE WHEN fund_id IS NULL OR fund_id = '' THEN 'MISSING_FUND_ID' END,
                CASE WHEN investor_id IS NULL OR investor_id = '' THEN 'MISSING_INVESTOR_ID' END,
                CASE WHEN valid_fund_id IS NULL THEN 'UNKNOWN_FUND_ID' END,
                CASE WHEN valid_investor_id IS NULL THEN 'UNKNOWN_INVESTOR_ID' END,
                CASE WHEN parsed_valuation_date IS NULL THEN 'INVALID_VALUATION_DATE' END,
                CASE WHEN asset_class NOT IN ('Equity','Bond','Cash','ETF','Alternative') THEN 'INVALID_ASSET_CLASS' END,
                CASE WHEN units IS NULL THEN 'INVALID_UNITS' END,
                CASE WHEN units <= 0 THEN 'NON_POSITIVE_UNITS' END,
                CASE WHEN market_value IS NULL THEN 'INVALID_MARKET_VALUE' END,
                CASE WHEN market_value < 0 THEN 'NEGATIVE_MARKET_VALUE' END,
                CASE WHEN currency NOT IN ('EUR','USD','SEK') THEN 'INVALID_CURRENCY' END
            ), x -> x IS NOT NULL)
        """)
    )
)

display(positions_validated.limit(10))

# COMMAND ----------


silver_positions = (
    positions_validated
    .filter(F.size("dq_issues") == 0)
    .withColumn("valuation_date", F.to_date("parsed_valuation_date"))
    .drop(
        "valuation_date_raw",
        "parsed_valuation_date",
        "units_raw",
        "market_value_raw",
        "dq_issues",
        "id_count",
        "valid_fund_id",
        "valid_investor_id"
    )
    .withColumn("processed_at", F.current_timestamp())
)

positions_quarantine = (
    positions_validated
    .filter(F.size("dq_issues") > 0)
    .withColumn("quarantined_at", F.current_timestamp())
)

silver_positions.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("fund_admin.silver.positions")
positions_quarantine.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("fund_admin.ops.dq_positions_quarantine")

display(silver_positions.limit(10))
display(positions_quarantine.limit(20))

# COMMAND ----------


total_records = bronze_positions.count()
valid_records = silver_positions.count()
invalid_records = positions_quarantine.count()

dq_score = round((valid_records / total_records) * 100, 2)

print(f"Total records: {total_records}")
print(f"Valid records: {valid_records}")
print(f"Invalid records: {invalid_records}")
print(f"DQ Score: {dq_score}%")


# COMMAND ----------

metrics = [
    Row(
        dataset="positions",
        total_records=total_records,
        valid_records=valid_records,
        invalid_records=invalid_records,
        dq_score=dq_score,
        execution_date=datetime.now()
    )
]

spark.createDataFrame(metrics) \
    .write \
    .format("delta") \
    .mode("append") \
    .saveAsTable("fund_admin.ops.dq_metrics")

display(spark.table("fund_admin.ops.dq_metrics"))