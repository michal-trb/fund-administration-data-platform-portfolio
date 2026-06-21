# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql import Row
from datetime import datetime

VALID_CURRENCIES = ["EUR", "USD", "SEK"]


# COMMAND ----------

bronze_market_prices = spark.table("fund_admin.bronze.market_prices")
silver_funds_ref = spark.table("fund_admin.silver.funds").select(F.col("id").alias("valid_fund_id"))

market_prices_base = (
    bronze_market_prices
    .select(
        F.trim("asset_id").alias("asset_id"),
        F.trim("fund_id").alias("fund_id"),
        F.trim("price_date").alias("price_date_raw"),
        F.trim("close_price").alias("close_price_raw"),
        F.trim("currency").alias("currency")
    )
    .withColumn("parsed_price_date", F.try_to_timestamp(F.col("price_date_raw"), F.lit("yyyy-MM-dd")))
    .withColumn("close_price", F.col("close_price_raw").cast("decimal(18,4)"))
)

# COMMAND ----------

key_counts = (
    market_prices_base
    .groupBy("asset_id", "fund_id", "price_date_raw")
    .agg(F.count("*").alias("key_count"))
)

currencies_sql = ",".join([f"'{x}'" for x in VALID_CURRENCIES])

market_prices_validated = (
    market_prices_base.alias("base")
    .join(key_counts, on=["asset_id", "fund_id", "price_date_raw"], how="left")
    .join(silver_funds_ref, F.col("base.fund_id") == silver_funds_ref.valid_fund_id, "left")
    .withColumn(
        "dq_issues",
        F.expr(f"""
            filter(array(
                CASE WHEN asset_id IS NULL OR asset_id = '' THEN 'MISSING_ASSET_ID' END,
                CASE WHEN fund_id IS NULL OR fund_id = '' THEN 'MISSING_FUND_ID' END,
                CASE WHEN valid_fund_id IS NULL THEN 'UNKNOWN_FUND_ID' END,
                CASE WHEN parsed_price_date IS NULL THEN 'INVALID_PRICE_DATE' END,
                CASE WHEN close_price IS NULL THEN 'INVALID_CLOSE_PRICE' END,
                CASE WHEN close_price <= 0 THEN 'NON_POSITIVE_CLOSE_PRICE' END,
                CASE WHEN currency NOT IN ({currencies_sql}) THEN 'INVALID_CURRENCY' END,
                CASE WHEN key_count > 1 THEN 'DUPLICATE_MARKET_PRICE_KEY' END
            ), x -> x IS NOT NULL)
        """)
    )
)

display(market_prices_validated.limit(10))

# COMMAND ----------


silver_market_prices = (
    market_prices_validated
    .filter(F.size("dq_issues") == 0)
    .withColumn("price_date", F.to_date("parsed_price_date"))
    .drop(
        "price_date_raw",
        "parsed_price_date",
        "close_price_raw",
        "dq_issues",
        "key_count",
        "valid_fund_id"
    )
    .withColumn("processed_at", F.current_timestamp())
)

market_prices_quarantine = (
    market_prices_validated
    .filter(F.size("dq_issues") > 0)
    .withColumn("quarantined_at", F.current_timestamp())
)

silver_market_prices.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("fund_admin.silver.market_prices")
market_prices_quarantine.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("fund_admin.ops.dq_market_prices_quarantine")

display(silver_market_prices.limit(10))
display(market_prices_quarantine.limit(20))

# COMMAND ----------

total_records = bronze_market_prices.count()
valid_records = silver_market_prices.count()
invalid_records = market_prices_quarantine.count()

dq_score = round((valid_records / total_records) * 100, 2)

print(f"Total records: {total_records}")
print(f"Valid records: {valid_records}")
print(f"Invalid records: {invalid_records}")
print(f"DQ Score: {dq_score}%")

# COMMAND ----------

metrics = [
    Row(
        dataset="market_prices",
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