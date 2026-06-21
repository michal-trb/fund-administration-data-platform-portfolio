# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql import Row
from datetime import datetime

VALID_TRANSACTION_TYPES = ["Subscription", "Redemption", "Switch", "Dividend"]
VALID_CURRENCIES = ["EUR", "USD", "SEK"]

# COMMAND ----------

bronze_transactions = spark.table("fund_admin.bronze.transactions")

silver_funds_ref = spark.table("fund_admin.silver.funds").select(F.col("id").alias("valid_fund_id"))
silver_investors_ref = spark.table("fund_admin.silver.investors").select(F.col("id").alias("valid_investor_id"))

# COMMAND ----------

transactions_base = (
    bronze_transactions
    .select(
        F.trim("transaction_id").alias("id"),
        F.trim("fund_id").alias("fund_id"),
        F.trim("investor_id").alias("investor_id"),
        F.trim("transaction_date").alias("transaction_date_raw"),
        F.trim("transaction_type").alias("transaction_type"),
        F.trim("amount").alias("amount_raw"),
        F.trim("currency").alias("currency")
    )
    .withColumn(
        "parsed_transaction_date",
        F.try_to_timestamp(F.col("transaction_date_raw"), F.lit("yyyy-MM-dd"))
    )
    .withColumn("amount", F.col("amount_raw").cast("decimal(18,2)"))
)


# COMMAND ----------


id_counts = transactions_base.groupBy("id").agg(F.count("*").alias("id_count"))

transactions_validated = (
    transactions_base
    .join(id_counts, on="id", how="left")
    .join(silver_funds_ref, transactions_base.fund_id == silver_funds_ref.valid_fund_id, "left")
    .join(silver_investors_ref, transactions_base.investor_id == silver_investors_ref.valid_investor_id, "left")
    .withColumn(
        "dq_issues",
        F.expr("""
            filter(array(
                CASE WHEN id IS NULL OR id = '' THEN 'MISSING_TRANSACTION_ID' END,
                CASE WHEN id_count > 1 THEN 'DUPLICATE_TRANSACTION_ID' END,
                CASE WHEN fund_id IS NULL OR fund_id = '' THEN 'MISSING_FUND_ID' END,
                CASE WHEN investor_id IS NULL OR investor_id = '' THEN 'MISSING_INVESTOR_ID' END,
                CASE WHEN valid_fund_id IS NULL THEN 'UNKNOWN_FUND_ID' END,
                CASE WHEN valid_investor_id IS NULL THEN 'UNKNOWN_INVESTOR_ID' END,
                CASE WHEN parsed_transaction_date IS NULL THEN 'INVALID_TRANSACTION_DATE' END,
                CASE WHEN transaction_type NOT IN ('Subscription','Redemption','Switch','Dividend') THEN 'INVALID_TRANSACTION_TYPE' END,
                CASE WHEN amount IS NULL THEN 'INVALID_AMOUNT' END,
                CASE WHEN amount <= 0 THEN 'NON_POSITIVE_AMOUNT' END,
                CASE WHEN currency NOT IN ('EUR','USD','SEK') THEN 'INVALID_CURRENCY' END
            ), x -> x IS NOT NULL)
        """)
    )
)

display(transactions_validated.limit(10))


# COMMAND ----------


silver_transactions = (
    transactions_validated
    .filter(F.size("dq_issues") == 0)
    .withColumn("transaction_date", F.to_date("parsed_transaction_date"))
    .drop(
        "transaction_date_raw",
        "parsed_transaction_date",
        "amount_raw",
        "dq_issues",
        "id_count",
        "valid_fund_id",
        "valid_investor_id"
    )
    .withColumn("processed_at", F.current_timestamp())
)

transactions_quarantine = (
    transactions_validated
    .filter(F.size("dq_issues") > 0)
    .withColumn("quarantined_at", F.current_timestamp())
)

silver_transactions.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("fund_admin.silver.transactions")
transactions_quarantine.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("fund_admin.ops.dq_transactions_quarantine")

display(silver_transactions.limit(10))
display(transactions_quarantine.limit(20))


# COMMAND ----------

total_records = bronze_transactions.count()
valid_records = silver_transactions.count()
invalid_records = transactions_quarantine.count()

dq_score = round((valid_records / total_records) * 100, 2)

print(f"Total records: {total_records}")
print(f"Valid records: {valid_records}")
print(f"Invalid records: {invalid_records}")
print(f"DQ Score: {dq_score}%")

# COMMAND ----------

metrics = [
    Row(
        dataset="transactions",
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