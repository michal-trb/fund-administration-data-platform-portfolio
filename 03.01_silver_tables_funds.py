# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window

VALID_FUND_TYPES = ["Equity", "Bond", "Mixed"]
VALID_CURRENCIES = ["EUR", "USD", "SEK"]
VALID_STATUSES = ["Active", "Closed"]
VALID_DOMICILES = ["LU", "IE"]
VALID_RISK_LEVELS = ["Low", "Medium", "High"]




# COMMAND ----------

bronze_funds = spark.table("fund_admin.bronze.funds")
funds_validated = (
    bronze_funds
    .select(
        F.trim("fund_id").alias("id"),
        F.trim("fund_name").alias("name"),
        F.trim("fund_type").alias("type"),
        F.trim("base_currency").alias("currency"),
        F.trim("inception_date").alias("inception_date_raw"),
        F.trim("status").alias("status"),
        F.trim("domicile").alias("domicile"),
        F.trim("fund_manager").alias("manager"),
        F.trim("risk_level").alias("risk_level")
    )    
    .withColumn(
        "parsed_inception_date",
        F.try_to_timestamp(F.col("inception_date_raw"), F.lit("yyyy-MM-dd"))
    )
    .withColumn(
        "dq_issues",
        F.expr("""
            filter(array(
                CASE WHEN type NOT IN ('Equity','Bond','Mixed') THEN 'INVALID_FUND_TYPE' END,
                CASE WHEN currency NOT IN ('EUR','USD','SEK') THEN 'INVALID_CURRENCY' END,
                CASE WHEN parsed_inception_date IS NULL THEN 'INVALID_INCEPTION_DATE' END,
                CASE WHEN status NOT IN ('Active','Closed') THEN 'INVALID_STATUS' END,
                CASE WHEN domicile NOT IN ('LU','IE') THEN 'INVALID_DOMICILE' END,
                CASE WHEN risk_level NOT IN ('Low','Medium','High') THEN 'INVALID_RISK_LEVEL' END
            ), x -> x IS NOT NULL)
        """)
    )
)

display(funds_validated.limit(10))

# COMMAND ----------

# DBTITLE 1,Cell 3
silver_funds = (
    funds_validated
    .filter(F.size("dq_issues") == 0)
    .withColumn("inception_date", F.to_date("parsed_inception_date"))
    .drop("inception_date_raw", "parsed_inception_date", "dq_issues")
    .withColumn("processed_at", F.current_timestamp())
)

funds_quarantine = (
    funds_validated
    .filter(F.size("dq_issues") > 0)
    .withColumn("quarantined_at", F.current_timestamp())
)

silver_funds.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("fund_admin.silver.funds")

funds_quarantine.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("fund_admin.ops.dq_funds_quarantine")

display(silver_funds.limit(10))
display(funds_quarantine.limit(10))

# COMMAND ----------

total_records = bronze_funds.count()
valid_records = silver_funds.count()
invalid_records = funds_quarantine.count()

dq_score = round((valid_records / total_records) * 100, 2)

print(f"Total records: {total_records}")
print(f"Valid records: {valid_records}")
print(f"Invalid records: {invalid_records}")
print(f"DQ Score: {dq_score}%")

# COMMAND ----------

from pyspark.sql import Row
from datetime import datetime

metrics = [
    Row(
        dataset="funds",
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

# COMMAND ----------

