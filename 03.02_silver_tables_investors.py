# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window

VALID_COUNTRIES = ["LU", "DE", "FR", "NL", "BE", "CH", "AT", "SE", "DK", "FI", "IE", "ES", "IT", "PL"]
VALID_RISK_PROFILES = ["Low", "Medium", "High"]
VALID_INVESTOR_STATUSES = ["Active", "Inactive"]
VALID_INVESTOR_TYPES = ["Institutional", "Pension Fund", "Asset Manager", "Insurance", "Family Office"]

# COMMAND ----------

bronze_investors = spark.table("fund_admin.bronze.investors")

investors_base = (
    bronze_investors
    .select(
        F.trim("investor_id").alias("id"),
        F.trim("investor_name").alias("name"),
        F.trim("country").alias("country"),
        F.trim("risk_profile").alias("risk_profile"),
        F.trim("onboarding_date").alias("onboarding_date_raw"),
        F.trim("status").alias("status"),
        F.trim("investor_type").alias("investor_type")
    )
    .withColumn(
        "parsed_onboarding_date",
        F.try_to_timestamp(F.col("onboarding_date_raw"), F.lit("yyyy-MM-dd"))
    )
)

# COMMAND ----------

# DBTITLE 1,Cell 3
# Detect duplicate investor IDs
id_counts = investors_base.groupBy("id").agg(F.count("*").alias("id_count"))

investors_validated = (
    investors_base
    .join(id_counts, on="id", how="left")
    .withColumn(
        "dq_issues",
        F.expr("""
            filter(array(
                CASE WHEN id IS NULL OR id = '' THEN 'MISSING_INVESTOR_ID' END,
                CASE WHEN name IS NULL OR name = '' THEN 'MISSING_INVESTOR_NAME' END,
                CASE WHEN country NOT IN ('LU','DE','FR','NL','BE','CH','AT','SE','DK','FI','IE','ES','IT','PL') THEN 'INVALID_COUNTRY' END,
                CASE WHEN risk_profile NOT IN ('Low','Medium','High') THEN 'INVALID_RISK_PROFILE' END,
                CASE WHEN parsed_onboarding_date IS NULL THEN 'INVALID_ONBOARDING_DATE' END,
                CASE WHEN status NOT IN ('Active','Inactive') THEN 'INVALID_STATUS' END,
                CASE WHEN investor_type NOT IN ('Institutional','Pension Fund','Asset Manager','Insurance','Family Office') THEN 'INVALID_INVESTOR_TYPE' END,
                CASE WHEN id_count > 1 THEN 'DUPLICATE_INVESTOR_ID' END
            ), x -> x IS NOT NULL)
        """)
    )
)

display(investors_validated.limit(10))

# COMMAND ----------

# DBTITLE 1,Cell 4

silver_investors = (
    investors_validated
    .filter(F.size("dq_issues") == 0)
    .withColumn("onboarding_date", F.to_date("parsed_onboarding_date"))
    .drop("onboarding_date_raw", "parsed_onboarding_date", "dq_issues", "id_count")
    .withColumn("processed_at", F.current_timestamp())
)

investors_quarantine = (
    investors_validated
    .filter(F.size("dq_issues") > 0)
    .withColumn("quarantined_at", F.current_timestamp())
)

silver_investors.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("fund_admin.silver.investors")
investors_quarantine.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("fund_admin.ops.dq_investors_quarantine")

display(silver_investors.limit(10))
display(investors_quarantine.limit(20))

# COMMAND ----------

total_records = bronze_investors.count()
valid_records = silver_investors.count()
invalid_records = investors_quarantine.count()

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
        dataset="investors",
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

display(spark.table("fund_admin.ops.dq_metrics"))