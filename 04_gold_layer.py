# Databricks notebook source
from pyspark.sql import functions as F

funds = spark.table("fund_admin.silver.funds")
investors = spark.table("fund_admin.silver.investors")
transactions = spark.table("fund_admin.silver.transactions")
positions = spark.table("fund_admin.silver.positions")
market_prices = spark.table("fund_admin.silver.market_prices")

# COMMAND ----------

gold_fund_nav_daily = (
    positions
    .groupBy("fund_id", "valuation_date", "currency")
    .agg(
        F.sum("market_value").alias("nav_amount"),
        F.countDistinct("investor_id").alias("investor_count"),
        F.count("*").alias("position_count")
    )
    .join(
        funds.select(
            F.col("id").alias("fund_id_ref"),
            F.col("name").alias("fund_name"),
            F.col("type").alias("fund_type"),
            F.col("manager")
        ),
        F.col("fund_id") == F.col("fund_id_ref"),
        "left"
    )
    .drop("fund_id_ref")
    .withColumn("created_at", F.current_timestamp())
)

gold_fund_nav_daily.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("fund_admin.gold.fund_nav_daily")

display(gold_fund_nav_daily.limit(10))

# COMMAND ----------

gold_investor_positions = (
    positions
    .join(
        funds.select(
            F.col("id").alias("fund_id_ref"),
            F.col("name").alias("fund_name"),
            F.col("type").alias("fund_type"),
            F.col("manager")
        ),
        F.col("fund_id") == F.col("fund_id_ref"),
        "left"
    )
    .join(
        investors.select(
            F.col("id").alias("investor_id_ref"),
            F.col("name").alias("investor_name"),
            "country",
            "risk_profile",
            "investor_type"
        ),
        F.col("investor_id") == F.col("investor_id_ref"),
        "left"
    )
    .groupBy(
        "investor_id",
        "investor_name",
        "country",
        "risk_profile",
        "investor_type",
        "fund_id",
        "fund_name",
        "fund_type",
        "manager",
        "valuation_date",
        "currency"
    )
    .agg(
        F.sum("market_value").alias("market_value"),
        F.sum("units").alias("units"),
        F.count("*").alias("position_count")
    )
    .withColumn("created_at", F.current_timestamp())
    .drop("fund_id_ref", "investor_id_ref")
)

gold_investor_positions.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("fund_admin.gold.investor_positions")

display(gold_investor_positions.limit(10))

# COMMAND ----------

# DBTITLE 1,Cell 4
gold_fund_cashflows = (
    transactions
    .join(
        funds.select(
            F.col("id").alias("fund_id_ref"),
            F.col("name").alias("fund_name"),
            F.col("type").alias("fund_type"),
            F.col("manager")
        ),
        F.col("fund_id") == F.col("fund_id_ref"),
        "left"
    )
    .groupBy(
        "fund_id",
        "fund_name",
        "fund_type",
        "manager",
        "transaction_date",
        "transaction_type",
        "currency"
    )
    .agg(
        F.sum("amount").alias("total_amount"),
        F.count("*").alias("transaction_count"),
        F.countDistinct("investor_id").alias("investor_count")
    )
    .withColumn("created_at", F.current_timestamp())
    .drop("fund_id_ref")
)

gold_fund_cashflows.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("fund_admin.gold.fund_cashflows")

display(gold_fund_cashflows.limit(10))

# COMMAND ----------

asset_allocation_base = (
    positions
    .join(
        funds.select(
            F.col("id").alias("fund_id_ref"),
            F.col("name").alias("fund_name"),
            F.col("type").alias("fund_type"),
            F.col("manager")
        ),
        F.col("fund_id") == F.col("fund_id_ref"),
        "left"
    )
    .groupBy(
        "fund_id",
        "fund_name",
        "fund_type",
        "manager",
        "valuation_date",
        "asset_class",
        "currency"
    )
    .agg(
        F.sum("market_value").alias("market_value")
    )
)

fund_totals = (
    asset_allocation_base
    .groupBy("fund_id", "valuation_date", "currency")
    .agg(
        F.sum("market_value").alias("total_market_value")
    )
)

gold_asset_allocation = (
    asset_allocation_base
    .join(
        fund_totals,
        on=["fund_id", "valuation_date", "currency"],
        how="left"
    )
    .withColumn(
        "allocation_pct",
        F.round((F.col("market_value") / F.col("total_market_value")) * 100, 2)
    )
    .withColumn("created_at", F.current_timestamp())
)

gold_asset_allocation.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("fund_admin.gold.asset_allocation")

display(gold_asset_allocation.limit(10))

# COMMAND ----------

latest_nav_date = (
    gold_fund_nav_daily
    .groupBy("fund_id")
    .agg(F.max("valuation_date").alias("latest_valuation_date"))
)

latest_nav = (
    gold_fund_nav_daily
    .join(latest_nav_date, on="fund_id", how="inner")
    .filter(F.col("valuation_date") == F.col("latest_valuation_date"))
    .select(
        "fund_id",
        F.col("fund_name").alias("latest_fund_name"),
        "valuation_date",
        "currency",
        "nav_amount",
        "investor_count",
        "position_count"
    )
)

latest_cashflow_date = (
    transactions
    .groupBy("fund_id")
    .agg(F.max("transaction_date").alias("latest_transaction_date"))
)

gold_api_fund_summary = (
    funds
    .select(
        F.col("id").alias("fund_id"),
        F.col("name").alias("fund_name"),
        F.col("type").alias("fund_type"),
        F.col("currency").alias("base_currency"),
        "status",
        "domicile",
        "manager",
        "risk_level"
    )
    .join(latest_nav, on="fund_id", how="left")
    .join(latest_cashflow_date, on="fund_id", how="left")
    .drop("latest_fund_name")
    .withColumn("api_version", F.lit("v1"))
    .withColumn("created_at", F.current_timestamp())
)

gold_api_fund_summary.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("fund_admin.gold.api_fund_summary")

display(gold_api_fund_summary.limit(10))