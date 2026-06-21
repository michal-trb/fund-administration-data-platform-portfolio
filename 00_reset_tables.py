# Databricks notebook source
tables_to_drop = [
    "fund_admin.gold.api_fund_summary",
    "fund_admin.gold.asset_allocation",
    "fund_admin.gold.fund_cashflows",
    "fund_admin.gold.investor_positions",
    "fund_admin.gold.fund_nav_daily",

    "fund_admin.ops.pipeline_health",
    "fund_admin.ops.dq_issue_summary",
    "fund_admin.ops.dq_latest_summary",
    "fund_admin.ops.dq_metrics",
    "fund_admin.ops.dq_market_prices_quarantine",
    "fund_admin.ops.dq_positions_quarantine",
    "fund_admin.ops.dq_transactions_quarantine",
    "fund_admin.ops.dq_investors_quarantine",
    "fund_admin.ops.dq_funds_quarantine",

    "fund_admin.silver.market_prices",
    "fund_admin.silver.positions",
    "fund_admin.silver.transactions",
    "fund_admin.silver.investors",
    "fund_admin.silver.funds",

    "fund_admin.bronze.market_prices",
    "fund_admin.bronze.positions",
    "fund_admin.bronze.transactions",
    "fund_admin.bronze.investors",
    "fund_admin.bronze.funds",
]

for table_name in tables_to_drop:
    try:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")
        print(f"Dropped: {table_name}")
    except Exception as e:
        print(f"Skipped: {table_name} | Reason: {str(e)}")
