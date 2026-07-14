# Fund Administration Data Platform

> **Portfolio Project by Michał Trybulec**
> 
> This is a demonstration project showcasing modern data platform architecture and best practices.
> All data used in this project is synthetic and randomly generated for demonstration purposes only.
> No real financial data or personally identifiable information (PII) is used.

## Overview
Enterprise-grade data platform for fund administration, providing comprehensive NAV calculation, investor management, portfolio analytics, and regulatory reporting capabilities.

## Architecture

### Unity Catalog: fund_admin

**Medallion Architecture:**
<img width="1915" height="894" alt="image" src="/1.png" />

#### Bronze Layer (Raw Data)
- `funds` - Fund master data from source systems
- `investors` - Investor/client master data
- `positions` - Daily position holdings snapshots
- `transactions` - All fund transactions (subscriptions, redemptions, trades)
- `market_prices` - Daily market prices for securities

Retention: 90 days | Data Quality: Raw

#### Silver Layer (Validated Data)
- `funds` - Cleansed fund master data
- `investors` - Validated investor data
- `positions` - Quality-checked position holdings
- `transactions` - Validated transactions
- `market_prices` - Validated market prices with outlier detection

Retention: 2 years | Data Quality: Validated

#### Gold Layer (Business-Ready Analytics)
- `fund_nav_daily` - Daily NAV by fund (PRIMARY REPORTING TABLE)
  * SLA: Daily 9 AM
  * Criticality: HIGH
  * Regulatory: YES
- `fund_cashflows` - Aggregated cashflows by fund and transaction type
- `api_fund_summary` - Fund summary for external API consumption
- `asset_allocation` - Portfolio allocation by fund and asset class
- `investor_positions` - Investor-level holdings (PII data)

Retention: 7 years | Data Quality: Business-Ready

#### Ops Layer (Monitoring & Data Quality)
- `pipeline_health` - Pipeline execution health metrics
- `dq_metrics` - Historical data quality metrics
- `dq_latest_summary` - Current DQ status summary
- `dq_issue_summary` - Data quality issues aggregation
- `dq_*_quarantine` - Quarantine tables for failed validations (5 tables)

Retention: 1 year | Purpose: Monitoring

## Data Quality Framework

Implemented comprehensive DQ framework:
- Automated data validation rules
- Quarantine tables for failed records
- DQ score calculation (0-100)
- Quality metrics persistence and trending
- Health status monitoring (Healthy/Warning/Critical)\
  
<img width="984" height="555" alt="image" src="/2.png" />

## Governance & Compliance

### Catalog-Level Tags
- Domain: Finance
- Classification: Confidential
- Owner: Fund Operations Team

### Data Classification Tags
- **PII Data**: investors tables (bronze, silver, gold.investor_positions)
- **Regulatory Data**: All gold tables
- **High Criticality**: fund_nav_daily, api_fund_summary, asset_allocation, investor_positions
- **GDPR Applicable**: investor tables

### Retention Policies
- Bronze: 90 days
- Silver: 2 years
- Gold: 7 years (regulatory requirement)
- Ops: 1 year

<img width="1891" height="898" alt="image" src="/3.png" />

## Analytics & Reporting

### Power BI Executive Dashboard
Comprehensive fund analytics dashboard with:

**Key Metrics:**
- Total NAV: $160.07bn across all funds
- Active Funds: 99 funds under management
- Total Investors: 682 active clients
- Total Cashflow: $2.49bn

**Visualizations:**
1. NAV by Fund Type (Equity, Bond, Mixed)
2. NAV by Manager (top 10 fund managers)
3. Fund Types Distribution (donut chart)
4. Risk Distribution by Level (High/Medium/Low)

**Dashboard Pages:**
- Executive Dashboard - High-level fund metrics and KPIs
- Data Quality & Observability Dashboard - Pipeline health and DQ monitoring

**Data Sources:**
- Primary: fund_admin.gold.fund_nav_daily
- Secondary: fund_admin.gold.fund_cashflows, api_fund_summary, asset_allocation
  
<img width="905" height="714" alt="image" src="/4.png" />

## Key Features

✅ Medallion architecture (Bronze → Silver → Gold)
✅ Automated data quality checks
✅ Row-level quarantine for bad data
✅ Comprehensive audit trail
✅ Unity Catalog governance (tags, comments, lineage)
✅ Power BI integration
✅ API-ready gold tables
✅ Regulatory compliance (7-year retention)
✅ PII data protection
✅ Daily NAV calculation with SLA monitoring

## Data Lineage

Bronze → Silver → Gold pipeline flow:
- Source systems → Bronze (raw ingestion)
- Bronze → Silver (validation, cleansing, standardization)
- Silver → Gold (aggregation, business logic, denormalization)
- Gold → Analytics/API (consumption layer)

## Technical Stack

- **Data Platform**: Databricks Lakehouse (AWS)
- **Data Catalog**: Unity Catalog (fund_admin)
- **Storage Format**: Delta Lake
- **Orchestration**: Databricks Workflows / Lakeflow Pipelines
- **Compute**: Serverless SQL Warehouse (Photon-enabled)
- **Data Quality**: Custom DQ framework with quarantine
- **BI Tools**: Power BI
- **Governance**: Unity Catalog tags, comments, row-level security

## Contact

Author: Michał Trybulec
Catalog: fund_admin
Environment: Databricks Workspace (AWS)
Last Updated: 2026-06-21
