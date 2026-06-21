-- FUNDS

DROP TABLE IF EXISTS fund_admin.bronze.funds;

CREATE TABLE fund_admin.bronze.funds (
    fund_id STRING,
    fund_name STRING,
    fund_type STRING,
    base_currency STRING,
    inception_date STRING,
    status STRING,
    domicile STRING,
    fund_manager STRING,
    risk_level STRING
);

-- INVESTORS

DROP TABLE IF EXISTS fund_admin.bronze.investors;

CREATE TABLE fund_admin.bronze.investors (
    investor_id STRING,
    investor_name STRING,
    country STRING,
    risk_profile STRING,
    onboarding_date STRING,
    status STRING,
    investor_type STRING
);

-- TRANSACTIONS

DROP TABLE IF EXISTS fund_admin.bronze.transactions;

CREATE TABLE fund_admin.bronze.transactions (
    transaction_id STRING,
    fund_id STRING,
    investor_id STRING,
    transaction_date STRING,
    transaction_type STRING,
    amount STRING,
    currency STRING
);

-- POSITIONS

DROP TABLE IF EXISTS fund_admin.bronze.positions;

CREATE TABLE fund_admin.bronze.positions (
    position_id STRING,
    fund_id STRING,
    investor_id STRING,
    valuation_date STRING,
    asset_class STRING,
    units STRING,
    market_value STRING,
    currency STRING
);

DROP TABLE IF EXISTS fund_admin.bronze.market_prices;

CREATE TABLE fund_admin.bronze.market_prices (
    asset_id STRING,
    fund_id STRING,
    price_date STRING,
    close_price STRING,
    currency STRING
);

COPY INTO fund_admin.bronze.funds
FROM 'file:/Workspace/Users/<your-user>/Fund Administration Data Platform/funds.csv'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header'='true');

COPY INTO fund_admin.bronze.investors
FROM 'file:/Workspace/Users/<your-user>/Fund Administration Data Platform/investors.csv'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header'='true');

COPY INTO fund_admin.bronze.transactions
FROM 'file:/Workspace/Users/<your-user>/Fund Administration Data Platform/transactions.csv'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header'='true');

COPY INTO fund_admin.bronze.positions
FROM 'file:/Workspace/Users/<your-user>/Fund Administration Data Platform/positions.csv'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header'='true');

COPY INTO fund_admin.bronze.market_prices
FROM 'file:/Workspace/Users/<your-user>/Fund Administration Data Platform/market_prices.csv'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header'='true');

SELECT 'funds' AS table_name, COUNT(*) AS row_count
FROM fund_admin.bronze.funds

UNION ALL

SELECT 'investors', COUNT(*)
FROM fund_admin.bronze.investors

UNION ALL

SELECT 'transactions', COUNT(*)
FROM fund_admin.bronze.transactions

UNION ALL

SELECT 'positions', COUNT(*)
FROM fund_admin.bronze.positions

UNION ALL

SELECT 'market_prices', COUNT(*)
FROM fund_admin.bronze.market_prices;