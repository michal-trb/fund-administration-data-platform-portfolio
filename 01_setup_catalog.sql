CREATE CATALOG IF NOT EXISTS fund_admin;

CREATE SCHEMA IF NOT EXISTS fund_admin.bronze;
CREATE SCHEMA IF NOT EXISTS fund_admin.silver;
CREATE SCHEMA IF NOT EXISTS fund_admin.gold;
CREATE SCHEMA IF NOT EXISTS fund_admin.ops;

COMMENT ON CATALOG fund_admin IS 'Governed lakehouse platform for fund administration data products';

COMMENT ON SCHEMA fund_admin.bronze IS 'Raw ingested source data from fund administration systems';
COMMENT ON SCHEMA fund_admin.silver IS 'Validated, standardized and deduplicated fund administration data';
COMMENT ON SCHEMA fund_admin.gold IS 'Reusable business-ready data products for reporting and API consumption';
COMMENT ON SCHEMA fund_admin.ops IS 'Operational metadata, data quality results and observability metrics';