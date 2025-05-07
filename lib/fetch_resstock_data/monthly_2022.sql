DROP TABLE IF EXISTS monthly_2022;
CREATE TABLE monthly_2022 AS (
  SELECT * FROM read_parquet('/workspaces/reports/data/ResStock/temp/athena_monthly_results.parquet')
);