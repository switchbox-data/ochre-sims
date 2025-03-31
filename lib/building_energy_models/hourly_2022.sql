DROP TABLE IF EXISTS hourly_2022;
CREATE TABLE hourly_2022 AS (
  SELECT * FROM read_parquet('/workspaces/reports/data/ResStock/temp/athena_hourly_results.parquet')
);