DROP TABLE IF EXISTS daily_2022;
CREATE TABLE daily_2022 AS (
  SELECT * FROM read_parquet('/workspaces/reports/data/ResStock/temp/athena_daily_results.parquet')
);