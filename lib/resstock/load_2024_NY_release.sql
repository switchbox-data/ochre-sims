CREATE TABLE housing_units AS (
  WITH tmp AS (
    SELECT * FROM read_csv_auto('/workspaces/reports/data/resstock/2024_resstock_amy2018_release_2/NY_baseline_metadata_and_annual_results.csv')
  )
  SELECT bldg_id, weight, COLUMNS(c -> c LIKE 'in%') from tmp
);

CREATE TABLE simulations AS (
  WITH u4 AS (
    SELECT * FROM read_csv_auto('/workspaces/reports/data/resstock/2024_resstock_amy2018_release_2/NY_upgrade04_metadata_and_annual_results.csv')
  ),
  baseline AS (
    SELECT * FROM read_csv_auto('/workspaces/reports/data/resstock/2024_resstock_amy2018_release_2/NY_baseline_metadata_and_annual_results.csv')
  ),
  combo AS (
    SELECT * FROM u4
    UNION ALL BY NAME SELECT * FROM baseline
  )
  SELECT bldg_id, applicability, COLUMNS(c -> c LIKE 'out%'), COLUMNS(c -> c LIKE 'upgrade%')
  FROM combo
);