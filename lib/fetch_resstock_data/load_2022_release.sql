DROP TABLE IF EXISTS housing_units;
DROP TABLE IF EXISTS annuals_results;

CREATE TABLE housing_units AS (
  WITH tmp AS (
    SELECT * FROM read_csv_auto('/workspaces/ochre-sims/data/building_energy_models/2022_resstock_amy2018_release_1.1/' || getenv('STATE') || '_baseline_metadata_and_annual_results.csv')
  )
  SELECT bldg_id, weight, COLUMNS(c -> c LIKE 'in%') from tmp
);

CREATE TABLE annuals_results AS (
  WITH u3 AS (
    SELECT * FROM read_csv_auto('/workspaces/ochre-sims/data/building_energy_models/2022_resstock_amy2018_release_1.1/' || getenv('STATE') || '_upgrade03_metadata_and_annual_results.csv')
  ),
  u4 AS (
    SELECT * FROM read_csv_auto('/workspaces/ochre-sims/data/building_energy_models/2022_resstock_amy2018_release_1.1/' || getenv('STATE') || '_upgrade04_metadata_and_annual_results.csv')
  ),
  baseline AS (
    SELECT * FROM read_csv_auto('/workspaces/ochre-sims/data/building_energy_models/2022_resstock_amy2018_release_1.1/' || getenv('STATE') || '_baseline_metadata_and_annual_results.csv')
  ),
  combo AS (
    SELECT * FROM u3
    UNION ALL BY NAME SELECT * FROM u4
    UNION ALL BY NAME SELECT * FROM baseline
  )
  SELECT bldg_id, COLUMNS(c -> c LIKE 'out%'), COLUMNS(c -> c LIKE 'upgrade%')
  FROM combo
);

-- CREATE TABLE time_series AS (
--   WITH tmp AS (
--     SELECT * FROM read_csv_auto('/workspace/reports/data/resstock/2022_resstock_amy2018_release_1.1/resstock_monthly_totals_ny.csv')
--   )
--   SELECT *
--   FROM tmp
-- );