WITH
  area AS (
  SELECT
    DISTINCT area
  FROM
    `nth-boulder-368917.love_bonito_data_lake.cord_location`
  WHERE
    DATE(_PARTITIONTIME) IS NOT NULL ),
  dates AS (
  SELECT
    *
  FROM
    UNNEST(GENERATE_TIMESTAMP_ARRAY('2022-10-31 00:00:00','2022-11-30 00:00:00', INTERVAL 1 HOUR)) AS timestamp),
  all_possible_comb AS (
  SELECT
    *
  FROM
    area
  CROSS JOIN
    dates )
SELECT
  DATE_TRUNC(base.timestamp,hour) AS hour,
  base.area
FROM
  all_possible_comb AS base
LEFT JOIN
  `love_bonito_data_lake.cord_location` AS gen
ON
  base.area=gen.area
  AND base.timestamp = gen.timestamp
  AND DATE(_PARTITIONTIME) IS NOT NULL
WHERE
  gen.area IS NULL
  AND base.timestamp=(
  SELECT
    timestamp
  FROM
    `love_bonito_data_lake.cord_location`
  WHERE
    DATE(_PARTITIONTIME) IS NOT NULL
  ORDER BY
    1 DESC
  LIMIT
    1)