WITH
  count_taxis_temp AS (
  SELECT
    COUNT(*) AS count_taxi,
    area,
    coordinates,
    'Singapore' AS country,
  FROM
    `nth-boulder-368917.love_bonito_data_lake.cord_location`
  WHERE
    DATE(_PARTITIONTIME) IS NOT NULL
  GROUP BY
    area,
    coordinates
  ORDER BY
    1 DESC)
SELECT
  count_taxi,
  area,
  coordinates,
  ROW_NUMBER() OVER (PARTITION BY country ORDER BY count_taxi ASC) AS rank
FROM
  count_taxis_temp QUALIFY ROW_NUMBER() OVER (PARTITION BY country ORDER BY count_taxi ASC) <= 10
ORDER BY
  count_taxi ASC