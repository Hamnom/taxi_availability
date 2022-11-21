select * from `love_bonito_data_lake.cord_location` where   DATE(_PARTITIONTIME) IS NOT NULL

--data in the base table is hourly based