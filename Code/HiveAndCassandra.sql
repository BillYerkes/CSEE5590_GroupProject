//Hive

CREATE TABLE Energy (time String,generation_biomass FLOAT,generation_fossil_brown_coal_lignite FLOAT,generation_fossil_coal_derived_gas FLOAT,generation_fossil_gas FLOAT,generation_fossil_hard_coal FLOAT,generation_fossil_oil FLOAT,generation_fossil_oil_shale FLOAT,generation_fossil_peat FLOAT,generation_geothermal FLOAT,generation_hydro_pumped_storage_aggregated FLOAT,generation_hydro_pumped_storage_consumption FLOAT,generation_hydro_run_of_river_and_poundage FLOAT,generation_hydro_water_reservoir FLOAT,generation_marine FLOAT,generation_nuclear FLOAT,generation_other FLOAT,generation_other_renewable FLOAT,generation_solar FLOAT,generation_waste FLOAT,generation_wind_offshore FLOAT,generation_wind_onshore FLOAT,forecast_solar_day_ahead FLOAT,forecast_wind_offshore_eday_ahead FLOAT,forecast_wind_onshore_day_ahead FLOAT,total_load_forecast FLOAT,total_load_actual FLOAT,price_day_ahead FLOAT,price_actual FLOAT
)
row format delimited fields terminated by ',' 
stored AS textfile 
tblproperties("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH '/home/cloudera/Downloads/energy_dataset.csv' INTO TABLE Energy;

SELECT * FROM Energy LIMIT 10;

CREATE TABLE Weather (dt_iso String,city_name STRING,temp FLOAT,temp_min FLOAT,temp_max FLOAT,pressure INT,humidity INT,wind_speed INT,wind_deg INT,rain_1h FLOAT,rain_3h FLOAT,snow_3h FLOAT,clouds_all FLOAT,weather_id INT,weather_main STRING,weather_description STRING,weather_icon STRING
)
row format delimited fields terminated by ',' 
stored AS textfile 
tblproperties("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH '/home/cloudera/Downloads/weather_features.csv' INTO TABLE Weather;

create table merged as select * from (select * from Energy e  left join Weather w on e.time = w.dt_iso  )x

SELECT city_name, AVG(temp) as AvgTemp, AVG(wind_speed) as AvgWindSpeed, AVG(humidity) as AvgHumidity from merged group by city_name;

SELECT x.city_name AS city_name, x.year AS year, AVG(x.price_actual) AS avg_price_actual, AVG(x.price_day_ahead) AS avg_price_ahead
FROM (SELECT city_name, YEAR(time) AS YEAR, price_actual, price_day_ahead FROM merged) AS x
GROUP BY city_name, year;

SELECT x.year AS year, x.month AS month, AVG(x.generation_nuclear), AVG(x.generation_wind_onshore), AVG(x.generation_hydro_water_reservoir), AVG(x.generation_fossil_gas), AVG(x.generation_fossil_hard_coal), AVG(x.generation_fossil_brown_coal_lignite), AVG(x.generation_solar)
FROM (
SELECT YEAR(time) AS year, MONTH(time) AS month, generation_nuclear, generation_wind_onshore, generation_hydro_water_reservoir, generation_fossil_gas, generation_fossil_hard_coal, generation_fossil_brown_coal_lignite, generation_solar
FROM merged
) AS x
GROUP BY year, month;


//Cassandra

CREATE TABLE Energy(time TIMESTAMP PRIMARY KEY,generation_biomass FLOAT,generation_fossil_brown_coal_lignite FLOAT,generation_fossil_coal_derived_gas FLOAT,generation_fossil_gas FLOAT,generation_fossil_hard_coal FLOAT,generation_fossil_oil FLOAT,generation_fossil_oil_shale FLOAT,generation_fossil_peat FLOAT,generation_geothermal FLOAT,generation_hydro_pumped_storage_aggregated FLOAT,generation_hydro_pumped_storage_consumption FLOAT,generation_hydro_run_of_river_and_poundage FLOAT,generation_hydro_water_reservoir FLOAT,generation_marine FLOAT,generation_nuclear FLOAT,generation_other FLOAT,generation_other_renewable FLOAT,generation_solar FLOAT,generation_waste FLOAT,generation_wind_offshore FLOAT,generation_wind_onshore FLOAT,forecast_solar_day_ahead FLOAT,forecast_wind_offshore_eday_ahead FLOAT,forecast_wind_onshore_day_ahead FLOAT,total_load_forecast FLOAT,total_load_actual FLOAT,price_day_ahead FLOAT,price_actual FLOAT);


CREATE TABLE Weather(dt_iso TIMESTAMP,city_name TEXT,temp FLOAT,temp_min FLOAT,temp_max FLOAT,pressure INT,humidity INT,wind_speed INT,wind_deg INT,rain_1h FLOAT,rain_3h FLOAT,snow_3h FLOAT,clouds_all FLOAT,weather_id INT,weather_main 
TEXT,weather_description TEXT,weather_icon TEXT);

COPY Energy(time, generation_biomass, generation_fossil_brown_coal_lignite, generation_fossil_coal_derived_gas, generation_fossil_gas, generation_fossil_hard_coal, generation_fossil_oil, generation_fossil_oil_shale, generation_fossil_peat, generation_geothermal, generation_hydro_pumped_storage_aggregated, generation_hydro_pumped_storage_consumption, generation_hydro_run_of_river_and_poundage, generation_hydro_water_reservoir, generation_marine, generation_nuclear, generation_other, generation_other_renewable, generation_solar, generation_waste, generation_wind_offshore,generation_wind_onshore,forecast_solar_day_ahead,forecast_wind_offshore_eday_ahead,forecast_wind_onshore_day_ahead,total_load_forecast,total_load_actual,price_day_ahead,price_actual) FROM '/home/jk/Downloads/energy_dataset.csv' WITH DELIMITER=',' AND HEADER=TRUE;
ast,total_load_actual,price_day_ahead,price_actual) FROM '/home/jk/Downloads/energy_dataset.csv' WITH DELIMITER=',' AND HEADER=TRUE;

COPY Weather(dt_iso,city_name,temp,temp_min,temp_max,pressure,humidity,wind_speed,wind_deg,rain_1h,rain_3h,snow_3h,clouds_all,weather_id,weather_main,weather_description,weather_icon) FROM '/home/jk/Downloads/weather_features.csv' WITH DELIMITER=',' AND HEADER=TRUE;

SELECT time, total_load_forecast, total_load_actual, price_day_ahead, price_actual from energy where price_actual >70 LIMIT 10 ALLOW FILTERING;

SELECT * FROM weather where temp > 290 AND city_name = "Seville" ALLOW FILTERING;
