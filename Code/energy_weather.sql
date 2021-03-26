CREATE DATABASE energy_dbs;
USE energy_dbs;

CREATE TABLE energy_datas(time VARCHAR(50), biomass FLOAT, lignite FLOAT, coal_derived_gas FLOAT, fossil_gas FLOAT,
	hard_coal FLOAT, fossil_oil FLOAT, oil_shale FLOAT, peat FLOAT, geothermal FLOAT, hydro_pumped_agg FLOAT,
	hydro_pumped_consump FLOAT, hydro_run_of_river FLOAT, hydro_water_res FLOAT, marine FLOAT, nuclear FLOAT,
	gen_other FLOAT, gen_other_renew FLOAT, solar FLOAT, waste FLOAT, gen_wind_offshore FLOAT, gen_wind_onshore FLOAT, 
	forecast_solar FLOAT, forecast_wind_offshore FLOAT, forecast_wind_onshore FLOAT, total_load_forecast FLOAT,
	total_load_actual FLOAT, price_day_ahead FLOAT, price_actual FLOAT);

LOAD DATA LOCAL INFILE '/home/cloudera/Desktop/energy_dataset.csv' INTO TABLE energy_datas
	fields terminated by ','
	lines terminated by '\n';

SELECT AVG(waste), MAX(waste), MIN(waste) FROM energy_datas;

SELECT time, solar, fossil_oil FROM energy_datas WHERE solar > fossil_oil;

SELECT time, waste FROM energy_datas WHERE waste > 269 OR waste = 0;

SELECT waste, price_day_ahead, price_actual, time FROM energy_datas;

SELECT time, price_day_ahead, price_actual FROM energy_datas WHERE price_day_ahead < price_actual;

SELECT SUM(gen_other), sum(gen_other_renew) FROM energy_datas;

SELECT time, total_load_forecast, total_load_actual, price_actual, price_day_ahead FROM energy_datas WHERE
	total_load_forecast < total_load_actual;


CREATE DATABASE weather;

CREATE TABLE weather_data(dt_iso VARCHAR(100), city_name VARCHAR(100), temp FLOAT, temp_min FLOAT, temp_max FLOAT,
	pressure INT, humidity INT, wind_speed INT, wind_deg INT, rain_1h FLOAT, rain_3h FLOAT, snow_3h FLOAT, 
	clouds_all INT, weather_id INT, weather_main VARCHAR(100), description VARCHAR(100), weather_icon VARCHAR(20));

LOAD DATA LOCAL INFILE '/home/cloudera/Desktop/weather_features.csv' INTO TABLE weather_data
	fields terminated by ','
	lines terminated by '\n';

SELECT AVG(temp), MAX(temp), MIN(temp), city_name FROM weather_data GROUP BY city_name;

SELECT city_name, MAX(wind_speed), MIN(wind_speed), AVG(wind_speed) FROM weather_data GROUP BY city_name;

SELECT city_name, dt_iso, windspeed, temp FROM weather_data WHERE windspeed > AVG(windspeed) OR temp > AVG(temp) GROUP BY city_name;

SELECT dt_iso, city, wind_speed FROM weather_data ORDER BY dt_iso;
