drop table if exists src.KDZ13_flights;
create table src.KDZ13_flights ( 
year int  NOT NULL,
quarter int NULL, 
month int NOT NULL,
flight_date varchar (50)  NOT NULL, 
reporting_airline varchar(10), 
tail_number varchar(10), 
flight_number varchar(15) NOT NULL, 
origin varchar(10), 
dest varchar(10), 
crs_dep_time time NOT NULL, 
dep_time time, 
dep_delay_minutes float8, 
cancelled float8 NOT NULL, 
cancellation_code bpchar(1) NULL, 
air_time float8, distance float8, 
weather_delay float8 NULL, 
loaded_ts timestamp not null default (now())
);
 

drop table if exists etl.kdz13_new_data;
create table if not exists etl.kdz13_new_data (
  loaded_ts timestamp not null
);
drop table if exists etl.KDZ13_flight_min_max;
create table if not exists etl.KDZ13_flight_min_max as
select
		min(loaded_ts) as ts1
	,	max(loaded_ts) as ts2
from src.kdz13_flights
where loaded_ts > coalesce((select max(loaded_ts) from etl.kdz13_new_data), '1970-01-01')
;

drop table if exists etl.KDZ13_flights;
create table if not exists etl.kdz13_flights as
select distinct
  year
, quarter
, month
, flight_date
, reporting_airline
, tail_number
, flight_number
, origin
, dest
, crs_dep_time
, dep_time
, dep_delay_minutes
, cancelled
, cancellation_code
, air_time
, distance
, weather_delay
from src.kdz13_flights, etl.KDZ13_flight_min_max
where 1=1
and loaded_ts between ts1 and ts2
;
CREATE TABLE staging.kdz13_flights (
	"year" int4 NOT NULL,
	quarter int4 NULL,
	"month" int4 NOT NULL,
	flight_date varchar(50) NOT NULL,
	reporting_airline varchar(10) NULL,
	tail_number varchar(10) NULL,
	flight_number varchar(15) NOT NULL,
	origin varchar(10) NULL,
	dest varchar(10) NULL,
	crs_dep_time time NOT NULL,
	dep_time time NULL,
	dep_delay_minutes float8 NULL,
	cancelled float8 NOT NULL,
	cancellation_code bpchar(1) NULL,
	air_time float8 NULL,
	distance float8 NULL,
	weather_delay float8 NULL,


	loaded_ts timestamp NOT NULL DEFAULT now(),
	CONSTRAINT psa_flights_pkey PRIMARY KEY (flight_date, flight_number, origin, dest, crs_dep_time)
)


INSERT INTO staging.KDZ13_flights (
   year,
   quarter,
   month,
   flight_date,
   reporting_airline,
   tail_number,
   flight_number,
   origin,
   dest,
   crs_dep_time,
   dep_time,
   dep_delay_minutes,
   cancelled,
   cancellation_code,
   air_time,
   distance,
   weather_delay
)
SELECT
   year,
   quarter,
   month,
   flight_date,
   reporting_airline,
   tail_number,
   flight_number,
   origin,
   dest,
   crs_dep_time,
   dep_time,
   dep_delay_minutes,
   cancelled,
   cancellation_code,
   air_time,
   distance,
   weather_delay
FROM etl.KDZ13_flights
ON CONFLICT (flight_date, flight_number, origin, dest, crs_dep_time) DO UPDATE
SET
   year = excluded.year,
   quarter = excluded.quarter,
   month = excluded.month,
   flight_date = excluded.flight_date,
   reporting_airline = excluded.reporting_airline,
   tail_number = excluded.tail_number,
   origin = excluded.origin,
   dest = excluded.dest,
   crs_dep_time = excluded.crs_dep_time,
   dep_time = excluded.dep_time,
   dep_delay_minutes = excluded.dep_delay_minutes,
   cancelled = excluded.cancelled,
   cancellation_code = excluded.cancellation_code,
   air_time = excluded.air_time,
   distance = excluded.distance,
   weather_delay = excluded.weather_delay,
   loaded_ts = now();



drop table if exists dds.kdz13_flights;
CREATE TABLE dds.kdz13_flights (
	"year" int4 NULL,
	quarter int4 NULL,
	"month" int4 NULL,
	flight_scheduled_date date NULL,
	flight_actual_date date NULL,
	flight_dep_scheduled_ts timestamp NOT NULL,
	flight_dep_actual_ts timestamp NULL,
	report_airline varchar(10) NOT NULL,
	tail_number varchar(10) NOT NULL,
	flight_number_reporting_airline varchar(15) NOT NULL,
	airport_origin_dk int4 NULL,
	origin_code varchar(5) NOT NULL,
	airport_dest_dk int4 NULL,
	dest_code varchar(5) NOT NULL,
	dep_delay_minutes float8 NULL,
	cancelled int4 NOT NULL,
	cancellation_code bpchar(1) NULL,
	weather_delay float8 NULL,
	air_time float8 NULL,
	distance float8 NULL,
	loaded_ts timestamp NULL DEFAULT now(),
	CONSTRAINT lights_prk PRIMARY KEY (flight_dep_scheduled_ts, flight_number_reporting_airline, origin_code, dest_code)
);


insert into dds.kdz13_flights
   select
       st.year
   ,   st.quarter
   ,   st.month
   ,   st.flight_date as flight_scheduled_date
   ,   (to_timestamp((CONCAT(st.flight_date, ' ', st.crs_dep_time)), 'YYYY.MM.DD HH24:MI:SS') +
   INTERVAL '1 minute' * st.dep_delay_minutes)::DATE as flight_actual_date 
   ,   to_timestamp((CONCAT(st.flight_date, ' ', st.crs_dep_time)), 'YYYY.MM.DD HH24:MI:SS')::TIMESTAMP without time zone as flight_dep_scheduled_ts
   ,   (to_timestamp((CONCAT(st.flight_date, ' ', st.crs_dep_time)), 'YYYY.MM.DD HH24:MI:SS') +
   INTERVAL '1 minute' * st.dep_delay_minutes)::TIMESTAMP WITHOUT TIME ZONE AS flight_dep_actual_ts
   ,   st.reporting_airline
   ,   case when st.tail_number is null then '' else st.tail_number end as tail_number --нулевой номер заменяем на пустую строку
   ,   st.flight_number,   dwh_id1.dwh_dk as airport_origin_dk --постоянный ключ аэропорта. нужно взять из таблицы аэропортов
   ,   st.origin as origin_code
   ,   dwh_id2.dwh_dk as airport_dest_dk --постоянный ключ аэропорта. нужно взять из таблицы аэропортов
   ,   st.dest as dest_code
   ,   st.dep_delay_minutes as dep_delay_minutes
   ,   st.cancelled
   ,   st.cancellation_code as cancellation_code
   ,   st.weather_delay as weather_delay
   ,   st.air_time as air_time
   ,   st.distance
   from staging.kdz13_flights st
   inner join dwh.id_airport dwh_id1
       on dwh_id1.src_iata_id = st.origin
   inner join dwh.id_airport dwh_id2 --джойним дважды тк у нас одно поле из dwh соотвествует двум полям из st
       on dwh_id2.src_iata_id = st.dest
   on conflict (flight_dep_scheduled_ts, flight_number, origin_code, dest_code) do update
   set
           year = excluded.year
       ,   quarter = excluded.quarter
       ,   month = excluded.month
       ,   flight_scheduled_date = excluded.flight_scheduled_date
       ,   flight_actual_date = excluded.flight_actual_date
       ,   flight_dep_scheduled_ts = excluded.flight_dep_scheduled_ts
       ,   flight_dep_actual_ts = excluded.flight_dep_actual_ts
       ,   reporting_airline = excluded.reporting_airline
       ,   tail_number = excluded.tail_number
       ,   flight_number = excluded.flight_number
       ,   airport_origin_dk = excluded.airport_origin_dk
       ,   origin_code = excluded.origin_code
       ,   airport_dest_dk = excluded.airport_dest_dk
       ,   dest_code = excluded.dest_code
       ,   dep_delay_minutes = excluded.dep_delay_minutes
       ,   cancelled = excluded.cancelled
       ,   cancellation_code = excluded.cancellation_code
       ,   weather_delay = excluded.weather_delay
       ,   air_time = excluded.air_time
       ,   distance = excluded.distance
       ,   loaded_ts = now()
;




drop table if exists src.KDZ13_weather;
CREATE TABLE src.KDZ13_weather (
icao_code varchar(10) NOT NULL,
local_datetime varchar(25), 
t_air_temperature numeric(3, 1),
p0_sea_lvl numeric(4, 1), 
p_station_lvl numeric(4, 1),
u_humidity int4, 
dd_wind_direction varchar(100),
ff_wind_speed int4, 
ff10_max_gust_value int4,
ww_present varchar(100), 
ww_recent varchar(50),
c_total_clouds varchar(200), 
vv_horizontal_visibility numeric(3, 1),
td_temperature_dewpoint numeric(3, 1), 
loaded_ts timestamp NOT NULL DEFAULT now(),
PRIMARY KEY (icao_code, local_datetime));

drop table if exists etl.kdz13_load_new_data_weathe
CREATE TABLE etl.kdz13_load_new_data_weather (
	load_new_data timestamp NOT NULL
);



drop table if exists etl.kdz13_weather_min_max;
create table  if not exists etl.kdz13_weather_min_max as
select
min(loaded_ts) as ts1,
max(loaded_ts) as ts2
from src.kdz13_weather
where loaded_ts > coalesce((select max(load_new_data) from etl.kdz13_load_new_data_weather), '1970-01-01')
;



drop table if exists etl.KDZ13_weather;
create table if not exists etl.KDZ13_weather as
select distinct
icao_code,
local_datetime,
t_air_temperature,
p0_sea_lvl,
p_station_lvl,
u_humidity,
dd_wind_direction,
ff_wind_speed,
ff10_max_gust_value,
ww_present,
ww_recent,
c_total_clouds,
vv_horizontal_visibility,
td_temperature_dewpoint
from src.KDZ13_weather, etl.KDZ13_weather_min_max
where 1=1
and loaded_ts between ts1 and ts2
;



drop table if exists staging.kdz13_weather
CREATE TABLE staging.kdz13_weather (
icao_code varchar(10) NOT NULL,
local_datetime varchar(25) NOT NULL,
t_air_temperature numeric(3, 1) NULL,
p0_sea_lvl numeric(4, 1) NULL,
p_station_lvl numeric(4, 1) NULL,
u_humidity int4 NULL,
dd_wind_direction varchar(100) NULL,
ff_wind_speed int4 NULL,
ff10_max_gust_value int4 NULL,
ww_present varchar(100) NULL,
ww_recent varchar(50) NULL,
c_total_clouds varchar(200) NULL,
vv_horizontal_visibility numeric(3, 1) NULL,
td_temperature_dewpoint numeric(3, 1) NULL,
loaded_ts timestamp NOT NULL DEFAULT now(),
CONSTRAINT kdz13_weather_pkey PRIMARY KEY (icao_code, local_datetime)
);


INSERT INTO staging.KDZ13_weather (
 icao_code,
local_datetime,
t_air_temperature,
p0_sea_lvl,
p_station_lvl,
u_humidity,
dd_wind_direction,
ff_wind_speed,
ff10_max_gust_value,
ww_present,
ww_recent,
c_total_clouds,
vv_horizontal_visibility,
td_temperature_dewpoint,
loaded_ts)
SELECT
icao_code,
local_datetime,
t_air_temperature,
p0_sea_lvl,
p_station_lvl,
u_humidity,
dd_wind_direction,
ff_wind_speed,
ff10_max_gust_value,
ww_present,
ww_recent,
c_total_clouds,
vv_horizontal_visibility,
td_temperature_dewpoint


FROM etl.KDZ13_weather
ON CONFLICT ( icao_code,local_datetime) DO UPDATE
SET
   t_air_temperature = excluded.t_air_temperature,
  p0_sea_lvl = excluded.p0_sea_lvl,
   p_station_lvl = excluded.p_station_lvl,
  u_humidity = excluded.u_humidity,
  dd_wind_direction = excluded.dd_wind_direction,
ff_wind_speed = excluded.ff_wind_speed,
 ff10_max_gust_value = excluded.ff10_max_gust_value,
ww_present = excluded.ww_P,
   ww_recent= excluded.ww_recent,
   c_total_clouds = excluded.c_total_clouds,
   vv_horizontal_visibility = excluded.vv_horizontal_visibility,
   td_temperature_dewpoint =excluded.td_temperature_dewpoint,
   loaded_ts = now();



drop table if exists dds.kdz13_airport_weather;
CREATE TABLE dds.kdz13_airport_weather(
	airport_dk int4 NOT NULL,
	weather_type_dk bpchar(6) NOT NULL,
	cold int2 NULL DEFAULT 0,
	rain int2 NULL DEFAULT 0,
	snow int2 NULL DEFAULT 0,
	thunderstorm int2 NULL DEFAULT 0,
	drizzle int2 NULL DEFAULT 0,
	fog_mist int2 NULL DEFAULT 0,
	t int4 NULL,
	max_gws int4 NULL,
	w_speed int4 NULL,
	date_start timestamp NOT NULL,
	date_end timestamp NOT NULL DEFAULT '3000-01-01 00:00:00'::timestamp without time zone,
	loaded_ts timestamp NULL DEFAULT now(),
	CONSTRAINT dds_airport_weather_pkey PRIMARY KEY (airport_dk, date_start)
);


INSERT INTO dds.kdz13_airport_weather(airport_dk, weather_type_dk, cold, rain, snow, thunderstorm, drizzle, fog_mist, t, max_gws, w_speed, date_start, date_end, loaded_ts)
SELECT
(SELECT airport_dk FROM dds.airport WHERE icao_code = weather_conditions.icao_code) AS airport_dk,
weather_conditions.weather_type_dk,
weather_conditions.cold,
weather_conditions.rain,
weather_conditions.snow,
weather_conditions.thunderstorm,
weather_conditions.drizzle,
weather_conditions.fog_mist,
weather_conditions.t,
weather_conditions.max_gws,
weather_conditions.w_speed,
weather_conditions.date_start::timestamp,
COALESCE(weather_conditions.date_end::timestamp, '3000-01-01'::timestamp) AS date_end,
weather_conditions.loaded_ts
FROM
(SELECT *,
 CONCAT(cold, rain, snow, thunderstorm, drizzle, fog_mist) AS weather_type_dk
FROM(SELECT
icao_code, local_datetime AS date_start,
LEAD(local_datetime) OVER (PARTITION BY icao_code ORDER BY local_datetime) As date_end,
CASE WHEN t_air_temperature < 0 THEN 1 ELSE 0 END AS cold,
CASE WHEN ww_present LIKE '%rain%' OR ww_recent LIKE '%rain%' THEN 1 ELSE 0 END AS rain,
CASE WHEN ww_present LIKE '%snow%' OR ww_recent LIKE '%snow%' THEN 1 ELSE 0 END AS snow,
CASE WHEN ww_present LIKE 'thunderstorm%' OR ww_recent LIKE '%thunderstorm%' THEN 1 ELSE 0 END As thunderstorm,
CASE WHEN ww_present LIKE '%fog%' OR ww_present LIKE '%mist%' OR ww_recent LIKE '%fog%' OR ww_recent LIKE '%mist%' THEN 1 ELSE 0 END AS fog_mist,
CASE WHEN ww_present LIKE '%drizzle%' OR ww_recent LIKE '%drizzle%' THEN 1 ELSE 0 END AS drizzle,
ff_wind_speed AS w_speed,
ff10_max_gust_value AS max_gws,
t_air_temperature AS t,
loaded_ts
FROM staging.kdz13_weather) AS weather_conditions) AS weather_conditions
ON CONFLICT (airport_dk, date_start) DO UPDATE
SET
date_end = excluded.date_end,
weather_type_dk = excluded.weather_type_dk,
cold = excluded.cold,
rain = excluded.rain,
snow = excluded.snow,
thunderstorm = excluded.thunderstorm,
drizzle = excluded. drizzle,
fog_mist = excluded.fog_mist,
t = excluded.t,
max_gws = excluded.max_gws,
w_speed = excluded.w_speed,
loaded_ts = excluded.loaded_ts;


INSERT INTO mart.fact_departure 
SELECT
    flights.airport_origin_dk,
    flights.airport_dest_dk as airport_destination_dk,
    weather.weather_type_dk,
    flights.flight_dep_scheduled_ts as flight_scheduled_ts,
    flights.flight_dep_actual_ts as flight_actual_time,
    flights.flight_number,
    flights.distance,
    flights.tail_number,
    flights.reporting_airline as airline,
    flights.dep_delay_minutes as dep_delay_min,
    flights.cancelled,
    flights.cancellation_code,
    weather.t,
    weather.max_gws,
    weather.w_speed,
    flights.air_time,
    '13' AS author,
    now()
FROM
    dds.kdz13_flights flights
JOIN
    dds.kdz13_airport_weather weather ON flights.airport_origin_dk = weather.airport_dk
    AND flights.flight_dep_scheduled_ts BETWEEN weather.date_start AND weather.date_end
ON CONFLICT (airport_origin_dk, airport_destination_dk, flight_scheduled_ts, flight_number)
DO NOTHING;



