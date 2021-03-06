--1. Creating taxi_details_str table & loading the whole row as a string

CREATE TABLE IF NOT EXISTS chicago_taxis.taxi_details_str (
taxi_trip_details_str String)
STORED AS TEXTFILE
tblproperties("skip.header.line.count"="1");
LOAD DATA INPATH '/bigdatapgp/common_folder/midproject/taxi_trip_dataset/taxi_trip.csv'
OVERWRITE INTO TABLE chicago_taxis.taxi_details_str;


--2. Splitting one column in multiple columns and creating taxi_trip_details table,

CREATE TABLE IF NOT EXISTS chicago_taxis.taxi_trip_details
AS
select split(taxi_trip_details_str, ",")[0] as trip_id,
split(taxi_trip_details_str, ",")[1] as taxi_id,
split(taxi_trip_details_str, ",")[2] as trip_start_time,
split(taxi_trip_details_str, ",")[3] as trip_end_time,
cast(split(taxi_trip_details_str, ",")[4] as int) as trip_seconds,
cast(split(taxi_trip_details_str, ",")[5] as float) as trip_miles,
cast(split(taxi_trip_details_str, ",")[6] as bigint) as pickup_tract,
cast(split(taxi_trip_details_str, ",")[7] as bigint) as dropoff_tract,
cast(split(taxi_trip_details_str, ",")[8] as tinyint) as pickup_community,
cast(split(taxi_trip_details_str, ",")[9] as tinyint) as dropoff_community,
cast(split(taxi_trip_details_str, ",")[10] as float) as trip_fare,
cast(split(taxi_trip_details_str, ",")[11] as float) as tip_amt,
cast(split(taxi_trip_details_str, ",")[12] as float) as toll_amt,
cast(split(taxi_trip_details_str, ",")[13] as float) as extra_amt,
cast(split(taxi_trip_details_str, ",")[14] as float) as trip_total_amt,
split(taxi_trip_details_str, ",")[15] as payment_type,
split(taxi_trip_details_str, ",")[16] as company,
cast(split(taxi_trip_details_str, ",")[17] as double) as pickup_latitude,
cast(split(taxi_trip_details_str, ",")[18] as double) as pickup_longitude,
split(taxi_trip_details_str, ",")[19] as pickup_location,
cast(split(taxi_trip_details_str, ",")[20] as double) as dropoff_latitude,
cast(split(taxi_trip_details_str, ",")[21] as double) as dropoff_longitude,
split(taxi_trip_details_str, ",")[22] as dropoff_location,
split(taxi_trip_details_str, ",")[23] as community_areas
from
chicago_taxis.taxi_details_str;


select split(taxi_trip_details_str, ",")[0] as trip_id,
split(taxi_trip_details_str, ",")[1] as taxi_id,
split(taxi_trip_details_str, ",")[2] as trip_start_time
from
chicago_taxis.taxi_details_str limit 5;

--3. Numerical Mapping of taxi_id & trip_id to reduce the data volume

select count(distinct taxi_id) from taxi_trip_details;

-- There are only 8287 distinct values. But as we can see the uuid is a really large:
-- c1305c4490085b703eed20e95ab0c479c954ae3735a963578627d563fadbf2cc859e12ebcd12c4b8f34b7eb2d6c4782b17b56ba8ddfe896fff5763105e81e050
-- Keeping this column would result in significant performance implications.
-- Hence, we can either
-- ??? Drop this field
-- ??? Or move it to a separate table and create a numerical mapping of the same into the master table. This would reduce the data volume significantly as we are going to store only around 8K distinct values of taxi ids, which are repeating for over 90M trips.
-- Second option makes sense, as it would help us perform per taxi-wise analysis.
-- Similarly, we can drop the trip_id, which is a uuid and create a row_number which can be an integer in its place.



-- Following set of queries achieve the same.
-- 3.a. Creating a separate table with distinct taxi_id values
CREATE TABLE IF NOT EXISTS chicago_taxis.taxi_id_mapping
AS
select distinct taxi_id from taxi_trip_details
CREATE TABLE IF NOT EXISTS chicago_taxis.taxi_id_mapping_with_id
AS
select row_number() over() as id, taxi_id from taxi_id_mapping

--3.b. Joining taxi_id_mapping_with_id table to master table to replace the current taxi_id i.e uuid with a numerical id.

CREATE TABLE IF NOT EXISTS chicago_taxis.taxi_trip_details_taxi_id_removed
AS
SELECT
trip_id,
id as taxi_id_int,
trip_start_time,
trip_end_time,
trip_seconds,
trip_miles,
pickup_tract,
dropoff_tract,
pickup_community,
dropoff_community,
trip_fare,
tip_amt,
toll_amt,
extra_amt,
trip_total_amt,
payment_type,
company,
pickup_latitude,
pickup_longitude,
pickup_location,
dropoff_latitude,
dropoff_longitude,
dropoff_location,
community_areas
from
taxi_trip_details as a
join
taxi_id_mapping_with_id as b
on
a.taxi_id = b.taxi_id


--3.c. Removing trip_id (uuid) and adding an int id instead
CREATE TABLE IF NOT EXISTS chicago_taxis.taxi_trip_details_taxi_trip_id_removed
STORED AS ORC
AS
SELECT
row_number() over() as trip_id_int,
taxi_id_int,
trip_start_time,
trip_end_time,
trip_seconds,
trip_miles,
pickup_tract,
dropoff_tract,
pickup_community,
dropoff_community,
trip_fare,
tip_amt,
toll_amt,
extra_amt,
trip_total_amt,
payment_type,
company,
pickup_latitude,
pickup_longitude,
pickup_location,
dropoff_latitude,
dropoff_longitude,
dropoff_location,
community_areas
from
chicago_taxis.taxi_trip_details_taxi_id_removed

--4. Cleaning up the temp tables
drop table chicago_taxis.taxi_details_str
drop table chicago_taxis.taxi_trip_details
drop table chicago_taxis.taxi_trip_details_taxi_id_removed

--5. Casting date fields

CREATE TABLE IF NOT EXISTS chicago_taxis.taxi_trip_details_taxi_trip_id_removed_ts
STORED AS ORC
AS
SELECT
taxi_id_int,
trip_start_time,
trip_end_time,
to_date(from_unixtime(unix_timestamp(split(trip_start_time, " ")[0], 'MM/dd/yyyy'), 'yyyy-MM-dd')) as trip_start_date,
to_date(from_unixtime(unix_timestamp(split(trip_end_time, " ")[0], 'MM/dd/yyyy'), 'yyyy-MM-dd')) as trip_end_date,
trip_seconds,
trip_miles,
pickup_tract,
dropoff_tract,
pickup_community,
dropoff_community,
trip_fare,
tip_amt,
toll_amt,
extra_amt,
trip_total_amt,
payment_type,
company,
pickup_latitude,
pickup_longitude,
pickup_location,
dropoff_latitude,
dropoff_longitude,
dropoff_location,
community_areas
from
chicago_taxis.taxi_trip_details_taxi_trip_id_removed


--6. Adding two fields for the trip start & end day of the week
CREATE TABLE IF NOT EXISTS chicago_taxis.taxi_trip_details_processed_with_dayofweek
AS
SELECT
*,
from_unixtime(unix_timestamp(split(trip_start_time, " ")[0], 'MM/dd/yyyy'), 'u') as start_dayofweek,
from_unixtime(unix_timestamp(split(trip_end_time, " ")[0], 'MM/dd/yyyy'), 'u') as end_dayofweek
from
chicago_taxis.taxi_trip_details_taxi_trip_id_removed


--7. Adding a weekend field to store whether a day is weekday or weekend
CREATE TABLE IF NOT EXISTS chicago_taxis.taxi_trip_details_weekend_encoded
STORED AS ORC
AS
SELECT
*,
CASE
WHEN start_dayofweek in (6,7) THEN 1
WHEN start_dayofweek in (1,2,3,4,5) THEN 0
END AS weekend
from
chicago_taxis.taxi_trip_details_processed_with_dayofweek;

--8. Adding Month and weekend code in the Main table
CREATE TABLE IF NOT EXISTS chicago_taxis.taxi_trip_detail_with_Month
STORED AS ORC
AS
SELECT
*,
date_format(trip_start_date,'YYYY') AS TRIP_YEAR,
date_format(trip_start_date,'MM') AS TRIP_MONTH_NUMBER,
CASE WHEN date_format(trip_start_date,'MM')=01 THEN 'JAN'
WHEN date_format(trip_start_date,'MM')=02 THEN 'FEB'
WHEN date_format(trip_start_date,'MM')=03 THEN 'MAR'
WHEN date_format(trip_start_date,'MM')=04 THEN 'APR'
WHEN date_format(trip_start_date,'MM')=05 THEN 'MAY'
WHEN date_format(trip_start_date,'MM')=06 THEN 'JUN'
WHEN date_format(trip_start_date,'MM')=07 THEN 'JUL'
WHEN date_format(trip_start_date,'MM')=08 THEN 'AUG'
WHEN date_format(trip_start_date,'MM')=09 THEN 'SEP'
WHEN date_format(trip_start_date,'MM')=10 THEN 'OCT'
WHEN date_format(trip_start_date,'MM')=11 THEN 'NOV'
WHEN date_format(trip_start_date,'MM')=12 THEN 'DEC'
ELSE 'NULL' END AS TRIP_MONTH,
from_unixtime(unix_timestamp(split(trip_start_time, " ")[0], 'MM/dd/yyyy'), 'u') as start_dayofweek,
from_unixtime(unix_timestamp(split(trip_end_time, " ")[0], 'MM/dd/yyyy'), 'u') as end_dayofweek
from
chicago_taxis.taxi_trip_details_taxi_trip_id_removed_ts;

--9. Adding Trip ID (Sequence Number) and Weekend Code in the Main Table to be used for Processing

CREATE TABLE IF NOT EXISTS chicago_taxis.taxi_trip_detail_main
STORED AS ORC
AS
SELECT
row_number() over() as trip_id_int,
*,
CASE
WHEN start_dayofweek in (6,7) THEN 1
WHEN start_dayofweek in (1,2,3,4,5) THEN 0
END AS weekend
from
chicago_taxis.taxi_trip_detail_with_Month;

CREATE DATABASE edureka_972937;

use edureka_972937;

CREATE TABLE IF NOT EXISTS edureka_972937.taxi_trip_detail_edureka_972937
STORED AS ORC
AS
SELECT 
* 
from
chicago_taxis.taxi_trip_detail_main;


select count(1) from edureka_972937.taxi_trip_detail_edureka_972937;--78924385



