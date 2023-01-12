/* ============== step 2 ==============  */
-- Создать таблицу фактов поездок желтого такси.

DROP TABLE IF EXISTS yellow_taxi.ny_taxi_data;
CREATE EXTERNAL TABLE IF NOT EXISTS yellow_taxi.ny_taxi_data(
vendor_id INT,
tpep_pickup_datetime TIMESTAMP,
tpep_dropoff_datetime TIMESTAMP,
passenger_count INT,
trip_distance DOUBLE,
ratecode_id INT,
store_and_fwd_flag STRING,
pulocation_id INT,
dolocation_id INT,
payment_type_id INT,
fare_amount DOUBLE,
extra DOUBLE,
mta_tax DOUBLE,
tip_amount DOUBLE,
tolls_amount DOUBLE,
improvement_surcharge DOUBLE,
total_amount DOUBLE,
congestion_surcharge DOUBLE
)
PARTITIONED BY (dt STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
-- STORED AS parquet -- с таким форматом hive не отображает данные
-- При select-запросе выдает ошибку:
/*
OK
Failed with exception java.io.IOException:java.lang.RuntimeException: s3a://les3-step5/2020/dt=2020-04-01/yellow_tripdata_2020-04.csv is not a Parquet file. expected magic number at tail [80, 65, 82, 49] but found [44, 48, 13, 10]
*/
-- для теста загружал данные только с одного файла - yellow_tripdata_2020-04.csv
LOCATION 's3a://les3-step5/2020/'
TBLPROPERTIES("skip.header.line.count"="1");
