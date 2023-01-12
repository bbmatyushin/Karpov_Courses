/* ============== step 3 ==============  */
-- Наполнение таблицы-фактов данными с бакета на yandexcloud.

LOAD DATA INPATH 's3a://les3-step5/2020/yellow_tripdata_2020-01.csv'
INTO TABLE yellow_taxi.ny_taxi_data
PARTITION (dt='2020-01-01');

LOAD DATA INPATH 's3a://les3-step5/2020/yellow_tripdata_2020-02.csv'
INTO TABLE yellow_taxi.ny_taxi_data
PARTITION (dt='2020-02-01');

LOAD DATA INPATH 's3a://les3-step5/2020/yellow_tripdata_2020-03.csv'
INTO TABLE yellow_taxi.ny_taxi_data
PARTITION (dt='2020-03-01');

LOAD DATA INPATH 's3a://les3-step5/2020/yellow_tripdata_2020-04.csv'
INTO TABLE yellow_taxi.ny_taxi_data
PARTITION (dt='2020-04-01');

LOAD DATA INPATH 's3a://les3-step5/2020/yellow_tripdata_2020-05.csv'
INTO TABLE yellow_taxi.ny_taxi_data
PARTITION (dt='2020-05-01');

LOAD DATA INPATH 's3a://les3-step5/2020/yellow_tripdata_2020-06.csv'
INTO TABLE yellow_taxi.ny_taxi_data
PARTITION (dt='2020-06-01');

LOAD DATA INPATH 's3a://les3-step5/2020/yellow_tripdata_2020-07.csv'
INTO TABLE yellow_taxi.ny_taxi_data
PARTITION (dt='2020-07-01');

LOAD DATA INPATH 's3a://les3-step5/2020/yellow_tripdata_2020-08.csv'
INTO TABLE yellow_taxi.ny_taxi_data
PARTITION (dt='2020-08-01');

LOAD DATA INPATH 's3a://les3-step5/2020/yellow_tripdata_2020-09.csv'
INTO TABLE yellow_taxi.ny_taxi_data
PARTITION (dt='2020-09-01');

LOAD DATA INPATH 's3a://les3-step5/2020/yellow_tripdata_2020-10.csv'
INTO TABLE yellow_taxi.ny_taxi_data
PARTITION (dt='2020-10-01');

LOAD DATA INPATH 's3a://les3-step5/2020/yellow_tripdata_2020-11.csv'
INTO TABLE yellow_taxi.ny_taxi_data
PARTITION (dt='2020-11-01');

LOAD DATA INPATH 's3a://les3-step5/2020/yellow_tripdata_2020-12.csv'
INTO TABLE yellow_taxi.ny_taxi_data
PARTITION (dt='2020-12-01');

