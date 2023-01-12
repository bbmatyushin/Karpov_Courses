/* ============== step 0 ==============  */
-- Создание базы данных

DROP DATABASE IF EXISTS yellow_taxi CASCADE;

CREATE DATABASE IF NOT EXISTS yellow_taxi LOCATION 's3a://les3-step5/';

set hive.execution.engine=tez;
