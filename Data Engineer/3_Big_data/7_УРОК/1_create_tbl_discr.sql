/* ============== step 1 ==============  */
-- Создание и наполнение таблиц-справочников

CREATE EXTERNAL TABLE IF NOT EXISTS yellow_taxi.discription_vendor(
id INT,
name STRING
)
STORED AS parquet;

INSERT OVERWRITE TABLE yellow_taxi.discription_vendor
VALUES (1, 'Creative Mobile Technologies, LLC'), (2, 'VeriFone Inc.');


CREATE EXTERNAL TABLE IF NOT EXISTS yellow_taxi.discription_rate_code(
id INT,
name STRING
)
STORED AS parquet;

INSERT OVERWRITE TABLE yellow_taxi.discription_rate_code
VALUES (1, 'Standard rate'),
    (2, 'JFK'),
    (3, 'Newark'),
    (4, 'Nassau or Westchester'),
    (5, 'Negotiated fare'),
    (6, 'Group ride');


CREATE EXTERNAL TABLE IF NOT EXISTS yellow_taxi.discription_store_and_fwd_flag(
id INT,
flag STRING,
name STRING
)
STORED AS parquet;

INSERT OVERWRITE TABLE yellow_taxi.discription_store_and_fwd_flag
VALUES (1, 'Y', ' store and forward trip'),
    (2, 'N', ' not a store and forward trip');


CREATE EXTERNAL TABLE IF NOT EXISTS yellow_taxi.discription_payment_type(
id INT,
name STRING
)
STORED AS parquet;

INSERT OVERWRITE TABLE yellow_taxi.discription_payment_type
VALUES (1, ' Credit card'),
    (2, 'Cash'),
    (3, 'No charge'),
    (4, 'Dispute'),
    (5, 'Unknown'),
    (6, ' Voided trip');

