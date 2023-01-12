/* ============== 5 ==============  */
-- Создать таблицу — витрину вида:

DROP TABLE IF EXISTS yellow_taxi.taxi_showcase;

CREATE TABLE IF NOT EXISTS yellow_taxi.taxi_showcase(
payment_type STRING,
date_ DATE,
tips_average_amount DOUBLE,
passengers_total INT
)
CLUSTERED BY(date_) INTO 12 BUCKETS
STORED AS orc
TBLPROPERTIES('transactional'='true');

