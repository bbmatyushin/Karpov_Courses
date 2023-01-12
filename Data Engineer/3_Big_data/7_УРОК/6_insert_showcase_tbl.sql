/* ============== 6 ==============  */
-- Наполнение таблицу-витрину

USE yellow_taxi;

set hive.support.concurrency = true; 
set hive.enforce.bucketing = true;
set hive.exec.dynamic.partition.mode = nonstrict;
set hive.txn.manager = org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.compactor.initiator.on = true;
set hive.compactor.worker.threads = 1;

INSERT INTO TABLE yellow_taxi.taxi_showcase
SELECT payment_type,
    date_,
    tips_average_amount,
    passengers_total
FROM taxi_showcase_view;

UPDATE taxi_showcase
   SET payment_type = 'Unknown'
 WHERE payment_type IS NULL;
