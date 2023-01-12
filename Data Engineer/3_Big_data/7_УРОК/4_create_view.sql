/* ============== step 4 ==============  */
-- Создать представление (view) для вычисления витрины 

USE yellow_taxi;

DROP VIEW IF EXISTS taxi_showcase_view;

CREATE VIEW taxi_showcase_view AS
SELECT 
    name AS payment_type,
    to_date(tpep_pickup_datetime) AS date_,
    ROUND(AVG(tip_amount), 2) AS tips_average_amount,
    SUM(passenger_count) AS passengers_total
FROM ny_taxi_data
    LEFT JOIN discription_payment_type
        ON ny_taxi_data.payment_type_id = discription_payment_type.id
GROUP BY to_date(tpep_pickup_datetime), name;

