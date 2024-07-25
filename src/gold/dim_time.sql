DROP TABLE IF EXISTS olist_dataset.gold.dim_time;

CREATE TABLE olist_dataset.gold.dim_time (
    time_id DATE PRIMARY KEY,
    year INT,
    month INT,
    day INT,
    trimestre INT,
    weekday INT,
    week_number INT,
    is_weekend BOOLEAN
);

INSERT INTO olist_dataset.gold.dim_time (time_id, year, month, day, trimestre, weekday, week_number, is_weekend)
SELECT DISTINCT 
    order_purchase_timestamp AS time_id,
    YEAR(order_purchase_timestamp) AS year,
    MONTH(order_purchase_timestamp) AS month,
    DAY(order_purchase_timestamp) AS day,
    QUARTER(order_purchase_timestamp) AS trimestre,
    WEEKDAY(order_purchase_timestamp) AS weekday,
    WEEKOFYEAR(order_purchase_timestamp) AS week_number,
    CASE 
        WHEN WEEKDAY(order_purchase_timestamp) IN (5, 6) THEN TRUE 
        ELSE FALSE 
    END AS is_weekend
FROM 
    olist_dataset.silver.olist_orders;
