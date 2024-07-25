DROP TABLE IF EXISTS olist_dataset.gold.dim_customer;

CREATE TABLE IF NOT EXISTS olist_dataset.gold.dim_customer (
    customer_id STRING PRIMARY KEY,
    customer_unique_id STRING,
    customer_zip_code_prefix STRING,
    customer_city STRING,
    customer_state STRING
);

INSERT INTO olist_dataset.gold.dim_customer (
    customer_id, 
    customer_unique_id, 
    customer_zip_code_prefix, 
    customer_city, 
    customer_state
)
SELECT 
    customer_id AS customer_id, 
    customer_unique_id AS customer_unique_id, 
    CAST(customer_zip_code_prefix AS STRING) AS customer_zip_code_prefix, 
    customer_city AS customer_city, 
    customer_state AS customer_state
FROM 
    olist_dataset.silver.silver_olist_customers;
