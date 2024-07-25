DROP TABLE IF EXISTS olist_dataset.gold.dim_sellers;

CREATE TABLE olist_dataset.gold.dim_sellers (
    seller_id STRING PRIMARY KEY,
    seller_zip_code_prefix STRING,
    seller_city STRING,
    seller_state STRING
);

INSERT INTO olist_dataset.gold.dim_sellers (seller_id, seller_zip_code_prefix, seller_city, seller_state)
SELECT 
    seller_id, 
    seller_zip_code_prefix, 
    seller_city, 
    seller_state
FROM 
    olist_dataset.silver.silver_olist_sellers;
