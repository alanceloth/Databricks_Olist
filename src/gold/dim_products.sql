DROP TABLE IF EXISTS olist_dataset.gold.dim_products;

CREATE TABLE IF NOT EXISTS olist_dataset.gold.dim_products (
    product_id STRING PRIMARY KEY,
    product_category_name STRING,
    product_name_lenght INT,
    product_description_lenght INT,
    product_photos_qty INT,
    product_weight_g INT,
    product_length_cm INT,
    product_height_cm INT,
    product_width_cm INT
);

INSERT INTO olist_dataset.gold.dim_products
SELECT
    product_id,
    product_category_name,
    product_name_lenght,
    product_description_lenght,
    product_photos_qty,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm
FROM olist_dataset.silver.silver_olist_products;
