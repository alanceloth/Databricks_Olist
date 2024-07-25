DROP TABLE IF EXISTS olist_dataset.gold.fct_sales;

CREATE TABLE olist_dataset.gold.fct_sales (
    sale_id STRING PRIMARY KEY,
    customer_id STRING,
    product_id STRING,
    seller_id STRING,
    location_id STRING,
    time_id DATE,
    quantity INT,
    price DECIMAL(16,2),
    freight_value DECIMAL(16,2),
    payment_value DECIMAL(16,2),
    payment_installments INT,
    FOREIGN KEY (customer_id) REFERENCES olist_dataset.gold.dim_customer(customer_id),
    FOREIGN KEY (product_id) REFERENCES olist_dataset.gold.dim_products(product_id),
    FOREIGN KEY (seller_id) REFERENCES olist_dataset.gold.dim_sellers(seller_id),
    FOREIGN KEY (location_id) REFERENCES olist_dataset.gold.dim_location(location_id),
    FOREIGN KEY (time_id) REFERENCES olist_dataset.gold.dim_time(time_id)
);

INSERT INTO olist_dataset.gold.fct_sales (
    sale_id,
    customer_id,
    product_id,
    seller_id,
    location_id,
    time_id,
    quantity,
    price,
    freight_value,
    payment_value,
    payment_installments
)
SELECT DISTINCT
    o.order_id AS sale_id,
    o.customer_id,
    oi.product_id,
    s.seller_id,
    md5(concat(g.geolocation_zip_code_prefix, '_', g.geolocation_city, '_', g.geolocation_state)) AS location_id,
    o.order_purchase_timestamp AS time_id,
    oi.order_item_id AS quantity,
    CAST(oi.price AS DECIMAL(16, 2)) AS price,
    CAST(oi.freight_value AS DECIMAL(16, 2)) AS freight_value,
    CAST(p.payment_value AS DECIMAL(16, 2)) AS payment_value,
    p.payment_installments
FROM 
    olist_dataset.silver.olist_orders AS o
JOIN 
    olist_dataset.silver.olist_order_items AS oi ON o.order_id = oi.order_id
JOIN 
    olist_dataset.silver.silver_olist_sellers AS s ON oi.seller_id = s.seller_id
JOIN 
    olist_dataset.silver.silver_olist_geolocation AS g ON s.seller_zip_code_prefix = g.geolocation_zip_code_prefix
JOIN 
    olist_dataset.silver.olist_order_payments AS p ON o.order_id = p.order_id;
