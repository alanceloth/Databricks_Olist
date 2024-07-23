# Databricks notebook source
# MAGIC %md
# MAGIC # Datum Teste Tecnico
# MAGIC - Camada bronze
# MAGIC   - ETL do dataset da [Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
# MAGIC   - Data quality checks (schema, colunas obrigatórias)
# MAGIC - **Camada silver**
# MAGIC   - EDA no dataset e construção da camada silver
# MAGIC   - Data Quality checks (schema, colunas obrigatórias, regras de negócio)
# MAGIC   - Enriquecimento de dados na camada silver com modelo de segmentação (RFV)
# MAGIC   - Delta table
# MAGIC - Camada gold
# MAGIC   - Modelo dimensional (dimensão-fato)
# MAGIC   - Tabelas de report (principais KPIs já calculados, com suas respectivas agregações)
# MAGIC   - Carregamento dos dados em .parquet e Delta Table

# COMMAND ----------

# MAGIC %md
# MAGIC # Pipeline

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Silver

# COMMAND ----------

# MAGIC %sql
# MAGIC --DROP TABLE olist_dataset.silver.olist_customers;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS olist_dataset.silver.olist_customers (
# MAGIC     customer_id STRING,
# MAGIC     customer_unique_id STRING,
# MAGIC     customer_zip_code_prefix INT,
# MAGIC     customer_city STRING,
# MAGIC     customer_state STRING
# MAGIC )
# MAGIC USING DELTA;
# MAGIC
# MAGIC MERGE INTO olist_dataset.silver.olist_customers AS target
# MAGIC USING (
# MAGIC     SELECT
# MAGIC         customer_id,
# MAGIC         customer_unique_id,
# MAGIC         CAST(customer_zip_code_prefix AS INTEGER) AS customer_zip_code_prefix,
# MAGIC         customer_city,
# MAGIC         customer_state
# MAGIC     FROM
# MAGIC         olist_dataset.bronze.bronze_olist_customers_dataset
# MAGIC ) AS source
# MAGIC ON target.customer_id = source.customer_id
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC     INSERT (
# MAGIC         customer_id,
# MAGIC         customer_unique_id,
# MAGIC         customer_zip_code_prefix,
# MAGIC         customer_city,
# MAGIC         customer_state
# MAGIC     ) VALUES (
# MAGIC         source.customer_id,
# MAGIC         source.customer_unique_id,
# MAGIC         source.customer_zip_code_prefix,
# MAGIC         source.customer_city,
# MAGIC         source.customer_state
# MAGIC     );

# COMMAND ----------

# MAGIC %sql
# MAGIC --DROP TABLE olist_dataset.silver.olist_geolocation;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS olist_dataset.silver.olist_geolocation (
# MAGIC     geolocation_zip_code_prefix INT,
# MAGIC     geolocation_lat DOUBLE,
# MAGIC     geolocation_lng DOUBLE,
# MAGIC     geolocation_city STRING,
# MAGIC     geolocation_state STRING
# MAGIC )
# MAGIC USING DELTA;
# MAGIC
# MAGIC MERGE INTO olist_dataset.silver.olist_geolocation AS target
# MAGIC USING (
# MAGIC     SELECT
# MAGIC         geolocation_zip_code_prefix,
# MAGIC         CAST(geolocation_lat AS DOUBLE) AS geolocation_lat,
# MAGIC         CAST(geolocation_lng AS DOUBLE) AS geolocation_lng,
# MAGIC         geolocation_city,
# MAGIC         geolocation_state
# MAGIC     FROM
# MAGIC         olist_dataset.bronze.bronze_olist_geolocation_dataset
# MAGIC ) AS source
# MAGIC ON target.geolocation_zip_code_prefix = source.geolocation_zip_code_prefix
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC     INSERT (
# MAGIC         geolocation_zip_code_prefix,
# MAGIC         geolocation_lat,
# MAGIC         geolocation_lng,
# MAGIC         geolocation_city,
# MAGIC         geolocation_state
# MAGIC     ) VALUES (
# MAGIC         source.geolocation_zip_code_prefix,
# MAGIC         source.geolocation_lat,
# MAGIC         source.geolocation_lng,
# MAGIC         source.geolocation_city,
# MAGIC         source.geolocation_state
# MAGIC     );

# COMMAND ----------

# MAGIC %sql
# MAGIC --DROP TABLE olist_dataset.silver.olist_order_payments;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS olist_dataset.silver.olist_order_payments (
# MAGIC     order_id STRING,
# MAGIC     payment_sequential INT,
# MAGIC     payment_type STRING,
# MAGIC     payment_installments INT,
# MAGIC     payment_value DOUBLE
# MAGIC )
# MAGIC USING DELTA;
# MAGIC
# MAGIC MERGE INTO olist_dataset.silver.olist_order_payments AS target
# MAGIC USING (
# MAGIC     SELECT
# MAGIC         order_id,
# MAGIC         CAST(payment_sequential AS INTEGER) AS payment_sequential,
# MAGIC         payment_type,
# MAGIC         payment_installments,
# MAGIC         CAST(payment_value AS DOUBLE) AS payment_value
# MAGIC     FROM
# MAGIC         olist_dataset.bronze.bronze_olist_order_payments_dataset
# MAGIC ) AS source
# MAGIC ON target.order_id = source.order_id AND target.payment_sequential = source.payment_sequential
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC     INSERT (
# MAGIC         order_id,
# MAGIC         payment_sequential,
# MAGIC         payment_type,
# MAGIC         payment_installments,
# MAGIC         payment_value
# MAGIC     ) VALUES (
# MAGIC         source.order_id,
# MAGIC         source.payment_sequential,
# MAGIC         source.payment_type,
# MAGIC         source.payment_installments,
# MAGIC         source.payment_value
# MAGIC     );

# COMMAND ----------

# MAGIC %sql
# MAGIC --DROP TABLE IF EXISTS olist_dataset.silver.olist_order_reviews;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS olist_dataset.silver.olist_order_reviews (
# MAGIC     review_id STRING,
# MAGIC     order_id STRING,
# MAGIC     review_score STRING,
# MAGIC     review_comment_title STRING,
# MAGIC     review_comment_message STRING,
# MAGIC     review_creation_date TIMESTAMP,
# MAGIC     review_answer_timestamp TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;
# MAGIC
# MAGIC MERGE INTO olist_dataset.silver.olist_order_reviews AS target
# MAGIC USING (
# MAGIC     SELECT
# MAGIC         review_id,
# MAGIC         order_id,
# MAGIC         review_score,
# MAGIC         review_comment_title,
# MAGIC         review_comment_message,
# MAGIC         TO_TIMESTAMP(review_creation_date) AS review_creation_date,
# MAGIC         TO_TIMESTAMP(review_answer_timestamp) AS review_answer_timestamp
# MAGIC     FROM
# MAGIC         olist_dataset.bronze.bronze_olist_order_reviews_dataset
# MAGIC ) AS source
# MAGIC ON target.review_id = source.review_id
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC     INSERT (
# MAGIC         review_id,
# MAGIC         order_id,
# MAGIC         review_score,
# MAGIC         review_comment_title,
# MAGIC         review_comment_message,
# MAGIC         review_creation_date,
# MAGIC         review_answer_timestamp
# MAGIC     ) VALUES (
# MAGIC         source.review_id,
# MAGIC         source.order_id,
# MAGIC         source.review_score,
# MAGIC         source.review_comment_title,
# MAGIC         source.review_comment_message,
# MAGIC         source.review_creation_date,
# MAGIC         source.review_answer_timestamp
# MAGIC     );

# COMMAND ----------

# MAGIC %sql
# MAGIC --DROP TABLE olist_dataset.silver.olist_order_items;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS olist_dataset.silver.olist_order_items (
# MAGIC     order_id STRING,
# MAGIC     order_item_id INTEGER,
# MAGIC     product_id STRING,
# MAGIC     seller_id STRING,
# MAGIC     shipping_limit_date TIMESTAMP,
# MAGIC     price DOUBLE,
# MAGIC     freight_value DOUBLE
# MAGIC )
# MAGIC USING DELTA;
# MAGIC
# MAGIC MERGE INTO olist_dataset.silver.olist_order_items AS target
# MAGIC USING (
# MAGIC     SELECT
# MAGIC         order_id,
# MAGIC         CAST(order_item_id AS INTEGER) AS order_item_id,
# MAGIC         product_id,
# MAGIC         seller_id,
# MAGIC         TO_TIMESTAMP(shipping_limit_date) AS shipping_limit_date,
# MAGIC         CAST(price AS DOUBLE) AS price,
# MAGIC         CAST(freight_value AS DOUBLE) AS freight_value
# MAGIC     FROM
# MAGIC         olist_dataset.bronze.bronze_olist_order_items_dataset
# MAGIC ) AS source
# MAGIC ON target.order_id = source.order_id AND target.order_item_id = source.order_item_id
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC     INSERT (
# MAGIC         order_id,
# MAGIC         order_item_id,
# MAGIC         product_id,
# MAGIC         seller_id,
# MAGIC         shipping_limit_date,
# MAGIC         price,
# MAGIC         freight_value
# MAGIC     ) VALUES (
# MAGIC         source.order_id,
# MAGIC         source.order_item_id,
# MAGIC         source.product_id,
# MAGIC         source.seller_id,
# MAGIC         source.shipping_limit_date,
# MAGIC         source.price,
# MAGIC         source.freight_value
# MAGIC     );

# COMMAND ----------

# MAGIC %sql
# MAGIC --DROP TABLE olist_dataset.silver.olist_orders;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS olist_dataset.silver.olist_orders(
# MAGIC     order_id STRING,
# MAGIC     customer_id STRING,
# MAGIC     order_status STRING,
# MAGIC     order_purchase_timestamp TIMESTAMP,
# MAGIC     order_approved_at TIMESTAMP,
# MAGIC     order_delivered_carrier_date TIMESTAMP,
# MAGIC     order_delivered_customer_date TIMESTAMP,
# MAGIC     order_estimated_delivery_date TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;
# MAGIC
# MAGIC MERGE INTO olist_dataset.silver.olist_orders AS target
# MAGIC USING (
# MAGIC     SELECT
# MAGIC         order_id,
# MAGIC         customer_id,
# MAGIC         order_status,
# MAGIC         TO_TIMESTAMP(order_purchase_timestamp) AS order_purchase_timestamp,
# MAGIC         TO_TIMESTAMP(order_approved_at) AS order_approved_at,
# MAGIC         TO_TIMESTAMP(order_delivered_carrier_date) AS order_delivered_carrier_date,
# MAGIC         TO_TIMESTAMP(order_delivered_customer_date) AS order_delivered_customer_date,
# MAGIC         TO_TIMESTAMP(order_estimated_delivery_date) AS order_estimated_delivery_date
# MAGIC     FROM
# MAGIC         olist_dataset.bronze.bronze_olist_orders_dataset
# MAGIC ) AS source
# MAGIC ON target.order_id = source.order_id
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC     INSERT (
# MAGIC         order_id,
# MAGIC         customer_id,
# MAGIC         order_status,
# MAGIC         order_purchase_timestamp,
# MAGIC         order_approved_at,
# MAGIC         order_delivered_carrier_date,
# MAGIC         order_delivered_customer_date,
# MAGIC         order_estimated_delivery_date
# MAGIC     ) VALUES (
# MAGIC         source.order_id,
# MAGIC         source.customer_id,
# MAGIC         source.order_status,
# MAGIC         source.order_purchase_timestamp,
# MAGIC         source.order_approved_at,
# MAGIC         source.order_delivered_carrier_date,
# MAGIC         source.order_delivered_customer_date,
# MAGIC         source.order_estimated_delivery_date
# MAGIC     );

# COMMAND ----------

# MAGIC %sql
# MAGIC --DROP TABLE olist_dataset.silver.olist_products;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS olist_dataset.silver.olist_products(
# MAGIC     product_id STRING,
# MAGIC     product_category_name STRING,
# MAGIC     product_name_lenght INT,
# MAGIC     product_description_lenght INT,
# MAGIC     product_photos_qty INT,
# MAGIC     product_weight_g INT,
# MAGIC     product_length_cm INT,
# MAGIC     product_height_cm INT,
# MAGIC     product_width_cm INT
# MAGIC )
# MAGIC USING DELTA;
# MAGIC
# MAGIC MERGE INTO olist_dataset.silver.olist_products AS target
# MAGIC USING (
# MAGIC     SELECT
# MAGIC         product_id,
# MAGIC         product_category_name,
# MAGIC         CAST(product_name_lenght AS INT) AS product_name_lenght,
# MAGIC         CAST(product_description_lenght AS INT) AS product_description_lenght,
# MAGIC         CAST(product_photos_qty AS INT) AS product_photos_qty,
# MAGIC         CAST(product_weight_g AS INT) AS product_weight_g,
# MAGIC         CAST(product_length_cm AS INT) AS product_length_cm,
# MAGIC         CAST(product_height_cm AS INT) AS product_height_cm,
# MAGIC         CAST(product_width_cm AS INT) AS product_width_cm
# MAGIC     FROM
# MAGIC         olist_dataset.bronze.bronze_olist_products_dataset
# MAGIC ) AS source
# MAGIC ON target.product_id = source.product_id
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC     INSERT (
# MAGIC         product_id,
# MAGIC         product_category_name,
# MAGIC         product_name_lenght,
# MAGIC         product_description_lenght,
# MAGIC         product_photos_qty,
# MAGIC         product_weight_g,
# MAGIC         product_length_cm,
# MAGIC         product_height_cm,
# MAGIC         product_width_cm
# MAGIC     ) VALUES (
# MAGIC         source.product_id,
# MAGIC         source.product_category_name,
# MAGIC         source.product_name_lenght,
# MAGIC         source.product_description_lenght,
# MAGIC         source.product_photos_qty,
# MAGIC         source.product_weight_g,
# MAGIC         source.product_length_cm,
# MAGIC         source.product_height_cm,
# MAGIC         source.product_width_cm
# MAGIC     );

# COMMAND ----------

# MAGIC %sql
# MAGIC --DROP TABLE olist_dataset.silver.olist_sellers;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS olist_dataset.silver.olist_sellers(
# MAGIC     seller_id STRING,
# MAGIC     seller_zip_code_prefix INT,
# MAGIC     seller_city STRING,
# MAGIC     seller_state STRING
# MAGIC )
# MAGIC USING DELTA;
# MAGIC
# MAGIC MERGE INTO olist_dataset.silver.olist_sellers AS target
# MAGIC USING (
# MAGIC     SELECT
# MAGIC         seller_id,
# MAGIC         CAST(seller_zip_code_prefix AS INT) AS seller_zip_code_prefix,
# MAGIC         seller_city,
# MAGIC         seller_state
# MAGIC     FROM
# MAGIC         olist_dataset.bronze.bronze_olist_sellers_dataset
# MAGIC ) AS source
# MAGIC ON target.seller_id = source.seller_id
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC     INSERT (
# MAGIC         seller_id,
# MAGIC         seller_zip_code_prefix,
# MAGIC         seller_city,
# MAGIC         seller_state
# MAGIC     ) VALUES (
# MAGIC         source.seller_id,
# MAGIC         source.seller_zip_code_prefix,
# MAGIC         source.seller_city,
# MAGIC         source.seller_state
# MAGIC     );

# COMMAND ----------

# MAGIC %sql
# MAGIC --DROP TABLE olist_dataset.silver.product_category_name_translation;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS olist_dataset.silver.product_category_name_translation(
# MAGIC     product_category_name STRING,
# MAGIC     product_category_name_english STRING
# MAGIC )
# MAGIC USING DELTA;
# MAGIC
# MAGIC MERGE INTO olist_dataset.silver.product_category_name_translation AS target
# MAGIC USING (
# MAGIC     SELECT
# MAGIC         product_category_name,
# MAGIC         product_category_name_english
# MAGIC     FROM
# MAGIC         olist_dataset.bronze.bronze_product_category_name_translation
# MAGIC ) AS source
# MAGIC ON target.product_category_name = source.product_category_name
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC     INSERT (
# MAGIC         product_category_name,
# MAGIC         product_category_name_english
# MAGIC     ) VALUES (
# MAGIC         source.product_category_name,
# MAGIC         source.product_category_name_english
# MAGIC     );
