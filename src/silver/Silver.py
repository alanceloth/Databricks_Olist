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
# MAGIC CREATE TABLE IF NOT EXISTS olist_dataset.silver.olist_customers
# MAGIC AS
# MAGIC SELECT
# MAGIC     customer_id,
# MAGIC     customer_unique_id,
# MAGIC     CAST(customer_zip_code_prefix AS INTEGER) AS customer_zip_code_prefix,
# MAGIC     customer_city,
# MAGIC     customer_state
# MAGIC FROM
# MAGIC     olist_dataset.bronze.olist_customers_dataset
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS olist_dataset.silver.olist_geolocation
# MAGIC AS
# MAGIC SELECT
# MAGIC     geolocation_zip_code_prefix,
# MAGIC     CAST(geolocation_lat AS DOUBLE) AS geolocation_lat,
# MAGIC     CAST(geolocation_lng AS DOUBLE) AS geolocation_lng,
# MAGIC     geolocation_city,
# MAGIC     geolocation_state
# MAGIC FROM
# MAGIC     olist_dataset.bronze.olist_geolocation_dataset
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS olist_dataset.silver.olist_order_payments
# MAGIC AS
# MAGIC SELECT
# MAGIC     order_id,
# MAGIC     CAST(payment_sequential AS INTEGER) AS payment_sequential,
# MAGIC     payment_type,
# MAGIC     payment_installments,
# MAGIC     CAST(payment_value AS DOUBLE) AS payment_value
# MAGIC FROM
# MAGIC     olist_dataset.bronze.olist_order_payments_dataset
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS olist_dataset.silver.olist_order_reviews
# MAGIC AS
# MAGIC SELECT
# MAGIC     review_id,
# MAGIC     order_id,
# MAGIC     review_score,
# MAGIC     review_comment_title,
# MAGIC     review_comment_message,
# MAGIC     TO_TIMESTAMP(review_creation_date) AS review_creation_date,
# MAGIC     TO_TIMESTAMP(review_answer_timestamp) AS review_answer_timestamp
# MAGIC FROM
# MAGIC     olist_dataset.bronze.olist_order_reviews_dataset
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS olist_dataset.silver.olist_order_items
# MAGIC AS
# MAGIC SELECT
# MAGIC     order_id,
# MAGIC     CAST(order_item_id AS INTEGER) AS order_item_id,
# MAGIC     product_id,
# MAGIC     seller_id,
# MAGIC     TO_TIMESTAMP(shipping_limit_date) AS shipping_limit_date,
# MAGIC     CAST(price AS DOUBLE) AS price,
# MAGIC     CAST(freight_value AS DOUBLE) AS freight_value
# MAGIC FROM
# MAGIC     olist_dataset.bronze.olist_order_items_dataset
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS olist_dataset.silver.olist_orders
# MAGIC AS
# MAGIC SELECT
# MAGIC     order_id,
# MAGIC     customer_id,
# MAGIC     order_status,
# MAGIC     TO_TIMESTAMP(order_purchase_timestamp) AS order_purchase_timestamp,
# MAGIC     TO_TIMESTAMP(order_approved_at) AS order_approved_at,
# MAGIC     TO_TIMESTAMP(order_delivered_carrier_date) AS order_delivered_carrier_date,
# MAGIC     TO_TIMESTAMP(order_delivered_customer_date) AS order_delivered_customer_date,
# MAGIC     TO_TIMESTAMP(order_estimated_delivery_date) AS order_estimated_delivery_date
# MAGIC FROM
# MAGIC     olist_dataset.bronze.olist_orders_dataset
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS olist_dataset.silver.olist_products
# MAGIC AS
# MAGIC SELECT
# MAGIC     product_id,
# MAGIC     product_category_name,
# MAGIC     CAST(product_name_length AS INT) AS product_name_length,
# MAGIC     CAST(product_description_length AS INT) AS product_description_length,
# MAGIC     CAST(product_photos_qty AS INT) AS product_photos_qty,
# MAGIC     CAST(product_weight_g AS INT) AS product_weight_g,
# MAGIC     CAST(product_length_cm AS INT) AS product_length_cm,
# MAGIC     CAST(product_height_cm AS INT) AS product_height_cm,
# MAGIC     CAST(product_width_cm AS INT) AS product_width_cm
# MAGIC FROM
# MAGIC     olist_dataset.bronze.olist_products_dataset
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS olist_dataset.silver.olist_sellers
# MAGIC AS
# MAGIC SELECT
# MAGIC     seller_id,
# MAGIC     CAST(seller_zip_code_prefix AS INT) AS seller_zip_code_prefix,
# MAGIC     seller_city,
# MAGIC     seller_state
# MAGIC FROM
# MAGIC     olist_dataset.bronze.olist_sellers_dataset
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS olist_dataset.silver.product_category_name_translation
# MAGIC AS
# MAGIC SELECT
# MAGIC     product_category_name,
# MAGIC     product_category_name_english
# MAGIC FROM
# MAGIC     olist_dataset.bronze.product_category_name_translation
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality

# COMMAND ----------

# MAGIC %md
# MAGIC [Os princípios de qualidade de dados](https://www.databricks.com/discover/pages/data-quality-management#principles-of-data-quality) sugerem um modelo de seis dimensões:
# MAGIC - Consistência – Os valores dos dados não devem conflitar com outros valores em conjuntos de dados
# MAGIC - Precisão – Não deve haver erros nos dados
# MAGIC - Validade – Os dados devem estar em conformidade com um certo formato
# MAGIC - Completude – Não deve haver dados faltando
# MAGIC - Atualidade – Os dados devem estar atualizados
# MAGIC - Unicidade – Não deve haver duplicatas
# MAGIC
# MAGIC Para a camada silver
# MAGIC - Usar verificações do tipo @dlt.expect_all_or_drop, para criar conjuntos de dados limpos.
# MAGIC - Criar tabela de [quarentena de dados](https://www.databricks.com/discover/pages/data-quality-management#data-quarantine)

# COMMAND ----------


