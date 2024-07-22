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

# COMMAND ----------

# MAGIC %md
# MAGIC # TODO
# MAGIC - Tratar valores nulos (e também as inconsistências nos dados) [ok]
# MAGIC - Adicionar coluna de valor total de cada transação [ok]
# MAGIC - A partir de agregações, obter estatísticas de vendas (total de vendas por produto, por categoria, por seller) [ok]
# MAGIC - Calcular RFV para os clientes [ok]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Limpeza de nulos e inconsistências
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### olist_dataset.silver.olist_customers

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT customer_id
# MAGIC FROM olist_dataset.silver.olist_customers
# MAGIC WHERE NOT customer_id RLIKE '^[0-9a-fA-F]{32}$'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT customer_unique_id
# MAGIC FROM olist_dataset.silver.olist_customers
# MAGIC WHERE NOT customer_unique_id RLIKE '^[0-9a-fA-F]{32}$'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT customer_state
# MAGIC FROM olist_dataset.silver.olist_customers
# MAGIC WHERE LEN(customer_state) > 2 OR customer_state IS NULL OR customer_state = 'NA' OR customer_state = 'nan' OR customer_state = 'NaN' OR customer_state RLIKE '[0-9]'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM olist_dataset.silver.olist_customers
# MAGIC WHERE customer_city IS NULL OR customer_city = 'NA' OR customer_city = 'nan' OR customer_city = 'NaN'

# COMMAND ----------

# MAGIC %md
# MAGIC #### olist_dataset.silver.olist_geolocation

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) 
# MAGIC   FROM olist_dataset.silver.olist_geolocation
# MAGIC   WHERE geolocation_lat IS NULL OR geolocation_lng IS NULL

# COMMAND ----------

import unicodedata
from pyspark.sql.types import StringType

# Definir a UDF para remover acentos
def remove_accents(text):
    if text is not None:
        # Normalize to NFD form and filter out diacritical marks
        nfkd_form = unicodedata.normalize('NFD', text)
        return ''.join([c for c in nfkd_form if not unicodedata.combining(c)])
    return None

remove_accents_udf = udf(remove_accents, StringType())

# Registrar a UDF no Spark
spark.udf.register("remove_accents", remove_accents, StringType())

# COMMAND ----------

df = spark.read.table("olist_dataset.silver.olist_geolocation")

df_normalized = df.withColumn("geolocation_city", remove_accents_udf(df["geolocation_city"]))

df_normalized.write.format("delta").mode("overwrite").saveAsTable("olist_dataset.silver.olist_geolocation")

# COMMAND ----------

from pyspark.sql.functions import col, count, max as spark_max, row_number, when
from pyspark.sql.window import Window

df = spark.read.table("olist_dataset.silver.olist_geolocation")

# Agrupar por geolocation_zip_code_prefix e geolocation_city e contar as ocorrências
city_counts = df.groupBy("geolocation_zip_code_prefix", "geolocation_city")\
    .agg(count("*").alias("city_count"))

# Definir uma janela de partição por geolocation_zip_code_prefix e ordenar por city_count
window = Window.partitionBy("geolocation_zip_code_prefix").orderBy(col("city_count").desc())

# Adicionar uma coluna com a classificação das cidades dentro de cada prefixo
city_counts = city_counts.withColumn("rank", row_number().over(window))

# Filtrar para obter apenas as cidades mais frequentes
most_frequent_city = city_counts.filter(col("rank") == 1).select(
    "geolocation_zip_code_prefix",
    col("geolocation_city").alias("most_frequent_city")
)

# Unir a tabela original com a tabela das cidades mais frequentes
df_updated = df.join(most_frequent_city, "geolocation_zip_code_prefix", "left")\
    .withColumn("geolocation_city",
                when(col("geolocation_city") != col("most_frequent_city"), col("most_frequent_city"))
                .otherwise(col("geolocation_city")))\
    .drop("most_frequent_city")

df_updated.write.format("delta").mode("overwrite").saveAsTable("olist_dataset.silver.olist_geolocation")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     geolocation_zip_code_prefix,
# MAGIC     COUNT(DISTINCT geolocation_city) AS distinct_city_count
# MAGIC FROM 
# MAGIC     olist_dataset.silver.olist_geolocation
# MAGIC GROUP BY 
# MAGIC     geolocation_zip_code_prefix
# MAGIC HAVING 
# MAGIC     COUNT(DISTINCT geolocation_city) > 1
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### olist_dataset.silver.olist_order_items

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM olist_dataset.silver.olist_order_items
# MAGIC WHERE NOT order_id RLIKE '^[0-9a-fA-F]{32}$' OR NOT product_id RLIKE '^[0-9a-fA-F]{32}$' OR NOT seller_id RLIKE '^[0-9a-fA-F]{32}$'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM olist_dataset.silver.olist_order_items
# MAGIC WHERE price  <= 0 OR price IS NULL OR freight_value  < 0 OR freight_value IS NULL

# COMMAND ----------

# MAGIC %md
# MAGIC #### olist_dataset.silver.olist_orders

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) 
# MAGIC   FROM olist_dataset.silver.olist_orders
# MAGIC   WHERE NOT order_id RLIKE '^[0-9a-fA-F]{32}$' OR NOT customer_id RLIKE '^[0-9a-fA-F]{32}$'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT order_status
# MAGIC   FROM olist_dataset.silver.olist_orders

# COMMAND ----------

# MAGIC %md
# MAGIC #### olist_dataset.silver.olist_sellers

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) 
# MAGIC   FROM olist_dataset.silver.olist_sellers
# MAGIC   WHERE NOT seller_id RLIKE '^[0-9a-fA-F]{32}$'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     seller_zip_code_prefix,
# MAGIC     COUNT(DISTINCT seller_city) AS distinct_city_count
# MAGIC FROM 
# MAGIC     olist_dataset.silver.olist_sellers
# MAGIC GROUP BY 
# MAGIC     seller_zip_code_prefix
# MAGIC HAVING 
# MAGIC     COUNT(DISTINCT seller_city) > 1

# COMMAND ----------

df = spark.read.table("olist_dataset.silver.olist_sellers")

df_normalized = df.withColumn("seller_city", remove_accents_udf(df["seller_city"]))

df_normalized.write.format("delta").mode("overwrite").saveAsTable("olist_dataset.silver.olist_sellers")

# COMMAND ----------

df = spark.read.table("olist_dataset.silver.olist_sellers")

# Agrupar por seller_zip_code_prefix e seller_city e contar as ocorrências
city_counts = df.groupBy("seller_zip_code_prefix", "seller_city")\
    .agg(count("*").alias("city_count"))

# Definir uma janela de partição por seller_zip_code_prefix e ordenar por city_count
window = Window.partitionBy("seller_zip_code_prefix").orderBy(col("city_count").desc())

# Adicionar uma coluna com a classificação das cidades dentro de cada prefixo
city_counts = city_counts.withColumn("rank", row_number().over(window))

# Filtrar para obter apenas as cidades mais frequentes
most_frequent_city = city_counts.filter(col("rank") == 1).select(
    "seller_zip_code_prefix",
    col("seller_city").alias("most_frequent_city")
)

# Unir a tabela original com a tabela das cidades mais frequentes
df_updated = df.join(most_frequent_city, "seller_zip_code_prefix", "left")\
    .withColumn("seller_city",
                when(col("seller_city") != col("most_frequent_city"), col("most_frequent_city"))
                .otherwise(col("seller_city")))\
    .drop("most_frequent_city")

df_updated.write.format("delta").mode("overwrite").saveAsTable("olist_dataset.silver.olist_sellers")

# COMMAND ----------

# MAGIC %md
# MAGIC #### product_category_name_translation

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, col, countDistinct, when, lit

# Carregar a tabela olist_products
df = spark.read.table("olist_dataset.silver.product_category_name_translation")

# Extrair a base da categoria removendo sufixos numéricos
df = df.withColumn("base_category_name", regexp_replace(col("product_category_name"), '_\\d+$', ''))
df = df.withColumn("base_category_name_eng", regexp_replace(col("product_category_name_english"), '_\\d+$', ''))

# Contar as categorias base e identificar duplicadas
duplicate_categories = df.groupBy("base_category_name").agg(countDistinct("product_category_name").alias("num_variants"))\
    .filter(col("num_variants") > 1)
duplicate_categories_eng = df.groupBy("base_category_name_eng").agg(countDistinct("product_category_name_english").alias("num_variants_eng"))\
    .filter(col("num_variants_eng") > 1)

# Identificar categorias base duplicadas
base_categories = duplicate_categories.select("base_category_name").rdd.flatMap(lambda x: x).collect()
base_categories_eng = duplicate_categories_eng.select("base_category_name_eng").rdd.flatMap(lambda x: x).collect()

# Atualizar as categorias duplicadas para suas versões base
for base_category in base_categories:
    df = df.withColumn("product_category_name",
                       when(col("base_category_name") == base_category, lit(base_category))
                       .otherwise(col("product_category_name")))

for base_category_eng in base_categories_eng:
    df = df.withColumn("product_category_name_english",
                       when(col("base_category_name_eng") == base_category_eng, lit(base_category_eng))
                       .otherwise(col("product_category_name_english")))

# Salvar as mudanças na tabela olist_products
df.drop("base_category_name", "base_category_name_eng").write.format("delta").mode("overwrite").saveAsTable("olist_dataset.silver.product_category_name_translation")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Problemas reportados na camada bronze
# MAGIC
# MAGIC - bronze_olist_order_payments_dataset
# MAGIC - bronze_olist_order_reviews_dataset
# MAGIC - bronze_olist_products_dataset

# COMMAND ----------

# MAGIC %md
# MAGIC ##### bronze_olist_order_payments_dataset: 
# MAGIC - payment_value_is_above_zero (9 registros)
# MAGIC

# COMMAND ----------

df = spark.read.table("olist_dataset.silver.olist_order_payments")
df.createOrReplaceTempView("olist_order_payments")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM olist_order_payments
# MAGIC WHERE payment_value <= 0 OR payment_value IS NULL
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM olist_order_payments
# MAGIC WHERE 
# MAGIC (payment_value <= 0 OR payment_value IS NULL)
# MAGIC AND payment_type = "not_defined"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT payment_type 
# MAGIC   FROM olist_order_payments

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT payment_type 
# MAGIC   FROM olist_order_payments
# MAGIC   WHERE payment_value <= 0 OR payment_value IS NULL

# COMMAND ----------

# MAGIC %md
# MAGIC Dado que o valor de pagamento é zero, e para a maioria dos tipos de pagamento igual a zero o tipo predominante é "voucher", então todos os tipos de pagamento iguais a zero serão marcados como "voucher". Outro motivo é que não existe pagamento com valor zero para "boleto", "credit_card" e "debit_card".

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE olist_dataset.silver.olist_order_payments
# MAGIC SET payment_type = 'voucher'
# MAGIC WHERE payment_type = 'not_defined'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*)
# MAGIC   FROM olist_order_payments
# MAGIC   WHERE payment_type = 'not_defined'

# COMMAND ----------

# MAGIC %md
# MAGIC Order_id está consistente? Sim

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT order_id FROM olist_order_payments
# MAGIC WHERE NOT order_id RLIKE '^[0-9a-fA-F]{32}$'

# COMMAND ----------

# MAGIC %md
# MAGIC ##### bronze_olist_order_reviews_dataset:
# MAGIC -   review_creation_date_not_null	(8764 registros)
# MAGIC -   review_score_not_null	(2380 registros)
# MAGIC -   order_id_not_null	(2236 registros)
# MAGIC -   review_id_not_null	(1 registro)

# COMMAND ----------

df = spark.read.table("olist_dataset.silver.olist_order_reviews")
df.createOrReplaceTempView("olist_order_reviews")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM olist_order_reviews
# MAGIC WHERE review_creation_date IS NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM olist_dataset.silver.olist_order_reviews
# MAGIC WHERE review_creation_date IS NULL;

# COMMAND ----------

# MAGIC %md
# MAGIC Reviews com problema no review_id

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*)
# MAGIC FROM olist_order_reviews
# MAGIC WHERE NOT review_id RLIKE '^[0-9a-fA-F]{32}$' OR review_id IS NULL
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM olist_order_reviews
# MAGIC WHERE NOT review_id RLIKE '^[0-9a-fA-F]{32}$' OR review_id IS NULL

# COMMAND ----------

# MAGIC %md
# MAGIC Reviews com problema no order_id

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(order_id)
# MAGIC FROM olist_order_reviews
# MAGIC WHERE NOT order_id RLIKE '^[0-9a-fA-F]{32}$' OR order_id IS NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM olist_order_reviews
# MAGIC WHERE NOT order_id RLIKE '^[0-9a-fA-F]{32}$' OR order_id IS NULL

# COMMAND ----------

# MAGIC %md
# MAGIC Review score nulo

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM olist_order_reviews
# MAGIC WHERE review_score IS NULL

# COMMAND ----------

# MAGIC %md
# MAGIC ##### bronze_olist_products_dataset:
# MAGIC -   product_category_name_not_null	(610 registros)

# COMMAND ----------

df = spark.read.table("olist_dataset.silver.olist_products")
df.createOrReplaceTempView("olist_products")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM olist_products
# MAGIC WHERE product_category_name IS NULL

# COMMAND ----------

# MAGIC %md
# MAGIC Correção das categorias com nome duplicado:
# MAGIC - eletrodomesticos e eletrodomesticos_2
# MAGIC - casa_conforto e casa_conforto_2

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, col, countDistinct, when, lit

# Carregar a tabela olist_products
df = spark.read.table("olist_dataset.silver.olist_products")

# Extrair a base da categoria removendo sufixos numéricos
df = df.withColumn("base_category_name", regexp_replace(col("product_category_name"), '_\\d+$', ''))

# Contar as categorias base e identificar duplicadas
duplicate_categories = df.groupBy("base_category_name").agg(countDistinct("product_category_name").alias("num_variants"))\
    .filter(col("num_variants") > 1)

# Identificar categorias base duplicadas
base_categories = duplicate_categories.select("base_category_name").rdd.flatMap(lambda x: x).collect()

# Atualizar as categorias duplicadas para suas versões base
for base_category in base_categories:
    df = df.withColumn("product_category_name",
                       when(col("base_category_name") == base_category, lit(base_category))
                       .otherwise(col("product_category_name")))

# Salvar as mudanças na tabela olist_products
df.drop("base_category_name").write.format("delta").mode("overwrite").saveAsTable("olist_dataset.silver.olist_products")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT product_category_name,COUNT(*) 
# MAGIC   FROM olist_products
# MAGIC WHERE product_category_name LIKE '%casa_conforto%'
# MAGIC GROUP BY product_category_name

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT product_category_name,COUNT(*) 
# MAGIC   FROM olist_products
# MAGIC WHERE product_category_name LIKE '%eletrodomesticos%'
# MAGIC GROUP BY product_category_name
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Para produtos sem categoria, será substituído pela categoria "sem_categoria"

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE olist_dataset.silver.olist_products 
# MAGIC SET product_category_name = 'sem_categoria' 
# MAGIC WHERE product_category_name IS NULL;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC product_id consistente

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM olist_products
# MAGIC WHERE NOT product_id RLIKE '^[0-9a-fA-F]{32}$'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Agregações

# COMMAND ----------

from pyspark.sql.functions import sum as spark_sum

# Carregar as tabelas
df_orders = spark.read.table("olist_dataset.silver.olist_orders")
df_order_items = spark.read.table("olist_dataset.silver.olist_order_items")

# Fazer a junção das tabelas
df_joined = df_orders.join(df_order_items, df_orders["order_id"] == df_order_items["order_id"], "left")

# Calcular o valor total da venda para cada pedido
df_total_order_value = df_joined.groupBy(df_orders["order_id"]).agg(spark_sum("price").alias("total_order_value"))

# Adicionar a coluna de valor total de venda à tabela de pedidos
df_orders_with_total = df_orders.join(df_total_order_value, "order_id", "left")

# Salvar a tabela atualizada como Delta Table
df_orders_with_total.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable("olist_dataset.silver.olist_orders")


# COMMAND ----------

# MAGIC %md
# MAGIC #### Produtos vendidos por categoria com status (delivered, invoiced, shipped e approved)

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH product_counts AS (
# MAGIC     SELECT A.product_category_name, COUNT(B.product_id) AS quantity
# MAGIC     FROM olist_dataset.silver.olist_products AS A
# MAGIC     LEFT JOIN olist_dataset.silver.olist_order_items AS B ON A.product_id = B.product_id
# MAGIC     LEFT JOIN olist_dataset.silver.olist_orders O ON B.order_id = O.order_id
# MAGIC     WHERE O.order_status IN ("delivered", "invoiced", "shipped", "approved")
# MAGIC     GROUP BY A.product_category_name
# MAGIC )
# MAGIC SELECT 
# MAGIC     product_category_name, 
# MAGIC     quantity, 
# MAGIC     (quantity * 100.0 / SUM(quantity) OVER ()) AS percentage_of_total
# MAGIC FROM 
# MAGIC     product_counts
# MAGIC ORDER BY 
# MAGIC     quantity DESC;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### TOP 15 - Total de vendas por produto

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH product_counts AS (
# MAGIC     SELECT A.product_id, COUNT(B.product_id) AS quantity
# MAGIC     FROM olist_dataset.silver.olist_products AS A
# MAGIC     LEFT JOIN olist_dataset.silver.olist_order_items AS B ON A.product_id = B.product_id
# MAGIC     LEFT JOIN olist_dataset.silver.olist_orders O ON B.order_id = O.order_id
# MAGIC     --WHERE O.order_status IN ("delivered", "invoiced", "shipped", "approved")
# MAGIC     GROUP BY A.product_id
# MAGIC )
# MAGIC SELECT 
# MAGIC     product_id, 
# MAGIC     quantity, 
# MAGIC     (quantity * 100.0 / SUM(quantity) OVER ()) AS percentage_of_total
# MAGIC FROM 
# MAGIC     product_counts
# MAGIC ORDER BY 
# MAGIC     quantity DESC
# MAGIC     
# MAGIC LIMIT 15;

# COMMAND ----------

# MAGIC %md
# MAGIC ## RFV (Recencia, Frequência e Valor)

# COMMAND ----------

from pyspark.sql.functions import col, sum as spark_sum, count, max as spark_max, datediff, current_date

# Carregar as tabelas
df_orders = spark.read.format("delta").table("olist_dataset.silver.olist_orders")
df_order_items = spark.read.format("delta").table("olist_dataset.silver.olist_order_items")
df_customers = spark.read.format("delta").table("olist_dataset.silver.olist_customers")

# COMMAND ----------

# Filtrar os pedidos pelos status desejados
filtered_orders = df_orders.filter(col("order_status").isin("delivered", "invoiced", "shipped", "approved"))

# Obter a data da última compra de cada cliente
df_last_purchase = filtered_orders.groupBy("customer_id").agg(spark_max("order_purchase_timestamp").alias("last_purchase_date"))

# Calcular a recência
df_recency = df_last_purchase.withColumn("recency", datediff(current_date(), col("last_purchase_date")))


# Contar o número de pedidos de cada cliente
df_frequency = filtered_orders.groupBy("customer_id").agg(count("order_id").alias("frequency"))

# Fazer a junção das tabelas de pedidos e itens de pedido
df_order_items_joined = df_order_items.join(filtered_orders, "order_id")

# Calcular o valor total gasto por cada cliente
df_value = df_order_items_joined.groupBy("customer_id").agg(spark_sum("price").alias("monetary_value"))

# Combinar as métricas RFV
df_rfv = df_customers.join(df_recency, "customer_id", "left")\
    .join(df_frequency, "customer_id", "left")\
    .join(df_value, "customer_id", "left")

# Salvar a tabela RFV como Delta Table
df_rfv.write.format("delta").mode("overwrite").saveAsTable("olist_dataset.silver.rfv")


# COMMAND ----------

from pyspark.sql.functions import col, ntile, concat_ws, lit
from pyspark.sql.window import Window

# Carregar a tabela RFV
df_rfv = spark.read.format("delta").table("olist_dataset.silver.rfv")

# Definir janelas de classificação para cada métrica
window_spec = Window.orderBy(col("recency").asc())
df_rfv = df_rfv.withColumn("R_quartile", ntile(4).over(window_spec).cast("string"))

window_spec = Window.orderBy(col("frequency").desc())
df_rfv = df_rfv.withColumn("F_quartile", ntile(4).over(window_spec).cast("string"))

window_spec = Window.orderBy(col("monetary_value").desc())
df_rfv = df_rfv.withColumn("V_quartile", ntile(4).over(window_spec).cast("string"))

# Criar um código RFV combinando as pontuações R, F e V
df_rfv = df_rfv.withColumn("RFV_score", concat_ws("", col("R_quartile"), col("F_quartile"), col("V_quartile")))


# Definir segmentos com base nos códigos RFV
def segment(rfv_score, frequency):
    if frequency is None or frequency == 0:
        return 'Lead'
    
    if rfv_score in ['444', '443', '434', '433']:
        return 'Champions'
    elif rfv_score in ['344', '343', '334', '333','324']:
        return 'Loyal Customers'
    elif rfv_score in ['244', '243', '234', '233','342', '332', '322', '312','242', '232', '222', '212','224','214']:
        return 'Potential Loyalists'
    elif rfv_score in ['144', '143', '134', '133','142', '132', '122', '112', '114']:
        return 'New Customers'
    elif rfv_score in ['441', '431', '421', '411','443', '433', '423', '413']:
        return 'Promising'
    elif rfv_score in ['341', '331', '321', '311','343', '333', '323', '313']:
        return 'Needs Attention'
    elif rfv_score in ['241', '231', '221', '211','243', '233', '223', '213']:
        return 'About to Sleep'
    elif rfv_score in ['141', '131', '121', '111','143', '133', '123', '113']:
        return 'At Risk'
    elif rfv_score in ['442', '432', '422', '412']:
        return 'Potential Champions'
    else:
        return 'Others'

# Registrar a UDF para segmentação
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

segment_udf = udf(segment, StringType())

# Aplicar a UDF de segmentação corretamente
df_rfv = df_rfv.withColumn("segment", segment_udf(col("RFV_score"), col("frequency")))

# Salvar a tabela RFV segmentada como Delta Table
df_rfv.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable("olist_dataset.silver.olist_customers")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT segment, COUNT(DISTINCT customer_id) as customers
# MAGIC   FROM olist_dataset.silver.olist_customers
# MAGIC   GROUP BY segment
# MAGIC   ORDER BY customers DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC   FROM olist_dataset.silver.olist_customers
# MAGIC   WHERE segment = "Others"
# MAGIC   LIMIT 10;
# MAGIC   

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

expectations = {
    "customer_id_not_null": "customer_id IS NOT NULL",
    "customer_unique_id_not_null": "customer_unique_id IS NOT NULL",
    "customer_zip_code_prefix_not_null": "customer_zip_code_prefix IS NOT NULL"
}

@dlt.table(
    comment="Olist Customers Dataset Silver Table"
)
@dlt.expect_all(expectations)
def silver_olist_customers_dataset():
    return spark.table("olist_dataset.silver.olist_customers")
