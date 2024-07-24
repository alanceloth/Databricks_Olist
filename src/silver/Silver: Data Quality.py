# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import dlt

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
# MAGIC - Usar verificações do tipo @dlt.expect_all_or_drop ou @dlt.expect_all_or_fail, para criar conjuntos de dados limpos.
# MAGIC - Criar tabela de [quarentena de dados](https://www.databricks.com/discover/pages/data-quality-management#data-quarantine)

# COMMAND ----------

expectations = {
    "customer_id_not_null": "customer_id IS NOT NULL",
    "customer_unique_id_not_null": "customer_unique_id IS NOT NULL",
    "customer_zip_code_prefix_not_null": "customer_zip_code_prefix IS NOT NULL",
    "segment_not_null": "segment IS NOT NULL"
}

@dlt.table(
    comment="Olist Customers Dataset Silver Table"
)
@dlt.expect_all_or_fail(expectations)
def silver_olist_customers():
    return spark.table("olist_dataset.silver.olist_customers")

# COMMAND ----------

expectations = {
    "geolocation_city_not_null": "geolocation_city IS NOT NULL",
    "geolocation_state_not_null": "geolocation_state IS NOT NULL",
    "geolocation_zip_code_prefix_not_null": "geolocation_zip_code_prefix IS NOT NULL"
}
@dlt.table(comment="Olist Geolocation Dataset Silver Table")
@dlt.expect_all_or_fail(expectations)
def silver_olist_geolocation():
    df = spark.table("olist_dataset.silver.olist_geolocation")
    return df

# COMMAND ----------

expectations = {
    "order_id_not_null": "order_id IS NOT NULL",
    "order_item_id_not_null": "order_item_id IS NOT NULL",
    "product_id_not_null": "product_id IS NOT NULL",
    "seller_id_not_null": "seller_id IS NOT NULL",
    "price_not_null": "price IS NOT NULL",
    "freight_value_not_null": "freight_value IS NOT NULL",
    "price_is_above_zero": "price > 0.0"
}
@dlt.table(comment="Olist Order Items Dataset Silver Table")
@dlt.expect_all_or_fail(expectations)
def silver_olist_order_items():
    df = spark.table("olist_dataset.silver.olist_order_items")
    return df

# COMMAND ----------

expectations = {
    "order_id_not_null": "order_id IS NOT NULL",
    "payment_type_not_null": "payment_type IS NOT NULL",
    "payment_value_is_above_zero": "payment_value > 0.0 OR payment_value == 0.0"
}
@dlt.table(comment="Olist Order Payments Dataset Silver Table")
@dlt.expect_all_or_fail(expectations)
def silver_olist_order_payments():
    df = spark.table("olist_dataset.silver.olist_order_payments")
    return df

# COMMAND ----------

expectations = {
    "review_id_not_null": "review_id IS NOT NULL",
    "order_id_not_null": "order_id IS NOT NULL",
    "review_score_not_null": "review_score IS NOT NULL",
    "review_creation_date_not_null": "review_creation_date IS NOT NULL"
}
@dlt.table(comment="Olist Order Reviews Dataset Silver Table")
@dlt.expect_all_or_drop(expectations)
def silver_olist_order_reviews():
    df = spark.table("olist_dataset.silver.olist_order_reviews")
    return df

# COMMAND ----------

expectations = {
    "customer_id_not_null": "customer_id IS NOT NULL",
    "order_id_not_null": "order_id IS NOT NULL",
    "order_status_not_null": "order_status IS NOT NULL",
    "order_purchase_timestamp_not_null": "order_purchase_timestamp IS NOT NULL"
}
@dlt.table(comment="Olist Orders Dataset Silver Table")
@dlt.expect_all_or_fail(expectations)
def silver_olist_orders():
    df = spark.table("olist_dataset.silver.olist_orders")
    return df

# COMMAND ----------

expectations = {
    "product_id_not_null": "product_id IS NOT NULL",
    "product_category_name_not_null": "product_category_name IS NOT NULL"
}
@dlt.table(comment="Olist Products Dataset Silver Table")
@dlt.expect_all_or_fail(expectations)
def silver_olist_products():
    df = spark.table("olist_dataset.silver.olist_products")
    return df

# COMMAND ----------

expectations = {
    "seller_id_not_null": "seller_id IS NOT NULL",
    "seller_zip_code_prefix_not_null": "seller_zip_code_prefix IS NOT NULL",
    "seller_city_prefix_not_null": "seller_city IS NOT NULL",
    "seller_state_not_null": "seller_state IS NOT NULL"
}
@dlt.table(comment="Olist Sellers Dataset Silver Table")
@dlt.expect_all_or_fail(expectations)
def silver_olist_sellers():
    df = spark.table("olist_dataset.silver.olist_sellers")
    return df

# COMMAND ----------

expectations = {
    "product_category_name_not_null": "product_category_name IS NOT NULL",
    "product_category_name_english_not_null": "product_category_name_english IS NOT NULL"
}
@dlt.table(comment="Product Category Name Translation Silver Table")
@dlt.expect_all_or_fail(expectations)
def silver_product_category_name_translation():
    df = spark.table("olist_dataset.silver.product_category_name_translation")
    return df
