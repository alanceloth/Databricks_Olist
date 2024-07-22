# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import dlt

# COMMAND ----------

spark = SparkSession.builder.appName("KaggleETL").getOrCreate()


# COMMAND ----------

# MAGIC %md
# MAGIC # Pipeline

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quality Checks

# COMMAND ----------

expectations = {
    "customer_id_not_null": "customer_id IS NOT NULL",
    "customer_unique_id_not_null": "customer_unique_id IS NOT NULL",
    "customer_zip_code_prefix_not_null": "customer_zip_code_prefix IS NOT NULL"
}

@dlt.table(
    comment="Olist Customers Dataset Bronze Table"
)
@dlt.expect_all(expectations)
def bronze_olist_customers_dataset():
    return spark.table("olist_dataset.bronze.olist_customers_dataset")

# COMMAND ----------

expectations = {
    "geolocation_city_not_null": "geolocation_city IS NOT NULL",
    "geolocation_state_not_null": "geolocation_state IS NOT NULL",
    "geolocation_zip_code_prefix_not_null": "geolocation_zip_code_prefix IS NOT NULL"
}
@dlt.table(comment="Olist Geolocation Dataset Bronze Table")
@dlt.expect_all(expectations)
def bronze_olist_geolocation_dataset():
    df = spark.table("olist_dataset.bronze.olist_geolocation_dataset")
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
@dlt.table(comment="Olist Order Items Dataset Bronze Table")
@dlt.expect_all(expectations)
def bronze_olist_order_items_dataset():
    df = spark.table("olist_dataset.bronze.olist_order_items_dataset")
    return df

# COMMAND ----------

expectations = {
    "order_id_not_null": "order_id IS NOT NULL",
    "payment_type_not_null": "payment_type IS NOT NULL",
    "payment_value_is_above_zero": "payment_value > 0.0"
}
@dlt.table(comment="Olist Order Payments Dataset Bronze Table")
@dlt.expect_all(expectations)
def bronze_olist_order_payments_dataset():
    df = spark.table("olist_dataset.bronze.olist_order_payments_dataset")
    return df

# COMMAND ----------

expectations = {
    "review_id_not_null": "review_id IS NOT NULL",
    "order_id_not_null": "order_id IS NOT NULL",
    "review_score_not_null": "review_score IS NOT NULL",
    "review_creation_date_not_null": "review_creation_date IS NOT NULL"
}
@dlt.table(comment="Olist Order Reviews Dataset Bronze Table")
@dlt.expect_all(expectations)
def bronze_olist_order_reviews_dataset():
    df = spark.table("olist_dataset.bronze.olist_order_reviews_dataset")
    return df

# COMMAND ----------

expectations = {
    "customer_id_not_null": "customer_id IS NOT NULL",
    "order_id_not_null": "order_id IS NOT NULL",
    "order_status_not_null": "order_status IS NOT NULL",
    "order_purchase_timestamp_not_null": "order_purchase_timestamp IS NOT NULL"
}
@dlt.table(comment="Olist Orders Dataset Bronze Table")
@dlt.expect_all(expectations)
def bronze_olist_orders_dataset():
    df = spark.table("olist_dataset.bronze.olist_orders_dataset")
    return df

# COMMAND ----------

expectations = {
    "product_id_not_null": "product_id IS NOT NULL",
    "product_category_name_not_null": "product_category_name IS NOT NULL"
}
@dlt.table(comment="Olist Products Dataset Bronze Table")
@dlt.expect_all(expectations)
def bronze_olist_products_dataset():
    df = spark.table("olist_dataset.bronze.olist_products_dataset")
    return df

# COMMAND ----------

expectations = {
    "seller_id_not_null": "seller_id IS NOT NULL",
    "seller_zip_code_prefix_not_null": "seller_zip_code_prefix IS NOT NULL",
    "seller_city_prefix_not_null": "seller_city IS NOT NULL",
    "seller_state_not_null": "seller_state IS NOT NULL"
}
@dlt.table(comment="Olist Sellers Dataset Bronze Table")
@dlt.expect_all(expectations)
def bronze_olist_sellers_dataset():
    df = spark.table("olist_dataset.bronze.olist_sellers_dataset")
    return df

# COMMAND ----------

expectations = {
    "product_category_name_not_null": "product_category_name IS NOT NULL",
    "product_category_name_english_not_null": "product_category_name_english IS NOT NULL"
}
@dlt.table(comment="Product Category Name Translation Bronze Table")
@dlt.expect_all(expectations)
def bronze_product_category_name_translation():
    df = spark.table("olist_dataset.bronze.product_category_name_translation")
    return df
