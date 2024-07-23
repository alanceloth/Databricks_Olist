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
# MAGIC ## TODO
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
