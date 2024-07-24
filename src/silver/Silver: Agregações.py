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
# MAGIC CREATE OR REPLACE TEMP VIEW produtos_vendidos_por_categoria AS
# MAGIC WITH product_counts AS (
# MAGIC     SELECT A.product_category_name, COUNT(B.product_id) AS quantity
# MAGIC     FROM olist_dataset.silver.olist_products AS A
# MAGIC     LEFT JOIN olist_dataset.silver.olist_order_items AS B ON A.product_id = B.product_id
# MAGIC     LEFT JOIN olist_dataset.silver.olist_orders O ON B.order_id = O.order_id
# MAGIC     WHERE O.order_status IN ('delivered', 'invoiced', 'shipped', 'approved')
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

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create or replace the table from the temporary view
# MAGIC CREATE OR REPLACE TABLE delta.`/Volumes/olist_dataset/silver/silver_volume/delta/produtos_vendidos_por_categoria` USING DELTA AS
# MAGIC SELECT * FROM produtos_vendidos_por_categoria;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE produtos_vendidos_por_categoria
# MAGIC USING PARQUET
# MAGIC LOCATION '/Volumes/olist_dataset/silver/silver_volume/parquet/produtos_vendidos_por_categoria'
# MAGIC AS
# MAGIC SELECT * FROM produtos_vendidos_por_categoria;

# COMMAND ----------

# MAGIC %md
# MAGIC #### TOP 15 - Total de vendas por produto

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Criando uma visualização temporária para os top 15 produtos
# MAGIC CREATE OR REPLACE TEMP VIEW top_15_products_temp AS
# MAGIC WITH product_counts AS (
# MAGIC     SELECT A.product_id, COUNT(B.product_id) AS quantity
# MAGIC     FROM olist_dataset.silver.olist_products AS A
# MAGIC     LEFT JOIN olist_dataset.silver.olist_order_items AS B ON A.product_id = B.product_id
# MAGIC     LEFT JOIN olist_dataset.silver.olist_orders O ON B.order_id = O.order_id
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
# MAGIC LIMIT 15;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create or replace the table from the temporary view
# MAGIC CREATE OR REPLACE TABLE delta.`/Volumes/olist_dataset/silver/silver_volume/delta/top_15_produtos_vendidos` USING DELTA AS
# MAGIC SELECT * FROM top_15_products_temp
# MAGIC
# MAGIC CREATE OR REPLACE TABLE top_15_products_temp
# MAGIC USING PARQUET
# MAGIC LOCATION '/Volumes/olist_dataset/silver/silver_volume/parquet/top_15_produtos_vendidos'
# MAGIC AS
# MAGIC SELECT * FROM top_15_products_temp;
