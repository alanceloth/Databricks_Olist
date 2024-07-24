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

# Verificar se a coluna 'total_order_value' já existe na tabela de pedidos e renomear se necessário
if 'total_order_value' in df_orders.columns:
    df_orders = df_orders.drop('total_order_value')

# Fazer a junção das tabelas
df_joined = df_orders.join(df_order_items, df_orders["order_id"] == df_order_items["order_id"], "left")

# Calcular o valor total da venda para cada pedido
df_total_order_value = df_joined.groupBy(df_orders["order_id"]).agg(spark_sum("price").alias("total_order_value"))

# Adicionar a coluna de valor total de venda à tabela de pedidos
df_orders_with_total = df_orders.join(df_total_order_value, "order_id", "left")

# Salvar a tabela atualizada como Delta Table
try:
  df_orders_with_total.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable("olist_dataset.silver.olist_orders")
except:
  df_orders_with_total.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("olist_dataset.silver.olist_orders")


# COMMAND ----------

# MAGIC %md
# MAGIC #### Produtos vendidos por categoria com status (delivered, invoiced, shipped e approved)

# COMMAND ----------

# Criar a Temp View
query = """
WITH product_counts AS (
    SELECT A.product_category_name, COUNT(B.product_id) AS quantity
    FROM olist_dataset.silver.olist_products AS A
    LEFT JOIN olist_dataset.silver.olist_order_items AS B ON A.product_id = B.product_id
    LEFT JOIN olist_dataset.silver.olist_orders O ON B.order_id = O.order_id
    WHERE O.order_status IN ('delivered', 'invoiced', 'shipped', 'approved')
    GROUP BY A.product_category_name
)
SELECT 
    product_category_name, 
    quantity, 
    (quantity * 100.0 / SUM(quantity) OVER ()) AS percentage_of_total
FROM 
    product_counts
ORDER BY 
    quantity DESC;
"""

# Carregar os dados da Temp View
produtos_vendidos_por_categoria  = spark.sql(query)

# Salvar em formato Delta
produtos_vendidos_por_categoria .write.format("delta").mode("overwrite").save("/Volumes/olist_dataset/silver/silver_volume/delta/produtos_vendidos_por_categoria ")

# Salvar em formato Parquet
produtos_vendidos_por_categoria .write.format("parquet").mode("overwrite").save("/Volumes/olist_dataset/silver/silver_volume/parquet/produtos_vendidos_por_categoria ")

# COMMAND ----------

# MAGIC %md
# MAGIC #### TOP 15 - Total de vendas por produto

# COMMAND ----------

# Criar a Temp View
query = """
WITH product_counts AS (
    SELECT A.product_id, COUNT(B.product_id) AS quantity
    FROM olist_dataset.silver.olist_products AS A
    LEFT JOIN olist_dataset.silver.olist_order_items AS B ON A.product_id = B.product_id
    LEFT JOIN olist_dataset.silver.olist_orders O ON B.order_id = O.order_id
    GROUP BY A.product_id
)
SELECT 
    product_id, 
    quantity, 
    (quantity * 100.0 / SUM(quantity) OVER ()) AS percentage_of_total
FROM 
    product_counts
ORDER BY 
    quantity DESC
LIMIT 15;
"""

# Carregar os dados da Temp View
top_15_products = spark.sql(query)

# Salvar em formato Delta
top_15_products.write.format("delta").mode("overwrite").save("/Volumes/olist_dataset/silver/silver_volume/delta/top_15_products")

# Salvar em formato Parquet
top_15_products.write.format("parquet").mode("overwrite").save("/Volumes/olist_dataset/silver/silver_volume/parquet/top_15_products")
