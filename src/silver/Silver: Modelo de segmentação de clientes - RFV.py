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
# MAGIC ## RFV (Recencia, Frequência e Valor)

# COMMAND ----------

from pyspark.sql.functions import col, sum as spark_sum, count, max as spark_max, datediff, current_date

# Carregar as tabelas
df_orders = spark.read.format("delta").table("olist_dataset.silver.olist_orders")
df_order_items = spark.read.format("delta").table("olist_dataset.silver.olist_order_items")
df_customers = spark.read.format("delta").table("olist_dataset.silver.olist_customers")

# COMMAND ----------

# Lista de colunas a serem removidas
columns_to_drop = [
    'recency', 'frequency', 'monetary_value', 
    'R_quartile', 'F_quartile', 'V_quartile', 
    'RFV_score', 'segment', 'last_purchase_date'
]

# Remover as colunas listadas se existirem
for column in columns_to_drop:
    if column in df_customers.columns:
        df_customers = df_customers.drop(column)

spark.sql("DROP TABLE IF EXISTS olist_dataset.silver.rfv")

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
