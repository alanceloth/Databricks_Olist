# Databricks notebook source
# MAGIC %md
# MAGIC # Datum Teste Tecnico
# MAGIC - Camada bronze
# MAGIC   - ETL do dataset da [Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
# MAGIC   - Data quality checks (schema, colunas obrigatórias)
# MAGIC - Camada silver
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
# MAGIC # Bibliotecas

# COMMAND ----------

from pyspark.sql import SparkSession
import os
import json
import kaggle
import subprocess
from kaggle.api.kaggle_api_extended import KaggleApi


# COMMAND ----------

spark = SparkSession.builder.appName("KaggleETL").getOrCreate()


# COMMAND ----------

# MAGIC %md
# MAGIC # Funções

# COMMAND ----------

# MAGIC %md
# MAGIC ## Auxiliares

# COMMAND ----------

def auth_kaggle():
    load_kaggle_json()
    username = os.environ['KAGGLE_USERNAME']
    key = os.environ['KAGGLE_KEY']

    api = KaggleApi()
    try:
        api.authenticate()
        print(f"{username} autenticado!")
        return api
    except Exception as e:
        print(f"Erro na autenticação: {e}")
    

# COMMAND ----------

def load_kaggle_json():
  try:
    file_location = "/FileStore/tables/kaggle.json"
    file_type = "json"
  
    infer_schema = "false"
    first_row_is_header = "false"
    delimiter = ","

    kaggle_credentials_df = spark.read.format(file_type) \
      .option("inferSchema", infer_schema) \
      .option("header", first_row_is_header) \
      .option("sep", delimiter) \
      .load(file_location)
  except Exception as e:
    print(f'Erro: {e}')
  
  try:
    first_row = kaggle_credentials_df.first()

    os.environ['KAGGLE_USERNAME'] = first_row['username']
    os.environ['KAGGLE_KEY'] = first_row['key']

  except Exception as e:
    print(f'Erro: {e}')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extração

# COMMAND ----------

def listar_dataset(api, nome_dataset):
    try:
        datasets = api.dateset_list(search=nome_dataset)
        for item in datasets:
            if item.ref == nome_dataset:
                return item
        print(f'Dataset {nome_dataset} não foi encontrado.')
        return None
    except Exception as e:
        print(f"Erro {e}")
        return None

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformação

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## Carga

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Pipeline

# COMMAND ----------

try:
    api = auth_kaggle()
except Exception as e:
    print(e)
    subprocess.check_call([sys.executable, "-m", "pip", "install", "kaggle"])
    try:
        api = auth_kaggle()
    except Exception as e:
        print(e)
