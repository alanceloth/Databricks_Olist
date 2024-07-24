# Databricks notebook source
# MAGIC %pip install --upgrade pip -q
# MAGIC %pip install kaggle -q
# MAGIC
# MAGIC dbutils.library.restartPython()
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Datum Teste Tecnico
# MAGIC - **Camada bronze**
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
# MAGIC # Configurações

# COMMAND ----------

KAGGLE_DATASET = "olistbr/brazilian-ecommerce"
KAGGLE_JSON_FILE = "/FileStore/Kaggle/kaggle.json"
DBFS_ENTRADA = "/FileStore/Kaggle/entrada/"
DBFS_DESTINO = "/Volumes/olist_dataset/raw/volume/csv/"
DBFS_PROCESSADO = "/Volumes/olist_dataset/raw/volume/processado"
NOME_PROJETO = "Olist Brazillian Ecommerce ETL"
DELTA_TABLE_PATH = "/FileStore/tables/delta/brazilian-ecommerce/"

# COMMAND ----------

# MAGIC %md
# MAGIC # Bibliotecas

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import dlt

import delta
import os
import json
import subprocess
import tempfile
import datetime
import shutil


# COMMAND ----------

spark = SparkSession.builder.appName("KaggleETL").getOrCreate()


# COMMAND ----------

# MAGIC %md
# MAGIC # Funções

# COMMAND ----------

# MAGIC %md
# MAGIC ## Auxiliares

# COMMAND ----------

def load_kaggle_json():
    try:
        file_location = KAGGLE_JSON_FILE 
        file_type = "json"
    
        infer_schema = "false"
        first_row_is_header = "false"
        delimiter = ","

        kaggle_credentials_df = spark.read.format(file_type) \
          .option("inferSchema", infer_schema) \
          .option("header", first_row_is_header) \
          .option("sep", delimiter) \
          .load(file_location)

        first_row = kaggle_credentials_df.first()

        os.environ['KAGGLE_USERNAME'] = first_row['username']
        os.environ['KAGGLE_KEY'] = first_row['key']
        print("Variáveis de ambiente (KAGGLE_USERNAME e KAGGLE_KEY) configuradas.")
    except Exception as e:
        print(f'Erro ao carregar credenciais do Kaggle: {e}')

# COMMAND ----------

def auth_kaggle():
    try:
        import kaggle
    except ImportError:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "kaggle"])

    from kaggle.api.kaggle_api_extended import KaggleApi
    
    username = os.environ['KAGGLE_USERNAME']
    key = os.environ['KAGGLE_KEY']
    api = KaggleApi()
    try:
        api.authenticate()
        print(f"{username} autenticado!")
        return api
    except Exception as e:
        print(f"Erro na autenticação: {e}")
        return None

def install_and_auth_kaggle():
    load_kaggle_json()
    return auth_kaggle()

# COMMAND ----------

def criar_esquema_bronze():
    try:
        spark.sql("CREATE SCHEMA IF NOT EXISTS olist_dataset.bronze")
        print("Esquema 'bronze' criado no catálogo 'olist_dataset' ou já existe.")
    except Exception as e:
        print(f"Erro ao criar o esquema 'bronze': {e}")


# COMMAND ----------

def criar_tabela_metadados():
    try:
        criar_esquema_bronze()
        spark.sql("""
            CREATE TABLE IF NOT EXISTS olist_dataset.bronze.kaggle_metadata (
                projeto STRING,
                dataset STRING,
                arquivo STRING,
                data_atualizacao TIMESTAMP,
                linhas_totais LONG,
                linhas_atualizadas LONG
            ) USING DELTA
        """)
        print("Tabela de metadados criada ou já existe.")
    except Exception as e:
        print(f"Erro ao criar a tabela de metadados: {e}")

# COMMAND ----------

def verificar_ultima_atualizacao(nome_projeto, nome_dataset, nome_arquivo):
    try:
        query = f"""
            SELECT * FROM olist_dataset.bronze.kaggle_metadata
            WHERE projeto = '{nome_projeto}' AND dataset = '{nome_dataset}' AND arquivo = '{nome_arquivo}'
            ORDER BY data_atualizacao DESC
            LIMIT 1
        """
        ultima_atualizacao = spark.sql(query).collect()
        if ultima_atualizacao:
            return ultima_atualizacao[0]
        else:
            return None
    except Exception as e:
        print(f"Erro ao verificar a última atualização: {e}")
        return None


# COMMAND ----------

def criar_diretorios(caminhos):
    for caminho in caminhos:
        try: 
            dbutils.fs.ls(caminho)
            if not dbutils.fs.ls(caminho):
                dbutils.fs.mkdirs(caminho)
        except:
            dbutils.fs.mkdirs(caminho)


# COMMAND ----------

def remover_arquivos_csv(diretorio):
    try:
        # Listar todos os arquivos no diretório
        arquivos = dbutils.fs.ls(diretorio)
        #print(arquivos)
        # Remover cada arquivo .csv
        for arquivo in arquivos:
            #print(arquivo)
            if arquivo.path.endswith(".csv") or arquivo.path.endswith(".csv/"):
                dbutils.fs.rm(arquivo.path, recurse=True)
                print(f"Arquivo removido: {arquivo.path}")
    except Exception as e:
        print(f"Erro ao remover arquivos: {e}")

# COMMAND ----------

def limpar_recursos(diretorios_para_excluir, schemas_para_excluir):
    # Excluir diretórios no DBFS
    for diretorio in diretorios_para_excluir:
        try:
            dbutils.fs.rm(diretorio, recurse=True)
            print(f"Diretório excluído: {diretorio}")
        except Exception as e:
            print(f"Erro ao excluir o diretório {diretorio}: {e}")

    # Excluir schemas e suas tabelas
    for schema in schemas_para_excluir:
        try:
            # Listar tabelas no schema
            tabelas = spark.catalog.listTables(schema)
            for tabela in tabelas:
                tabela_completa = f"{schema}.{tabela.name}"
                # Excluir tabela
                spark.sql(f"DROP TABLE IF EXISTS {tabela_completa}")
                print(f"Tabela excluída: {tabela_completa}")
            
            # Excluir schema
            spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
            print(f"Schema excluído: {schema}")
        except Exception as e:
            print(f"Erro ao excluir o schema {schema}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extração

# COMMAND ----------

def listar_dataset(api, nome_dataset):
    try:
        datasets = api.dataset_list(search=nome_dataset)
        for item in datasets:
            if item.ref == nome_dataset:
                return item
        print(f'Dataset {nome_dataset} não foi encontrado.')
        return None
    except Exception as e:
        print(f"Erro {e}")
        return None

# COMMAND ----------

def download_dataset(api, nome_dataset, saida_dbfs):
    with tempfile.TemporaryDirectory() as tmpdir:
        api.dataset_download_files(dataset=nome_dataset, path=tmpdir, unzip=True)
        
        arquivos = os.listdir(tmpdir)
        for arquivo in arquivos:
            path_entrada = os.path.join(tmpdir, arquivo)
            path_destino = os.path.join("/dbfs", saida_dbfs.strip("'"), arquivo)
            
            try:
                dbutils.fs.cp(f"file:{path_entrada}", f"dbfs:{path_destino}")
                print(f"Arquivo {arquivo} salvo em {path_destino}")
            except Exception as e:
                print(f"Erro ao copiar o arquivo para {path_destino}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformação

# COMMAND ----------

def processar_arquivo(path_destino, path_processado, delta_path, ultima_atualizacao, nome_projeto, nome_dataset, nome_arquivo, SCHEMA_NAME="olist_dataset.bronze"):
    try:
        # Verificar se o arquivo existe
        if not dbutils.fs.ls(path_destino):
            print(f"Arquivo não encontrado: {path_destino}")
            return
        
        # Atualizar o cache do Spark
        spark.sql(f"REFRESH TABLE {SCHEMA_NAME}.kaggle_metadata")

        df_novo = spark.read.csv(path_destino, header=True, inferSchema=True)

        if ultima_atualizacao:
            df_bronze = spark.read.format("delta").load(delta_path)
            df_atualizado = df_novo.subtract(df_bronze)

            if not df_atualizado.rdd.isEmpty():
                df_atualizado.write.mode("append").csv(path_processado)
                df_atualizado.write.mode("append").parquet(path_processado.replace('.csv', '.parquet'))
                df_atualizado.write.format("delta").mode("append").saveAsTable(f"{SCHEMA_NAME}.{nome_arquivo.replace('.csv', '')}")
                linhas_atualizadas = df_atualizado.count()
            else:
                linhas_atualizadas = 0
        else:
            df_novo.write.mode("overwrite").csv(path_processado)
            df_novo.write.mode("overwrite").parquet(path_processado.replace('.csv', '.parquet'))
            df_novo.write.format("delta").mode("overwrite").saveAsTable(f"{SCHEMA_NAME}.{nome_arquivo.replace('.csv', '')}")
            linhas_atualizadas = df_novo.count()

        atualizar_metadados(nome_projeto, nome_dataset, nome_arquivo, linhas_atualizadas, df_novo.count())
    except Exception as e:
        print(f"Erro ao processar o arquivo {nome_arquivo}: {e}")

# COMMAND ----------

def atualizar_metadados(projeto, dataset, arquivo, linhas_atualizadas, linhas_totais):
    try:
        spark.createDataFrame([(projeto, dataset, arquivo, datetime.datetime.now(), linhas_totais, linhas_atualizadas)], 
                              ["projeto", "dataset", "arquivo", "data_atualizacao", "linhas_totais", "linhas_atualizadas"]) \
            .write.mode("append").saveAsTable("olist_dataset.bronze.kaggle_metadata")
        print(f"Metadados atualizados para o arquivo {arquivo}.")
    except Exception as e:
        print(f"Erro ao atualizar metadados: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Carga

# COMMAND ----------

def criar_delta_tables(delta_path, schema_name):
    # Criar o esquema se não existir
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
    
    # Listar todos os diretórios no delta_path
    diretorios = dbutils.fs.ls(delta_path)
    
    for diretorio in diretorios:
        if diretorio.isDir():  # Verificar se é um diretório
            nome_diretorio = os.path.basename(diretorio.path.rstrip('/'))
            tabela_delta = f"{schema_name}.{nome_diretorio}"
            tabela_path = diretorio.path
            
            # Criar a Delta Table
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {tabela_delta}
                USING DELTA
                LOCATION '{tabela_path}'
            """)
            print(f"Tabela Delta {tabela_delta} criada no caminho {tabela_path}")


# COMMAND ----------

# MAGIC %md
# MAGIC # Pipeline

# COMMAND ----------

# MAGIC %md
# MAGIC ## Caso necessário limpar o projeto

# COMMAND ----------

diretorios_para_excluir = [
    DELTA_TABLE_PATH,
    DBFS_PROCESSADO,
    DBFS_DESTINO,
    DBFS_ENTRADA,
    "/user/hive/warehouse/"
]

schemas_para_excluir = [
    "raw",
    "bronze",
    "silver",
    "gold",
    "olist_dataset.bronze",
    "olist_dataset.silver",
    "olist_dataset.gold"
]
# Descomente a linha abaixo para usar a função quando necessário
limpar_recursos(diretorios_para_excluir, schemas_para_excluir)

spark.sql(f"CREATE CATALOG IF NOT EXISTS olist_dataset")
spark.sql(f"USE CATALOG olist_dataset")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS bronze")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS silver")
spark.sql("CREATE VOLUME olist_dataset.silver.silver_volume")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS gold")

# COMMAND ----------

# Chamar a função para garantir que a tabela de metadados exista
criar_tabela_metadados()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Autenticar no Kaggle

# COMMAND ----------

# Instalar e autenticar na API do Kaggle
api = install_and_auth_kaggle()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Listar datasets

# COMMAND ----------

if api:
    # Listar e obter informações do dataset
    dataset_info = listar_dataset(api, KAGGLE_DATASET)
    print(dataset_info)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Full Load (Load Raw e Raw -> Bronze)

# COMMAND ----------

if dataset_info:
    print(f"Dataset encontrado: {dataset_info.ref}")
    # Baixar e processar o dataset no DBFS
    download_dataset(api, KAGGLE_DATASET, DBFS_DESTINO)
else:
    print("Dataset não encontrado.")
    
arquivos = dbutils.fs.ls(DBFS_DESTINO)
for arquivo in arquivos:
    if arquivo.path.endswith(".csv"):
        nome_arquivo = os.path.basename(arquivo.path)
        path_processado = os.path.join(DBFS_PROCESSADO, nome_arquivo)
        delta_path = os.path.join(DELTA_TABLE_PATH, nome_arquivo.replace('.csv', ''))
        ultima_atualizacao = verificar_ultima_atualizacao(NOME_PROJETO, KAGGLE_DATASET, nome_arquivo)
        print(f"Processando: {arquivo}")
        processar_arquivo(arquivo.path, path_processado, delta_path, ultima_atualizacao, NOME_PROJETO, KAGGLE_DATASET, nome_arquivo)

remover_arquivos_csv(DBFS_PROCESSADO)

# COMMAND ----------

# MAGIC %md
# MAGIC ## CDC (caso necessário)

# COMMAND ----------

# # Merge caso usar CDC
# staging_data = delta.DeltaTable.forName(spark, "olist_dataset.bronze.olist_customers_dataset") 
# staging_data.alias("s") \
# .merge(delta_data.alias("d"),
# "s.customer_id = d.customer_id") \
# .whenMatchedDelete(condition = "d.CHANGE_TYPE = 'D'") \    #Trocar o CHANGE_TYPE pelo nome da coluna que indique mudança de status do CDC
# .whenMatchedUpdateAll(condition = "d.CHANGE_TYPE ='A'") \
# .whenNotMatchedInsertAll(condition = "d.CHANGE_TYPE = 'I'") \
# .execute() 
