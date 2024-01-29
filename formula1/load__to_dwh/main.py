from datetime import datetime, date
import json
import logging
import psycopg2
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy import create_engine
from os import listdir
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType
from pyspark.sql import SQLContext, SparkSession
from pyspark import SparkContext
from utils.constants import  db_postgresql_properties, processed_folder, fact_races_query



logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s | %(name)s'
                           '| %(levelname)s | %(message)s')


"""
spark = SparkSession.builder \
    .appName("Clean data") \
    .config("spark.jars.packages", "io.github.spark-redshift-community:spark-redshift_2.12:5.1.0,org.apache.hadoop:hadoop-aws:3.2.2,org.apache.spark:spark-avro_2.12:2.4.5") \
    .getOrCreate()

spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", "AKIAVRUVPVA5Q6FBRG6C")
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "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")
spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.InstanceProfileCredentialsProvider")
# Utilizar DefaultAWSCredentialsProviderChain
spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
"""


def get_file_list(folder):
    """
    Obtiene una lista de archivos en el directorio especificado.

    Args:
        folder (str): Ruta del directorio del cual se quieren listar los archivos.

    Returns:
        list: Lista de nombres de archivos en el directorio especificado.
    """

    try:
        file_list = [file for file in listdir(folder)]
        print(f"files found are {file_list}")
    except FileNotFoundError:
            print(f'folder {folder} does not exist')
            file_list = []
    return file_list


def get_table_view(table):
     """
    Crea o reemplaza una vista temporal en Spark a partir de un archivo Parquet.

    Args:
        table (str): Nombre de la tabla (archivo Parquet) para crear la vista.
    """

    parquet_file = f"{processed_folder}/{table}"
    df=spark.read.parquet(parquet_file)
    df.createOrReplaceTempView(table)

def load_data_to_postgresql( df, table_name, db_properties):
    """
    Carga un DataFrame de Spark a una tabla en PostgreSQL utilizando la conexión de Redshift.

    Args:
        df (DataFrame): DataFrame de Spark que se va a cargar.
        table_name (str): Nombre de la tabla destino en PostgreSQL.
        db_properties (dict): Diccionario con propiedades de la base de datos como URL, usuario, contraseña, etc.
    """
    
    schema = db_properties['schema']
    df.write \
        .format("io.github.spark_redshift_community.spark.redshift") \
        .option("url", db_properties['redshift_jdbc_url']) \
        .option("tempdir", db_properties['tempdir']) \
        .option("dbtable", table_name) \
        .option("user", db_properties['user']) \
        .option("password", db_properties['password']) \
        .option("aws_iam_role", "arn:aws:iam::381491849275:role/data-developer-rol") \
        .mode("overwrite") \
        .save()

folder_list = get_file_list(processed_folder)
for folder in folder_list:
    print(folder)
    #get_table_view(folder)

#df_fact_races_result = spark.sql(fact_races_query)
#load_data_to_postgresql(df_fact_races_result, "fact_races_rsult", db_properties)
host = "localhost"
port = "5432"
dbname = db_postgresql_properties['dbname']
user = db_postgresql_properties['user']
password = db_postgresql_properties['password']

postgresql_connection = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
redshift_connection = f"postgresql+psycopg2://pecafa:C41c3d01982@formula1-workgroup.381491849275.us-east-1.redshift-serverless.amazonaws.com:5439/formula1_dwh"

# Crear motor de conexión
postgresql_engine = create_engine(postgresql_connection)
redshift_engine = create_engine(redshift_connection)

# Consulta SQL

# Leer los datos en un DataFrame de pandas
df = pd.read_sql(fact_races_query, postgresql_engine)
print(df.head())
df.to_sql('fact_races_result', redshift_engine, index=False, if_exists='replace')

