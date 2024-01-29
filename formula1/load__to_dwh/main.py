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
from utils.constants import  db_postgresql_properties, processed_folder, fact_races_query, db_redshift_properties, table_queries



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

def load_df_to_redshift( df, table_name, db_properties):
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


def load_pq_to_redshift(table_name, query, db_postgresql_properties, db_redshift_properties):
    host = db_postgresql_properties['host']
    port = db_postgresql_properties['port']
    dbname = db_postgresql_properties['dbname']
    user = db_postgresql_properties['user']
    password = db_postgresql_properties['password']
    dwh_user=db_redshift_properties['user']
    dwh_password=db_redshift_properties['password']
    dwh_endpoint=db_redshift_properties['redshift_jdbc_url']

    postgresql_connection = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
    redshift_connection = f"postgresql+psycopg2://{dwh_user}:{dwh_password}@{dwh_endpoint}"
    postgresql_engine = create_engine(postgresql_connection)
    redshift_engine = create_engine(redshift_connection)
    df = pd.read_sql(query, postgresql_engine)
    print(df.head())
    try:
        df.to_sql(table_name, redshift_engine, index=False, if_exists='replace')
    except Exception as e:
        print(e)

def main():
    list_table_queries = list(table_queries.keys())
    print(list_table_queries)


    for table_name in list_table_queries:
        query=table_queries[table_name]
        load_pq_to_redshift(table_name, query, db_postgresql_properties, db_redshift_properties)
        
main()

    #folder_list = get_file_list(processed_folder)
#get_table_view(folder)

#df_fact_races_result = spark.sql(fact_races_query)
#load_data_to_postgresql(df_fact_races_result, "fact_races_rsult", db_properties)
