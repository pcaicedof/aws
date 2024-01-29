from datetime import datetime, date
import json
import logging
import boto3
from os import listdir
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType
from pyspark.sql import SparkSession
from utils.schemas.circuits import circuits_schema
from utils.schemas.races import races_schema
from utils.schemas.constructors import constructors_schema
from utils.schemas.drivers import drivers_schema
from utils.schemas.pit_stops import pit_stops_schema
from utils.schemas.results import results_schema
from utils.schemas.lap_times import lap_times_schema
from utils.schemas.qualifying import qualifying_schema
from utils.constants import target_bucket, prefix_raw, prefix_processed,folder, processed_folder, extension, db_properties



logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s | %(name)s'
                           '| %(levelname)s | %(message)s')



spark = SparkSession.builder \
    .appName("Clean data") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.2.5") \
    .getOrCreate()

def get_secret():
    """
    Recupera un secreto (credenciales de acceso de AWS) del Administrador de Secretos de AWS.

    Devuelve:
        Un diccionario que contiene la cadena de secreto, que incluye el ID de clave de acceso y la clave de acceso secreta de AWS.
    """

    secret_name = "aws_access"
    region_name = "us-east-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        raise e
    return json.loads(get_secret_value_response['SecretString'])



def map_data_type(data_type):
    """
    Mapea un string de tipo de dato a un tipo de dato SQL de PySpark correspondiente.

    Args:
        data_type (str): El tipo de dato como string (por ejemplo, 'int', 'varchar', 'float', 'date').

    Devuelve:
        Un objeto de tipo de dato SQL de PySpark (por ejemplo, IntegerType, StringType, FloatType, DateType).
    """

    data_types = {
        "int": IntegerType(),
        "varchar": StringType(),
        "float": FloatType(),
        "date": DateType()
    }.get(data_type, StringType())

    return data_types

def rename_dataframe(df, new_fields):
    """
    Renombra columnas de un DataFrame de PySpark basado en un mapeo dado.

    Args:
        df (DataFrame): El DataFrame de PySpark original.
        new_fields (dict): Un diccionario que mapea nombres antiguos de columnas a nombres nuevos.

    Devuelve:
        DataFrame: El DataFrame con columnas renombradas.
    """

    fields_list = list(new_fields.keys())
    df_renamed= df
    for field in fields_list:
        df_renamed = df_renamed.withColumnRenamed(field, new_fields[field])
    logging.info('dataframe has been renamed')
    return df_renamed


def get_pyspark_schema(table, schema_dict):
    """
    Genera un esquema de PySpark y un diccionario de renombrado de campos para una tabla dada.

    Args:
        table (str): El nombre de la tabla.
        schema_dict (dict): Un diccionario que contiene información del esquema.

    Devuelve:
        tuple: Una tupla que contiene un esquema StructType de PySpark y un diccionario para el renombrado de campos.
    """

    fields = schema_dict["fields"]
    spark_schema = StructType([
        StructField(list(field.keys())[0], map_data_type(field[list(field.keys())[0]]["field_type"]))
        for field in fields])
    new_fields = {list(field.keys())[0]: field[list(field.keys())[0]]["field_name"] for field in fields} 
    return spark_schema, new_fields

def get_file_list(bucket, prefix):
    """
    Recupera una lista de archivos de un bucket S3 con un prefijo especificado.

    Args:
        bucket (str): El nombre del bucket S3.
        prefix (str): El prefijo para filtrar objetos en el bucket S3.

    Devuelve:
        list: Una lista de nombres de archivos en el bucket S3 que coinciden con el prefijo dado.
    """


    secret = get_secret()
    aws_access_key_id = secret['access_key']
    aws_secret_access_key = secret['secret_access_key']
    s3_client = boto3.client('s3',
                            aws_access_key_id=aws_access_key_id,
                            aws_secret_access_key=aws_secret_access_key)
    objects = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)['Contents']

    file_list = []
    for obj in objects:
        if obj['Key'] != f"{prefix}/":
            file =obj['Key'].split('/')[1]
            file_list.append(file)
    return list(set(file_list))


def load_data_to_postgresql( df, table_name, db_properties):
    """
    Carga un DataFrame de PySpark en una tabla de base de datos PostgreSQL.

    Args:
        df (DataFrame): El DataFrame de PySpark que se cargará.
        table_name (str): El nombre de la tabla de PostgreSQL.
        db_properties (dict): Un diccionario que contiene propiedades de la base de datos como URL, usuario, contraseña y esquema.
    """

    schema = db_properties['schema']
    df.write \
        .format("jdbc") \
        .option("url", db_properties['url']) \
        .option("dbtable", f"{schema}.{table_name}") \
        .option("user", db_properties['user']) \
        .option("password", db_properties['password']) \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()


def load_table_to_processed(file):
    """
    Procesa un archivo desde formato crudo, lo transforma en un DataFrame, renombra columnas, lo guarda en formato procesado,
    y lo carga en un bucket de s3 y en PostgreSQL.

    Args:
        file (str): El nombre del archivo a procesar.
    """

    table = file.split('.')[0]
    schema_dict = globals()[f"{table}_schema"]
    #raw_s3_file = f"s3://{target_bucket}/{prefix_raw}/{file}"
    raw_s3_file = f"{folder}/{file}"
    spark_schema, new_fields = get_pyspark_schema(table, schema_dict)
    if schema_dict['file_type'] == 'csv':
        header= schema_dict['header']
        df = spark.read.csv(raw_s3_file, header=header, schema=spark_schema)
    elif schema_dict['file_type'] == 'json':
        multiline= schema_dict['multiline']
        df = spark.read.json(raw_s3_file, schema=spark_schema, multiLine=multiline)
    df_renamed = rename_dataframe(df, new_fields)
    #processed_file = f"s3a://{target_bucket}/{prefix_raw}/{table}"
    processed_file = f"{processed_folder}/{table}"
    df_renamed.write.mode("overwrite").parquet(processed_file)
    logging.info(f"{table} was loaded to proccesed folder" )
    load_data_to_postgresql( df_renamed, table, db_properties)
    logging.info(f"{table} was loaded to postgresql" )
    

def main():
    """
    La función principal para ejecutar el flujo de trabajo de procesamiento de datos. Recupera una lista de archivos, procesa cada archivo,
    y registra el inicio y fin del proceso.
    """
    
    logging.info("Start of process")
    file_list = get_file_list(target_bucket, prefix_raw)
    for file in file_list:
        logging.info(f"{file} will be processed")
        load_table_to_processed(file)
    logging.info("End of process")

main()