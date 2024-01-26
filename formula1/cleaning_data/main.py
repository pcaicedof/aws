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
from utils.constants import target_bucket, prefix, folder, processed_folder, extension



logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s | %(name)s'
                           '| %(levelname)s | %(message)s')



spark = SparkSession.builder \
    .appName("Clean data") \
    .getOrCreate()

def get_secret():

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
    data_types = {
        "int": IntegerType(),
        "varchar": StringType(),
        "float": FloatType(),
        "date": DateType()
    }.get(data_type, StringType())

    return data_types

def rename_dataframe(df, new_fields):
    fields_list = list(new_fields.keys())
    df_renamed= df
    for field in fields_list:
        df_renamed = df_renamed.withColumnRenamed(field, new_fields[field])
    logging.info('dataframe has been renamed')
    return df_renamed


def get_pyspark_schema(table, schema_dict):
    fields = schema_dict["fields"]
    spark_schema = StructType([
        StructField(list(field.keys())[0], map_data_type(field[list(field.keys())[0]]["field_type"]))
        for field in fields])
    new_fields = {list(field.keys())[0]: field[list(field.keys())[0]]["field_name"] for field in fields} 
    return spark_schema, new_fields

def get_file_list(bucket, prefix):
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

def load_table_to_processed(file):
    table = file.split('.')[0]
    schema_dict = globals()[f"{table}_schema"]
    #s3_file = f"s3://{target_bucket}/{prefix}/{file}"
    s3_file = f"{folder}/{file}"
    spark_schema, new_fields = get_pyspark_schema(table, schema_dict)
    if schema_dict['file_type'] == 'csv':
        header= schema_dict['header']
        df = spark.read.csv(s3_file, header=header, schema=spark_schema)
    elif schema_dict['file_type'] == 'json':
        multiline= schema_dict['multiline']
        df = spark.read.json(s3_file, schema=spark_schema, multiLine=multiline)
    
    df_renamed = rename_dataframe(df, new_fields)
    df_renamed.write.mode("overwrite").parquet(f"{processed_folder}/{table}")
    logging.info(f"{table} was loaded to proccesed" )

def main():
    logging.info("Start of process")
    file_list = get_file_list(target_bucket, prefix)
    for file in file_list:
        logging.info(f"{file} will be processed")
        load_table_to_processed(file)

main()