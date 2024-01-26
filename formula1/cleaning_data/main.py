
from datetime import datetime, date
from utils.schemas.circuits import circuits_schema
from utils.schemas.races import races_schema
from os import listdir
import json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType
from pyspark.sql import SparkSession

folder = '/home/pedrodev/Documents/pcf_repository/aws/formula1/cleaning_data/raw'
processed_folder = '/home/pedrodev/Documents/pcf_repository/aws/formula1/cleaning_data/processed'
extension = 'DS_Store'

spark = SparkSession.builder \
    .appName("Clean data") \
    .getOrCreate()


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
    df_renamed.printSchema()
    return df_renamed


def get_pyspark_schema(table, schema_dict):
    fields = schema_dict["fields"]
    spark_schema = StructType([
        StructField(list(field.keys())[0], map_data_type(field[list(field.keys())[0]]["field_type"]))
        for field in fields])
    new_fields = {list(field.keys())[0]: field[list(field.keys())[0]]["field_name"] for field in fields} 
    return spark_schema, new_fields

def get_file_list(folder, extension):
    try:
        file_list = [file for file in listdir(folder) if file.endswith(extension) is False]
        #print(f"files found are {file_list}")
    except FileNotFoundError:
            print(f'folder {folder} does not exist')
            file_list = []
    return file_list

def load_table_to_processed(file):
    table = file.split('.')[0]
    schema_dict = globals()[f"{table}_schema"]
    s3_file = f"{folder}/{file}"
    spark_schema, new_fields = get_pyspark_schema(table, schema_dict)
    if schema_dict['file_type'] == 'csv':   
        df = spark.read.csv(s3_file, header=True, schema=spark_schema)
    elif schema_dict['file_type'] == 'json':
        df = spark.read.json(s3_file, schema=spark_schema)
    
    df_renamed = rename_dataframe(df, new_fields)
    df_renamed.write.mode("overwrite").parquet(f"{processed_folder}/{table}")
    print(f"{table} was loaded to proccesed" )

def main():
    file_list = ['circuits.csv', 'races.csv'] #get_file_list(folder, extension)
    for file in file_list:
        print(file)
        load_table_to_processed(file)

main()