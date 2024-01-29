import boto3
import json
import csv
from utils.constants import stream_name, lap_times_file, region_name

kinesis_client = boto3.client('kinesis', region_name=region_name)


def put_records_on_data_stream(file):
    with open(lap_times_file, newline='') as file:
        reader = csv.DictReader(file, fieldnames = ( "race_id","driver_id","lap","position", "time", "milliseconds" ))
        
        for idx,row in enumerate(reader):
            record_data = json.dumps(row)
            # Poner el registro en el stream de Kinesis        
            response = kinesis_client.put_record(
                StreamName=stream_name,
                Data=record_data,
                PartitionKey='lap'  # Puedes modificar esto según tus necesidades
            )
            
            if idx == 50:
                break

            print("Registro enviado:", record_data)


def main():
    put_records_on_data_stream(lap_times_file)

main()