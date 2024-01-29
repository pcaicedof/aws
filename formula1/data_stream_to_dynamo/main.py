import json
import boto3
import base64

def main(event, context):
    # Cliente de DynamoDB
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('lap_times')

    # Procesar registros de Kinesis
    for record in event['Records']:
        # Decodificar y cargar el registro de Kinesis
        payload = json.loads(base64.b64decode(record["kinesis"]["data"]))
        print(payload)
        print(type(payload))
        # Insertar el registro en DynamoDB
        response = table.put_item(Item=payload)

    return {
        'statusCode': 200,
        'body': json.dumps('Registros procesados con éxito')
    }