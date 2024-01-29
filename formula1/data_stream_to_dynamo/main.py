import json
import boto3
import base64

def main(event, context):
     """
    Función principal para procesar eventos de transmisión de datos de Kinesis y almacenarlos en una tabla DynamoDB.

    Esta función se diseñada para ser utilizada como un controlador de eventos en un entorno de AWS Lambda,
    donde `event` y `context` son parámetros proporcionados por AWS Lambda.

    Args:
        event: Contiene la información del evento de Kinesis, incluyendo los registros de datos.
        context: Información de contexto proporcionada por AWS Lambda.

    Devuelve:
        dict: Un diccionario con el código de estado y el cuerpo de la respuesta, indicando el éxito del procesamiento.
    """
    
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