import boto3
import logging
import json
from utils.constants import source_bucket, target_bucket




logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s | %(name)s'
                           '| %(levelname)s | %(message)s')

def get_secret(secret_name):
    """
    Recupera un secreto específico (como credenciales de AWS) del Administrador de Secretos de AWS.

    Args:
        secret_name (str): El nombre del secreto que se va a recuperar.

    Devuelve:
        dict: Un diccionario que contiene los detalles del secreto, típicamente credenciales de acceso.
    """

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


def copy_files_to_raw(source_bucket, target_bucket):
     """
    Copia archivos desde un bucket S3 fuente a un bucket S3 destino en la carpeta 'raw'.

    Utiliza credenciales de AWS obtenidas a través de la función `get_secret` para autenticarse
    y realizar las operaciones de copia.

    Args:
        source_bucket (str): El nombre del bucket S3 fuente de donde se copiarán los archivos.
        target_bucket (str): El nombre del bucket S3 destino donde se copiarán los archivos.
    """

    aws_access = get_secret("aws_access")
    aws_access_key_id = aws_access['access_key']
    aws_secret_access_key = aws_access['secret_access_key']
    s3_client = boto3.client('s3',
                         aws_access_key_id=aws_access_key_id,
                         aws_secret_access_key=aws_secret_access_key)
    objects = s3_client.list_objects_v2(Bucket=source_bucket)['Contents']    
    for obj in objects:
        copy_source = {
            'Bucket': source_bucket,
            'Key': obj['Key']
        }
        print(copy_source)
        target_key = f"raw/{obj['Key']}" if target_bucket else obj['Key']
        s3_client.copy(copy_source, target_bucket, target_key)
        logging.info(f"File {obj['Key']} has been copied")


def main(event, context):
    """
    Función principal que se ejecuta para iniciar el proceso de copia de archivos.

    Esta función se diseñó para ser utilizada como un controlador de eventos en un entorno de AWS Lambda,
    donde `event` y `context` son parámetros proporcionados por AWS Lambda.

    Args:
        event: Información sobre el evento que desencadenó la ejecución de esta función.
        context: Información de contexto proporcionada por AWS Lambda.
    """
    
    logging.info("Process started")
    copy_files_to_raw(source_bucket, target_bucket)