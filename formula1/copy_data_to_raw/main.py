import boto3
import logging
import json
from utils.constants import source_bucket, target_bucket




logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s | %(name)s'
                           '| %(levelname)s | %(message)s')
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


def copy_files_to_raw(source_bucket, target_bucket):
    secret = get_secret()
    aws_access_key_id = secret['access_key']
    aws_secret_access_key = secret['secret_access_key']
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


def main():
    #print(event)
    logging.info("Process started")
    copy_files_to_raw(source_bucket, target_bucket)


main()