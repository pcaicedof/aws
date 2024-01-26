import boto3

aws_access_key_id = 'AKIAVRUVPVA5Q6FBRG6C'
aws_secret_access_key = '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'
region_name = 'TU_REGION'
source_bucket = 'orbidi-data-sources'
target_bucket = 'orbidi-data-project'


s3_client = boto3.client('s3',
                         aws_access_key_id=aws_access_key_id,
                         aws_secret_access_key=aws_secret_access_key)



def copy_files_to_raw(source_bucket, target_bucket, s3_client):
    # Listar todos los objetos en el bucket de origen
    objects = s3_client.list_objects_v2(Bucket=source_bucket)['Contents']    
    # Copiar cada objeto al bucket de destino
    for obj in objects:
        copy_source = {
            'Bucket': source_bucket,
            'Key': obj['Key']
        }
        target_key = f"raw/{obj['Key']}" if target_bucket else obj['Key']
        s3_client.copy(copy_source, target_bucket, target_key)
        print(f"Copiado: {obj['Key']}")


def main(event, context):
    print(event)
    copy_files_to_raw(source_bucket, target_bucket, s3_client)


