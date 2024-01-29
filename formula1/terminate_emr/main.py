import boto3

# Crear un cliente de EMR
emr_client = boto3.client('emr')

# ID del clúster a terminar
cluster_id = 'j-3I9L66QWY8ZPI'

# Terminar el clúster
emr_client.terminate_job_flows(JobFlowIds=[cluster_id])