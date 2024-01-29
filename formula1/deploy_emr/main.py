import boto3

# Crear un cliente de EMR
emr_client = boto3.client('emr', region_name='us-east-1')

# Configuración del cluster EMR
cluster_id = emr_client.run_job_flow(
    Name='MiClusterEMR',
    ServiceRole='EMRService_Rol',
    JobFlowRole='EMRInstance_Rol',
    VisibleToAllUsers=True,
    LogUri='s3://orbidi-logs/emr',
    ReleaseLabel='emr-5.32.0',
    Instances={
        'InstanceGroups': [
            {
                'Name': "Master nodes",
                'Market': 'ON_DEMAND',
                'InstanceRole': 'MASTER',
                'InstanceType': 'm5.xlarge',
                'InstanceCount': 1,
            },
            {
                'Name': "Worker nodes",
                'Market': 'ON_DEMAND',
                'InstanceRole': 'CORE',
                'InstanceType': 'm5.xlarge',
                'InstanceCount': 1,
            }
        ],
        'Ec2KeyName': 'emr_key',
        'Ec2SubnetId': 'subnet-004654368a87e1418',
        'KeepJobFlowAliveWhenNoSteps': True,
        'TerminationProtected': False
    },
    Applications=[{'Name': 'Spark'}]
)

print(f"Cluster creado con ID: {cluster_id['JobFlowId']}")


step_response = emr_client.add_job_flow_steps(
    JobFlowId=cluster_id['JobFlowId'],
    Steps=[
        {
            'Name': 'cleaning_data',
            'ActionOnFailure': 'TERMINATE_CLUSTER',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    'spark-submit',
                    '--deploy-mode', 'cluster',
                    's3://orbidi-code/cleaning_data/main.py'
                ]
            }
        }
    ]
)

print(f"Paso añadido: {step_response['StepIds'][0]}")