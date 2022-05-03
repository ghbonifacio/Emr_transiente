import boto3

def lambda_handler(event, context):
    conn = boto3.client("emr", region_name='us-east-1')
    cluster_id = conn.run_job_flow(
        Name='emrTransiente',
        ServiceRole='', 
        JobFlowRole='', 
        VisibleToAllUsers=True,
        LogUri='s3://****', #Uri de logs
        ReleaseLabel='emr-6.5.0',
        Instances={
            'InstanceGroups': [
                {
                    'Name': 'Master nodes',
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'MASTER',
                    'InstanceType': 'r4.xlarge',
                    'InstanceCount': 1,
                },
                {
                    'Name': 'Slave nodes',
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'CORE',
                    'InstanceType': 'r4.xlarge',
                    'InstanceCount': 2,
                },
                
            ],
            'Ec2KeyName': '', #Keyname 
            'KeepJobFlowAliveWhenNoSteps': True,
            'TerminationProtected': True,
            'Ec2SubnetId': '', #subnet
        },
        
        Applications=[{'Name': 'Spark'}],
        
         Configurations=[{
            "Classification":"spark-env",
            "Properties":{},
            "Configurations":[{
                "Classification":"export",
                "Properties":{
                    "PYSPARK_PYTHON":"/usr/bin/python3",
                    "PYSPARK_DRIVER_PYTHON":"/usr/bin/python3"
                }
            }]
        },
        {
            "Classification": "spark-hive-site",
            "Properties": {"hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"}
        }],
        
        BootstrapActions=[
        {
            'Name': 'Maximize Spark Default Config',
            'ScriptBootstrapAction': {
                'Path': '', #path S3 do arquivo .sh que contem as libs necessárias 
            }   
        },
    ],
    
        Steps=[{
            'Name': 'Processa_PySpark',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': ['spark-submit',
                "--packages",
                "com.oracle.database.jdbc:ojdbc8:21.5.0.0,io.delta:delta-core_2.12:1.0.0",
                '--master','yarn',
                '--deploy-mode','cluster',
                '--executor-memory', '15g',
                '--driver-memory', '15g',
                's3://***' #path do arquivo .py com o ETL a ser executado pelo EMR
                ]
            }
        }],
    )
    return "Started cluster {}".format(cluster_id)