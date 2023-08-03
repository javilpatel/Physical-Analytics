import os
from aws_cdk import (
    aws_s3 as _s3,
    aws_s3_notifications,
    aws_kinesis as _kinesis,
    aws_kinesisfirehose as firehose,
    aws_lambda as _lambda,
    aws_iam as iam,
    aws_glue as glue,
    aws_s3_deployment,
    Duration,
    RemovalPolicy,
    custom_resources,
    aws_kms as kms,
    Stack
)
from constructs import Construct

class PhysicalAnalyticsStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)


        # first bucket to store exported xml data from ios device
        bucket1 = _s3.Bucket(self, 'MyBucket1', bucket_name= 'rawhealthdata', removal_policy=RemovalPolicy.DESTROY, auto_delete_objects=True, encryption=_s3.BucketEncryption.S3_MANAGED)

        # second bucket to store the processed data after the ETL pipeline
        bucket2 = _s3.Bucket(self, 'MyBucket2', bucket_name= 'processedhealthdata', removal_policy=RemovalPolicy.DESTROY, auto_delete_objects=True, encryption=_s3.BucketEncryption.S3_MANAGED)


        # third bucket to store glue script
        bucket3 = _s3.Bucket(self, 'MyBucket3', bucket_name= 'gluecfnjob-script', removal_policy=RemovalPolicy.DESTROY, auto_delete_objects=True, encryption=_s3.BucketEncryption.S3_MANAGED)

        #fourth bucket to store data from kinesis data stream
        bucket4 = _s3.Bucket(self, 'MyBucket4', bucket_name= 'streamrawdata', removal_policy=RemovalPolicy.DESTROY, auto_delete_objects=True, encryption=_s3.BucketEncryption.S3_MANAGED)

        #lifecycle policy to archive data
        bucket4.add_lifecycle_rule(
            enabled=True,
            transitions=[
                _s3.Transition(
                    storage_class=_s3.StorageClass.INTELLIGENT_TIERING, # .GLACIER
                    transition_after=Duration.days(1) # increase days 
                ),
            ],
            )
        
        #kms key to encypt data
        kms_key = kms.Key(self, 'myKMSkey',
            removal_policy=RemovalPolicy.DESTROY,
            enable_key_rotation=True
        )

        kms_key_arn = kms_key.key_arn

        # kinesis stream to stream health data to ETL pipline
        stream = _kinesis.Stream(self, 'MyStream1', stream_name='health_data', stream_mode=_kinesis.StreamMode.PROVISIONED, shard_count=5 ,encryption=_kinesis.StreamEncryption.KMS, encryption_key= kms_key) #KMS

        stream.node.add_dependency(kms_key)


        # function to send data from s3 to kinesis in batches which simlaties streaming
        function = _lambda.Function(self, 'lambda_function', 
            runtime=_lambda.Runtime.PYTHON_3_7, 
            handler='json-producer.lambda_handler', 
            code=_lambda.Code.from_asset('./lambda'),
            memory_size=10240, 
            timeout= Duration.minutes(15))

        # trigger for when file is stored in s3 and send it to data streams
        notification = aws_s3_notifications.LambdaDestination(function)
        bucket1.add_event_notification(_s3.EventType.OBJECT_CREATED, notification)


        # adding the policy to lambda's role to read data from s3 and stream to kinesis
        function.add_to_role_policy(iam.PolicyStatement(
            actions=[
                's3:GetObject',
                's3:ListBucket',
                'kinesis:PutRecord',
                'kinesis:PutRecords',
                'kms:Encrypt',
                'kms:Decrypt',
                'kms:GenerateDataKey',
            ],
            resources=['*']
        ))

        #role to access data streams and kms 
        firehose_role = iam.Role(self, 'FirehoseRole', assumed_by=iam.ServicePrincipal('firehose.amazonaws.com'))

        firehose_role.add_to_policy(iam.PolicyStatement(
            actions=[
                'kinesis:DescribeStream',
                'kinesis:ListRecord',
                'kinesis:ListRecords',
                'kinesis:ListStreams',
                'kinesis:Get*',
                'kinesis:DescribeStreamSummary',
                'kms:Encrypt',
                'kms:Decrypt',
                'kms:GenerateDataKey',
            ],
            resources=['*']
        ))

        # add s3 permissions
        firehose_role.add_to_policy(iam.PolicyStatement(
            actions=[
                's3:AbortMultipartUpload',
                's3:GetBucketLocation',
                's3:GetObject',
                's3:ListBucket',
                's3:ListBucketMultipartUploads',
                's3:PutObject'
            ],
            resources=[bucket4.bucket_arn, bucket4.arn_for_objects('*')]
        ))

        # Define Kinesis Data Firehose Stream
        firehose1 = firehose.CfnDeliveryStream(self, "Firehose",
            delivery_stream_name='healthfirehose',
            delivery_stream_type='KinesisStreamAsSource',
            kinesis_stream_source_configuration=firehose.CfnDeliveryStream.KinesisStreamSourceConfigurationProperty(
            kinesis_stream_arn=stream.stream_arn,
            role_arn=firehose_role.role_arn
            ),
            s3_destination_configuration=firehose.CfnDeliveryStream.S3DestinationConfigurationProperty(
                bucket_arn=bucket4.bucket_arn,
                role_arn=firehose_role.role_arn,
                buffering_hints=firehose.CfnDeliveryStream.BufferingHintsProperty(
                    interval_in_seconds=60,
                    size_in_m_bs=1
                ),
                encryption_configuration=firehose.CfnDeliveryStream.EncryptionConfigurationProperty(
                    kms_encryption_config=firehose.CfnDeliveryStream.KMSEncryptionConfigProperty(
                        awskms_key_arn=kms_key_arn
                    )
                )
            )
        )

        firehose1.node.add_dependency(firehose_role)
        

        # store glue script to s3 bucket
        deploy = aws_s3_deployment.BucketDeployment(self, 'DeployScript',
            sources=[aws_s3_deployment.Source.asset('./glue')],
            destination_bucket=bucket3,
            destination_key_prefix='',
            )
        
        # create glue role
        glue_role = iam.Role(self, 'MyGlueJobRole',
        assumed_by=iam.ServicePrincipal('glue.amazonaws.com'),
        )

        # Add necessary policies to the role
        glue_role.add_managed_policy(iam.ManagedPolicy.from_aws_managed_policy_name('service-role/AWSGlueServiceRole'))
        glue_role.add_to_policy(iam.PolicyStatement(
            actions=[
                's3:GetObject',
                's3:PutObject',
                's3:ListBucket',
                's3:DeleteObject',
                'kinesis:DescribeStream',
                'kinesis:ListShards',
                'kinesis:GetRecords',
                'kinesis:GetShardIterator',
                'kms:Encrypt',
                'kms:Decrypt',
                'kms:GenerateDataKey',
            ],
            resources=[bucket2.bucket_arn, bucket2.arn_for_objects('*'), bucket3.bucket_arn, bucket3.arn_for_objects('*'), stream.stream_arn, kms_key_arn]
        ))

        # Create data catalog dasebase to store metadata for records 
        database = glue.CfnDatabase(self, 'Database',
            catalog_id=self.account,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
            name='health_database'
            ),
        )


        # create data catalog table for kinesis records
        table = glue.CfnTable(self, 'healthtable',
            catalog_id= self.account,
            database_name='health_database',
            table_input={
                'name': 'health_table',
                'tableType': 'EXTERNAL_TABLE',
                'parameters': {
                    'classification': 'json',
                    'kinesisStreamEncryptionType' : 'KMS',
                    'kinesisStreamEncryptionKeyId' : kms_key_arn,
                },
                'storageDescriptor': {
                    'columns': [
                        {'name': 'type', 'type': 'string'},
                        {'name': 'creationdate', 'type': 'string'},
                        {'name': 'value', 'type': 'string',},
                        {'name': 'device', 'type': 'string'}
                    ],
                    'location': 'health_data',
                    'inputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
                    'outputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                    'compressed': False,
                    'serdeInfo': {
                        'serializationLibrary': 'org.openx.data.jsonserde.JsonSerDe'
                    },
                    'parameters': {
                        'streamARN': stream.stream_arn,
                        'typeOfData': 'kinesis'
                    },
                    'storedAsSubDirectories': False
                }
            }
        )
        
        table.node.add_dependency(stream)
        table.node.add_dependency(database)

        # create glue streaming job to run 
        glue_job = glue.CfnJob(self, 'MyGlueJob',
            name='healthdata_ETL',
            role=glue_role.role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name='gluestreaming',
                script_location=f's3://'+bucket3.bucket_name+'/glue_script.py',
                python_version='3'
            ),
            default_arguments={
                '--enable-metrics': 'true',
                '--enable-spark-ui': 'true',
                '--spark-event-logs-path': f's3://aws-glue-assets-{self.account}-{self.region}/sparkHistoryLogs/',
                '--enable-job-insights': 'false',
                '--aws_region': 'us-east-1',
                '--enable-glue-datacatalog': 'true',
                '--enable-continuous-cloudwatch-log': 'true',
                '--job-bookmark-option': 'job-bookmark-disable',
                '--job-language': 'python',
                '--TempDir': f's3://aws-glue-assets-{self.account}-{self.region}/temporary/',
                '--output_path': f's3://'+bucket2.bucket_name+'/'
            },
            max_retries=0,
            worker_type='G.1X',
            number_of_workers=2,
            glue_version='4.0',
            execution_class='STANDARD'
        )
        
        glue_job.node.add_dependency(glue_role)
        glue_job.node.add_dependency(deploy)
        glue_job.node.add_dependency(bucket3)
        glue_job.node.add_dependency(stream)

        glue_job_arn=f'arn:aws:glue:{self.region}:{self.account}:job/{glue_job.name}'

        # run glue job
        job_run = custom_resources.AwsCustomResource(self, 'MyGlueETLJobRun',
        on_create={
            'service':'Glue',
            'action':'startJobRun',
            'parameters': {
                'JobName': glue_job.name
            },
            'physical_resource_id': custom_resources.PhysicalResourceId.of('JobRunID')
        },
        policy= custom_resources.AwsCustomResourcePolicy.from_sdk_calls(
            resources=[glue_job_arn]
        )
        )

        job_run.node.add_dependency(glue_job)

        # role so glue crawler can access data and store in s3 bucket
        glue_crawler_role = iam.Role(self, 'GlueCrawlerRole', assumed_by=iam.ServicePrincipal('glue.amazonaws.com'))
        glue_crawler_role.add_managed_policy(iam.ManagedPolicy.from_aws_managed_policy_name('service-role/AWSGlueServiceRole'))

        glue_crawler_role.add_to_policy(iam.PolicyStatement(
            actions=[
                's3:GetObject',
                's3:PutObject',
                's3:ListBucket',
                's3:DeleteObject'
            ],
            resources=[bucket2.bucket_arn, bucket2.arn_for_objects('*')]
        ))

        # crawler to send processed data to s3 bucket
        crawler = glue.CfnCrawler(self, 'GlueCrawler',
            name='health_data',
            role=glue_crawler_role.role_arn,
            database_name='health_database',
            targets=glue.CfnCrawler.TargetsProperty(
                s3_targets=[glue.CfnCrawler.S3TargetProperty(path=f's3://'+bucket2.bucket_name+'/')]
            ),
            schedule=glue.CfnCrawler.ScheduleProperty(schedule_expression='cron(0/5 * * * ? *)'),
            recrawl_policy=glue.CfnCrawler.RecrawlPolicyProperty(
                recrawl_behavior='CRAWL_EVERYTHING'
            ),
            schema_change_policy=glue.CfnCrawler.SchemaChangePolicyProperty(
                update_behavior='UPDATE_IN_DATABASE',
                delete_behavior='DEPRECATE_IN_DATABASE'
            ),
            configuration='{"Version":1.0,"CreatePartitionIndex":true}'
        )
