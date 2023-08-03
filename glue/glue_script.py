import sys
import datetime
import boto3
import base64
from pyspark.sql import DataFrame, Row
from pyspark.context import SparkContext
from pyspark.sql.types import *
from pyspark.sql.functions import *
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from pyspark.sql.functions import sha2


args = getResolvedOptions(sys.argv, ['JOB_NAME', 'aws_region', 'output_path'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# S3 sink locations
aws_region = args['aws_region']
output_path = args['output_path']

s3_target = output_path + 'health_metrics'
temp_path = output_path + 'temp/'
checkpoint_location = temp_path + 'checkpoint/'

def processBatch(data_frame, batchId):
    now = datetime.datetime.now()
    year = now.year
    month = now.month
    day = now.day
    hour = now.hour
    minute = now.minute
    if data_frame.count() > 0:
        dynamic_frame = DynamicFrame.fromDF(data_frame, glueContext, 'from_data_frame')
        apply_mapping = ApplyMapping.apply(
            frame=dynamic_frame,
            mappings=[
                ('type', 'string', 'type', 'string'),
                ('creationDate', 'string', 'creationDate', 'string'),
                ('value', 'string', 'value', 'double'),
                ('device', 'string', 'device', 'string')
            ],
            transformation_ctx='apply_mapping'
        )

        transformed_frame = apply_mapping.toDF()
        # Encrypt device name
        transformed_frame = transformed_frame.withColumn('device', sha2(transformed_frame['device'], 256))
        dynamic_frame_hashed = DynamicFrame.fromDF(transformed_frame, glueContext, 'from_data_frame_hashed')

        dynamic_frame.printSchema()
        dynamic_frame_hashed.printSchema()

        # Write to S3 Sink
        s3path = s3_target + '/ingest_year=' + '{:0>4}'.format(str(year)) + '/ingest_month=' + '{:0>2}'.format(str(month)) + '/ingest_day=' + '{:0>2}'.format(str(day)) + '/ingest_hour=' + '{:0>2}'.format(str(hour)) + '/'
        s3sink = glueContext.write_dynamic_frame.from_options(
            frame=dynamic_frame_hashed,
            connection_type='s3',
            connection_options={'path': s3path},
            format='json',
            transformation_ctx='s3sink'
        )

# Read from Kinesis Data Stream
sourceData = glueContext.create_data_frame.from_catalog(
    database='health_database',
    table_name='health_table',
    transformation_ctx='datasource0',
    additional_options={'startingPosition': 'TRIM_HORIZON', 'inferSchema': 'true'}
)

sourceData.printSchema()

glueContext.forEachBatch(
    frame=sourceData,
    batch_function=processBatch,
    options={'windowSize': '100 seconds', 'checkpointLocation': checkpoint_location}
)
job.commit()
