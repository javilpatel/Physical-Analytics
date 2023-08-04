import boto3
import xml.etree.ElementTree as ET
import json
import uuid

def stream_to_kinesis(data_stream, record_payload):
    kinesis = boto3.client('kinesis')
    kinesis.put_record(
        StreamName=data_stream,
        PartitionKey=str(uuid.uuid4()),
        Data=record_payload.encode('utf-8')
    )

def process_xml(obj, kinesis_stream, batch_size):
    # Stream and send records to Kinesis directly during parsing
    record_payload = ''
    for event, elem in ET.iterparse(obj.get()['Body'], events=("start", "end")):
        if event == "start" and elem.tag == "Record":
            record_data = {
                'type': elem.get('type'),
                'sourceName': elem.get('sourceName'),
                'sourceVersion': elem.get('sourceVersion'),
                'device': elem.get('device'),
                'creationDate': elem.get('creationDate'),
                'startDate': elem.get('startDate'),
                'endDate': elem.get('endDate'),
                'value': elem.get('value')
            }

            record_payload += json.dumps(record_data) + "\n"

            # Clear the element to free up memory as we process it
            elem.clear()

            # Send the payload to Kinesis if the batch size is reached
            if len(record_payload) >= batch_size:
                stream_to_kinesis(kinesis_stream, record_payload)
                record_payload = ''

    # Send any remaining records in the payload to Kinesis
    if record_payload:
        stream_to_kinesis(kinesis_stream, record_payload)

def lambda_handler(event, context):
    s3_bucket = event['Records'][0]['s3']['bucket']['name']
    s3_key = event['Records'][0]['s3']['object']['key']
    kinesis_stream = 'health_data'
    batch_size = 20 * 15024  

    s3 = boto3.resource('s3')
    obj = s3.Object(s3_bucket, s3_key)

    # Process XML data in smaller batches
    process_xml(obj, kinesis_stream, batch_size)

    return {
        'statusCode': 200,
        'body': 'Data has been successfully sent to Kinesis Data Streams'
    }
