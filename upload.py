import boto3

def upload(local_file, bucket, s3_file):
    s3 = boto3.client('s3')

    try:
        s3.upload_file(local_file, bucket, s3_file)
        print("Upload Successful")
        return True
    except FileNotFoundError:
        print("The file was not found")
        return False
    
    # change to file directory and bucket name 
uploaded = upload('/Users/zjavilpa/Desktop/apple_health_export/export.xml', 'rawhealthdata', 'export.xml')