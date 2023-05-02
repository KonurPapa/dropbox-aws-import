import boto3

s3 = boto3.client(‘s3’)

def lambda_handler(event, context):
    
    file_path = “EXCEL_SHEET”
    bucket_name = “BUCKET_NAME”
    object_key = “SHEET_NAME.xlsx”

    with open(file_path, ‘rb’) as file:
        s3.upload_fileobj(file, bucket_name, object_key)

    return {
        ‘statusCode’: 200,
        ‘body’: ‘File uploaded successfully!’
    }
