import boto3
import io
import pandas as pd

table = ‘TABLE_NAME’
bucket = ‘BUCKET_NAME’
key = ‘SHEET_NAME.xlsx’

s3 = boto3.client(‘s3’)
dynamodb = boto3.resource(‘dynamodb’)
db = dynamodb.Table(table)

def lambda_handler(event, context):
    response = s3.get_object(Bucket=bucket, Key=key)
    body = response[‘Body’].read()

    df = pd.read_excel(io.BytesIO(body), engine=’openpyxl’)

    for i, row in df.iterrows():
        item = {}

        for col in df.columns:
            item[col] = str(row[col])
        db.put_item(Item=item)

    return {
        ‘statusCode’: 200,
        ‘body’: ‘File successfully imported to DynamoDB table!’
    }
