import dropbox
import boto3
import io
import pandas as pd

# s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

def lambda_handler(event, context):
    
    # S3 bucket to import into
    # bucket = "BUCKET"
    
    # DynamoDB database to import into
    table = "TABLE"
    
    # App key and secret
    appKey = "APP_KEY"
    appSecret = "APP_SECRET"
    
    # Refresh token, which will perpetually generate new sessions
    refresh_token = "REFRESH_TOKEN"
    
    
    # Create a Dropbox client using the authorizer
    dbx = dropbox.Dropbox(app_key=appKey, app_secret=appSecret, oauth2_refresh_token=refresh_token)
    
    
    # List the contents of the folder (no path because the root is the directory we're saving to)
    result = dbx.files_list_folder("")
    
    # Loop through the results and retrieve metadata for each file
    file_list = []
    # List of sheet name strings
    name_list = []
    while True:
        for entry in result.entries:
            # Only add files to the list
            if isinstance(entry, dropbox.files.FileMetadata):
                file_list.append(entry)
        # If there are more results, continue to the next page
        if result.has_more:
            result = dbx.files_list_folder_continue(result.cursor)
        else:
            break
    
    # Loop through all returned files
    for index in file_list:
        sheet = index.name
        
        # Download file from Dropbox
        download = dbx.files_download_to_file("/tmp/temp_file.xlsx", "/" + sheet)
        
        # Read the file into a variable
        with open('/tmp/temp_file.xlsx', 'rb') as f:
            file_data = f.read()
        
        # Check if the object has the desired file extension
        if sheet.endswith('.xlsx'):
            name_list.append(sheet)
            
            # Parse sheet data
            df = pd.read_excel(io.BytesIO(file_data), engine='openpyxl')
            
            # Loop through sheet data, format for DynamoDB, and upload
            for i, row in df.iterrows():
                item = {}
                for col in df.columns:
                    item[col] = str(row[col])
                dynamodb.Table(table).put_item(Item=item)
        
        # Write to S3 bucket (if we want to store our data in S3 first before uploading to DynamoDB)
        # s3.put_object(Bucket=bucket, Key=sheet, Body=file_data)
    
    return {
        'statusCode': 200,
        'body': "Sheets [" + ", ".join(name_list) + "] successfully imported into '" + table + "' DynamoDB database!"
    }
