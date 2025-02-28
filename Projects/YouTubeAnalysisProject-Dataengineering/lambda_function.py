import awswrangler as wr
import pandas as pd
import json
import boto3
import os

# Initialize S3 client
s3_client = boto3.client('s3')

# Environment variables
os_input_s3_cleansed_layer = os.environ['s3_cleansed_layer']
os_input_glue_catalog_db_name = os.environ['glue_catalog_db_name']
os_input_glue_catalog_table_name = os.environ['glue_catalog_table_name']
os_input_write_data_operation = os.environ['write_data_operation']


def lambda_handler(event, context):
    # Parse the S3 event using json instead of urllib.parse
    record = event['Records'][0]
    bucket = record['s3']['bucket']['name']
    key = record['s3']['object']['key']

    try:
        # Read the object from S3 using boto3
        s3_response = s3_client.get_object(Bucket=bucket, Key=key)
        file_content = s3_response['Body'].read().decode('utf-8')

        # Load the JSON data
        json_data = json.loads(file_content)

        # Convert to a DataFrame
        df_raw = pd.DataFrame([json_data])

        # Normalize nested 'items' field if it exists
        if 'items' in df_raw.columns:
            df_step_1 = pd.json_normalize(df_raw['items'].iloc[0])
        else:
            df_step_1 = df_raw  # Use raw data if no 'items' field

        # Write to S3 in Parquet format
        wr_response = wr.s3.to_parquet(
            df=df_step_1,
            path=os_input_s3_cleansed_layer,
            dataset=True,
            database=os_input_glue_catalog_db_name,
            table=os_input_glue_catalog_table_name,
            mode=os_input_write_data_operation
        )

        return wr_response
    except Exception as e:
        print(e)
        print(f"Error processing object {key} from bucket {bucket}. Ensure it exists and is accessible.")
        raise e
