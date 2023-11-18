import json
import csv
import boto3
from io import StringIO
import logging

# Add logging configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

s3 = boto3.client('s3')

def lambda_handler(event, context):
    try:
        # Check if the event has 'Records' key
        if 'Records' in event:
            # Get the S3 bucket and key from the S3 event
            bucket = event['Records'][0]['s3']['bucket']['name']
            key = event['Records'][0]['s3']['object']['key']

            # Read JSON data from S3
            response = s3.get_object(Bucket=bucket, Key=key)
            data = response['Body'].read().decode('utf-8')

            # Convert JSON to CSV
            csv_data = convert_json_to_csv(data)

            # Write CSV to another S3 bucket
            output_bucket = 'csvfiless3'
            output_key = f'output/{key.split("/")[-1]}.csv'
            s3.put_object(Body=csv_data.encode('utf-8'), Bucket=output_bucket, Key=output_key)

            logger.info(f"Successfully processed and stored data from {key} to {output_key}")
        else:
            logger.warning("Received event without 'Records' key. No action taken.")
    except Exception as e:
        logger.error(f"Error processing data: {e}")
        raise e

def convert_json_to_csv(json_data):
    try:
        # Split the JSON data into lines
        lines = json_data.split('\n')

        # Filter out empty lines
        non_empty_lines = filter(lambda line: line.strip(), lines)

        # Initialize an empty list to store valid JSON objects
        valid_json_objects = []

        # Parse each line as JSON and filter out lines that are not valid JSON
        for line in non_empty_lines:
            try:
                json_obj = json.loads(line)
                valid_json_objects.append(json_obj)
            except json.JSONDecodeError as e:
                logger.warning(f"Skipping line due to JSON decoding error: {e}")

        # Assuming 'data' field has the 'value' property
        header = ['sensor_id', 'timestamp', 'value']
        csv_data = StringIO()
        csv_writer = csv.DictWriter(csv_data, fieldnames=header)
        csv_writer.writeheader()
        csv_writer.writerows([{'sensor_id': row['sensor_id'], 'timestamp': row['timestamp'], 'value': row['data']['value']} for row in valid_json_objects])

        return csv_data.getvalue()
    except Exception as e:
        logger.error(f"Error converting JSON to CSV: {e}")
        raise e
