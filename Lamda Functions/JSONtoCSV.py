import json
import csv
import boto3
from io import StringIO

s3 = boto3.client('s3')

def lambda_handler(event, context):
    records = event.get('Records', [])
    for record in records:
        # Get the S3 bucket and key from the event
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']

        # Read JSON data from S3
        response = s3.get_object(Bucket=bucket, Key=key)
        json_data = response['Body'].read().decode('utf-8')

        # Convert JSON to CSV
        csv_data = convert_json_to_csv(json_data)

        # Write CSV back to S3
        output_bucket = 'outputavg'  # Replace with your S3 bucket for CSV output
        output_key = f'output/{key.split("/")[-1]}.csv'
        s3.put_object(Body=csv_data.encode('utf-8'), Bucket=output_bucket, Key=output_key)

    return {
        'statusCode': 200,
        'body': json.dumps('Records processed successfully')
    }
def convert_json_to_csv(json_data):
    # Split the input into lines and filter out empty lines
    lines = [line.strip() for line in json_data.split('}{') if line.strip()]

    # Create a list to store individual JSON objects
    json_objects = []

    # Iterate through lines and extract individual JSON objects
    for line in lines:
        # Add braces to each line to form a valid JSON object
        line_with_braces = f'{{{line}}}'
        try:
            # Load the JSON object
            json_obj = json.loads(line_with_braces)
            json_objects.append(json_obj)
        except json.JSONDecodeError:
            print(f"Ignoring invalid JSON line: {line}")

    # Define header based on your attributes
    header = ['sensor_id', 'timestamp', 'data', 'partition_key']
    csv_data = StringIO()
    csv_writer = csv.DictWriter(csv_data, fieldnames=header)
    csv_writer.writeheader()

    # Write rows to CSV
    csv_writer.writerows(json_objects)

    return csv_data.getvalue()


