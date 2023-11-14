import boto3
import json
import base64
from decimal import Decimal

# Set up the DynamoDB client
dynamodb = boto3.resource('dynamodb', region_name='eu-north-1')
table_name = 'House'  # Replace with your DynamoDB table name
table = dynamodb.Table(table_name)

# Dictionary to store running sums and counts for gas and electricity
running_totals = {'gas': 0, 'electricity': 0}
running_counts = {'gas': 0, 'electricity': 0}

def lambda_handler(event, context):
    records = event.get('Records', [])
    for record in records:
        # Parse the Kinesis record
        try:
            payload = json.loads(base64.b64decode(record['kinesis']['data']).decode('utf-8'))
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON: {str(e)}")
            print(f"Problematic payload: {record['kinesis']['data']}")
            continue

        sensor_id = payload['sensor_id']
        timestamp = payload['timestamp']
        data_value = payload['data']['value']
        partition_key = record['kinesis']['partitionKey']

        # Determine the item type (electricity or gas) based on the partition key
        item_type = 'electricity' if 'electricity' in partition_key.lower() else 'gas'

        # Update running totals and counts
        running_totals[item_type] += data_value
        running_counts[item_type] += 1

        # If we have processed 5 values, calculate the average and save to DynamoDB
        if running_counts[item_type] == 5:
            avg_reading = running_totals[item_type] / 5
            save_to_dynamodb(timestamp, sensor_id, item_type, avg_reading)

            # Reset counters for the next 5 values
            running_totals[item_type] = 0
            running_counts[item_type] = 0

    return {
        'statusCode': 200,
        'body': json.dumps('Records processed successfully')
    }

def save_to_dynamodb(timestamp, sensor_id, item_type, avg_value):
    # Convert avg_value to Decimal
    avg_value_decimal = Decimal(str(avg_value))

    # Save data to DynamoDB
    table.put_item(
        Item={
            'timestamp': timestamp,
            'sensor_id': sensor_id,
            'thing_measured': item_type,
            'avg': avg_value_decimal
        }
    )

    print(f"Record processed and saved to DynamoDB. Sensor: {sensor_id}, Type: {item_type}, Avg: {avg_value_decimal}")
