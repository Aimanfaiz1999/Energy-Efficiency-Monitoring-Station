import boto3
import json
import random
import time

# Initialize AWS SQS client for the Gas queue
gas_queue_name = 'Gas_reading'
sqs = boto3.client('sqs', region_name='eu-north-1')

# Mock sensor ID
gas_sensor_id = 'gas_sensor'

while True:
    # Simulate gas consumption data
    gas_reading = random.uniform(0, 10)  # Change the range as needed
    gas_timestamp = time.strftime('%Y-%m-%d %H:%M:%S')


    # Create a message with gas sensor data
    gas_message = {
        'sensor_id': gas_sensor_id,
        'time': gas_timestamp,
        'thing_measured': 'gas',
        'reading_measured': gas_reading
    }

    # Send the gas message to the Gas queue
    gas_queue_url = sqs.get_queue_url(QueueName=gas_queue_name)['QueueUrl']
    sqs.send_message(
        QueueUrl=gas_queue_url,
        MessageBody=json.dumps(gas_message)
    )

    print(f"Sent gas reading: {gas_reading} at {gas_timestamp}")

    time.sleep(15)  # Send data every hour
