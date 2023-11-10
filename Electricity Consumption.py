import boto3
import json
import random
import time

# Initialize AWS SQS client for the Electricity queue
electricity_queue_name = 'Electricity_reading'
sqs = boto3.client('sqs', region_name='eu-north-1')

# Mock sensor ID
electricity_sensor_id = 'electricity_sensor'

while True:
    # Simulate electricity consumption data
    electricity_reading = random.uniform(0, 5)  # Change the range as needed
    electricity_timestamp = time.strftime('%Y-%m-%d %H:%M:%S')

    # Create a message with electricity sensor data
    electricity_message = {
        'sensor_id': electricity_sensor_id,
        'time': electricity_timestamp,
        'thing_measured': 'electricity',
        'reading_measured': electricity_reading
    }

    # Send the electricity message to the Electricity queue
    electricity_queue_url = sqs.get_queue_url(QueueName=electricity_queue_name)['QueueUrl']
    sqs.send_message(
        QueueUrl=electricity_queue_url,
        MessageBody=json.dumps(electricity_message)
    )

    print(f"Sent electricity reading: {electricity_reading} at {electricity_timestamp}")

    time.sleep(15)  # Send data every hour
