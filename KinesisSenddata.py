import boto3
import json
import random
import time
from datetime import datetime

# Set up the Kinesis client
kinesis = boto3.client('kinesis', region_name='eu-north-1')

# Define the Kinesis stream names
delivery_stream_name = 'DeliveryStream'
gas_stream_name = 'Gas'

def send_continuous_data():
    while True:
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # Generate random data for the electricity sensor
        electricity_sensor_id = 'electricity-sensor'
        electricity_data = round(random.uniform(10, 100), 2)

        # Send data to both Kinesis streams with partition key for electricity
        send_to_kinesis(electricity_sensor_id, electricity_data, 'electricity', timestamp, delivery_stream_name)
        send_to_kinesis(electricity_sensor_id, electricity_data, 'electricity', timestamp, gas_stream_name)

        # Generate random data for the gas sensor
        gas_sensor_id = 'gas-sensor'
        gas_data = round(random.uniform(1, 10), 2)

        # Send data to both Kinesis streams with partition key for gas
        send_to_kinesis(gas_sensor_id, gas_data, 'gas', timestamp, delivery_stream_name)
        send_to_kinesis(gas_sensor_id, gas_data, 'gas', timestamp, gas_stream_name)

        # Wait for 5 seconds before sending the next set of data
        time.sleep(5)

def send_to_kinesis(sensor_id, data, partition_key, timestamp, stream_name):
    payload = {
        'sensor_id': sensor_id,
        'timestamp': timestamp,
        'data': data,
        'partition_key': partition_key
    }

    # Convert the payload to JSON
    payload_json = json.dumps(payload)

    # Send the payload to the specified Kinesis stream with the specified partition key
    response = kinesis.put_record(
        StreamName=stream_name,
        Data=payload_json,
        PartitionKey=partition_key
    )

    print(f"Record sent to Kinesis. Sensor: {payload_json}")

# Start sending continuous data to Kinesis
send_continuous_data()
