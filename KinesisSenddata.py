import boto3
import json
import random
import time

# Set up the Kinesis client
kinesis = boto3.client('kinesis', region_name='eu-north-1')

# Define the Kinesis stream name
stream_name = 'Gas'

def send_continuous_data():
    while True:
        # Use the same timestamp for both gas and electricity readings within the same iteration
        timestamp = int(time.time())

        # Generate random data for the electricity sensor
        electricity_sensor_id = 'electricity-sensor'
        electricity_data = {'value': round(random.uniform(10, 100), 2)}

        # Send data to Kinesis with partition key for electricity
        send_to_kinesis(electricity_sensor_id, electricity_data, 'electricity', timestamp)

        # Generate random data for the gas sensor
        gas_sensor_id = 'gas-sensor'
        gas_data = {'value': round(random.uniform(1, 10), 2)}

        # Send data to Kinesis with partition key for gas
        send_to_kinesis(gas_sensor_id, gas_data, 'gas', timestamp)

        # Wait for 5 seconds before sending the next set of data
        time.sleep(5)

def send_to_kinesis(sensor_id, data, partition_key, timestamp):
    payload = {
        'sensor_id': sensor_id,
        'timestamp': timestamp,
        'data': data
    }

    # Convert the payload to JSON
    payload_json = json.dumps(payload)

    # Send the payload to the Kinesis stream with the specified partition key
    response = kinesis.put_record(
        StreamName=stream_name,
        Data=payload_json,
        PartitionKey=partition_key
    )

    print(f"Record sent to Kinesis. Sensor: {sensor_id}, PartitionKey: {partition_key}, data: {data}")

# Start sending continuous data to Kinesis
send_continuous_data()
