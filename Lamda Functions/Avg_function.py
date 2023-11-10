import json
import decimal
import boto3

dynamodb = boto3.client('dynamodb')

def lambda_handler(event, context):
    sqs = boto3.resource('sqs', region_name='eu-north-1')
    queues = ['Gas_reading', 'Electricity_reading']

    for queue_name in queues:
        queue = sqs.get_queue_by_name(QueueName=queue_name)
        messages = queue.receive_messages()

        gas_sum = 0
        gas_count = 0
        electricity_sum = 0
        electricity_count = 0

        for message in messages:
            message_body = json.loads(message.body)
            sensor_id = message_body.get('sensor_id', 'default_sensor_id')
            time = message_body.get('time', 'default_time')
            thing_measured = message_body.get('thing_measured', 'default_thing_measured')
            reading_measured = decimal.Decimal(message_body['reading_measured'])
            reading_measured = reading_measured.quantize(decimal.Decimal('0.000'))

            # Create a placeholder item
            item = {
                'sensor_id': {'S': sensor_id},
                'time': {'S': time},
                'thing_measured': {'S': thing_measured},
            }

            if thing_measured == 'gas':
                gas_sum += reading_measured
                gas_count += 1
            elif thing_measured == 'electricity':
                electricity_sum += reading_measured
                electricity_count += 1

            # Calculate the average values for gas and electricity after processing messages
            avg_gas_reading = gas_sum / gas_count if gas_count > 0 else 0
            avg_electricity_reading = electricity_sum / electricity_count if electricity_count > 0 else 0

            # Update the item based on the thing_measured
            if thing_measured == 'gas':
                item['avg_gas_reading'] = {'N': str(avg_gas_reading)}
            elif thing_measured == 'electricity':
                item['avg_electricity_reading'] = {'N': str(avg_electricity_reading)}

            # Put the data in DynamoDB
            response = dynamodb.put_item(
                TableName='House',
                Item=item
            )

            # Delete the message from the queue after processing it
            message.delete()

    return {
        'message': 'Processing complete'
    }
