import boto3

dynamodb = boto3.client('dynamodb')
sns = boto3.client('sns')  # Add this line to create an SNS client


def lambda_handler(event, context):
    # Define your DynamoDB table name
    table_name = 'House'

    # Query DynamoDB to get the energy efficiency value
    response = dynamodb.get_item(
        TableName=table_name,
        Key={
            'sensor_id': {'S': 'gas_sensor'},  # Replace with the appropriate sensor ID
        }
    )

    if 'Item' in response:
        avg_gas_reading = float(response['Item']['avg_gas_reading']['N'])
    else:
        # Handle the case where the item is not found in DynamoDB
        avg_gas_reading = 0  # Provide a default value or handle it as needed

    threshold = 0.5  # Set your desired threshold value

    if avg_gas_reading > threshold:
        message = "The usage of gas is above the threshold! Current value: " + str(avg_gas_reading)
        sns.publish(
            TopicArn='arn:aws:sns:eu-north-1:127865895568:Gas_Notification_SNS',
            Message=message
        )

