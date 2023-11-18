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
            'sensor_id': {'S': 'gas-sensor'},  # Replace with the appropriate sensor ID
        }
    )

    if 'Item' in response:
        avg = float(response['Item']['avg']['N'])
    else:
        # Handle the case where the item is not found in DynamoDB
        avg = 0  # Provide a default value or handle it as needed

    threshold = 5  # Set your desired threshold value

    if avg > threshold:
        message = "The usage of gas is above the threshold of 0.5! Current value: " + str(avg)
        sns.publish(
            TopicArn='arn:aws:sns:eu-north-1:127865895568:Gas_Notification_SNS',
            Message=message
        )
