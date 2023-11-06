import boto3
from botocore.exceptions import NoCredentialsError
from tabulate import tabulate

# Initialize the DynamoDB client
dynamodb = boto3.client('dynamodb', region_name='eu-north-1')

# Specify the DynamoDB table name
table_name = 'House'  # Replace with your table name

try:
    # Query the DynamoDB table
    response = dynamodb.scan(
        TableName=table_name
    )

    # Create a list of dictionaries to store attribute data
    items_list = []

    # Loop through the DynamoDB items and extract attributes
    for item in response.get('Items', []):
        item_dict = {}
        for attribute_name, attribute_value in item.items():
            # The attribute_value can be of various types (e.g., N, S, etc.), so we handle them accordingly
            if 'S' in attribute_value:
                item_dict[attribute_name] = attribute_value['S']
            elif 'N' in attribute_value:
                item_dict[attribute_name] = attribute_value['N']
            # You can add more handling for other types as needed
        items_list.append(item_dict)

    # Print the attributes in a tabular format
    if items_list:
        print(tabulate(items_list, headers="keys", tablefmt="fancy_grid"))
    else:
        print("No items found in the DynamoDB table.")

except NoCredentialsError:
    print("AWS credentials are missing or incorrect. Please configure your AWS credentials.")
except Exception as e:
    print(f"An error occurred: {str(e)}")
