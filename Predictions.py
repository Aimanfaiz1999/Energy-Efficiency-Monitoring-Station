import boto3

# Specify your S3 bucket and key
bucket_name = 'sagemakerbuckets3'
key = 'predictions.txt'

# Download the file
s3 = boto3.client('s3')
s3.download_file(bucket_name, key, 'C:/Users/mtaim/OneDrive/Desktop/SCIOT/predictions.txt')

