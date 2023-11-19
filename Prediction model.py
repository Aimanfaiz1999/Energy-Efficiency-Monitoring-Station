import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error
from sagemaker import get_execution_role
from sagemaker.sklearn import SKLearn
import pandas as pd
from sagemaker import get_execution_role
import matplotlib.pyplot as plt
import boto3
import os
role = get_execution_role()

#  S3 bucket and prefix where CSV files are stored
bucket = 'csvfiless3'
prefix = 'output'

file_names = [
    'KDS-S3-L42C0-1-2023-11-18-21-04-50-10822eab-367d-4044-983d-88881ec569a2.csv',
    'KDS-S3-L42C0-1-2023-11-18-21-06-33-e6da44d9-a51f-4cf9-a09c-cd7a82735aff.csv'
]


# Create a DataFrame to store your data
data_frames = []

# Loop through each file and read the data
for file_name in file_names:
    # Construct the S3 path for the file
    file_path = f's3://{bucket}/{prefix}/{file_name}'

    # Read the CSV file into a DataFrame
    df = pd.read_csv(file_path)
    # Append the DataFrame to the list
    data_frames.append(df)

    # Print the number of rows in each DataFrame
    print(f"Number of rows in {file_name}: {len(df)}")

# Concatenate the DataFrames into a single DataFrame
combined_df = pd.concat(data_frames, ignore_index=True)

# Explore and setup the data
print(combined_df.info())
dfhead = pd.DataFrame(combined_df.head())
dfhead

combined_df=pd.DataFrame(combined_df)
combined_df

#Preprocessing, converting categorical into numerical
from sklearn.preprocessing import LabelEncoder
label_encoder = LabelEncoder()
combined_df['sensor_id'] = label_encoder.fit_transform(combined_df['sensor_id'])
combined_df['partition_key'] = label_encoder.fit_transform(combined_df['partition_key'])
combined_df['timestamp'] = label_encoder.fit_transform(combined_df['timestamp'])
#drop partition key
combined_df.drop(['partition_key'], axis=1, inplace=True)
combined_df

# Apply StandardScaler
scaler = StandardScaler()
scaled_features = scaler.fit_transform(combined_df)
scaled_df = pd.DataFrame(scaled_features, columns=combined_df.columns)
scaled_df

plt.figure(figsize=(20, 2))
plt.plot(df['timestamp'], df['data'])
plt.title('Energy Consumption Over Time')
plt.xlabel('Timestamp')
plt.ylabel('Energy Consumption')
plt.show()

scaled_df = scaled_df[['data', 'sensor_id', 'timestamp']]
scaled_df

train_data, test_data = np.split(scaled_df.sample(frac=1, random_state=1729), [int(0.9 * len(scaled_df))])
print(train_data.shape, test_data.shape)

bucket_name = 'sagemakerbuckets3'

train_data.to_csv('train.csv', index=False, header=False)

# Save the training features and target to CSV file
train_data.to_csv('train.csv', index=False, header=False)
boto3.Session().resource('s3').Bucket(bucket_name).Object(os.path.join(prefix, 'train/train.csv')).upload_file('train.csv')
s3_input_train = TrainingInput(s3_data='s3://{}/{}/train'.format(bucket_name, prefix), content_type='csv')

# Save the testing features and target to CSV file
test_data.to_csv('test.csv', index=False, header=False)
boto3.Session().resource('s3').Bucket(bucket_name).Object(os.path.join(prefix, 'test/test.csv')).upload_file('test.csv')
s3_input_train = TrainingInput(s3_data='s3://{}/{}/test'.format(bucket_name, prefix), content_type='csv')
print(train_data.shape, test_data.shape)

#1. Build Models Xgboot- Inbuilt Algorithm

container = get_image_uri(boto3.Session().region_name,
                          'xgboost',
                          repo_version='1.2-1')

hyperparameters = {
    'objective': 'reg:squarederror',
    'colsample_bytree': 0.8,   # Modified from 0.3 to 0.8
    'learning_rate': 0.05,     # Modified from 0.1 to 0.05
    'max_depth': 7,            # Modified from 5 to 7
    'alpha': 5,                # Modified from 10 to 5
    'num_round': 150           # Added the number of boosting rounds
}

#Path to save my model
prefix = 'Model'
output_path ='s3://{}/{}/output'.format(bucket_name, prefix)
print(output_path)

from sagemaker.estimator import Estimator

# construct a SageMaker estimator that calls the xgboost-container
estimator = sagemaker.estimator.Estimator(image_uri=container,
                                          hyperparameters=hyperparameters,
                                          role=sagemaker.get_execution_role(),
                                          train_instance_count=1,
                                          train_instance_type='ml.m5.xlarge',
                                          train_volume_size=5, # 5 GB
                                          output_path=output_path,
                                          train_use_spot_instances=True,
                                          train_max_run=300,
                                          train_max_wait=600)

#2.train
estimator.fit({'train': s3_input_train,'validation': s3_input_test})
#3.deploy
xgb_predictor = estimator.deploy(initial_instance_count=1,instance_type='ml.r5.large')
from sagemaker.serializers import CSVSerializer

# Use CSVSerializer for inference
csv_serializer = CSVSerializer()
xgb_predictor.serializer = csv_serializer
xgb_predictor.content_type = 'text/csv'

# Prepare test data
test_data_array = test_data.drop(['data'], axis=1).values

# Make predictions
result = xgb_predictor.predict(test_data_array).decode('utf-8')
predictions_array = np.fromstring(result[1:], sep=',')
print("shape: ", predictions_array.shape)
print("actual predictions ",predictions_array)

from sklearn.metrics import mean_squared_error

# Assuming 'test_data' contains the actual values and 'predictions_array' contains the predicted values
actual_values = test_data['data'].values
mse = mean_squared_error(actual_values, predictions_array)

print(f"Mean Squared Error: {mse}")
