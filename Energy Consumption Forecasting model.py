import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error
from sagemaker import get_execution_role
from sagemaker.sklearn import SKLearnimport pandas as pd
from sagemaker import get_execution_role
import matplotlib.pyplot as plt
role = get_execution_role()

#  S3 bucket and prefix where CSV files are stored
bucket = 'outputavg'
prefix = 'output'

# file names
file_names = [
    'KDS-S3-delivery-1-2023-11-14-21-54-05-5bbde42d-a57a-40e9-a40f-1f7540d12621.csv',
    'KDS-S3-delivery-1-2023-11-14-21-55-09-37019286-4ef8-459d-bc64-a99258523a6a.csv',
    'KDS-S3-delivery-1-2023-11-14-21-56-12-06b72280-8982-4cc7-8484-d2c2d043df88.csv',
    'KDS-S3-delivery-1-2023-11-14-21-57-16-38d8da24-cec9-4708-8ffb-587b3a622e46.csv'
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

# DataFrame
pd.DataFrame(combined_df)

#Preprocessing, converting categorical into numerical
from sklearn.preprocessing import LabelEncoder
label_encoder = LabelEncoder()
combined_df['sensor_id'] = label_encoder.fit_transform(combined_df['sensor_id'])
combined_df['partition_key'] = label_encoder.fit_transform(combined_df['partition_key'])
combined_df['timestamp'] = label_encoder.fit_transform(combined_df['timestamp'])
#drop partition key
combined_df.drop(['partition_key'], axis=1, inplace=True)
pd.DataFrame(combined_df)

original_column_names = combined_df.columns
numeric_features = combined_df.select_dtypes(include=['float64', 'int64'])
# Apply StandardScaler
scaler = StandardScaler()
scaled_features = scaler.fit_transform(numeric_features)
scaled_df = pd.DataFrame(scaled_features, columns=numeric_features.columns)
scaled_df


plt.figure(figsize=(20, 2))
plt.plot(df['timestamp'], df['data'])
plt.title('Energy Consumption Over Time')
plt.xlabel('Timestamp')
plt.ylabel('Energy Consumption')
plt.show()

#Feature Engineering
combined_df['timestamp'] = pd.to_datetime(combined_df['timestamp'])
combined_df = combined_df.sort_values('timestamp')
combined_df['day_of_week'] = combined_df['timestamp'].dt.dayofweek
combined_df['month'] = combined_df['timestamp'].dt.month
combined_df

#Target and features split
target_column = combined_df.columns[2]
features_columns = combined_df.columns[:2].append(combined_df.columns[3:])
X_train, y_train = train_data[features_columns], train_data[target_column]
X_test, y_test = test_data[features_columns], test_data[target_column]

# Specify the S3 bucket name
bucket_name = 'predictivemodel'

# Save the training features and target to CSV file
X_train.to_csv('train.csv', index=False, header=False)
boto3.Session().resource('s3').Bucket(bucket_name).Object(os.path.join(prefix, 'train/train.csv')).upload_file('train.csv')
s3_input_train = sagemaker.session.s3_input(s3_data='s3://{}/{}/train'.format(bucket_name, prefix), content_type='csv')

# Save the testing features and target to CSV file
X_test.to_csv('test.csv', index=False, header=False)
boto3.Session().resource('s3').Bucket(bucket_name).Object(os.path.join(prefix, 'test/test.csv')).upload_file('test.csv')
s3_input_test = sagemaker.session.s3_input(s3_data='s3://{}/{}/test'.format(bucket_name, prefix), content_type='csv')

print(train_data.shape, test_data.shape)



#Path to save my model
prefix = 'Model'
output_path ='s3://{}/{}/output'.format(bucket_name, prefix)
print(output_path)

hyperparameters = {
    'objective': 'reg:squarederror',
    'colsample_bytree': 0.3,
    'learning_rate': 0.1,
    'max_depth': 5,
    'alpha': 10,
    'n_estimators': 100
}
#1. Build Models Xgboot- Inbuilt Algorithm
# specify the repo_version depending on your preference.
container = get_image_uri(boto3.Session().region_name,
                          'xgboost',
                          repo_version='1.2-1')
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

#3. deploy
xgb_predictor = estimator.deploy(initial_instance_count=1,instance_type='ml.c5.2xlarge')
