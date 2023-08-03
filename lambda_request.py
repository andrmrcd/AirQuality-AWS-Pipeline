import requests
import boto3
from datetime import datetime
import os

s3 = boto3.client('s3')

def lambda_handler(event, context):
    bucket = os.environ['S3_Bucket']
    API_key = os.environ['API_key']

    # Make the API request
    response = requests.get(f'https://www.airnowapi.org/aq/observation/latLong/current/?format=text/csv&latitude=35.474092&longitude=-97.513289&distance=25&API_KEY={API_key}')

    # Check if the API request was successful
    if response.status_code == 200:
        # Get current date and time in a specific format
        current_datetime = datetime.today().strftime('%Y-%m-%d_%H')
        # Create filename with current date and time
        filename = f'air_quality_{current_datetime}.csv'
        # Upload response.text data to s3 bucket with datetime in filename as csv
        s3.put_object(Bucket=bucket, Key=filename, Body=response.text)
        result = {"message": f"Csv data uploaded to S3 with filename: {filename}"}
    else:
        # If the API request failed, return an error message
        result = {"error": "Failed to retrieve data from the API"}

    return result