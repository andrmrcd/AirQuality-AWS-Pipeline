import boto3

s3 = boto3.client("s3")

def lambda_handler(event,context):
    # Load event source bucket and key
    bucketname = event["Records"][0]["s3"]["bucket"]["name"]
    bucketobj = event["Records"][0]["s3"]["object"]["key"]

    # Set destination for compiled csv
    outputbucket = "airnow-compiled"
    outputfile = "AirQuality_compiled.csv"

    # Read data from uploaded data
    response = s3.get_object(Bucket=bucketname, Key=bucketobj)
    new_data = response["Body"].read().decode("utf-8")

    # Read existing data from destination (if it exists)
    try:
        existing_data_response = s3.get_object(Bucket=outputbucket, Key=outputfile)
        existing_data = existing_data_response["Body"].read().decode("utf-8")
    except s3.exceptions.NoSuchKey:
        # If csv file doesn't exist, create empty string
        existing_data = ''

    # Combine existing data with newly uploaded data
    combined_data = existing_data + '\n' + new_data

    # Upload the combined data to the destination bucket
    s3.put_object(Bucket=outputbucket, Key=outputfile,Body=combined_data) 
