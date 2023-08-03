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
    header_row = False
    try:
        existing_data_response = s3.get_object(Bucket=outputbucket, Key=outputfile)
        existing_data = existing_data_response["Body"].read().decode("utf-8")
        header_row = True
    except s3.exceptions.NoSuchKey:
        # If csv file doesn't exist, create empty string
        existing_data = ''

    # If existing_data exists
    if header_row == True:
        # Remove header row from new_data
        striped_data = new_data.strip().split('\n')
        new_data_without_header = striped_data[1:]
        upload_data = '\n'.join(new_data_without_header)
    else:
        upload_data = new_data

    # Combine existing data with newly uploaded data
    combined_data = existing_data + upload_data

    # Upload the combined data to the destination bucket
    s3.put_object(Bucket=outputbucket, Key=outputfile,Body=combined_data) 
