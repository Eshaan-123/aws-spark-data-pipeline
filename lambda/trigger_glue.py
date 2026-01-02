import json
import boto3
import urllib.parse

glue = boto3.client("glue")

GLUE_JOB_NAME = "bronze_to_silver_job"
SILVER_BASE_PATH = "s3://eshaan-spark-silver/aml"

def lambda_handler(event, context):
    print("Received event:", json.dumps(event))

    # Extract S3 info
    record = event["Records"][0]
    bucket = record["s3"]["bucket"]["name"]
    key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])

    # Build bronze path (folder level)
    bronze_path = f"s3://{bucket}/" + "/".join(key.split("/")[:-1])

    print("Bronze path:", bronze_path)

    response = glue.start_job_run(
        JobName=GLUE_JOB_NAME,
        Arguments={
            "--BRONZE_PATH": bronze_path,
            "--SILVER_PATH": SILVER_BASE_PATH
        }
    )

    print("Glue job triggered:", response["JobRunId"])

    return {
        "statusCode": 200,
        "body": json.dumps("Glue job triggered successfully")
    }
