import json
import boto3

glue = boto3.client("glue")

def lambda_handler(event, context):
    print("Received event:", json.dumps(event))

    if "Records" not in event:
        print("Not an S3 event. Skipping.")
        return {"status": "ignored"}

    record = event["Records"][0]
    bucket = record["s3"]["bucket"]["name"]
    key = record["s3"]["object"]["key"]

    if not key.endswith(".csv"):
        print(f"Skipping non-CSV file: {key}")
        return {"status": "skipped"}

    response = glue.start_job_run(
        JobName="bronze_to_silver_job",
        Arguments={
            "--SOURCE_BUCKET": bucket,
            "--SOURCE_KEY": key
        }
    )

    return {
        "status": "started",
        "jobRunId": response["JobRunId"]
    }
