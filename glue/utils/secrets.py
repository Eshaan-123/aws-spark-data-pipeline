import boto3
import json

def get_snowflake_secrets(secret_name, region="ap-south-1"):
    client = boto3.client("secretsmanager", region_name=region)
    response = client.get_secret_value(SecretId=secret_name)
    return json.loads(response["SecretString"])
