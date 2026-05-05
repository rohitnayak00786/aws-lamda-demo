import boto3
import csv
import json
import requests
from requests_aws4auth import AWS4Auth

# ---------------- CONFIG ----------------
BUCKET_NAME = "assignment-amplify-athena-os"
PREFIX = "landing_zone/customers/load_date=04052026/"
OPENSEARCH_HOST = "https://search-customer-search-aro2iag3h4amni6dltkkigjvse.aos.us-east-2.on.aws"
INDEX_NAME = "customers"

# ---------------- AWS AUTH ----------------
region = "us-east-2"
service = "es"

session = boto3.Session()
credentials = session.get_credentials()

awsauth = AWS4Auth(
    credentials.access_key,
    credentials.secret_key,
    region,
    service,
    session_token=credentials.token
)

# ---------------- CLIENTS ----------------
s3 = boto3.client("s3")


def lambda_handler(event, context):
    try:
        # list files in partition folder
        response = s3.list_objects_v2(
            Bucket=BUCKET_NAME,
            Prefix=PREFIX
        )

        inserted_count = 0

        for obj in response.get("Contents", []):
            key = obj["Key"]

            # skip folders
            if key.endswith("/"):
                continue

            # read file
            file_obj = s3.get_object(Bucket=BUCKET_NAME, Key=key)
            lines = file_obj["Body"].read().decode("utf-8").splitlines()

            # parse CSV
            reader = csv.DictReader(lines)

            for row in reader:
                url = f"{OPENSEARCH_HOST}/{INDEX_NAME}/_doc"

                res = requests.post(
                    url,
                    auth=awsauth,
                    headers={"Content-Type": "application/json"},
                    data=json.dumps(row)
                )

                if res.status_code in [200, 201]:
                    inserted_count += 1
                else:
                    print(f"Failed row: {row}")
                    print(res.text)

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Data inserted successfully",
                "inserted_count": inserted_count
            })
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({
                "error": str(e)
            })
        }
