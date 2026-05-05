import json
import boto3
import requests
import base64
from requests_aws4auth import AWS4Auth

region = "us-east-2"
service = "es"

host = "https://search-customer-search-aro2iag3h4amni6dltkkigjvse.aos.us-east-2.on.aws"
index = "customers"

session = boto3.Session()
credentials = session.get_credentials()

awsauth = AWS4Auth(
    credentials.access_key,
    credentials.secret_key,
    region,
    service,
    session_token=credentials.token
)


def extract_query(event):
    """
    Extract 'query' from:
    1. Query string params (GET)
    2. JSON body (POST/PUT)
    3. Direct Lambda invocation
    """

    query = None

    # Query string (GET)
    if event.get("queryStringParameters"):
        query = event["queryStringParameters"].get("query")

    # Body (POST/PUT)
    if not query and event.get("body"):
        body = event["body"]

        # Handle base64 encoding (API Gateway sometimes sends this)
        if event.get("isBase64Encoded"):
            body = base64.b64decode(body).decode("utf-8")

        try:
            body_json = json.loads(body)
            query = body_json.get("query")
        except Exception:
            pass

    # Direct invocation fallback
    if not query:
        query = event.get("query")

    return query


def lambda_handler(event, context):
    try:
        query = extract_query(event)

        if not query:
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "Missing query"})
            }

        url = f"{host}/{index}/_search"

        search_body = {
            "size": 20,
            "query": {
                "multi_match": {
                    "query": query,
                    "fields": [
                        "First Name",
                        "Last Name",
                        "Company",
                        "City",
                        "Country",
                        "Email"
                    ]
                }
            }
        }

        response = requests.get(
            url,
            auth=awsauth,
            headers={"Content-Type": "application/json"},
            data=json.dumps(search_body)
        )

        raw = response.json()

        # 🔥 Extract hits
        hits = raw.get("hits", {}).get("hits", [])
        data = [hit.get("_source", {}) for hit in hits]

        # 🔁 Normalize keys → lowercase
        def normalize_key(key):
            return key.strip().lower()

        normalized_data = []
        for row in data:
            normalized_row = {normalize_key(k): v for k, v in row.items()}
            normalized_data.append(normalized_row)

        data = normalized_data

        # 📊 Extract columns dynamically
        columns = list(data[0].keys()) if data else []

        # 🔒 OPTIONAL: restrict columns (edit this)
        # Example: ["first name", "email"]
        ALLOWED_COLUMNS = ["first name", "last name",
                           "company", "city", "country", "email"]

        if ALLOWED_COLUMNS:
            allowed_set = set([c.lower() for c in ALLOWED_COLUMNS])

            data = [
                {k: v for k, v in row.items() if k in allowed_set}
                for row in data
            ]

            columns = ALLOWED_COLUMNS

        # ✅ FINAL RESPONSE (UI READY)
        return {
            "statusCode": 200,
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Content-Type": "application/json"
            },
            "body": json.dumps({
                "columns": columns,
                "data": data
            })
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }
