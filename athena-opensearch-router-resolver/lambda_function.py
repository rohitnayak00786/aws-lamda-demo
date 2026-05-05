import json
import boto3
import base64
import sqlglot
from sqlglot import expressions as exp

lambda_client = boto3.client("lambda")

LAMBDA_SQL = "fetchDataFromAthena"          # Lambda B
LAMBDA_KEYWORD = "fetchDataFromOpenSearch"  # Lambda A


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


def is_safe_select_query(query: str) -> bool:
    """
    Check:
    - Valid SQL
    - Only 1 statement
    - Must be SELECT
    """
    try:
        parsed = sqlglot.parse(query)

        # ❌ multiple statements not allowed
        if len(parsed) != 1:
            return False

        statement = parsed[0]

        # ❌ only SELECT allowed
        if not isinstance(statement, exp.Select):
            return False

        return True

    except Exception:
        return False


def invoke_lambda(function_name, payload):
    response = lambda_client.invoke(
        FunctionName=function_name,
        InvocationType="RequestResponse",
        Payload=json.dumps(payload)
    )
    return json.loads(response["Payload"].read())


def lambda_handler(event, context):
    try:
        query = extract_query(event)

        if not query:
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "Missing 'query'"})
            }

        # 🔀 Route decision
        if is_safe_select_query(query):
            route = "SQL"
            target_lambda = LAMBDA_SQL
        else:
            route = "KEYWORD"
            target_lambda = LAMBDA_KEYWORD

        # 🚀 Invoke target lambda
        result = invoke_lambda(target_lambda, {"query": query})

        # 🔥 Parse inner response body safely
        inner_body = result.get("body", {})
        if isinstance(inner_body, str):
            inner_body = json.loads(inner_body)

        data = []
        columns = []

        # 🟦 SQL Response
        if route == "SQL":
            data = inner_body.get("data", [])

        # 🟩 Keyword (OpenSearch)
        else:
            hits = inner_body.get("hits", {}).get("hits", [])
            data = [hit.get("_source", {}) for hit in hits]

        # 🔁 Normalize keys (lowercase)
        def normalize_key(key):
            return key.strip().lower()

        normalized_data = []
        for row in data:
            normalized_row = {normalize_key(k): v for k, v in row.items()}
            normalized_data.append(normalized_row)

        data = normalized_data

        # 📊 Extract columns dynamically
        if data:
            columns = list(data[0].keys())

        # 🔒 OPTIONAL: restrict columns (case-insensitive)
        ALLOWED_COLUMNS = []  # Example: ["first name", "email"]

        if ALLOWED_COLUMNS:
            allowed_set = set([normalize_key(c) for c in ALLOWED_COLUMNS])

            data = [
                {k: v for k, v in row.items() if k in allowed_set}
                for row in data
            ]

            columns = list(allowed_set)

        # ✅ Final response (UI friendly)
        return {
            "statusCode": 200,
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Content-Type": "application/json"
            },
            "body": json.dumps({
                "type": route,
                "columns": columns,
                "data": data
            })
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }
