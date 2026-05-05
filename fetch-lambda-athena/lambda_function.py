import json
import time
import base64
import boto3

# AWS clients
athena = boto3.client('athena')

# CONFIG
DATABASE = "project_db"
OUTPUT_LOCATION = "s3://assignment-amplify-athena-os/query_result/"


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
        # Parse request body
        # body = json.loads(event.get("body", "{}"))
        query = extract_query(event)

        if not query:
            return response(400, {"error": "Query is required"})

        # Step 1: Start Athena query
        start_response = athena.start_query_execution(
            QueryString=query,
            QueryExecutionContext={
                'Database': DATABASE
            },
            ResultConfiguration={
                'OutputLocation': OUTPUT_LOCATION
            }
        )

        query_execution_id = start_response['QueryExecutionId']

        # Step 2: Wait for query completion
        state = "RUNNING"
        while state in ["RUNNING", "QUEUED"]:
            time.sleep(2)
            query_status = athena.get_query_execution(
                QueryExecutionId=query_execution_id
            )
            state = query_status['QueryExecution']['Status']['State']

        if state != "SUCCEEDED":
            return response(500, {"error": f"Query failed with state: {state}"})

        # Step 3: Fetch results
        results = athena.get_query_results(
            QueryExecutionId=query_execution_id
        )

        rows = results['ResultSet']['Rows']

        # Extract headers
        headers = [col['VarCharValue'] for col in rows[0]['Data']]

        # Extract data rows
        data = []
        for row in rows[1:]:
            values = [col.get('VarCharValue', None) for col in row['Data']]
            data.append(dict(zip(headers, values)))

        return response(200, {
            "queryExecutionId": query_execution_id,
            "data": data
        })

    except Exception as e:
        return response(500, {"error": str(e)})


def response(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*"
        },
        "body": json.dumps(body)
    }
