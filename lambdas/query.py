import os, json, boto3
from boto3.dynamodb.conditions import Key
from decimal import Decimal

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(os.environ.get("TABLE_NAME"))


def decimal_default(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError

def lambda_handler(event, context):
    params = event.get('queryStringParameters') or {}
    patient = params.get('patientId') or 'P0001'
    limit = int(params.get('limit') or 20)
    resp = table.query(
        KeyConditionExpression=Key('patientId').eq(patient),
        ScanIndexForward=False,
        Limit=limit
    )
    items = resp.get('Items', [])
    return {
        "statusCode": 200,
        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type"
        },
        "body": json.dumps(items, default=decimal_default)
    }
