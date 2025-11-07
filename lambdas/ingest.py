
import os, json, boto3, datetime, uuid
from decimal import Decimal


dynamodb = boto3.resource("dynamodb")
TABLE = os.environ.get("TABLE_NAME")
table = dynamodb.Table(TABLE)

def lambda_handler(event, context):
   
    body = event.get('body')
    if body and isinstance(body, str):
        payload = json.loads(body)
    else:
        payload = body or event or {}

    # Validate patientId
    patient = payload.get('patientId') or payload.get('patient_id')
    if not patient:
        return {
            "statusCode": 400,
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
                "Access-Control-Allow-Headers": "Content-Type"
            },
            "body": json.dumps({"error": "patientId required"})
        }

    # Timestamp
    ts = datetime.datetime.utcnow().isoformat() + "Z"

    # Prepare item for DynamoDB (convert floats to Decimal)
    item = {
        "patientId": str(patient),
        "ts": payload.get("timestamp", ts),
        "department": payload.get("department", "General"),
        "hr": Decimal(str(payload.get("heartRate", payload.get("hr", 0)))),
        "spo2": Decimal(str(payload.get("spo2", payload.get("SpO2", 0)))),
        "temp": Decimal(str(payload.get("temperature", payload.get("temp", 0.0)))),
        "deviceId": payload.get("deviceId", f"sim-{uuid.uuid4().hex[:6]}")
    }

    # Write to DynamoDB
    table.put_item(Item=item)

    # Return response
    return {
        "statusCode": 200,
        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type"
        },
        "body": json.dumps({"message": "stored", "item": item}, default=str)
    }
