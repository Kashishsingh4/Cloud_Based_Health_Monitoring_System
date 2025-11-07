
import os, json, boto3, math

sns = boto3.client("sns")
dynamodb = boto3.resource("dynamodb")

TABLE = os.environ.get("TABLE_NAME")
TOPIC_ARN = os.environ.get("ALERT_TOPIC_ARN")
table = dynamodb.Table(TABLE)

WINDOW = 6
HR_THRESHOLD = 120
SPO2_THRESHOLD = 90
TEMP_THRESHOLD = 38.0



def parse_image(img):
    def g(k):
        v = img.get(k)
        if not v:
            return None
        if 'S' in v:
            return v['S']
        if 'N' in v:
            return float(v['N'])
        return None

    return {
        "patientId": g('patientId'),
        "department": g('department'),   
        "ts": g('ts'),
        "hr": int(g('hr') or 0),
        "spo2": int(g('spo2') or 0),
        "temp": float(g('temp') or 0.0)
    }



def mean_std(vals):
    if not vals:
        return None, None
    n = len(vals)
    mean = sum(vals) / n
    var = sum((x - mean) ** 2 for x in vals) / n
    return mean, math.sqrt(var)


def slope(vals):
    n = len(vals)
    if n < 2:
        return 0.0
    xs = list(range(n))
    mean_x = sum(xs) / n
    mean_y = sum(vals) / n
    num = sum((xs[i] - mean_x) * (vals[i] - mean_y) for i in range(n))
    den = sum((xs[i] - mean_x) ** 2 for i in range(n))
    return num / den if den != 0 else 0.0


# ---- Publish SNS alert ----
def publish_alert(patient, dept, ts, measure, value, reason, context):
    body = {
        "patientId": patient,
        "department": dept,
        "timestamp": ts,
        "measure": measure,
        "value": value,
        "reason": reason,
        "context": context
    }
    sns.publish(
        TopicArn=TOPIC_ARN,
        Subject=f"Health Alert - {patient} ({dept})",
        Message=json.dumps(body)
    )


# ---- Lambda entrypoint ----
def lambda_handler(event, context):
    for rec in event.get('Records', []):
        if rec.get('eventName') not in ('INSERT', 'MODIFY'):
            continue

        img = rec.get('dynamodb', {}).get('NewImage')
        if not img:
            continue

        data = parse_image(img)
        patient = data['patientId']
        dept = data.get('department', 'Unknown')

        # Query last few items for this patient
        try:
            resp = table.query(
                KeyConditionExpression=boto3.dynamodb.conditions.Key('patientId').eq(patient),
                ScanIndexForward=False,
                Limit=WINDOW
            )
            items = resp.get('Items', [])
        except Exception:
            items = []

        hr_vals = [int(i.get('hr', 0)) for i in items if 'hr' in i]
        temp_vals = [float(i.get('temp', 0.0)) for i in items if 'temp' in i]
        spo2_vals = [int(i.get('spo2', 0)) for i in items if 'spo2' in i]

        # --- Threshold alerts ---
        if data['hr'] and data['hr'] >= HR_THRESHOLD:
            publish_alert(patient, dept, data['ts'], 'hr', data['hr'], 'High HR threshold', {'recent': hr_vals})
        if data['spo2'] and data['spo2'] <= SPO2_THRESHOLD:
            publish_alert(patient, dept, data['ts'], 'spo2', data['spo2'], 'Low SpO2 threshold', {'recent': spo2_vals})
        if data['temp'] and data['temp'] >= TEMP_THRESHOLD:
            publish_alert(patient, dept, data['ts'], 'temp', data['temp'], 'High temp threshold', {'recent': temp_vals})

        # --- Statistical anomaly (Z-score) ---
        if len(hr_vals) >= 3:
            mean, std = mean_std(hr_vals)
            if std and (data['hr'] - mean) / std > 2.0:
                publish_alert(patient, dept, data['ts'], 'hr', data['hr'], 'HR z-score anomaly',
                              {'mean': mean, 'std': std, 'recent': hr_vals})

        # --- Trend detection (Temperature rising trend) ---
        if len(temp_vals) >= 3:
            sl = slope(list(reversed(temp_vals)))
            if sl > 0.1:
                publish_alert(patient, dept, data['ts'], 'temp', data['temp'], 'Rising temp trend',
                              {'slope': sl, 'recent': temp_vals})

    return {"statusCode": 200}
