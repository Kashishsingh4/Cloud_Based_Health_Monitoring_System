import requests, csv, argparse

parser = argparse.ArgumentParser()
parser.add_argument("--url", required=True, help="https://w76luodf64.execute-api.ap-south-1.amazonaws.com/prod/vitals")
parser.add_argument("--csv", default="patients_data.csv", help="Path to patient CSV file")
args = parser.parse_args()


patients = []
with open(args.csv, newline='') as f:
    reader = csv.DictReader(f)
    for row in reader:
        patients.append({
            "department": row["department"],
            "patientId": row["patientId"],
            "heartRate": int(row["heartRate"]),
            "spo2": int(row["spo2"]),
            "temperature": float(row["temperature"])
        })


for patient in patients:
    payload = {
        "department": patient["department"],
        "patientId": patient["patientId"],
        "heartRate": patient["heartRate"],
        "spo2": patient["spo2"],
        "temperature": patient["temperature"]
    }
    try:
        r = requests.post(args.url, json=payload, timeout=5)
        print(f"[{patient['patientId']}] {patient['department']} {r.status_code} {payload}")
    except Exception as e:
        print(f"[{patient['patientId']}] ERROR {e}")
