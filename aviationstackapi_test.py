import requests
import time
import json
import os
from datetime import datetime

API_KEY = os.environ.get("AVIATIONSTACK_API_KEY")
BASE_URL = "http://api.aviationstack.com/v1/flights"
timestamp = datetime.utcnow().strftime("%Y%m%d%H%M")

os.makedirs("data/flights", exist_ok=True)

flights_all = []
limit = 100
max_pages = 5

for i in range(max_pages):
    offset = i * limit
    params = {
        "access_key": API_KEY,
        "limit": limit,
        "offset": offset,
        "flight_status": "landed"
    }

    print(f"🔄 Página {i+1}...")
    res = requests.get(BASE_URL, params=params)
    data = res.json()

    if "data" in data:
        flights_all.extend(data["data"])
    else:
        print("⚠️ Error o límite alcanzado:", data)
        break

    time.sleep(1)  # evitar rate limit


file_path = f"data/flights/call_{timestamp}.json"
with open(file_path, "w") as f:
    json.dump(flights_all, f, indent=2)
print(f"✅ Guardado: {file_path} ({len(flights_all)} vuelos)")
