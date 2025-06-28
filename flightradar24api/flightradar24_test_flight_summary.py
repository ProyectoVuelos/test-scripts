import requests
import json
import os
from dotenv import load_dotenv
from datetime import datetime
from pathlib import Path

dotenv_path = Path(__file__).resolve().parent / '.env'
load_dotenv(dotenv_path)

api_key = os.environ.get("FR24_API_KEY")

if not api_key:
    raise ValueError("❌ No API key found. Make sure FR24_API_KEY is set in your .env file.")

url = "https://fr24api.flightradar24.com/api/flight-summary/full"
params = {
    'flights': 'EK184', 
    'flight_datetime_from': '2025-02-14T01:17:14',
    'flight_datetime_to': '2025-02-15T13:17:14'
}
headers = {
    'Accept': 'application/json',
    'Accept-Version': 'v1',
    'Authorization': f'Bearer {api_key}'
}

print(f"⏳ Sending request to {url}")
print(f"🔧 Params: {params}")
print(f"🔐 Headers: {headers}")

try:
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()

    data = response.json()

    if 'data' not in data:
        raise ValueError("❌ 'data' key not found in the response.")

    timestamp = datetime.utcnow().strftime("%Y%m%d%H%M")
    output_dir = Path("data/flights")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"call_{timestamp}.json"

    with open(output_path, "w") as f:
        json.dump(data['data'], f, indent=2)
    
    print(f"✅ Saved response to: {output_path} ({len(data['data'])} flights)")

except requests.exceptions.HTTPError as http_err:
    print(f"❌ HTTP error: {http_err} - Response: {response.text}")
except requests.exceptions.RequestException as req_err:
    print(f"❌ Request failed: {req_err}")
except Exception as err:
    print(f"❌ General error: {err}")
