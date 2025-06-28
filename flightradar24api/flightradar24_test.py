import requests
import json
import os
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()
api_key = os.environ.get("FR24_API_KEY")

url = "https://fr24api.flightradar24.com/api/flight-summary/full"
params = {
#   'flights': 'EK184',
  'flight_datetime_from': '2025-02-14T01:17:14',
  'flight_datetime_to': '2025-02-15T13:17:14'
}
headers = {
  'Accept': 'application/json',
  'Accept-Version': 'v1',
  'Authorization': f'Bearer {api_key}'
}

try:
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    data = response.json()
    timestamp = datetime.utcnow().strftime("%Y%m%d%H%M")

    with open(f"data/flights/call_{timestamp}.json", "w") as f:
        json.dump(data['data'], f, indent=2)
    
    print(f"✅ Guardado: data/flights/call_{timestamp}.json ({len(data['data'])} vuelos)")

except requests.exceptions.HTTPError as http_err:
    print(f"HTTP error occurred: {http_err}")
except Exception as err:
    print(f"An error occurred: {err}")
