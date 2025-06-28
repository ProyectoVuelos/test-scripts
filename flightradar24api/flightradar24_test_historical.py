from datetime import datetime, timedelta
import requests, os, json
from dotenv import load_dotenv
from pathlib import Path

dotenv_path = Path(__file__).resolve().parent / '.env'
load_dotenv(dotenv_path)
api_key = os.getenv("FR24_API_KEY")
if not api_key:
    raise ValueError("Missing FR24_API_KEY")

start_time = datetime.utcnow() - timedelta(days=1)
interval = timedelta(minutes=10)
iterations = 6  # 1 hour = 6 intervals of 10 minutes

# Output
all_flights = {}

for i in range(iterations):
    timestamp = int((start_time + i * interval).timestamp())
    print(f"⏳ Requesting data for {datetime.utcfromtimestamp(timestamp)} (timestamp={timestamp})")

    url = "https://fr24api.flightradar24.com/api/historic/flight-positions/full"
    params = {
        "bounds": "90,-90,-180,180",  # Entire globe
        "timestamp": timestamp
    }
    headers = {
        "Accept": "application/json",
        "Accept-Version": "v1",
        "Authorization": f"Bearer {api_key}"
    }

    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()

        positions = data.get("positions", []) or data.get("data", [])  # fallback if key is different
        print(f"✅ {len(positions)} flights at this snapshot")

        for flight in positions:
            flight_id = flight.get("id") or flight.get("icao24") or str(flight)
            all_flights[flight_id] = flight  # De-duplicate using flight ID

    except Exception as e:
        print(f"❌ Error at timestamp {timestamp}: {e}")

output_dir = Path("data/positions")
output_dir.mkdir(parents=True, exist_ok=True)
filename = f"aggregate_positions_{start_time.strftime('%Y%m%d%H%M')}.json"
with open(output_dir / filename, "w") as f:
    json.dump(list(all_flights.values()), f, indent=2)

print(f"📦 Aggregated {len(all_flights)} unique flights across {iterations} snapshots")
