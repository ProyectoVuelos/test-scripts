# import json
# from opensky_api import OpenSkyApi
# from datetime import datetime, timedelta
# import time


# output_file = 'data/opensky_flights.json'

# # two hours data
# end = int(time.time())
# begin = end - 2 * 60 * 60

# api = OpenSkyApi()
# flights = api.get_flights_from_interval(begin, end)

# if flights:
#     flights_list = []
#     for f in flights:
#         flights_list.append({
#             "icao24": f.icao24,
#             "callsign": f.callsign,
#             "departure_airport": f.estDepartureAirport,
#             "arrival_airport": f.estArrivalAirport,
#             "departure_time": f.firstSeen,
#             "arrival_time": f.lastSeen,
#         })

#     with open(output_file, "w") as f:
#         json.dump(flights_list, f, indent=2)

#     print(f"Saved to {output_file}")

# else:
#     print("No flights available at the moment.")

import os
import json
import time
from datetime import datetime
from opensky_api import OpenSkyApi

os.makedirs("data/flights", exist_ok=True)
timestamp = datetime.utcnow().strftime("%Y%m%d%H%M")

api = OpenSkyApi()

end_time = int(time.time())
start_time = end_time - 2 * 60 * 60  # 2 horas

print("🛫 Obteniendo vuelos...")
flights = api.get_flights_from_interval(start_time, end_time)

flight_data = []
for flight in flights:
    flight_data.append({
        "icao24": flight.icao24,
        "callsign": flight.callsign.strip() if flight.callsign else None,
        "departure_airport": flight.estDepartureAirport,
        "arrival_airport": flight.estArrivalAirport,
        "first_seen": flight.firstSeen,
        "last_seen": flight.lastSeen
    })

with open(f"data/flights/flights_{timestamp}.json", "w") as f:
    json.dump(flight_data, f, indent=2)
print(f"✅ Guardado: data/flights/flights_{timestamp}.json")

interval = 60  # cada 60 segundos

for flight in flight_data:
    icao24 = flight["icao24"]
    start = flight["first_seen"]
    end = flight["last_seen"]
    detailed_states = []

    current = start
    print(f"\n📡 Recolectando datos para {icao24}...")

    while current < end:
        try:
            states = api.get_states(time_secs=current)
            if states and states.states:
                for s in states.states:
                    if s.icao24 == icao24:
                        detailed_states.append({
                            "timestamp": current,
                            "latitude": s.latitude,
                            "longitude": s.longitude,
                            "velocity": s.velocity,
                            "altitude": s.geo_altitude,
                            "vertical_rate": s.vertical_rate,
                            "on_ground": s.on_ground,
                        })
        except Exception as e:
            print(f"⚠️ Error at {current}: {e}")
        current += interval
        time.sleep(1)  # Para evitar límites de la API

    if detailed_states:
        file_path = f"data/flights/{icao24}_{timestamp}.json"
        with open(file_path, "w") as f:
            json.dump(detailed_states, f, indent=2)
        print(f"✅ Guardado: {file_path}")
    else:
        print("⚠️ No se encontraron estados detallados.")
