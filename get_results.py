import json


with open("data/flights/call_202506271914.json", 'r') as f:
    data = json.load(f)

results = []
for flight in data:
    if flight["live"] is not None:
        results.append(flight)

with open("data/flights/results.json", 'w') as f:
    json.dump(results, f, indent=2)
