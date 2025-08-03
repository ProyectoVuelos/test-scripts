import json
from collections import defaultdict


def open_test_file():
    with open("data/positions/aggregate_positions_202507021645.json") as f:
        return json.load(f)


def organize_flight_data(data):
    """
    Organizes flight data into a dictionary where keys are 'fr24_id'
    and values are dictionaries containing 'fr24_id' and a list of
    historical flight data (without the 'fr24_id' key in the historical entries).

    Args:
        data (list of dict): A list of flight dictionaries, where each dictionary
                             is expected to have an 'fr24_id' key.

    Returns:
        defaultdict: An organized dictionary of flight data.
    """
    organized_data = defaultdict(lambda: {"fr24_id": None, "historical": []})

    for flight in data:
        fr24_id = flight.get("fr24_id")
        if fr24_id is None:
            # Handle cases where 'fr24_id' might be missing in a flight entry
            print(f"Warning: Flight entry missing 'fr24_id': {flight}")
            continue

        # Use setdefault to initialize the dictionary for fr24_id if it doesn't exist
        # This is more concise than the 'if not in' check
        current_flight_data = organized_data.setdefault(
            fr24_id, {"fr24_id": fr24_id, "historical": []}
        )

        # Remove 'fr24_id' from the flight dictionary before appending to historical
        # Create a copy to avoid modifying the original 'flight' dictionary
        flight_copy = flight.copy()
        flight_copy.pop(
            "fr24_id", None
        )  # Use None as default to prevent KeyError if already popped

        current_flight_data["historical"].append(flight_copy)

        # Ensure 'fr24_id' is correctly set in the top-level dictionary
        current_flight_data["fr24_id"] = fr24_id

    return organized_data


if __name__ == "__main__":
    data = open_test_file()
    organized_data = organize_flight_data(data)

    with open(
        "data/positions/aggregate_positions_202507021645_organized.json", "w"
    ) as f:
        json.dump(organized_data, f, indent=2)
