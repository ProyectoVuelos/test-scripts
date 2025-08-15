import time
import json
import logging
import requests
from datetime import datetime, timezone
from tqdm import tqdm
import config


def get_recent_flight_ids(airport_list):
    """Fetches IDs of all recent flights (landed, en-route, etc.) from a list of airports."""
    flight_ids = set()
    pbar = tqdm(airport_list, desc="✈️  Collecting Recent Flight IDs")

    for airport_icao in pbar:
        pbar.set_postfix_str(f"Querying {airport_icao}")
        page = 1
        while True:
            try:
                params = {
                    "airport": airport_icao,
                    "direction": "any",
                    "limit": 100,
                    "page": page,
                }
                r = requests.get(
                    config.FLIGHTS_URL, headers=config.HEADERS, params=params
                )
                r.raise_for_status()
                data = r.json()

                flights_on_page = data.get("data", [])
                if not flights_on_page:
                    break

                for flight in flights_on_page:
                    flight_ids.add(flight.get("fr24_id"))

                page += 1
                time.sleep(0.5)

            except requests.RequestException as e:
                logging.error(f"Could not fetch flights for {airport_icao}: {e}")
                break

    return list(flight_ids)


def main():
    config.setup_logging()

    run_timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    run_output_dir = config.BASE_OUTPUT_DIR / f"run_{run_timestamp}"
    run_output_dir.mkdir(parents=True, exist_ok=True)
    logging.info(f"📂 Output directory for this run: {run_output_dir}")

    if not config.AIRPORTS_FILE.exists():
        logging.critical(f"Airport list not found at: {config.AIRPORTS_FILE}")
        return
    with open(config.AIRPORTS_FILE, "r") as f:
        airport_list = [line.strip() for line in f if line.strip()]

    candidate_ids = get_recent_flight_ids(airport_list)

    if not candidate_ids:
        logging.warning("No candidate flights found.")
        return

    candidate_file = run_output_dir / "candidate_flights.json"
    with open(candidate_file, "w") as f:
        json.dump(candidate_ids, f)

    logging.info(
        f"✅ Successfully saved {len(candidate_ids)} candidate flight IDs to {candidate_file}"
    )
    logging.info(
        "-> Please wait 24 hours before running acquire_data.py on this directory."
    )


if __name__ == "__main__":
    main()
