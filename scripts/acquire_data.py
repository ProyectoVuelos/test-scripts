import time
import json
import logging
import requests
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import config


def get_flight_ids_from_airports(airport_list):
    """Fetches a list of unique flight IDs from a list of airport ICAO codes."""
    flight_ids = set()
    pbar = tqdm(airport_list, desc="✈️  Seeding Flight IDs from Airports")
    for airport_icao in pbar:
        pbar.set_postfix_str(f"Querying {airport_icao}")
        try:
            params = {"airport": airport_icao, "direction": "any", "limit": 100}
            r = requests.get(config.FLIGHTS_URL, headers=config.HEADERS, params=params)
            r.raise_for_status()
            data = r.json()
            for flight in data.get("data", []):
                flight_ids.add(flight.get("fr24_id"))
        except requests.RequestException as e:
            logging.error(f"Could not fetch flights for {airport_icao}: {e}")
        time.sleep(0.5)
    return list(flight_ids)


def get_data_for_flight_id(flight_id):
    """Worker function to get position history and summary for a single flight ID."""
    try:
        # 1. Get Position History
        pos_params = {"fr24_id": flight_id}
        pos_r = requests.get(
            config.POSITIONS_URL, headers=config.HEADERS, params=pos_params
        )
        pos_r.raise_for_status()
        positions_data = pos_r.json().get("data", [])

        if not positions_data or len(positions_data) < config.MINIMUM_DATA_POINTS:
            return None

        # 2. Get Flight Summary
        summary_params = {"flight": flight_id}
        sum_r = requests.get(
            config.SUMMARY_URL, headers=config.HEADERS, params=summary_params
        )
        sum_r.raise_for_status()
        summary_data = sum_r.json()

        return {"id": flight_id, "positions": positions_data, "summary": summary_data}
    except requests.RequestException as e:
        logging.error(f"Failed to get full data for {flight_id}: {e}")
        return None


def main():
    """Main function to run the optimized data acquisition process."""
    config.setup_logging()

    run_timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    run_output_dir = config.BASE_OUTPUT_DIR / f"run_{run_timestamp}"
    run_output_dir.mkdir(parents=True, exist_ok=True)
    logging.info(f"📂 Output directory for this run: {run_output_dir}")

    # --- Step 1: Seed flight IDs from airports ---
    if not config.AIRPORTS_FILE.exists():
        logging.critical(f"Airport list not found at: {config.AIRPORTS_FILE}")
        return
    with open(config.AIRPORTS_FILE, "r") as f:
        airport_list = [line.strip() for line in f if line.strip()]

    unique_flight_ids = get_flight_ids_from_airports(airport_list)
    if not unique_flight_ids:
        logging.warning("No flight IDs were found. Exiting.")
        return

    logging.info(f"Found {len(unique_flight_ids)} unique flight IDs.")

    # --- Step 2: Estimate cost and process flights ---
    flights_to_process = unique_flight_ids[: config.MAX_FLIGHTS_TO_PROCESS]
    estimated_cost = len(flights_to_process) * (40 + 250 * 2)
    logging.info(
        f"Processing {len(flights_to_process)} flights. Estimated cost: ~{estimated_cost:,} credits."
    )

    all_flight_data = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_flight = {
            executor.submit(get_data_for_flight_id, fid): fid
            for fid in flights_to_process
        }
        pbar = tqdm(
            as_completed(future_to_flight),
            total=len(flights_to_process),
            desc="📊 Acquiring Full Flight Data",
        )
        for future in pbar:
            result = future.result()
            if result:
                all_flight_data.append(result)

    # --- Step 3: Save data in the format expected by process_data.py ---
    if not all_flight_data:
        logging.warning("No complete flight data was acquired. Exiting.")
        return

    flight_details_map = {}
    all_summaries = []
    date_str = datetime.now().strftime("%Y%m%d")

    for flight in all_flight_data:
        fid = flight["id"]
        summary = flight["summary"]
        all_summaries.append(summary)

        reformatted_positions = [
            {
                "timestamp": p.get("ts"),
                "latitude": p.get("lat"),
                "longitude": p.get("lon"),
                "altitude": p.get("alt"),
                "ground_speed": p.get("gs"),
                "vertical_rate": p.get("vs"),
            }
            for p in flight["positions"]
        ]

        flight_details_map[fid] = {
            "positions": reformatted_positions,
            "callsign_or_flight": summary.get("callsign") or summary.get("flight"),
        }

    details_map_path = run_output_dir / f"flight_details_map_{date_str}.json"
    with open(details_map_path, "w", encoding="utf-8") as f:
        json.dump(flight_details_map, f, indent=2)
    logging.info(f"Saved flight details map to {details_map_path}")

    summaries_dir = run_output_dir / "summaries"
    summaries_dir.mkdir(parents=True, exist_ok=True)
    summary_file_path = summaries_dir / f"flights_summary_{date_str}.json"
    with open(summary_file_path, "w", encoding="utf-8") as f:
        json.dump(all_summaries, f, indent=2)
    logging.info(f"Saved {len(all_summaries)} summaries to {summary_file_path}")

    logging.info("✅ Data acquisition finished successfully.")


if __name__ == "__main__":
    main()
