import time
import json
import logging
import requests
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import config


def get_flight_ids_for_day(airport_list, target_date_str):
    """
    Fetches all unique, landed flight IDs from a list of airports for a single specific date.
    """
    flight_ids_for_day = set()
    start_of_day_utc = datetime.strptime(target_date_str, "%Y-%m-%d").replace(
        tzinfo=timezone.utc
    )
    end_of_day_utc = start_of_day_utc + timedelta(days=1) - timedelta(seconds=1)

    pbar = tqdm(airport_list, desc=f"✈️  Seeding Day: {target_date_str}", leave=False)
    for airport_icao in pbar:
        pbar.set_postfix_str(f"Querying {airport_icao}")
        page = 1
        while True:
            try:
                params = {
                    "airport": airport_icao,
                    "direction": "arrivals",
                    "limit": 100,
                    "page": page,
                    "flight_date_from": start_of_day_utc.isoformat(),
                    "flight_date_to": end_of_day_utc.isoformat(),
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
                    if flight.get("status", {}).get("text", "").lower() == "landed":
                        flight_ids_for_day.add(flight.get("fr24_id"))

                page += 1
                time.sleep(0.5)
            except requests.RequestException as e:
                logging.error(
                    f"Could not fetch flights for {airport_icao} on page {page}: {e}"
                )
                break

    return flight_ids_for_day


def get_data_for_flight_id(flight_id):
    """Worker function to get position history and summary for a single flight ID."""
    try:
        pos_params = {"fr24_id": flight_id}
        pos_r = requests.get(
            config.POSITIONS_URL, headers=config.HEADERS, params=pos_params
        )
        pos_r.raise_for_status()
        positions_data = pos_r.json().get("data", [])
        if not positions_data or len(positions_data) < config.MINIMUM_DATA_POINTS:
            return None

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
    """Main function to run the data acquisition process for the entire last week."""
    config.setup_logging()
    run_timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    run_output_dir = config.BASE_OUTPUT_DIR / f"run_{run_timestamp}"
    run_output_dir.mkdir(parents=True, exist_ok=True)
    logging.info(f"📂 Output directory for this run: {run_output_dir}")

    today = datetime.now(timezone.utc)
    last_week_sunday = today - timedelta(days=today.weekday() + 1)
    last_week_monday = last_week_sunday - timedelta(days=6)
    dates_to_process = [last_week_monday + timedelta(days=i) for i in range(7)]
    logging.info(
        f"Targeting last week for data acquisition: {last_week_monday.strftime('%Y-%m-%d')} to {last_week_sunday.strftime('%Y-%m-%d')}"
    )

    if not config.AIRPORTS_FILE.exists():
        logging.critical(f"Airport list not found at: {config.AIRPORTS_FILE}")
        return
    with open(config.AIRPORTS_FILE, "r") as f:
        airport_list = [line.strip() for line in f if line.strip()]

    all_unique_flight_ids = set()
    for day in dates_to_process:
        date_str = day.strftime("%Y-%m-%d")
        ids_for_day = get_flight_ids_for_day(airport_list, date_str)
        all_unique_flight_ids.update(ids_for_day)
        logging.info(f"Found {len(ids_for_day)} completed flights for {date_str}.")

    unique_flight_ids_list = list(all_unique_flight_ids)
    if not unique_flight_ids_list:
        logging.warning("No completed flight IDs were found for last week. Exiting.")
        return

    logging.info(
        f"Found a total of {len(unique_flight_ids_list)} unique and completed flight IDs for the entire week."
    )

    if len(unique_flight_ids_list) > config.MAX_FLIGHTS_TO_PROCESS:
        logging.info(
            f"Selecting a random sample of {config.MAX_FLIGHTS_TO_PROCESS} flights from the total pool."
        )
        flights_to_process = random.sample(
            unique_flight_ids_list, k=config.MAX_FLIGHTS_TO_PROCESS
        )
    else:
        logging.info("Processing all found flights as the total is within the limit.")
        flights_to_process = unique_flight_ids_list

    estimated_cost = len(flights_to_process) * 540
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

    if not all_flight_data:
        logging.warning("No complete flight data was acquired. Exiting.")
        return

    flight_details_map, all_summaries = {}, []
    date_range_str = (
        f"{last_week_monday.strftime('%Y%m%d')}-{last_week_sunday.strftime('%Y%m%d')}"
    )
    for flight in all_flight_data:
        fid, summary = flight["id"], flight["summary"]
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

    details_map_path = run_output_dir / f"flight_details_map_{date_range_str}.json"
    with open(details_map_path, "w", encoding="utf-8") as f:
        json.dump(flight_details_map, f, indent=2)
    logging.info(f"Saved flight details map to {details_map_path}")

    summaries_dir = run_output_dir / "summaries"
    summaries_dir.mkdir(parents=True, exist_ok=True)
    summary_file_path = summaries_dir / f"flights_summary_{date_range_str}.json"
    with open(summary_file_path, "w", encoding="utf-8") as f:
        json.dump(all_summaries, f, indent=2)
    logging.info(f"Saved {len(all_summaries)} summaries to {summary_file_path}")

    logging.info("✅ Data acquisition finished successfully.")


if __name__ == "__main__":
    main()
