import csv
import json
import logging
import argparse
import requests
from datetime import datetime
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import config


def get_processed_keys(base_dir: Path):
    """
    Scans all existing run directories to find which callsign+date combinations
    have already been processed. Returns a set of keys like 'SWA1232025-08-14'.
    """
    processed_keys = set()
    search_pattern = "run_*/processed/flights_processed_*.json"
    for processed_file in base_dir.glob(search_pattern):
        try:
            with open(processed_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                for flight in data:
                    if flight.get("callsign") and flight.get("departure_time_utc"):
                        key = flight["callsign"] + flight["departure_time_utc"][:10]
                        processed_keys.add(key)
        except (json.JSONDecodeError, OSError):
            logging.warning(f"Could not read or parse existing file: {processed_file}")

    if processed_keys:
        logging.info(
            f"Found {len(processed_keys)} flights that have already been processed in previous runs."
        )
    return processed_keys


def get_flight_details(callsign, date_str):
    """
    Finds a flight by callsign and date, then fetches its full summary and position data.
    """
    try:
        summary_params = {"flight": callsign, "date": date_str}
        sum_r = requests.get(
            config.SUMMARY_URL, headers=config.HEADERS, params=summary_params
        )
        sum_r.raise_for_status()
        summary_data_list = sum_r.json()

        if not isinstance(summary_data_list, list) or not summary_data_list:
            return None
        summary_data = summary_data_list[0]

        fr24_id = summary_data.get("fr24_id")
        if not fr24_id:
            return None

        pos_params = {"fr24_id": fr24_id}
        pos_r = requests.get(
            config.POSITIONS_URL, headers=config.HEADERS, params=pos_params
        )
        pos_r.raise_for_status()
        positions_data = pos_r.json().get("data", [])

        if len(positions_data) < config.MINIMUM_DATA_POINTS:
            return None

        return {"id": fr24_id, "positions": positions_data, "summary": summary_data}
    except requests.RequestException:
        return None


def main():
    config.setup_logging()
    parser = argparse.ArgumentParser(
        description="Enrich a list of flights from a CSV file with detailed API data."
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Limit the number of NEW flights to process in this run.",
    )
    parser.add_argument(
        "--skip-processed",
        action="store_true",
        help="Scan existing runs and skip flights that have already been processed.",
    )
    args = parser.parse_args()

    input_file = Path("flights_to_track.csv")
    if not input_file.is_file():
        logging.critical(f"Input file not found: {input_file}. Please create it.")
        return

    with open(input_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        flights_to_enrich = list(reader)

    if args.skip_processed:
        processed_keys = get_processed_keys(config.BASE_OUTPUT_DIR)
        original_count = len(flights_to_enrich)
        flights_to_enrich = [
            f
            for f in flights_to_enrich
            if (f["callsign"] + f["date"]) not in processed_keys
        ]
        logging.info(
            f"Skipped {original_count - len(flights_to_enrich)} flights that were already processed."
        )

    if not flights_to_enrich:
        logging.warning("No new flights to process from the CSV file.")
        return

    if args.limit:
        flights_to_enrich = flights_to_enrich[: args.limit]
        logging.info(
            f"Applying limit. Will process a maximum of {len(flights_to_enrich)} new flights."
        )

    run_timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    run_output_dir = config.BASE_OUTPUT_DIR / f"run_{run_timestamp}"
    run_output_dir.mkdir(parents=True, exist_ok=True)
    logging.info(f"📂 Output directory for this run: {run_output_dir}")

    estimated_cost = len(flights_to_enrich) * 540
    logging.info(
        f"Enriching {len(flights_to_enrich)} flights. Estimated cost: ~{estimated_cost:,} credits."
    )

    all_flight_data = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_flight = {
            executor.submit(get_flight_details, f["callsign"], f["date"]): f
            for f in flights_to_enrich
        }
        pbar = tqdm(
            as_completed(future_to_flight),
            total=len(flights_to_enrich),
            desc="📊 Enriching Flight Data",
        )
        for future in pbar:
            result = future.result()
            if result:
                all_flight_data.append(result)

    if not all_flight_data:
        logging.warning("No flight data was enriched.")
        return

    logging.info(f"Successfully enriched data for {len(all_flight_data)} flights.")

    flight_details_map, all_summaries = {}, []
    date_str = run_timestamp.split("_")[0].replace("-", "")

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

    details_map_path = run_output_dir / f"flight_details_map_{date_str}.json"
    with open(details_map_path, "w", encoding="utf-8") as f:
        json.dump(flight_details_map, f, indent=2)

    summaries_dir = run_output_dir / "summaries"
    summaries_dir.mkdir(parents=True, exist_ok=True)
    summary_file_path = summaries_dir / f"flights_summary_{date_str}.json"
    with open(summary_file_path, "w", encoding="utf-8") as f:
        json.dump(all_summaries, f, indent=2)

    logging.info(f"✅ Data enrichment finished. Results saved in {run_output_dir}")


if __name__ == "__main__":
    main()
