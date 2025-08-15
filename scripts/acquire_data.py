import json
import logging
import argparse
import requests
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import config


def get_data_for_flight_id(flight_id):
    """
    Fetches summary and positions for a flight ID, but only if its final status is 'Landed'.
    """
    try:
        summary_params = {"flight": flight_id}
        sum_r = requests.get(
            config.SUMMARY_URL, headers=config.HEADERS, params=summary_params
        )
        sum_r.raise_for_status()
        summary_data = sum_r.json()

        if summary_data.get("status", {}).get("text", "").lower() != "landed":
            return None

        pos_params = {"fr24_id": flight_id}
        pos_r = requests.get(
            config.POSITIONS_URL, headers=config.HEADERS, params=pos_params
        )
        pos_r.raise_for_status()
        positions_data = pos_r.json().get("data", [])

        if len(positions_data) < config.MINIMUM_DATA_POINTS:
            return None

        return {"id": flight_id, "positions": positions_data, "summary": summary_data}

    except requests.RequestException as e:
        if e.response and e.response.status_code == 404:
            return None
        logging.error(f"Failed to get full data for {flight_id}: {e}")
        return None


def main():
    config.setup_logging()
    parser = argparse.ArgumentParser(
        description="Acquire full data for a list of candidate flights from a previous run."
    )
    parser.add_argument(
        "run_directory",
        help="Path to the 'run_...' directory containing candidate_flights.json",
    )
    args = parser.parse_args()

    run_dir = Path(args.run_directory)
    candidate_file = run_dir / "candidate_flights.json"

    if not candidate_file.is_file():
        logging.critical(
            f"Candidate file not found in {run_dir}. Please run acquire_ids.py first."
        )
        return

    with open(candidate_file, "r") as f:
        candidate_ids = json.load(f)

    flights_to_process = candidate_ids[: config.TOTAL_FLIGHTS_TO_PROCESS]

    estimated_cost = len(flights_to_process) * 540
    logging.info(
        f"Verifying and processing up to {len(flights_to_process)} flights. Estimated cost: ~{estimated_cost:,} credits."
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
            desc="📊 Verifying and Acquiring Data",
        )
        for future in pbar:
            result = future.result()
            if result:
                all_flight_data.append(result)

    if not all_flight_data:
        logging.warning("No completed flight data was acquired.")
        return

    logging.info(
        f"Successfully acquired data for {len(all_flight_data)} completed flights."
    )

    date_range_str = run_dir.name.split("_")[1]
    flight_details_map, all_summaries = {}, []

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

    details_map_path = run_dir / f"flight_details_map_{date_range_str}.json"
    with open(details_map_path, "w", encoding="utf-8") as f:
        json.dump(flight_details_map, f, indent=2)

    summaries_dir = run_dir / "summaries"
    summaries_dir.mkdir(parents=True, exist_ok=True)
    summary_file_path = summaries_dir / f"flights_summary_{date_range_str}.json"
    with open(summary_file_path, "w", encoding="utf-8") as f:
        json.dump(all_summaries, f, indent=2)

    logging.info("✅ Data acquisition finished successfully.")


if __name__ == "__main__":
    main()
