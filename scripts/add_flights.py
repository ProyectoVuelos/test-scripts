import json
import logging
import argparse
import random
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import config
from acquire_data import get_data_for_flight_id


def add_more_flights(run_dir: Path, num_to_add: int):
    """Adds a specified number of new flights to an existing run directory."""
    logging.info(f"Attempting to add {num_to_add} new flights to run: {run_dir.name}")

    candidate_dir = run_dir / "candidates"
    details_map_file = next(run_dir.glob("flight_details_map_*.json"), None)
    summary_file = next(run_dir.glob("summaries/flights_summary_*.json"), None)

    if not candidate_dir.is_dir():
        logging.error(f"Directory de candidatos no encontrado en: {run_dir}")
        return
    if not details_map_file or not summary_file:
        logging.error(
            f"No se encontraron los archivos de detalles o de resumen en: {run_dir}"
        )
        return

    all_candidates = set()
    for candidate_file in candidate_dir.glob("*.json"):
        with open(candidate_file, "r", encoding="utf-8") as f:
            all_candidates.update(json.load(f))

    with open(details_map_file, "r", encoding="utf-8") as f:
        existing_details = json.load(f)
        processed_ids = set(existing_details.keys())

    unprocessed_candidates = list(all_candidates - processed_ids)
    if not unprocessed_candidates:
        logging.warning("No unprocessed flights available to add.")
        return

    flights_to_process = random.sample(
        unprocessed_candidates, k=min(num_to_add, len(unprocessed_candidates))
    )
    logging.info(
        f"Found {len(unprocessed_candidates)} available flights. Selecting {len(flights_to_process)} to add."
    )

    new_flight_data = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_flight = {
            executor.submit(get_data_for_flight_id, fid): fid
            for fid in flights_to_process
        }
        pbar = tqdm(
            as_completed(future_to_flight),
            total=len(flights_to_process),
            desc="📊 Adding New Flights",
        )
        for future in pbar:
            result = future.result()
            if result:
                new_flight_data.append(result)

    if not new_flight_data:
        logging.warning("Failed to acquire data for any of the new flights.")
        return

    with open(summary_file, "r+", encoding="utf-8") as f:
        existing_summaries = json.load(f)
        for flight in new_flight_data:
            existing_summaries.append(flight["summary"])
        f.seek(0)
        f.truncate()
        json.dump(existing_summaries, f, indent=2)

    for flight in new_flight_data:
        fid, summary = flight["id"], flight["summary"]
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
        existing_details[fid] = {
            "positions": reformatted_positions,
            "callsign_or_flight": summary.get("callsign") or summary.get("flight"),
        }

    with open(details_map_file, "w", encoding="utf-8") as f:
        json.dump(existing_details, f, indent=2)

    logging.info(
        f"✅ Successfully added {len(new_flight_data)} new flights to the dataset."
    )


def main():
    config.setup_logging()
    parser = argparse.ArgumentParser(
        description="Add more flights to an existing data acquisition run."
    )
    parser.add_argument("run_directory", help="Path to the 'run_...' directory.")
    parser.add_argument(
        "--add",
        type=int,
        required=True,
        help="Number of additional flights to acquire.",
    )
    args = parser.parse_args()

    run_dir = Path(args.run_directory)
    if not run_dir.is_dir():
        logging.error(f"Directory not found: {run_dir}")
        return
    add_more_flights(run_dir, args.add)


if __name__ == "__main__":
    main()
