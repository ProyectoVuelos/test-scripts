import json
import logging
import argparse
import requests
import time
from pathlib import Path
from tqdm import tqdm
import config


def get_summaries_for_batch(id_batch):
    try:
        params = {"flight_ids": ",".join(id_batch)}
        r = requests.get(config.SUMMARY_URL, headers=config.HEADERS, params=params)
        r.raise_for_status()
        return r.json().get("data", [])
    except requests.RequestException as e:
        logging.error(f"Failed to get summary for batch {id_batch[0]}...: {e}")
        return []


def main():
    config.setup_logging()
    parser = argparse.ArgumentParser(
        description="Fetch flight summaries for a list of discovered IDs."
    )
    parser.add_argument(
        "run_directory",
        help="Path to the 'run_...' directory containing discovered_ids.json",
    )
    args = parser.parse_args()

    run_dir = Path(args.run_directory)
    id_file = run_dir / "discovered_ids.json"

    if not id_file.is_file():
        logging.critical(
            f"ID file not found in {run_dir}. Please run discover_flights.py first."
        )
        return

    with open(id_file, "r") as f:
        discovered_ids = json.load(f)

    flights_to_process = discovered_ids[: config.TOTAL_FLIGHTS_TO_PROCESS]
    logging.info(
        f"--- Iniciando la obtención de resúmenes para {len(flights_to_process)} vuelos ---"
    )

    all_summaries = []
    id_batches = [
        flights_to_process[i : i + 15] for i in range(0, len(flights_to_process), 15)
    ]

    pbar_summaries = tqdm(id_batches, desc="📊 Fetching Summaries in Batches")
    for batch in pbar_summaries:
        summaries = get_summaries_for_batch(batch)
        if summaries:
            all_summaries.extend(summaries)
        time.sleep(2.1)

    if not all_summaries:
        logging.warning("No flight summaries could be retrieved.")
        return

    summaries_dir = run_dir / "summaries"
    summaries_dir.mkdir(parents=True, exist_ok=True)
    date_str = run_dir.name.split("_")[1]
    summary_file_path = summaries_dir / f"flights_summary_{date_str}.json"
    with open(summary_file_path, "w", encoding="utf-8") as f:
        json.dump(all_summaries, f, indent=2)

    logging.info(
        f"✅ Successfully retrieved and saved {len(all_summaries)} summaries to {summary_file_path}"
    )


if __name__ == "__main__":
    main()
