import json
import logging
import argparse
import time
import requests
import re
from pathlib import Path
from tqdm import tqdm
import config


def main():
    config.setup_logging()
    parser = argparse.ArgumentParser(
        description="Reconstruct flight paths by taking periodic snapshots."
    )
    parser.add_argument(
        "run_directory",
        help="Path to the 'run_...' directory containing flight_timelines.json",
    )
    args = parser.parse_args()

    run_dir = Path(args.run_directory)
    timeline_file = run_dir / "flight_timelines.json"

    if not timeline_file.is_file():
        logging.critical(
            f"Timeline file not found in {run_dir}. Please run prepare_timelines.py first."
        )
        return

    with open(timeline_file, "r") as f:
        timelines = json.load(f)

    raw_dir = run_dir / "raw_positions"
    raw_dir.mkdir(exist_ok=True)

    min_start_ts = min(tl["start_ts"] for tl in timelines)
    max_end_ts = max(tl["end_ts"] for tl in timelines)
    interval_seconds = 6 * 60

    logging.info("--- Iniciando Fase de Reconstrucción de Rutas ---")

    timestamp_range = range(min_start_ts, max_end_ts, interval_seconds)
    pbar = tqdm(timestamp_range, desc="📸 Taking Snapshots")

    valid_flight_pattern = re.compile(r"^[A-Z0-9]{2,4}\d{1,4}$")

    for ts in pbar:
        active_flights = [
            tl for tl in timelines if tl["start_ts"] <= ts <= tl["end_ts"]
        ]

        if not active_flights:
            continue

        active_flight_numbers = [f["flight_number"] for f in active_flights]
        sanitized_flight_numbers = [
            fn for fn in active_flight_numbers if valid_flight_pattern.match(fn)
        ]

        batches = [
            sanitized_flight_numbers[i : i + 15]
            for i in range(0, len(sanitized_flight_numbers), 15)
        ]

        for i, batch in enumerate(batches):
            if not batch:
                continue
            try:
                params = {"flights": ",".join(batch), "timestamp": ts}
                r = requests.get(
                    config.POSITIONS_URL, headers=config.HEADERS, params=params
                )
                r.raise_for_status()
                data = r.json()

                snapshot_file = raw_dir / f"snapshot_{ts}_batch_{i}.json"
                with open(snapshot_file, "w") as f:
                    json.dump(data, f)

                time.sleep(2.1)

            except requests.RequestException as e:
                logging.error(f"Error getting snapshot at {ts} for batch {i}: {e}")

    logging.info(
        "✅ Reconstrucción de rutas finalizada. Los snapshots crudos han sido guardados."
    )


if __name__ == "__main__":
    main()
