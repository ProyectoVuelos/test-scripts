import json
import logging
import argparse
from pathlib import Path
from collections import defaultdict
from datetime import datetime, timezone
from tqdm import tqdm
import config


def main():
    config.setup_logging()
    parser = argparse.ArgumentParser(
        description="Assemble flight paths from raw snapshot files."
    )
    parser.add_argument(
        "run_directory",
        help="Path to the 'run_...' directory with the raw_positions folder.",
    )
    args = parser.parse_args()

    run_dir = Path(args.run_directory)
    raw_dir = run_dir / "raw_positions"
    summary_file = next(run_dir.glob("summaries/flights_summary_*.json"), None)

    if not raw_dir.is_dir():
        logging.critical(
            f"Raw positions directory not found in {run_dir}. Please run reconstruct_paths.py first."
        )
        return
    if not summary_file:
        logging.critical(
            f"Summary file not found in {run_dir}. Cannot assemble without it for callsigns."
        )
        return

    with open(summary_file, "r") as f:
        summaries = json.load(f)
    id_to_callsign_map = {
        s["fr24_id"]: s.get("callsign") or s.get("flight") for s in summaries
    }

    flight_paths = defaultdict(list)
    snapshot_files = list(raw_dir.glob("*.json"))

    logging.info("--- Iniciando Ensamblaje de Rutas ---")
    pbar = tqdm(snapshot_files, desc="🧩 Assembling Paths")
    for snapshot_file in pbar:
        with open(snapshot_file, "r") as f:
            try:
                data = json.load(f)
                positions = data.get("positions", []) or data.get("data", [])
                for pos in positions:
                    fr24_id = pos.get("fr24_id")
                    if fr24_id:
                        flight_paths[fr24_id].append(pos)
            except json.JSONDecodeError:
                logging.warning(f"Could not decode JSON from file: {snapshot_file}")

    logging.info(
        f"Ensamblaje completado. Se encontraron datos para {len(flight_paths)} vuelos únicos."
    )

    flight_details_map = {}
    for fr24_id, positions in flight_paths.items():
        unique_positions = {p["timestamp"]: p for p in positions}.values()
        sorted_positions = sorted(unique_positions, key=lambda p: p["timestamp"])

        reformatted_positions = []
        for p in sorted_positions:
            try:
                ts_str = p.get("timestamp")
                if isinstance(ts_str, str):
                    dt_obj = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                    timestamp_int = int(dt_obj.timestamp())
                else:
                    timestamp_int = int(ts_str)

                reformatted_positions.append(
                    {
                        "timestamp": timestamp_int,
                        "latitude": p.get("lat"),
                        "longitude": p.get("lon"),
                        "altitude": p.get("alt", 0),
                        "ground_speed": p.get("gspeed", 0),
                        "vertical_rate": p.get("vspeed", 0),
                    }
                )
            except (ValueError, TypeError, AttributeError):
                continue

        flight_details_map[fr24_id] = {
            "positions": reformatted_positions,
            "callsign_or_flight": id_to_callsign_map.get(fr24_id, "UNKNOWN"),
        }

    date_str = run_dir.name.split("_")[1]
    output_file = run_dir / f"flight_details_map_{date_str}.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(flight_details_map, f, indent=2)

    logging.info(f"✅ Archivo final 'flight_details_map.json' creado en {run_dir}")


if __name__ == "__main__":
    main()
