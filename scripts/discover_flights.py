import time
import json
import logging
import requests
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tqdm import tqdm
import config


def discover_flight_ids(target_count, target_dates):
    """
    Discovers unique flight IDs by taking snapshots on specific target dates.
    """
    logging.info(f"--- Iniciando Fase de Descubrimiento de Vuelos ---")
    unique_flights = set()

    pbar = tqdm(total=target_count, desc="🔎 Discovering Flights")

    for target_day in target_dates:
        logging.info(f"Tomando snapshots del día: {target_day.strftime('%Y-%m-%d')}")
        base_timestamp = target_day.replace(hour=0, minute=0, second=0, microsecond=0)
        timestamps = [
            int((base_timestamp + timedelta(hours=h)).timestamp())
            for h in [2, 8, 14, 20]
        ]

        for ts in timestamps:
            if len(unique_flights) >= target_count:
                break

            try:
                params = {"bounds": config.USA_BOUNDS, "timestamp": ts}
                r = requests.get(
                    config.POSITIONS_URL, headers=config.HEADERS, params=params
                )
                r.raise_for_status()
                data = r.json()

                flights_in_snapshot = data.get("positions", []) or data.get("data", [])

                for flight_data in flights_in_snapshot:
                    fr24_id = flight_data.get("fr24_id")
                    if fr24_id and fr24_id not in unique_flights:
                        unique_flights.add(fr24_id)
                        pbar.update(1)

                time.sleep(2.1)

            except requests.RequestException as e:
                logging.error(f"Error discovering flights at timestamp {ts}: {e}")
                continue

        if len(unique_flights) >= target_count:
            logging.info("Límite de vuelos alcanzado. Finalizando descubrimiento.")
            break

    pbar.close()
    return list(unique_flights)[:target_count]


def main():
    config.setup_logging()

    today = datetime.now(timezone.utc)
    wednesday_two_weeks_ago = today - timedelta(days=today.weekday() + 7 - 2)
    friday_two_weeks_ago = wednesday_two_weeks_ago + timedelta(days=2)
    saturday_two_weeks_ago = friday_two_weeks_ago + timedelta(days=1)

    target_dates = [saturday_two_weeks_ago]

    discovered_ids = discover_flight_ids(config.TOTAL_FLIGHTS_TO_PROCESS, target_dates)
    if not discovered_ids:
        logging.critical("No flights were discovered. Halting.")
        return

    run_timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    run_output_dir = config.BASE_OUTPUT_DIR / f"run_{run_timestamp}"
    run_output_dir.mkdir(parents=True, exist_ok=True)

    output_file = run_output_dir / "discovered_ids.json"
    with open(output_file, "w") as f:
        json.dump(discovered_ids, f, indent=2)

    logging.info(
        f"✅ Descubrimiento completo. Se guardaron {len(discovered_ids)} IDs en {output_file}"
    )
    logging.info(
        f"-> Ahora puedes ejecutar 'get_summaries.py' y 'get_positions.py' apuntando a la carpeta: {run_output_dir}"
    )


if __name__ == "__main__":
    main()
