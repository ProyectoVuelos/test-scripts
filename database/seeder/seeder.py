import os
import json
import logging
import argparse
import psycopg2
from psycopg2.extras import execute_values
from pathlib import Path
from dotenv import load_dotenv

# --- Load Environment Variables ---
load_dotenv()

# --- Logging Config ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)

# --- Configuration ---
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT", "5432"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "dbname": os.getenv("DB_NAME"),
}

if not all(DB_CONFIG.values()):
    raise RuntimeError(
        "One or more database environment variables are missing from your .env file."
    )

BASE_DATA_DIR = Path("data/flights")


def get_all_processed_files(base_directory: Path):
    """Finds all 'flights_processed_*.json' files across all run directories."""
    if not base_directory.is_dir():
        logging.error(f"Base data directory not found: {base_directory}")
        return []

    search_pattern = "run_*/processed/flights_processed_*.json"
    files = list(base_directory.glob(search_pattern))
    logging.info(f"Found {len(files)} processed JSON files.")
    return files


def seed_database(json_files_to_process):
    """Connects to the DB and seeds the data from the provided list of files."""
    try:
        logging.info("Connecting to the PostgreSQL database...")
        conn = psycopg2.connect(**DB_CONFIG)
        logging.info("✅ Database connection established.")
    except psycopg2.OperationalError as e:
        logging.critical(f"❌ Could not connect to database: {e}")
        return

    for json_file in json_files_to_process:
        logging.info(f"--- Processing file: {json_file} ---")
        run_dir = json_file.parents[
            1
        ]  # The parent of the 'processed' directory is the run directory

        try:
            with open(json_file, "r", encoding="utf-8") as f:
                flights_data = json.load(f)
            logging.info(f"📄 Loaded {len(flights_data)} flights from {json_file.name}")

            with conn.cursor() as cur:
                # Prepare data for bulk upsert
                flight_tuples = [
                    (
                        f["fr24_id"],
                        f.get("flight"),
                        f.get("callsign"),
                        f.get("aircraft_model"),
                        f.get("aircraft_reg"),
                        f.get("departure_icao"),
                        f.get("arrival_icao"),
                        f.get("departure_time_utc"),
                        f.get("arrival_time_utc"),
                        f.get("flight_duration_s"),
                        f.get("distance_calculated_km"),
                        f.get("great_circle_distance_km"),
                        f["phase_durations_s"]["takeoff"],
                        f["phase_durations_s"]["climb"],
                        f["phase_durations_s"]["cruise"],
                        f["phase_durations_s"]["descent"],
                        f["phase_durations_s"]["landing"],
                        f["fuel_estimated_kg"]["takeoff"],
                        f["fuel_estimated_kg"]["climb"],
                        f["fuel_estimated_kg"]["cruise"],
                        f["fuel_estimated_kg"]["descent"],
                        f["fuel_estimated_kg"]["landing"],
                        f["co2_estimated_kg"]["takeoff"],
                        f["co2_estimated_kg"]["climb"],
                        f["co2_estimated_kg"]["cruise"],
                        f["co2_estimated_kg"]["descent"],
                        f["co2_estimated_kg"]["landing"],
                        f.get("co2_total_kg"),
                        f.get("co2_per_passenger_kg"),
                    )
                    for f in flights_data
                ]

                # Bulk upsert flight data
                inserted_flights = execute_values(
                    cur,
                    """
                    INSERT INTO flights (
                        fr24_id, flight, callsign, aircraft_model, aircraft_reg,
                        departure_icao, arrival_icao, departure_time_utc, arrival_time_utc,
                        flight_duration_s, distance_calculated_km, great_circle_distance_km,
                        duration_takeoff_s, duration_climb_s, duration_cruise_s, duration_descent_s, duration_landing_s,
                        fuel_takeoff_kg, fuel_climb_kg, fuel_cruise_kg, fuel_descent_kg, fuel_landing_kg,
                        co2_takeoff_kg, co2_climb_kg, co2_cruise_kg, co2_descent_kg, co2_landing_kg,
                        co2_total_kg, co2_per_passenger_kg
                    ) VALUES %s
                    ON CONFLICT (fr24_id) DO UPDATE SET
                        flight = EXCLUDED.flight, callsign = EXCLUDED.callsign,
                        distance_calculated_km = EXCLUDED.distance_calculated_km,
                        co2_total_kg = EXCLUDED.co2_total_kg, last_updated = NOW()
                    RETURNING flight_id, fr24_id;
                    """,
                    flight_tuples,
                    fetch=True,
                )

                flight_id_map = {fr24_id: db_id for db_id, fr24_id in inserted_flights}
                logging.info(
                    f"✅ Bulk upsert complete. {len(flight_id_map)} flights processed."
                )

                # Insert positions for each upserted flight
                date_str = json_file.stem.split("_")[-1]
                detailed_paths_dir = run_dir / "detailed_paths"

                for fr24_id, flight_id in flight_id_map.items():
                    position_file = (
                        detailed_paths_dir / f"{fr24_id}_detailed_path_{date_str}.json"
                    )
                    if not position_file.exists():
                        continue

                    with open(position_file, "r") as f:
                        positions = json.load(f)
                    if not positions:
                        continue

                    cur.execute(
                        "DELETE FROM flight_positions WHERE flight_id = %s;",
                        (flight_id,),
                    )
                    position_data = [
                        (
                            flight_id,
                            psycopg2.TimestampFromTicks(pos["timestamp"]),
                            pos.get("latitude"),
                            pos.get("longitude"),
                            pos.get("altitude"),
                            pos.get("ground_speed"),
                            pos.get("vertical_rate"),
                        )
                        for pos in positions
                    ]
                    execute_values(
                        cur,
                        'INSERT INTO flight_positions (flight_id, "timestamp", latitude, longitude, altitude, ground_speed, vertical_rate) VALUES %s;',
                        position_data,
                        page_size=500,
                    )

            conn.commit()
            logging.info("✅ File processed and committed.")

        except (Exception, psycopg2.Error) as error:
            logging.error(
                f"❌ A critical error occurred while processing {json_file}: {error}"
            )
            if conn:
                conn.rollback()

    if conn:
        conn.close()
        logging.info("Seeding process finished. Database connection closed.")


def main():
    """Parses arguments and determines which files to seed."""
    parser = argparse.ArgumentParser(
        description="Seed the PostgreSQL database with processed flight data."
    )
    parser.add_argument(
        "--file", help="Path to a specific processed JSON file to seed."
    )
    args = parser.parse_args()

    files_to_seed = []
    if args.file:
        logging.info(f"Seeding specific file: {args.file}")
        target_file = Path(args.file)
        if not target_file.is_file():
            logging.error(f"File not found: {target_file}")
            return
        files_to_seed.append(target_file)
    else:
        logging.info("No specific file provided. Searching for all processed files...")
        files_to_seed = get_all_processed_files(BASE_DATA_DIR)

    if not files_to_seed:
        logging.warning("No files found to seed.")
        return

    seed_database(files_to_seed)


if __name__ == "__main__":
    main()
