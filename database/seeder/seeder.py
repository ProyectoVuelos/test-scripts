import json
import logging
import psycopg2
from psycopg2.extras import execute_values
from pathlib import Path

# --- Logging Config ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

# --- Configuration ---
# 1. Database Connection Details (Update with your settings)
DB_CONFIG = {
    "host": "localhost",
    "port": "5432",
    "user": "listerineh",
    "password": "listerineh-test",
    "dbname": "flights-db"
}

# 2. Base path containing all your run folders
BASE_DATA_DIR = Path("data/flights")

# 3. Choose your schema type (uncomment the one you are using)
# USE_POSTGIS = True
USE_POSTGIS = False


def get_all_processed_files(base_directory: Path):
    """Finds all 'flights_processed_*.json' files across all 'run_*/processed' subdirectories."""
    if not base_directory.is_dir():
        logging.error(f"Base data directory not found: {base_directory}")
        return []

    # Use glob to find all matching files recursively from the base directory
    search_pattern = "run_*/processed/flights_processed_*.json"
    files = list(base_directory.glob(search_pattern))

    logging.info(f"Found {len(files)} processed JSON files across all run folders.")
    return files

def seed_database():
    """Reads all processed JSON files and inserts data into the PostgreSQL database."""
    json_files = get_all_processed_files(BASE_DATA_DIR)
    if not json_files:
        logging.warning("No processed JSON files found to seed.")
        return

    conn = None
    try:
        logging.info("Connecting to the PostgreSQL database...")
        conn = psycopg2.connect(**DB_CONFIG)
    except psycopg2.OperationalError as e:
        logging.critical(f"❌ Could not connect to database: {e}")
        return

    for json_file in json_files:
        logging.info(f"--- Processing file: {json_file} ---")
        with open(json_file, 'r', encoding='utf-8') as f:
            flights_data = json.load(f)

        with conn.cursor() as cur:
            for flight in flights_data:
                # --- Step 1: Insert or Update the main flight record ---
                try:
                    cur.execute(
                        """
                        INSERT INTO flights (
                            fr24_id, flight, callsign, aircraft_model, aircraft_reg,
                            departure, arrival, distance_km, circle_distance,
                            duration_takeoff_s, duration_climb_s, duration_cruise_s, duration_descent_s, duration_landing_s,
                            fuel_takeoff_kg, fuel_climb_kg, fuel_cruise_kg, fuel_descent_kg, fuel_landing_kg,
                            co2_takeoff_kg, co2_climb_kg, co2_cruise_kg, co2_descent_kg, co2_landing_kg,
                            co2_total_kg, co2_per_passenger_kg
                        ) VALUES (
                            %(fr24_id)s, %(flight)s, %(callsign)s, %(aircraft_model)s, %(aircraft_reg)s,
                            %(departure)s, %(arrival)s, %(distance_km)s, %(circle_distance)s,
                            %(duration_takeoff)s, %(duration_climb)s, %(duration_cruise)s, %(duration_descent)s, %(duration_landing)s,
                            %(fuel_takeoff)s, %(fuel_climb)s, %(fuel_cruise)s, %(fuel_descent)s, %(fuel_landing)s,
                            %(co2_takeoff)s, %(co2_climb)s, %(co2_cruise)s, %(co2_descent)s, %(co2_landing)s,
                            %(co2_total)s, %(co2_per_passenger)s
                        )
                        ON CONFLICT (fr24_id) DO UPDATE SET
                            flight = EXCLUDED.flight,
                            callsign = EXCLUDED.callsign,
                            distance_km = EXCLUDED.distance_km,
                            co2_total_kg = EXCLUDED.co2_total_kg
                        RETURNING flight_id;
                        """,
                        {
                            "fr24_id": flight['fr24_id'], "flight": flight.get('flight'), "callsign": flight.get('callsign'),
                            "aircraft_model": flight.get('aircraft_model'), "aircraft_reg": flight.get('aircraft_reg'),
                            "departure": flight.get('departure'), "arrival": flight.get('arrival'),
                            "distance_km": flight.get('distance_km'), "circle_distance": flight.get('circle_distance'),
                            "duration_takeoff": flight['phase_durations_s']['takeoff'], "duration_climb": flight['phase_durations_s']['climb'],
                            "duration_cruise": flight['phase_durations_s']['cruise'], "duration_descent": flight['phase_durations_s']['descent'],
                            "duration_landing": flight['phase_durations_s']['landing'],
                            "fuel_takeoff": flight['fuel_estimated_kg']['takeoff'], "fuel_climb": flight['fuel_estimated_kg']['climb'],
                            "fuel_cruise": flight['fuel_estimated_kg']['cruise'], "fuel_descent": flight['fuel_estimated_kg']['descent'],
                            "fuel_landing": flight['fuel_estimated_kg']['landing'],
                            "co2_takeoff": flight['co2_estimated_kg']['takeoff'], "co2_climb": flight['co2_estimated_kg']['climb'],
                            "co2_cruise": flight['co2_estimated_kg']['cruise'], "co2_descent": flight['co2_estimated_kg']['descent'],
                            "co2_landing": flight['co2_estimated_kg']['landing'],
                            "co2_total": flight.get('co2_total_kg'), "co2_per_passenger": flight.get('co2_per_passenger_kg')
                        }
                    )
                    flight_id = cur.fetchone()[0]

                    # --- Step 2: Clear old positions and insert new ones ---
                    cur.execute("DELETE FROM flight_positions WHERE flight_id = %s;", (flight_id,))
                    
                    positions = flight.get('raw_flight_path_points', [])
                    if not positions:
                        continue

                    # --- Batch insert positions for performance ---
                    if USE_POSTGIS:
                        # Prepare data for PostGIS insert (lon, lat)
                        position_data = [
                            (
                                flight_id, pos['timestamp'], pos['altitude'], pos['ground_speed'], pos['vertical_rate'],
                                pos['longitude'], pos['latitude']
                            ) for pos in positions
                        ]
                        execute_values(
                            cur,
                            """
                            INSERT INTO flight_positions (
                                flight_id, "timestamp", altitude, ground_speed, vertical_rate, location
                            ) VALUES %s;
                            """,
                            [
                                (
                                    p[0], psycopg2.TimestampFromTicks(p[1]), p[2], p[3], p[4],
                                    psycopg2.sql.SQL("ST_SetSRID(ST_MakePoint(%s, %s), 4326)"), (p[5], p[6])
                                ) for p in position_data
                            ],
                            template="(%s, %s, %s, %s, %s, %s)", # The template must match the values
                            page_size=200
                        )
                    else:
                        # Prepare data for standard lat/lon columns
                        position_data = [
                            (
                                flight_id, psycopg2.TimestampFromTicks(pos['timestamp']), pos['latitude'], pos['longitude'],
                                pos['altitude'], pos['ground_speed'], pos['vertical_rate']
                            ) for pos in positions
                        ]
                        execute_values(
                            cur,
                            """
                            INSERT INTO flight_positions (
                                flight_id, "timestamp", latitude, longitude, altitude, ground_speed, vertical_rate
                            ) VALUES %s;
                            """,
                            position_data,
                            page_size=200
                        )
                    
                    logging.debug(f"Upserted flight {flight['fr24_id']} (ID: {flight_id}) with {len(positions)} position points.")

                except (Exception, psycopg2.Error) as error:
                    logging.error(f"Error processing flight {flight.get('fr24_id')}: {error}")
                    conn.rollback() # Rollback the transaction for this single flight

    # Commit all successful transactions and close the connection
    if conn:
        conn.commit()
        conn.close()
        logging.info("--- Seeding complete. Database connection closed. ---")

if __name__ == "__main__":
    seed_database()
