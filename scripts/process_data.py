import json
import logging
import argparse
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional
from geopy.distance import geodesic
from tqdm import tqdm
import config


def get_flight_time(summary_time_obj, time_type):
    """Safely extracts a timestamp, prioritizing real, then estimated, then scheduled times."""
    if not summary_time_obj:
        return None

    for category in ["real", "estimated", "scheduled"]:
        time_obj = summary_time_obj.get(category)
        if time_obj and time_obj.get(time_type):
            return time_obj.get(time_type)

    return None


def calculate_distance(coords):
    """Calculates the total distance from a list of coordinates."""
    if len(coords) < 2:
        return 0
    return round(
        sum(geodesic(coords[i], coords[i + 1]).km for i in range(len(coords) - 1)), 2
    )


def detect_phases(points, vr_thr=3, low_alt=500):
    """Detects flight phases and calculates their durations."""
    durations = {"takeoff": 0, "climb": 0, "cruise": 0, "descent": 0, "landing": 0}
    if not points:
        return durations
    for i in range(len(points) - 1):
        p0, p1 = points[i], points[i + 1]
        dt = p1["timestamp"] - p0["timestamp"]
        vr = p0.get("vertical_rate", 0)
        alt = p0.get("altitude", 0)
        spd = p0.get("ground_speed", 0)

        # Specific conditions (takeoff/landing) are checked first.
        if spd > 30 and alt < low_alt and vr > 1:
            ph = "takeoff"
        elif alt < low_alt and spd < 50 and vr < 1:
            ph = "landing"
        # General conditions (climb/descent) are checked next.
        elif vr > vr_thr:
            ph = "climb"
        elif vr < -vr_thr:
            ph = "descent"
        # Default case is cruise.
        else:
            ph = "cruise"

        durations[ph] += dt
    return durations


def estimate_fuel(durations, model="default"):
    """Estimates fuel consumption by phase."""
    fuel_rates = config.FUEL_PROFILES.get(model, config.FUEL_PROFILES["default"])
    return {
        ph: round((durations[ph] / 3600) * fuel_rates.get(ph, 0), 2) for ph in durations
    }


def estimate_co2_by_passenger(fuel_kg, model="default"):
    """Estimates CO2 emissions per passenger."""
    co2_total = sum(fuel_kg.values()) * 3.16
    seats = config.FUEL_PROFILES.get(model, config.FUEL_PROFILES["default"]).get(
        "seats", 150
    )
    return round(co2_total / seats, 2)


def process_run_data(run_dir: Path, target_flight_id: Optional[str] = None):
    """
    Processes raw data. If target_flight_id is given, only processes that flight.
    """
    logging.info(f"Processing run directory: {run_dir}")
    if target_flight_id:
        logging.warning(f"Targeted processing for flight ID: {target_flight_id}")

    try:
        details_map_file = next(run_dir.glob("flight_details_map_*.json"))
        summary_file = next((run_dir / "summaries").glob("flights_summary_*.json"))
        date_str = details_map_file.stem.split("_")[-1]
    except StopIteration:
        logging.error(f"Could not find required data files in {run_dir}.")
        return

    with open(details_map_file, "r") as f:
        flight_details_map = json.load(f)
    with open(summary_file, "r") as f:
        summaries = json.load(f)
    summary_map = {s.get("fr24_id"): s for s in summaries}

    processed_dir = run_dir / "processed"
    processed_dir.mkdir(exist_ok=True)
    detailed_paths_dir = run_dir / "detailed_paths"
    detailed_paths_dir.mkdir(exist_ok=True)
    processed_file_path = processed_dir / f"flights_processed_{date_str}.json"

    if processed_file_path.exists() and target_flight_id:
        with open(processed_file_path, "r") as f:
            processed_flights_map = {
                flight["fr24_id"]: flight for flight in json.load(f)
            }
    else:
        processed_flights_map = {}

    flights_to_process = (
        {k: v for k, v in flight_details_map.items() if k == target_flight_id}
        if target_flight_id
        else flight_details_map
    )
    if not flights_to_process:
        logging.error(f"Flight ID {target_flight_id} not found in data.")
        return

    pbar = tqdm(flights_to_process.items(), desc="⚙️  Processing Flights")
    for fid, flight_data in pbar:
        pbar.set_postfix_str(f"ID: {fid}")
        pts = flight_data.get("positions", [])
        s = summary_map.get(fid, {})

        departure_ts = get_flight_time(s.get("time"), "departure")
        arrival_ts = get_flight_time(s.get("time"), "arrival")
        flight_duration = (
            (arrival_ts - departure_ts) if departure_ts and arrival_ts else None
        )

        gcd_obj = s.get("great_circle_distance")

        coords = [
            (p["latitude"], p["longitude"])
            for p in pts
            if p.get("latitude") is not None
        ]
        dist_calculated = calculate_distance(coords)
        durs = detect_phases(pts)
        model_for_fuel = s.get("type") or s.get("aircraft", {}).get("model", "default")
        fuel = estimate_fuel(durs, model_for_fuel)
        co2_by_phase = {ph: round(fuel[ph] * 3.16, 2) for ph in fuel}

        rec = {
            "fr24_id": fid,
            "flight": s.get("flight"),
            "callsign": s.get("callsign", flight_data["callsign_or_flight"]),
            "aircraft_model": model_for_fuel,
            "aircraft_reg": s.get("reg"),
            "departure_icao": s.get("origin", {}).get("icao"),
            "arrival_icao": s.get("destination", {}).get("icao"),
            "departure_time_utc": (
                datetime.fromtimestamp(departure_ts, tz=timezone.utc).isoformat()
                if departure_ts
                else None
            ),
            "arrival_time_utc": (
                datetime.fromtimestamp(arrival_ts, tz=timezone.utc).isoformat()
                if arrival_ts
                else None
            ),
            "flight_duration_s": flight_duration,
            "distance_calculated_km": dist_calculated,
            "great_circle_distance_km": gcd_obj.get("km") if gcd_obj else None,
            "phase_durations_s": durs,
            "fuel_estimated_kg": fuel,
            "co2_estimated_kg": co2_by_phase,
            "co2_total_kg": round(sum(co2_by_phase.values()), 2),
            "co2_per_passenger_kg": estimate_co2_by_passenger(fuel, model_for_fuel),
        }
        processed_flights_map[fid] = rec

        with open(
            detailed_paths_dir / f"{fid}_detailed_path_{date_str}.json", "w"
        ) as f:
            json.dump(pts, f, indent=2)

    with open(processed_file_path, "w") as f:
        json.dump(list(processed_flights_map.values()), f, indent=2)

    logging.info(
        f"✅ Processing complete. Saved {len(processed_flights_map)} flights to {processed_file_path}"
    )


def main():
    """Main function to run the data processing script."""
    config.setup_logging()

    parser = argparse.ArgumentParser(
        description="Process raw flight data into final calculated results."
    )
    parser.add_argument(
        "run_directory",
        nargs="?",
        help="Path to a specific run directory. Uses latest if not provided.",
    )
    parser.add_argument("-f", "--flight-id", help="Process only a single flight ID.")
    args = parser.parse_args()

    if args.run_directory:
        run_dir = Path(args.run_directory)
    else:
        try:
            run_dir = max(
                d
                for d in config.BASE_OUTPUT_DIR.iterdir()
                if d.is_dir() and d.name.startswith("run_")
            )
            logging.info(
                f"No directory specified. Using the latest run: {run_dir.name}"
            )
        except ValueError:
            logging.error(
                f"No run directories found in {config.BASE_OUTPUT_DIR}. Run acquire_data.py first."
            )
            return

    if not run_dir.exists():
        logging.error(f"Directory not found: {run_dir}")
        return

    process_run_data(run_dir, args.flight_id)


if __name__ == "__main__":
    main()
