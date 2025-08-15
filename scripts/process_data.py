import json
import logging
import argparse
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional
from geopy.distance import geodesic
from tqdm import tqdm
import config


def detect_phases(points, vr_thr=3, low_alt=500):
    durations = {"takeoff": 0, "climb": 0, "cruise": 0, "descent": 0, "landing": 0}
    if not points:
        return durations

    for p in points:
        if isinstance(p["timestamp"], str):
            try:
                p["timestamp"] = int(
                    datetime.fromisoformat(
                        p["timestamp"].replace("Z", "+00:00")
                    ).timestamp()
                )
            except (ValueError, TypeError):
                pass

    for i in range(len(points) - 1):
        p0, p1 = points[i], points[i + 1]
        dt = p1["timestamp"] - p0["timestamp"]
        vr = p0.get("vertical_rate", 0)
        alt = p0.get("altitude", 0)
        spd = p0.get("ground_speed", 0)

        if alt < low_alt and spd > 30 and vr > 1:
            ph = "takeoff"
        elif alt < low_alt and spd < 50 and vr < 1:
            ph = "landing"
        elif vr > vr_thr:
            ph = "climb"
        elif vr < -vr_thr:
            ph = "descent"
        else:
            ph = "cruise"
        durations[ph] += dt

    if (
        durations["takeoff"] == 0
        and durations["climb"] > 0
        and points[0].get("altitude", 0) < low_alt
    ):
        takeoff_duration = 180
        if durations["climb"] > takeoff_duration:
            durations["takeoff"] = takeoff_duration
            durations["climb"] -= takeoff_duration
        else:
            durations["takeoff"] = durations["climb"]
            durations["climb"] = 0

    return durations


def calculate_distance(coords):
    if len(coords) < 2:
        return 0
    return round(
        sum(geodesic(coords[i], coords[i + 1]).km for i in range(len(coords) - 1)), 2
    )


def estimate_fuel(durations, model="default"):
    fuel_rates = config.FUEL_PROFILES.get(
        model, config.FUEL_PROFILES.get("default", {})
    )
    return {
        ph: round((durations[ph] / 3600) * fuel_rates.get(ph, 0), 2) for ph in durations
    }


def estimate_co2_by_passenger(fuel_kg, model="default"):
    co2_total = sum(fuel_kg.values()) * 3.16
    seats = config.FUEL_PROFILES.get(
        model, config.FUEL_PROFILES.get("default", {})
    ).get("seats", 150)
    return round(co2_total / seats, 2)


def process_run_data(run_dir: Path, target_flight_id: Optional[str] = None):
    logging.info(f"Processing run directory: {run_dir}")

    try:
        details_map_file = next(run_dir.glob("flight_details_map_*.json"))
        summary_file = next(run_dir.glob("summaries/flights_summary_*.json"))
        date_str = details_map_file.stem.split("_")[-1]
    except StopIteration:
        logging.error(f"Could not find required data files in {run_dir}.")
        return

    with open(details_map_file, "r", encoding="utf-8") as f:
        flight_details_map = json.load(f)
    with open(summary_file, "r", encoding="utf-8") as f:
        summaries = json.load(f)
    summary_map = {s.get("fr24_id"): s for s in summaries}

    processed_dir = run_dir / "processed"
    processed_dir.mkdir(exist_ok=True)
    processed_file_path = processed_dir / f"flights_processed_{date_str}.json"

    flights_to_process = (
        {k: v for k, v in flight_details_map.items() if k == target_flight_id}
        if target_flight_id
        else flight_details_map
    )

    pbar = tqdm(flights_to_process.items(), desc="⚙️  Processing Flights")
    processed_flights = []

    for fid, flight_data in pbar:
        pbar.set_postfix_str(f"ID: {fid}")

        pts = flight_data.get("positions", [])
        if len(pts) < config.MINIMUM_DATA_POINTS:
            continue

        s = summary_map.get(fid, {})

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
            "callsign": s.get("callsign", flight_data.get("callsign_or_flight")),
            "aircraft_model": model_for_fuel,
            "aircraft_reg": s.get("reg"),
            "departure_icao": s.get("orig_icao"),
            "arrival_icao": s.get("dest_icao"),
            "departure_time_utc": s.get("datetime_takeoff"),
            "arrival_time_utc": s.get("datetime_landed"),
            "flight_duration_s": s.get("flight_time"),
            "distance_calculated_km": dist_calculated,
            "great_circle_distance_km": s.get("circle_distance"),
            "phase_durations_s": durs,
            "fuel_estimated_kg": fuel,
            "co2_estimated_kg": co2_by_phase,
            "co2_total_kg": round(sum(co2_by_phase.values()), 2),
            "co2_per_passenger_kg": estimate_co2_by_passenger(fuel, model_for_fuel),
        }
        processed_flights.append(rec)

    with open(processed_file_path, "w", encoding="utf-8") as f:
        json.dump(processed_flights, f, indent=2)

    logging.info(
        f"✅ Processing complete. Saved {len(processed_flights)} flights to {processed_file_path}"
    )


def main():
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

    run_dir = None
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
            logging.error(f"No run directories found in {config.BASE_OUTPUT_DIR}.")
            return

    if not run_dir.exists():
        logging.error(f"Directory not found: {run_dir}")
        return

    process_run_data(run_dir, args.flight_id)


if __name__ == "__main__":
    main()
