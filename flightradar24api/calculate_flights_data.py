import os
import time
import json
import requests
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from dotenv import load_dotenv
from geopy.distance import geodesic
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- Logging config ---
log_dir = Path("logs")
log_dir.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_dir / "logs.log"),
        logging.StreamHandler(),
    ]
)

# --- Load environment and config ---
load_dotenv()
API_KEY = os.getenv("PROD_FR24_API_KEY")
if not API_KEY:
    logging.critical("PROD_FR24_API_KEY no está definida en .env. Por favor, define esta variable de entorno.")
    raise RuntimeError("Define PROD_FR24_API_KEY en .env")

HEADERS = {
    "Accept": "application/json",
    "Accept-Version": "v1",
    "Authorization": f"Bearer {API_KEY}"
}

SUMMARY_URL = "https://fr24api.flightradar24.com/api/flight-summary/full"
POSITIONS_URL = "https://fr24api.flightradar24.com/api/historic/flight-positions/full"

# Define bounds for the contiguous United States
USA_BOUNDS = "49.38,24.52,-124.77,-66.95" # North, South, West, East

try:
    with open('data/fuel_profiles.json', 'r') as f:
        FUEL_PROFILES = json.load(f)
except FileNotFoundError:
    logging.critical("No se encontró 'data/fuel_profiles.json'. Asegúrate de que el archivo existe.")
    raise
except json.JSONDecodeError:
    logging.critical("Error al decodificar 'data/fuel_profiles.json'. Asegúrate de que es un JSON válido.")
    raise

DAYS = 1
INTERVAL_MINUTES = 240


def calculate_distance(coords):
    """Calcula la distancia total recorrida a partir de una lista de coordenadas."""
    if not coords or len(coords) < 2:
        return 0
    try:
        distance = round(sum(geodesic(coords[i], coords[i+1]).km for i in range(len(coords)-1)), 2)
        logging.debug(f"Distancia calculada: {distance} km")
        return distance
    except Exception as e:
        logging.error(f"Error al calcular la distancia: {e}")
        return 0

def detect_phases(points, vr_thr=3, low_alt=500):
    """Detecta las fases de vuelo (despegue, ascenso, crucero, descenso, aterrizaje) y calcula sus duraciones."""
    durations = {"takeoff": 0, "climb": 0, "cruise": 0, "descent": 0, "landing": 0}
    if not points:
        logging.warning("No hay puntos para detectar fases de vuelo.")
        return durations

    for i in range(len(points) - 1):
        p0, p1 = points[i], points[i+1]
        dt = p1["timestamp"] - p0["timestamp"]
        vr = p0.get("vertical_rate", 0)
        alt = p0.get("altitude", 0)
        spd = p0.get("ground_speed", 0)

        if spd > 30 and alt < low_alt and vr > 1:
            ph = "takeoff"
        elif vr > vr_thr:
            ph = "climb"
        elif vr < -vr_thr:
            ph = "descent"
        elif alt < low_alt and spd < 50 and vr < 1:
            ph = "landing"
        else:
            ph = "cruise"
        durations[ph] += dt
    logging.debug(f"Fases de vuelo detectadas: {durations}")
    return durations

def estimate_fuel(durations, model="default"):
    """Estima el consumo de combustible por fase de vuelo."""
    fuel_rates = FUEL_PROFILES.get(model, FUEL_PROFILES.get("default"))
    if not fuel_rates:
        logging.warning(f"No se encontraron perfiles de combustible para el modelo '{model}'. Usando 'default'.")
        fuel_rates = FUEL_PROFILES.get("default")
        if not fuel_rates:
            logging.error("No se encontró el perfil de combustible 'default'.")
            return {ph: 0 for ph in durations}

    estimated_fuel = {
        ph: round((durations[ph] / 3600) * fuel_rates.get(ph, 0), 2)
        for ph in durations
    }
    logging.debug(f"Combustible estimado: {estimated_fuel}")
    return estimated_fuel

def estimate_co2_by_passenger(fuel_kg, model="default"):
    """Estima las emisiones de CO2 por pasajero."""
    co2_total = sum(fuel_kg[ph] * 3.16 for ph in fuel_kg)
    seats = FUEL_PROFILES.get(model, FUEL_PROFILES["default"]).get("seats", 150)
    if not seats:
        logging.warning(f"Número de asientos no definido para el modelo '{model}'. Usando 150 por defecto.")
        seats = 150
    co2_per_passenger = round(co2_total / seats, 2)
    logging.debug(f"CO2 total: {co2_total} kg, CO2 por pasajero: {co2_per_passenger} kg")
    return co2_per_passenger

def collect_flight_ids_for_day(day_start: datetime, interval_minutes: int, run_output_dir: Path):
    """
    Recolecta IDs de vuelos, callsigns/flight numbers, y puntos de posición
    para un día específico a intervalos definidos.
    """
    all_flight_info = defaultdict(lambda: {'positions': [], 'callsign_or_flight': None})
    iterations = int((24 * 60) / interval_minutes)
    interval = timedelta(minutes=interval_minutes)
    logging.info(f"Comenzando la recolección de IDs, callsigns/flight numbers y posiciones detalladas para el día: {day_start.strftime('%Y-%m-%d')} en la región de EE.UU.")

    raw_data_dir = run_output_dir / "raw_snapshots" / day_start.strftime("%Y%m%d")
    raw_data_dir.mkdir(parents=True, exist_ok=True)
    logging.info(f"Directorio para datos raw del día {day_start.strftime('%Y-%m-%d')}: {raw_data_dir}")

    for i in range(iterations):
        ts = int((day_start + i * interval).timestamp())
        timestamp_utc_dt = datetime.fromtimestamp(ts, timezone.utc)
        timestamp_str = timestamp_utc_dt.strftime('%Y%m%d_%H%M%S_UTC')
        logging.info(f"🛰 Solicitando instantánea en {timestamp_utc_dt.strftime('%Y-%m-%d %H:%M:%S UTC')} para EE.UU. ({USA_BOUNDS})")

        for attempt in range(1, 6):
            try:
                r = requests.get(POSITIONS_URL, headers=HEADERS, params={
                    "bounds": USA_BOUNDS,
                    "timestamp": ts
                })
                if r.status_code == 429:
                    logging.warning(f"⚠️ Demasiadas solicitudes (429) en el intento {attempt}. Esperando 10 segundos...")
                    time.sleep(10)
                    continue
                if r.status_code == 400:
                    logging.error(f"❌ Error 400 Bad Request en el timestamp {ts} (Intento {attempt}/5): {r.text}. Posiblemente data no disponible para este timestamp o fuera del rango de la clave API.")
                    time.sleep(5)
                    continue
                r.raise_for_status()

                data = r.json()

                flights_in_snapshot = data.get("positions") or data.get("data") or []

                if flights_in_snapshot:
                    raw_file_path = raw_data_dir / f"snapshot_{timestamp_str}.json"
                    with open(raw_file_path, "w", encoding="utf-8") as f:
                        json.dump(data, f, indent=2)

                snapshot_fr24_ids = set()

                for flight_data in flights_in_snapshot:
                    fr24_id = flight_data.get("fr24_id")
                    if fr24_id:
                        snapshot_fr24_ids.add(fr24_id)
                        callsign_or_flight = flight_data.get("callsign") or flight_data.get("flight")
                        if callsign_or_flight and all_flight_info[fr24_id]['callsign_or_flight'] is None:
                            all_flight_info[fr24_id]['callsign_or_flight'] = callsign_or_flight

                        current_lat = flight_data.get("lat")
                        current_lon = flight_data.get("lon")
                        current_alt = flight_data.get("alt", 0)
                        current_gspeed = flight_data.get("gspeed", 0)
                        current_vspeed = flight_data.get("vspeed", 0)
                        timestamp_str_api = flight_data.get("timestamp")
                        
                        current_timestamp = None
                        if timestamp_str_api:
                            try:
                                dt_object = datetime.fromisoformat(timestamp_str_api.replace('Z', '+00:00'))
                                current_timestamp = int(dt_object.timestamp())
                            except ValueError as e:
                                logging.warning(f"No se pudo parsear el timestamp '{timestamp_str_api}' para FR24 ID {fr24_id}: {e}")

                        if current_lat is not None and current_lon is not None and current_timestamp is not None:
                            position_point = {
                                "timestamp": current_timestamp, "latitude": current_lat, "longitude": current_lon,
                                "vertical_rate": current_vspeed, "altitude": current_alt, "ground_speed": current_gspeed
                            }
                            all_flight_info[fr24_id]['positions'].append(position_point)

                logging.info(f"✅ Encontrados {len(snapshot_fr24_ids)} IDs de vuelo en la instantánea.")
                break
            except requests.exceptions.RequestException as e:
                logging.error(f"Error de red o HTTP al obtener vuelos en el timestamp {ts} (Intento {attempt}/5): {e}")
                time.sleep(5)
            except Exception as e:
                logging.error(f"Error inesperado al obtener vuelos en el timestamp {ts} (Intento {attempt}/5): {e}")
                time.sleep(5)
        else:
            logging.error(f"❌ Fallaron todos los intentos para el timestamp {ts}.")
        time.sleep(1)

    for fr24_id in all_flight_info:
        all_flight_info[fr24_id]['positions'].sort(key=lambda p: p["timestamp"])

    all_flight_info_filtered = {k: v for k, v in all_flight_info.items() if v['callsign_or_flight']}
    logging.info(f"Después de filtrar, {len(all_flight_info_filtered)} vuelos tienen callsign/flight y serán considerados para resumen.")
    return all_flight_info_filtered

# --- OPTIMIZED METHOD WITH RETRY LOGIC ---
def fetch_summaries_from_ids(flight_info_map, day_start, day_end):
    """
    Obtiene resúmenes de vuelos de forma concurrente y reintenta los lotes fallidos.
    """
    def fetch_single_batch(batch_of_fr24_ids):
        """Worker function to fetch summaries for a single batch of IDs."""
        batch_callsigns = [flight_info_map[fid]['callsign_or_flight'] for fid in batch_of_fr24_ids]
        batch_callsigns_str = ",".join(batch_callsigns)
        summary_datetime_from = day_start - timedelta(hours=12)
        summary_datetime_to = day_end + timedelta(hours=12)
        params = {
            "flights": batch_callsigns_str,
            "flight_datetime_from": summary_datetime_from.isoformat(timespec='seconds'),
            "flight_datetime_to": summary_datetime_to.isoformat(timespec='seconds'),
            "limit": 100,
        }
        max_retries = 3
        for attempt in range(1, max_retries + 1):
            try:
                r = requests.get(SUMMARY_URL, headers=HEADERS, params=params)
                if r.status_code == 429:
                    wait_time = 5 * (2 ** (attempt - 1))
                    logging.warning(f"⚠️ Demasiadas solicitudes (429) para lote '{batch_callsigns[0]}...'. Esperando {wait_time:.1f}s.")
                    time.sleep(wait_time)
                    continue
                r.raise_for_status()
                response_json = r.json()
                summaries = response_json.get("data", []) if isinstance(response_json, dict) else response_json if isinstance(response_json, list) else []
                return response_json, summaries, []
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 400: break
            except Exception: pass
        return None, [], batch_of_fr24_ids

    # --- Main logic with retry loop ---
    all_processed_summaries = []
    all_raw_summary_responses = []
    ids_to_process = list(flight_info_map.keys())
    
    # Initial attempt + 3 retries
    for attempt in range(1, 5):
        if not ids_to_process:
            break
            
        logging.info(f"🚀 Iniciando obtención de resúmenes. Intento {attempt}/4 para {len(ids_to_process)} IDs.")
        
        batches = [ids_to_process[i:i + 15] for i in range(0, len(ids_to_process), 15)]
        currently_failed_ids = []
        
        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_batch = {executor.submit(fetch_single_batch, batch): batch for batch in batches}
            for future in as_completed(future_to_batch):
                try:
                    raw_response, summaries, failed_ids = future.result()
                    if raw_response: all_raw_summary_responses.append(raw_response)
                    if summaries: all_processed_summaries.extend(summaries)
                    if failed_ids: currently_failed_ids.extend(failed_ids)
                except Exception as e:
                    batch = future_to_batch[future]
                    logging.critical(f"❌ Fallo crítico al procesar el futuro para el lote {batch}: {e}", exc_info=True)
                    currently_failed_ids.extend(batch)
        
        ids_to_process = currently_failed_ids
        if ids_to_process:
            logging.warning(f"Fin del intento {attempt}. Quedan {len(ids_to_process)} IDs fallidos. Reintentando...")

    logging.info(f"✅ Finalizada la obtención de resúmenes. Total obtenidos: {len(all_processed_summaries)}.")
    if ids_to_process:
        logging.error(f"⚠️ No se pudieron obtener resúmenes para {len(ids_to_process)} IDs después de todos los reintentos.")

    return all_processed_summaries, all_raw_summary_responses, ids_to_process


def process_day(day_start: datetime, run_output_dir: Path):
    """Procesa los datos de vuelos para un día completo, usando la nueva estructura de carpetas."""
    date_str = day_start.strftime("%Y%m%d")
    day_end = day_start + timedelta(days=1)
    logging.info(f"Comenzando el procesamiento de datos para el día: {day_start.strftime('%Y-%m-%d')}")

    # Paso 1: Recolectar IDs
    accumulated_flight_data = collect_flight_ids_for_day(day_start, INTERVAL_MINUTES, run_output_dir)
    if not accumulated_flight_data:
        logging.warning(f"No se encontraron vuelos para procesar el día {date_str}.")
        return

    # Paso 2: Obtener resúmenes
    summaries, all_raw_summary_responses, failed_ids = fetch_summaries_from_ids(accumulated_flight_data, day_start, day_end)
    summary_map = {s.get("fr24_id"): s for s in summaries if s.get("fr24_id")}
    
    # --- Guardar archivos en la nueva estructura de carpetas ---
    raw_summaries_dir = run_output_dir / "raw_summaries" / day_start.strftime("%Y%m%d")
    summaries_dir = run_output_dir / "summaries"
    processed_dir = run_output_dir / "processed"
    detailed_paths_dir = run_output_dir / "detailed_paths"
    for d in [raw_summaries_dir, summaries_dir, processed_dir, detailed_paths_dir]:
        d.mkdir(parents=True, exist_ok=True)

    if all_raw_summary_responses:
        consolidated_raw_summary_file = raw_summaries_dir / f"all_raw_summaries_{date_str}.json"
        with open(consolidated_raw_summary_file, "w", encoding="utf-8") as f:
            json.dump(all_raw_summary_responses, f, indent=2)
    
    summary_file_path = summaries_dir / f"flights_summary_{date_str}.json"
    with open(summary_file_path, "w", encoding="utf-8") as f:
        json.dump(summaries, f, indent=2)
    logging.info(f"Resúmenes de vuelos guardados en: {summary_file_path}")

    # --- Procesar y enriquecer cada vuelo ---
    processed_flights = []
    for fid, flight_data in accumulated_flight_data.items():
        if fid in failed_ids:
            continue

        pts = flight_data.get('positions', [])
        if not pts:
            continue

        s = summary_map.get(fid, {})
        callsign_or_flight = flight_data['callsign_or_flight']
        
        coords = [(p["latitude"], p["longitude"]) for p in pts if p.get("latitude") is not None]
        dist = calculate_distance(coords)
        durs = detect_phases(pts)
        
        model_for_fuel = s.get("type") or s.get("aircraft", {}).get("model") or "default"
        fuel = estimate_fuel(durs, model_for_fuel)
        co2_by_phase = {ph: round(fuel[ph] * 3.16, 2) for ph in fuel}
        
        rec = {
            "fr24_id": fid, "flight": s.get("flight"), "callsign": s.get("callsign") or callsign_or_flight,
            "aircraft_model": model_for_fuel, "aircraft_reg": s.get("reg"), "departure": s.get("orig_icao"),
            "arrival": s.get("dest_icao"), "distance_km": dist, "phase_durations_s": durs,
            "fuel_estimated_kg": fuel, "co2_estimated_kg": co2_by_phase,
            "co2_total_kg": round(sum(co2_by_phase.values()), 2),
            "co2_per_passenger_kg": estimate_co2_by_passenger(fuel, model_for_fuel),
            "raw_flight_path_points": pts, "circle_distance": s.get("circle_distance"),
        }
        processed_flights.append(rec)

        flight_detail_file_path = detailed_paths_dir / f"{fid}_detailed_path_{date_str}.json"
        with open(flight_detail_file_path, "w", encoding="utf-8") as f:
            json.dump(pts, f, indent=2)

    processed_file_path = processed_dir / f"flights_processed_{date_str}.json"
    with open(processed_file_path, "w", encoding="utf-8") as f:
        json.dump(processed_flights, f, indent=2)
    logging.info(f"Datos de vuelos procesados guardados en: {processed_file_path}")
    logging.info(f"✅ Finalizado el procesamiento de {len(processed_flights)} vuelos para {date_str}.")

if __name__ == "__main__":
    # --- Create a unique output directory for this script run ---
    run_timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    base_output_dir = Path("data/flights")
    run_output_dir = base_output_dir / f"run_{run_timestamp}"
    run_output_dir.mkdir(parents=True, exist_ok=True)
    logging.info(f"📂 Directorio de salida para esta ejecución: {run_output_dir}")

    today_utc_midnight = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    days_to_offset_from_today = 1
    start_processing_day = today_utc_midnight - timedelta(days=days_to_offset_from_today)

    logging.info("Iniciando el script de recolección y procesamiento de datos de FlightRadar24.")
    for day_offset in range(DAYS):
        day = start_processing_day - timedelta(days=day_offset)
        logging.info(f"\n--- 📅 Iniciando procesamiento para el día: {day.strftime('%Y-%m-%d')} ---")
        try:
            # Pass the unique run directory to the processing function
            process_day(day, run_output_dir)
        except Exception as e:
            logging.critical(f"❌ Error crítico al procesar el día {day.strftime('%Y-%m-%d')}: {e}", exc_info=True)
    logging.info("Procesamiento de datos de FlightRadar24 finalizado.")