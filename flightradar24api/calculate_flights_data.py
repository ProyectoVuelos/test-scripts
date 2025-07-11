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

out_dir = Path("data/flights")
out_dir.mkdir(parents=True, exist_ok=True)
logging.info(f"Directorio de salida configurado en: {out_dir}")

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

def collect_flight_ids_for_day(day_start: datetime, interval_minutes: int):
    """
    Recolecta IDs de vuelos, callsigns/flight numbers, y puntos de posición
    para un día específico a intervalos definidos.

    Retorna:
        dict: Un diccionario donde las claves son fr24_id y los valores son
              diccionarios con 'positions' (lista de puntos de posición) y
              'callsign_or_flight' (el callsign o flight number).
    """
    all_flight_info = defaultdict(lambda: {'positions': [], 'callsign_or_flight': None})
    iterations = int((24 * 60) / interval_minutes)
    interval = timedelta(minutes=interval_minutes)
    logging.info(f"Comenzando la recolección de IDs, callsigns/flight numbers y posiciones detalladas para el día: {day_start.strftime('%Y-%m-%d')} en la región de EE.UU.")

    raw_data_dir = out_dir / "raw_snapshots" / day_start.strftime("%Y%m%d")
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
                
                # --- Guardar el JSON raw de la instantánea SOLO si hay datos ---
                if flights_in_snapshot:
                    raw_file_path = raw_data_dir / f"snapshot_{timestamp_str}.json"
                    try:
                        with open(raw_file_path, "w", encoding="utf-8") as f:
                            json.dump(data, f, indent=2)
                        logging.debug(f"Datos raw de la instantánea guardados en: {raw_file_path}")
                    except IOError as e:
                        logging.error(f"Error al guardar los datos raw de la instantánea {timestamp_str}: {e}")
                else:
                    logging.info(f"No se encontraron vuelos en la instantánea para {timestamp_str}. No se guardará el archivo raw.")
                # --- FIN Guardar Raw ---

                snapshot_fr24_ids = set() 

                for flight_data in flights_in_snapshot:
                    fr24_id = flight_data.get("fr24_id")
                    if fr24_id:
                        snapshot_fr24_ids.add(fr24_id) # Acumular solo los IDs
                        
                        # Obtener callsign o flight number
                        callsign_or_flight = flight_data.get("callsign") or flight_data.get("flight")
                        if callsign_or_flight:
                            # Solo actualizar si no lo tenemos ya (el primero que veamos es suficiente)
                            if all_flight_info[fr24_id]['callsign_or_flight'] is None:
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
                                 "timestamp": current_timestamp,
                                 "latitude": current_lat,
                                 "longitude": current_lon,
                                 "vertical_rate": current_vspeed,
                                 "altitude": current_alt,
                                 "ground_speed": current_gspeed
                             }
                             all_flight_info[fr24_id]['positions'].append(position_point)
                        else:
                            logging.debug(f"Puntos de posición incompletos o faltantes para FR24 ID: {fr24_id}. Lat: {current_lat}, Lon: {current_lon}, Ts: {current_timestamp}")

                logging.info(f"✅ Encontrados {len(snapshot_fr24_ids)} IDs de vuelo en la instantánea.")
                logging.debug(f"Puntos acumulados para esta instantánea hasta ahora: {sum(len(v['positions']) for v in all_flight_info.values())} puntos de {len(all_flight_info)} vuelos.")

                break
            except requests.exceptions.RequestException as e:
                logging.error(f"Error de red o HTTP al obtener vuelos en el timestamp {ts} (Intento {attempt}/5): {e}")
                time.sleep(5)
            except json.JSONDecodeError as e:
                logging.error(f"Error al decodificar JSON de la respuesta en el timestamp {ts} (Intento {attempt}/5): {e}")
                time.sleep(5)
            except Exception as e:
                logging.error(f"Error inesperado al obtener vuelos en el timestamp {ts} (Intento {attempt}/5): {e}")
                time.sleep(5)
        else:
            logging.error(f"❌ Fallaron todos los intentos para el timestamp {ts}. No se pudieron obtener los IDs de vuelo.")
        time.sleep(1)

    for fr24_id in all_flight_info:
        all_flight_info[fr24_id]['positions'].sort(key=lambda p: p["timestamp"])

    total_accumulated_points = sum(len(v['positions']) for v in all_flight_info.values())
    total_unique_fr24_ids = len(all_flight_info)
    logging.info(f"Se recolectaron un total de {total_unique_fr24_ids} IDs únicos y se acumularon {total_accumulated_points} puntos de posición.")
    
    # Filtra vuelos que no tienen callsign_or_flight, ya que son necesarios para el summary en Modo 2
    all_flight_info_filtered = {
        fr24_id: data for fr24_id, data in all_flight_info.items() if data['callsign_or_flight']
    }
    logging.info(f"Después de filtrar, {len(all_flight_info_filtered)} vuelos tienen callsign/flight y serán considerados para resumen.")
    return all_flight_info_filtered

def fetch_summaries_from_ids(flight_info_map, day_start, day_end):
    """Obtiene resúmenes de vuelos a partir de un mapa de IDs de vuelo y sus callsigns/flight numbers.
    Utiliza el Modo 2 de la API de resumen: fechas + filtro por callsigns.
    
    Retorna:
        tuple: (list de resúmenes de vuelos procesados, list de todos los JSON raw de respuesta)
    """
    all_processed_summaries = []
    all_raw_summary_responses = [] # Nueva lista para almacenar todas las respuestas raw
    
    fr24_ids_with_callsigns = list(flight_info_map.keys())

    if not fr24_ids_with_callsigns:
        logging.info("No hay IDs de vuelo con callsigns/flight numbers para obtener resúmenes.")
        return [], [] # Retorna listas vacías

    BATCH_SIZE = 15 # Máximo permitido para 'flights' o 'callsigns' en la API de summary

    logging.info(f"Comenzando a obtener resúmenes para {len(fr24_ids_with_callsigns)} vuelos (en lotes de {BATCH_SIZE} elementos, usando callsigns/flight numbers).")

    # Los directorios raw_summaries_dir se manejan en process_day para el guardado consolidado.
    # No necesitamos crearlo aquí.

    # Ajustar el rango de fechas para la búsqueda de resúmenes:
    # Ampliar a un día antes y un día después del día de interés
    summary_datetime_from = day_start - timedelta(hours=12) 
    summary_datetime_to = day_end + timedelta(hours=12)    

    logging.debug(f"🔍 Rango de fechas para resúmenes extendido: de {summary_datetime_from.isoformat()} a {summary_datetime_to.isoformat()}")


    for i in range(0, len(fr24_ids_with_callsigns), BATCH_SIZE):
        current_batch_fr24_ids = fr24_ids_with_callsigns[i:i + BATCH_SIZE]
        
        # Construir la lista de callsigns/flight numbers para el lote actual
        current_batch_callsigns_or_flights = [
            flight_info_map[fr24_id]['callsign_or_flight'] 
            for fr24_id in current_batch_fr24_ids 
            if flight_info_map[fr24_id]['callsign_or_flight']
        ]

        if not current_batch_callsigns_or_flights:
            logging.debug(f"Saltando lote {i} porque no hay callsigns/flight numbers válidos.")
            continue

        current_batch_callsigns_str = ",".join(current_batch_callsigns_or_flights)
        
        current_offset = 0
        max_retries = 5
        initial_wait_time = 5 # segundos
        
        while True: # Bucle de paginación
            for attempt in range(1, max_retries + 1):
                params = {
                    "flights": current_batch_callsigns_str, # Usamos 'flights' con callsigns/flight numbers
                    "flight_datetime_from": summary_datetime_from.isoformat(timespec='seconds'),
                    "flight_datetime_to": summary_datetime_to.isoformat(timespec='seconds'),
                    "limit": 100,
                    "offset": current_offset
                }
                logging.debug(f"🔗 Solicitando resúmenes para lote de callsigns '{current_batch_callsigns_or_flights[0]}...' con parámetros {params} (Intento {attempt}/{max_retries})")

                try:
                    r = requests.get(SUMMARY_URL, headers=HEADERS, params=params)
                    
                    if r.status_code == 429:
                        wait_time = initial_wait_time * (2 ** (attempt - 1))
                        logging.warning(f"⚠️ Demasiadas solicitudes (429) en el intento {attempt}/{max_retries} para lote de resúmenes '{current_batch_callsigns_or_flights[0]}...'. Esperando {wait_time:.1f} segundos antes de reintentar...")
                        time.sleep(wait_time)
                        continue
                    
                    r.raise_for_status()
                    
                    response_json = r.json()
                    
                    # Almacenar la respuesta raw completa
                    all_raw_summary_responses.append(response_json)

                    data = []
                    if isinstance(response_json, dict) and "data" in response_json and isinstance(response_json["data"], list):
                        data = response_json["data"]
                    elif isinstance(response_json, list): 
                        logging.warning(f"La respuesta de la API de resumen fue una lista directa para el lote '{current_batch_callsigns_str}', no un diccionario con 'data'. Estructura inesperada pero manejada. Respuesta completa: {json.dumps(response_json, indent=2)}")
                        data = response_json
                    else:
                        logging.error(
                            f"❌ La respuesta de la API de resumen no coincide con la estructura documentada (dict con clave 'data' que contiene una lista), ni fue una lista directa. "
                            f"Tipo recibido: {type(response_json)}. "
                            f"Lote de callsigns: '{current_batch_callsigns_str}'. Offset: {current_offset}. "
                            f"Respuesta completa: {json.dumps(response_json, indent=2)}. "
                            f"Asumiendo lista vacía."
                        )
                        data = []

                    # Ya no guardamos el JSON raw del summary aquí, se hará al final del día.
                    
                    if not data:
                        logging.info(f"No se encontraron resúmenes en la respuesta para el lote '{current_batch_callsigns_or_flights[0]}...' (offset {current_offset}). Finalizando paginación para este lote.")
                        break
                    
                    all_processed_summaries.extend(data)
                    current_offset += len(data)
                    logging.info(f"✅ Obtenidos {len(data)} resúmenes para el lote '{current_batch_callsigns_or_flights[0]}...'. Total acumulado: {len(all_processed_summaries)} resúmenes.")
                    time.sleep(1)

                    break # Romper el bucle de reintentos y pasar a la siguiente página (o lote si no hay más páginas)
                
                except requests.exceptions.HTTPError as http_err:
                    error_detail = r.text if r.text else str(http_err)
                    logging.error(f"❌ Error HTTP {r.status_code} {r.reason} al obtener resúmenes para el lote de callsigns '{current_batch_callsigns_str}' (offset {current_offset}): Detalles: {error_detail} (Intento {attempt}/{max_retries})")
                    
                    if r.status_code == 400:
                        logging.warning(f"Error 400 Bad Request para el lote. Saltando al siguiente lote de resúmenes.")
                        break # Romper reintentos y pasar al siguiente lote de callsigns
                    
                    if attempt == max_retries:
                        logging.error(f"Agota dos los intentos ({max_retries}) para el lote '{current_batch_callsigns_str}' debido a error HTTP. Saltando al siguiente lote.")
                        break # Romper reintentos y pasar al siguiente lote de callsigns
                    time.sleep(initial_wait_time)

                except requests.exceptions.RequestException as e:
                    logging.error(f"❌ Error de red al obtener resúmenes para el lote de callsigns '{current_batch_callsigns_str}' (offset {current_offset}): {e} (Intento {attempt}/{max_retries})")
                    if attempt == max_retries:
                        logging.error(f"Agota dos los intentos ({max_retries}) para el lote '{current_batch_callsigns_str}' debido a error de red. Saltando al siguiente lote.")
                        break # Romper reintentos y pasar al siguiente lote de callsigns
                    time.sleep(initial_wait_time)
                    
                except json.JSONDecodeError as e:
                    logging.error(f"❌ Error al decodificar JSON de la respuesta del resumen para el lote de callsigns '{current_batch_callsigns_str}' (offset {current_offset}): {e} (Intento {attempt}/{max_retries}). Saltando al siguiente lote de resúmenes.")
                    break # Romper reintentos y pasar al siguiente lote de callsigns
                
                except Exception as e:
                    logging.critical(f"❌ Error inesperado y crítico al obtener resúmenes para el lote de callsigns '{current_batch_callsigns_str}' (offset {current_offset}): {e} (Intento {attempt}/{max_retries}). Saltando al siguiente lote.", exc_info=True)
                    break # Romper reintentos y pasar al siguiente lote de callsigns

            else: # Este else se ejecuta si el bucle for termina sin un 'break' (es decir, agotó los intentos)
                logging.error(f"❌ Fallaron todos los {max_retries} intentos para el lote de resúmenes '{current_batch_callsigns_str}' (offset {current_offset}). No se pudieron obtener resúmenes para este lote.")
            
            # Lógica para controlar el bucle de paginación (while True)
            if 'data' not in locals() or not data: # Si 'data' no se pudo extraer o está vacío, salir del bucle de paginación
                break 
            # Si hubo un Bad Request en el último intento, salir del bucle de paginación para este lote
            if 'r' in locals() and r.status_code == 400: 
                break
            # Si se agotaron los reintentos y no se obtuvo data, salir del bucle de paginación
            if 'attempt' in locals() and attempt == max_retries and not data:
                 break

    logging.info(f"Finalizada la obtención de resúmenes. Total: {len(all_processed_summaries)}.")
    return all_processed_summaries, all_raw_summary_responses

def process_day(day_start: datetime):
    """Procesa los datos de vuelos para un día completo, usando datos de posición acumulados."""
    date_str = day_start.strftime("%Y%m%d")
    day_end = day_start + timedelta(days=1)
    logging.info(f"Comenzando el procesamiento de datos para el día: {day_start.strftime('%Y-%m-%d')}")

    # Paso 1: Recolectar IDs de vuelos y puntos de posición
    accumulated_flight_data = collect_flight_ids_for_day(day_start, INTERVAL_MINUTES)
    flight_ids_from_snapshots_with_data = list(accumulated_flight_data.keys())
    logging.info(f"Se encontraron {len(flight_ids_from_snapshots_with_data)} IDs de vuelo con datos y callsigns/flight numbers en las instantáneas para el día {date_str}.")


    if not flight_ids_from_snapshots_with_data:
        logging.warning(f"No se encontraron IDs de vuelo o datos de posición en las instantáneas para el día {date_str}. Saltando el procesamiento para este día.")
        return

    # Paso 2: Obtener resúmenes de vuelos usando los IDs recolectados
    # Ahora fetch_summaries_from_ids devuelve los resúmenes procesados y las respuestas raw
    summaries, all_raw_summary_responses = fetch_summaries_from_ids(accumulated_flight_data, day_start, day_end) 
    summary_map = {s.get("fr24_id"): s for s in summaries if s.get("fr24_id")}
    
    logging.info(f"Se obtuvieron {len(summaries)} resúmenes de vuelos de la API de resumen.")
    logging.info(f"De esos, {len(summary_map)} resúmenes únicos se mapearon para combinación.")

    # --- Guardar TODAS las respuestas raw de los summaries en un ÚNICO archivo por día ---
    if all_raw_summary_responses:
        raw_summaries_dir = out_dir / "raw_summaries" / day_start.strftime("%Y%m%d")
        raw_summaries_dir.mkdir(parents=True, exist_ok=True) # Asegurarse de que el directorio exista
        
        consolidated_raw_summary_file = raw_summaries_dir / f"all_raw_summaries_{date_str}.json"
        try:
            with open(consolidated_raw_summary_file, "w", encoding="utf-8") as f:
                json.dump(all_raw_summary_responses, f, indent=2)
            logging.info(f"Todas las respuestas raw de los resúmenes del día guardadas en: {consolidated_raw_summary_file}")
        except IOError as e:
            logging.error(f"Error al guardar el archivo consolidado de resúmenes raw en {consolidated_raw_summary_file}: {e}")
    else:
        logging.info(f"No se obtuvieron respuestas raw de resúmenes para el día {date_str}. No se guardará el archivo consolidado.")
    # --- FIN Guardar Raw Consolidado ---

    summary_file_path = out_dir / f"flights_summary_{date_str}.json"
    try:
        with open(summary_file_path, "w", encoding="utf-8") as f:
            json.dump(summaries, f, indent=2)
        logging.info(f"Resúmenes de vuelos combinados guardados en: {summary_file_path}")
    except IOError as e:
        logging.error(f"Error al guardar los resúmenes combinados en {summary_file_path}: {e}")

    processed_flights = []
    logging.info(f"Procesando {len(accumulated_flight_data)} vuelos con datos de posición acumulados para enriquecerlos con resúmenes.")

    for fid, flight_data in accumulated_flight_data.items():
        pts = flight_data['positions'] # Obtener solo las posiciones
        callsign_or_flight = flight_data['callsign_or_flight'] # Obtener el callsign/flight

        if not pts:
            logging.warning(f"No hay puntos de posición para el vuelo FR24 ID: {fid}. Saltando cálculos para este vuelo.")
            continue

        s = summary_map.get(fid, {}) # Obtiene el resumen, si existe. Si no, es un diccionario vacío.

        # Si no se encontró un resumen para este vuelo, loguearlo y saltarlo o continuar con datos parciales
        if not s:
            logging.debug(f"ℹ️ No se encontró resumen detallado de la API para el vuelo FR24 ID: {fid} (Callsign: {callsign_or_flight}). Se procesará con datos parciales.")
            # Dependiendo de tu estrategia, podrías optar por saltar este vuelo aquí:
            # continue 
        
        coords = [(p["latitude"], p["longitude"]) for p in pts if p["latitude"] is not None and p["longitude"] is not None]
        dist = calculate_distance(coords) if coords else 0
        durs = detect_phases(pts)
        
        aircraft_type_from_summary = s.get("type") 
        aircraft_model = s.get("aircraft", {}).get("model") 
        
        model_for_fuel = aircraft_type_from_summary or aircraft_model or "default"

        fuel = estimate_fuel(durs, model_for_fuel)
        co2_by_phase = {ph: round(fuel[ph] * 3.16, 2) for ph in fuel}
        co2_total = round(sum(co2_by_phase.values()), 2)
        co2_per_passenger = estimate_co2_by_passenger(fuel, model_for_fuel)

        rec = {
            "fr24_id": fid,
            "flight": s.get("flight"),
            "callsign": s.get("callsign") or callsign_or_flight, # Preferir el de summary, si no, el de la instantánea
            "aircraft_model": model_for_fuel,
            "aircraft_reg": s.get("reg"),
            "departure": s.get("orig_icao"),
            "arrival": s.get("dest_icao"),
            "distance_km": dist,
            "phase_durations_s": durs,
            "fuel_estimated_kg": fuel,
            "co2_estimated_kg": co2_by_phase,
            "co2_total_kg": co2_total,
            "co2_per_passenger_kg": co2_per_passenger,
            "raw_flight_path_points": pts,
            "circle_distance": s.get("circle_distance"),
        }
        processed_flights.append(rec)

        flight_detail_file_path = out_dir / f"{fid}_detailed_path_{date_str}.json"
        try:
            with open(flight_detail_file_path, "w", encoding="utf-8") as f:
                json.dump(pts, f, indent=2)
            logging.debug(f"Puntos de posición detallados para el vuelo {fid} guardados en: {flight_detail_file_path}")
        except IOError as e:
            logging.error(f"Error al guardar los puntos de posición del vuelo {fid} en {flight_detail_file_path}: {e}")

    time.sleep(1)

    processed_file_path = out_dir / f"flights_processed_{date_str}.json"
    try:
        with open(processed_file_path, "w", encoding="utf-8") as f:
            json.dump(processed_flights, f, indent=2)
        logging.info(f"Datos de vuelos procesados guardados en: {processed_file_path}")
    except IOError as e:
        logging.error(f"Error al guardar los vuelos procesados en {processed_file_path}: {e}")

    logging.info(f"✅ Finalizado el procesamiento de {len(processed_flights)} vuelos para {date_str}.")

if __name__ == "__main__":
    today_utc_midnight = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    days_to_offset_from_today = 1 # Para procesar el día de ayer UTC (ej. hoy 04/07, se procesa 03/07)
    start_processing_day = today_utc_midnight - timedelta(days=days_to_offset_from_today)

    logging.info("Iniciando el script de recolección y procesamiento de datos de FlightRadar24.")
    for day_offset in range(DAYS):
        day = start_processing_day
        logging.info(f"\n--- 📅 Iniciando procesamiento para el día: {day.strftime('%Y-%m-%d')} ---")
        try:
            process_day(day)
        except Exception as e:
            logging.critical(f"❌ Error crítico al procesar el día {day.strftime('%Y-%m-%d')}: {e}", exc_info=True)
    logging.info("Procesamiento de datos de FlightRadar24 finalizado.")