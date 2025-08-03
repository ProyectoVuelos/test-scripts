import os
import time
import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from dotenv import load_dotenv
from geopy.distance import geodesic
from collections import defaultdict
import random

# --- Logging config ---
log_dir = Path("logs")
log_dir.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_dir / "logs.log"),
        logging.StreamHandler(),
    ],
)

# --- Load environment and config ---
load_dotenv()
# La API_KEY ya no es estrictamente necesaria para la simulación, pero la mantenemos
# para consistencia si el script se modificara para usar la API real en el futuro.
API_KEY = os.getenv("PROD_FR24_API_KEY")
if not API_KEY:
    logging.warning(
        "PROD_FR24_API_KEY no está definida en .env. No es crítica para la simulación."
    )

# Las URLs de la API ya no se usarán para la simulación, pero se mantienen como referencia.
SUMMARY_URL = "https://fr24api.flightradar24.com/api/flight-summary/full"
POSITIONS_URL = "https://fr24api.flightradar24.com/api/historic/flight-positions/full"

# Define bounds for the contiguous United States - aún relevantes para simular vuelos dentro de ellos
USA_BOUNDS = "49.38,24.52,-124.77,-66.95"  # North, South, West, East

try:
    with open("data/fuel_profiles.json", "r") as f:
        FUEL_PROFILES = json.load(f)
except FileNotFoundError:
    logging.critical(
        "No se encontró 'data/fuel_profiles.json'. Asegúrate de que el archivo existe."
    )
    raise
except json.JSONDecodeError:
    logging.critical(
        "Error al decodificar 'data/fuel_profiles.json'. Asegúrate de que es un JSON válido."
    )
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
        distance = round(
            sum(geodesic(coords[i], coords[i + 1]).km for i in range(len(coords) - 1)),
            2,
        )
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
        p0, p1 = points[i], points[i + 1]
        dt = p1["timestamp"] - p0["timestamp"]
        vr = p0.get("vertical_rate", 0)
        alt = p0.get("altitude", 0)
        spd = p0.get("ground_speed", 0)

        # Simplificación de lógica para simulación, puede necesitar ajuste fino
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
        logging.warning(
            f"No se encontraron perfiles de combustible para el modelo '{model}'. Usando 'default'."
        )
        fuel_rates = FUEL_PROFILES.get("default")
        if not fuel_rates:
            logging.error("No se encontró el perfil de combustible 'default'.")
            return {ph: 0 for ph in durations}

    estimated_fuel = {
        ph: round((durations[ph] / 3600) * fuel_rates.get(ph, 0), 2) for ph in durations
    }
    logging.debug(f"Combustible estimado: {estimated_fuel}")
    return estimated_fuel


def estimate_co2_by_passenger(fuel_kg, model="default"):
    """Estima las emisiones de CO2 por pasajero."""
    co2_total = sum(fuel_kg[ph] * 3.16 for ph in fuel_kg)
    seats = FUEL_PROFILES.get(model, FUEL_PROFILES["default"]).get("seats", 150)
    if not seats:
        logging.warning(
            f"Número de asientos no definido para el modelo '{model}'. Usando 150 por defecto."
        )
        seats = 150
    co2_per_passenger = round(co2_total / seats, 2)
    logging.debug(
        f"CO2 total: {co2_total} kg, CO2 por pasajero: {co2_per_passenger} kg"
    )
    return co2_per_passenger


# --- SIMULACIÓN DE RESPUESTAS DE API ---


def generate_simulated_positions(num_points=50, start_time=None, day_of_interest=None):
    """Genera una serie de puntos de posición simulados para un vuelo."""
    if start_time is None:
        start_time = int(datetime.utcnow().timestamp())

    # Coordenadas de ejemplo para simular un vuelo sobre USA
    # De San Francisco a Nueva York aproximadamente
    start_lat, start_lon = 37.7749, -122.4194  # San Francisco
    end_lat, end_lon = 40.7128, -74.0060  # Nueva York

    # Asegurarse de que los timestamps estén dentro del día de interés
    if day_of_interest:
        day_start_ts = int(day_of_interest.timestamp())
        day_end_ts = int((day_of_interest + timedelta(days=1)).timestamp())
        # Ajustar start_time si cae fuera del día de interés
        if start_time < day_start_ts or start_time >= day_end_ts:
            start_time = day_start_ts + random.randint(
                0, 23 * 3600
            )  # Algún punto aleatorio dentro del día

    points = []
    current_time = start_time
    total_duration_minutes = 300  # 5 horas de vuelo simulado
    time_per_point_s = (total_duration_minutes * 60) / num_points

    for i in range(num_points):
        # Interpolación lineal para latitud y longitud
        interp_factor = i / (num_points - 1)
        lat = (
            start_lat
            + (end_lat - start_lat) * interp_factor
            + random.uniform(-0.1, 0.1)
        )  # Pequeña variación
        lon = (
            start_lon
            + (end_lon - start_lon) * interp_factor
            + random.uniform(-0.1, 0.1)
        )

        # Altitud simulada (subida, crucero, bajada)
        if i < num_points * 0.15:  # Ascenso (primer 15%)
            alt = 100 + (35000 - 100) * (i / (num_points * 0.15))
            vspeed = random.uniform(500, 2000)  # pies/min
            gspeed = random.uniform(150, 400)  # nudos
        elif i > num_points * 0.85:  # Descenso (último 15%)
            alt = 35000 - (35000 - 500) * (
                (i - num_points * 0.85) / (num_points * 0.15)
            )
            vspeed = random.uniform(-2000, -500)  # pies/min
            gspeed = random.uniform(150, 400)  # nudos
        else:  # Crucero
            alt = 35000 + random.uniform(-500, 500)
            vspeed = random.uniform(-50, 50)  # pies/min (pequeñas variaciones)
            gspeed = random.uniform(450, 550)  # nudos

        points.append(
            {
                "timestamp": current_time,
                "latitude": round(lat, 4),
                "longitude": round(lon, 4),
                "vertical_rate": round(vspeed, 2),
                "altitude": round(alt, 0),
                "ground_speed": round(gspeed, 0),
            }
        )
        current_time += int(time_per_point_s)

    # Asegurar que el último punto no exceda el día de interés si se especificó
    if day_of_interest and current_time >= day_end_ts:
        points = [p for p in points if p["timestamp"] < day_end_ts]
        # Reajustar el último punto si fue truncado
        if points:
            points[-1]["timestamp"] = day_end_ts - 1  # Justo antes del final del día

    return points


def generate_simulated_summary(fr24_id, callsign_or_flight):
    """Genera un resumen de vuelo simulado."""
    aircraft_models = ["B738", "A320", "B77W", "A333", "E190"]
    airlines = ["American", "Delta", "United", "Southwest", "Spirit"]
    airports = ["KLAX", "KJFK", "KORD", "KATL", "KDFW"]

    model = random.choice(aircraft_models)
    airline_prefix = random.choice(["AA", "DL", "UA", "WN", "NK"])

    # Intentar derivar la aerolínea del callsign si sigue un patrón típico
    if (
        callsign_or_flight
        and len(callsign_or_flight) > 2
        and callsign_or_flight[:2].isalpha()
    ):
        derived_airline = callsign_or_flight[:2]
        if derived_airline in ["AA", "DL", "UA", "WN", "NK"]:  # Ejemplo de mapeo
            airline_prefix = derived_airline

    orig = random.choice(airports)
    dest = random.choice([ap for ap in airports if ap != orig])

    return {
        "fr24_id": fr24_id,
        "callsign": callsign_or_flight,
        "type": model,  # Tipo de aeronave
        "aircraft": {
            "model": model,  # Modelo de aeronave
            "seats": FUEL_PROFILES.get(model, FUEL_PROFILES["default"]).get(
                "seats", 150
            ),  # Usar los asientos del perfil
        },
        "reg": f"N{random.randint(100, 999)}{random.choice('AZQWERTY')}",  # Matrícula simulada
        "orig_icao": orig,
        "dest_icao": dest,
        "airline": {
            "name": f"{random.choice(airlines)} Airlines",
            "icao_code": airline_prefix,
        },
        "actual_sch_time_utc": datetime.utcnow().isoformat(timespec="seconds")
        + "Z",  # Tiempo simulado
    }


def collect_flight_ids_for_day_simulated(day_start: datetime, interval_minutes: int):
    """
    Simula la recolección de IDs de vuelos, callsigns/flight numbers y puntos de posición
    para un día específico a intervalos definidos, sin llamadas a la API.
    """
    all_flight_info = defaultdict(lambda: {"positions": [], "callsign_or_flight": None})
    iterations = int((24 * 60) / interval_minutes)
    interval = timedelta(minutes=interval_minutes)
    logging.info(
        f"SIMULADO: Comenzando la recolección de IDs, callsigns/flight numbers y posiciones para el día: {day_start.strftime('%Y-%m-%d')}"
    )

    # Número de vuelos a simular por día
    num_simulated_flights = 5  # Ajusta esto para más o menos vuelos simulados

    # Generar un conjunto de vuelos únicos para el día
    simulated_flights_for_day = {}
    for i in range(num_simulated_flights):
        fr24_id = f"sim_id_{day_start.strftime('%Y%m%d')}_{i}"
        callsign = f"SIM{random.randint(100,999)}"
        # Generar puntos para un vuelo completo o que cruce varias instantáneas
        start_ts = int(day_start.timestamp()) + random.randint(
            0, (24 * 60 * 60) - 3600
        )  # Vuelo empieza aleatoriamente dentro del día
        simulated_flights_for_day[fr24_id] = {
            "callsign_or_flight": callsign,
            "full_positions": generate_simulated_positions(
                num_points=random.randint(30, 80),
                start_time=start_ts,
                day_of_interest=day_start,
            ),
        }

    for i in range(iterations):
        current_snapshot_time_ts = int((day_start + i * interval).timestamp())
        timestamp_utc_dt = datetime.fromtimestamp(
            current_snapshot_time_ts, timezone.utc
        )
        logging.info(
            f"SIMULADO: Generando instantánea simulada en {timestamp_utc_dt.strftime('%Y-%m-%d %H:%M:%S UTC')}"
        )

        flights_in_snapshot = []
        for fr24_id, flight_data in simulated_flights_for_day.items():
            callsign_or_flight = flight_data["callsign_or_flight"]
            full_positions = flight_data["full_positions"]

            # Encontrar el punto de posición más cercano al timestamp actual de la instantánea
            closest_point = None
            min_time_diff = float("inf")

            # Filtrar solo los puntos relevantes para el día
            valid_positions_for_day = [
                p
                for p in full_positions
                if day_start.timestamp()
                <= p["timestamp"]
                < (day_start + timedelta(days=1)).timestamp()
            ]

            for p in valid_positions_for_day:
                time_diff = abs(p["timestamp"] - current_snapshot_time_ts)
                if time_diff < min_time_diff:
                    min_time_diff = time_diff
                    closest_point = p

            # Si el punto está "suficientemente" cerca del timestamp de la instantánea (ej. dentro del intervalo)
            if (
                closest_point and min_time_diff <= interval_minutes * 60
            ):  # Dentro del intervalo de la instantánea
                flights_in_snapshot.append(
                    {
                        "fr24_id": fr24_id,
                        "callsign": callsign_or_flight,
                        "lat": closest_point["latitude"],
                        "lon": closest_point["longitude"],
                        "alt": closest_point["altitude"],
                        "gspeed": closest_point["ground_speed"],
                        "vspeed": closest_point["vertical_rate"],
                        "timestamp": datetime.fromtimestamp(
                            closest_point["timestamp"], timezone.utc
                        ).isoformat(timespec="seconds")
                        + "Z",
                    }
                )

                # Acumular todos los puntos del vuelo completo en all_flight_info
                # Esto es clave: para los cálculos finales necesitamos la trayectoria completa
                # no solo los puntos que caen en las instantáneas.
                if all_flight_info[fr24_id]["callsign_or_flight"] is None:
                    all_flight_info[fr24_id]["callsign_or_flight"] = callsign_or_flight

                # Asegúrate de que solo se añaden puntos dentro del día de interés
                for p in full_positions:
                    if (
                        day_start.timestamp()
                        <= p["timestamp"]
                        < (day_start + timedelta(days=1)).timestamp()
                    ):
                        # Evitar duplicados si ya se añadió el punto en una iteración anterior
                        if p not in all_flight_info[fr24_id]["positions"]:
                            all_flight_info[fr24_id]["positions"].append(p)

        logging.info(
            f"SIMULADO: Encontrados {len(flights_in_snapshot)} IDs de vuelo en la instantánea simulada."
        )
        # No guardamos datos raw de instantáneas simuladas para simplificar.
        time.sleep(0.1)  # Pequeña pausa para simular el tiempo de procesamiento

    for fr24_id in all_flight_info:
        all_flight_info[fr24_id]["positions"].sort(key=lambda p: p["timestamp"])

    total_accumulated_points = sum(
        len(v["positions"]) for v in all_flight_info.values()
    )
    total_unique_fr24_ids = len(all_flight_info)
    logging.info(
        f"SIMULADO: Se recolectaron un total de {total_unique_fr24_ids} IDs únicos y se acumularon {total_accumulated_points} puntos de posición."
    )

    all_flight_info_filtered = {
        fr24_id: data
        for fr24_id, data in all_flight_info.items()
        if data["callsign_or_flight"]
    }
    logging.info(
        f"SIMULADO: Después de filtrar, {len(all_flight_info_filtered)} vuelos tienen callsign/flight y serán considerados para resumen."
    )
    return all_flight_info_filtered


def fetch_summaries_from_ids_simulated(flight_info_map, day_start, day_end):
    """
    Simula la obtención de resúmenes de vuelos a partir de un mapa de IDs de vuelo y sus callsigns/flight numbers,
    sin llamadas a la API.
    """
    all_processed_summaries = []
    all_raw_summary_responses = (
        []
    )  # Vacío en simulación, pero se mantiene la estructura

    fr24_ids_with_callsigns = list(flight_info_map.keys())

    if not fr24_ids_with_callsigns:
        logging.info(
            "SIMULADO: No hay IDs de vuelo con callsigns/flight numbers para obtener resúmenes."
        )
        return [], []

    logging.info(
        f"SIMULADO: Comenzando a generar resúmenes simulados para {len(fr24_ids_with_callsigns)} vuelos."
    )

    for fr24_id in fr24_ids_with_callsigns:
        callsign = flight_info_map[fr24_id]["callsign_or_flight"]
        if callsign:
            summary = generate_simulated_summary(fr24_id, callsign)
            all_processed_summaries.append(summary)
            # No hay respuestas raw reales para almacenar en simulación, pero mantenemos la estructura
            # all_raw_summary_responses.append({"simulated_summary": summary})
        time.sleep(0.05)  # Pequeña pausa para simular el tiempo de red

    logging.info(
        f"SIMULADO: Finalizada la generación de resúmenes. Total: {len(all_processed_summaries)}."
    )
    return all_processed_summaries, all_raw_summary_responses


# --- FIN SIMULACIÓN DE RESPUESTAS DE API ---


def process_day(day_start: datetime):
    """Procesa los datos de vuelos para un día completo, usando datos de posición acumulados."""
    date_str = day_start.strftime("%Y%m%d")
    day_end = day_start + timedelta(days=1)
    logging.info(
        f"Comenzando el procesamiento de datos para el día: {day_start.strftime('%Y-%m-%d')}"
    )

    # Paso 1: Recolectar IDs de vuelos y puntos de posición (usando la función simulada)
    accumulated_flight_data = collect_flight_ids_for_day_simulated(
        day_start, INTERVAL_MINUTES
    )
    flight_ids_from_snapshots_with_data = list(accumulated_flight_data.keys())
    logging.info(
        f"Se encontraron {len(flight_ids_from_snapshots_with_data)} IDs de vuelo con datos y callsigns/flight numbers en las instantáneas para el día {date_str}."
    )

    if not flight_ids_from_snapshots_with_data:
        logging.warning(
            f"No se encontraron IDs de vuelo o datos de posición en las instantáneas para el día {date_str}. Saltando el procesamiento para este día."
        )
        return

    # Paso 2: Obtener resúmenes de vuelos (usando la función simulada)
    summaries, all_raw_summary_responses = fetch_summaries_from_ids_simulated(
        accumulated_flight_data, day_start, day_end
    )
    summary_map = {s.get("fr24_id"): s for s in summaries if s.get("fr24_id")}

    logging.info(f"Se obtuvieron {len(summaries)} resúmenes de vuelos simulados.")
    logging.info(
        f"De esos, {len(summary_map)} resúmenes únicos se mapearon para combinación."
    )

    # --- Guardar TODAS las respuestas raw de los summaries en un ÚNICO archivo por día ---
    # En simulación, all_raw_summary_responses estará vacío, pero la lógica se mantiene
    if all_raw_summary_responses:
        raw_summaries_dir = out_dir / "raw_summaries" / day_start.strftime("%Y%m%d")
        raw_summaries_dir.mkdir(
            parents=True, exist_ok=True
        )  # Asegurarse de que el directorio exista

        consolidated_raw_summary_file = (
            raw_summaries_dir / f"all_raw_summaries_{date_str}.json"
        )
        try:
            with open(consolidated_raw_summary_file, "w", encoding="utf-8") as f:
                json.dump(all_raw_summary_responses, f, indent=2)
            logging.info(
                f"Todas las respuestas raw de los resúmenes del día guardadas en: {consolidated_raw_summary_file}"
            )
        except IOError as e:
            logging.error(
                f"Error al guardar el archivo consolidado de resúmenes raw en {consolidated_raw_summary_file}: {e}"
            )
    else:
        logging.info(
            f"SIMULADO: No se obtuvieron respuestas raw de resúmenes (simuladas) para el día {date_str}. No se guardará el archivo consolidado."
        )
    # --- FIN Guardar Raw Consolidado ---

    summary_file_path = out_dir / f"flights_summary_{date_str}.json"
    try:
        with open(summary_file_path, "w", encoding="utf-8") as f:
            json.dump(summaries, f, indent=2)
        logging.info(
            f"Resúmenes de vuelos combinados (simulados) guardados en: {summary_file_path}"
        )
    except IOError as e:
        logging.error(
            f"Error al guardar los resúmenes combinados en {summary_file_path}: {e}"
        )

    processed_flights = []
    logging.info(
        f"Procesando {len(accumulated_flight_data)} vuelos con datos de posición acumulados para enriquecerlos con resúmenes."
    )

    for fid, flight_data in accumulated_flight_data.items():
        pts = flight_data["positions"]  # Obtener solo las posiciones
        callsign_or_flight = flight_data[
            "callsign_or_flight"
        ]  # Obtener el callsign/flight

        if not pts:
            logging.warning(
                f"No hay puntos de posición para el vuelo FR24 ID: {fid}. Saltando cálculos para este vuelo."
            )
            continue

        s = summary_map.get(
            fid, {}
        )  # Obtiene el resumen, si existe. Si no, es un diccionario vacío.

        # Si no se encontró un resumen para este vuelo, loguearlo y saltarlo o continuar con datos parciales
        if not s:
            logging.debug(
                f"ℹ️ No se encontró resumen detallado (simulado) para el vuelo FR24 ID: {fid} (Callsign: {callsign_or_flight}). Se procesará con datos parciales."
            )

        coords = [
            (p["latitude"], p["longitude"])
            for p in pts
            if p["latitude"] is not None and p["longitude"] is not None
        ]
        dist = calculate_distance(coords) if coords else 0
        durs = detect_phases(pts)

        # Preferir el modelo de la aeronave del resumen simulado
        aircraft_model = s.get("aircraft", {}).get("model")
        aircraft_type_from_summary = s.get("type")

        # Priorizar el modelo del resumen simulado si está disponible
        model_for_fuel = aircraft_model or aircraft_type_from_summary or "default"

        fuel = estimate_fuel(durs, model_for_fuel)
        co2_by_phase = {ph: round(fuel[ph] * 3.16, 2) for ph in fuel}
        co2_total = round(sum(co2_by_phase.values()), 2)
        co2_per_passenger = estimate_co2_by_passenger(fuel, model_for_fuel)

        rec = {
            "fr24_id": fid,
            "callsign": s.get("callsign")
            or callsign_or_flight,  # Preferir el de summary, si no, el de la instantánea
            "aircraft_model": model_for_fuel,
            "icao24": s.get("reg"),
            "departure": s.get("orig_icao"),
            "arrival": s.get("dest_icao"),
            "distance_km": dist,
            "phase_durations_s": durs,
            "fuel_estimated_kg": fuel,
            "co2_estimated_kg": co2_by_phase,
            "co2_total_kg": co2_total,
            "co2_per_passenger_kg": co2_per_passenger,
            "raw_flight_path_points": pts,
        }
        processed_flights.append(rec)

        # Guarda los puntos detallados simulados
        flight_detail_file_path = out_dir / f"{fid}_detailed_path_{date_str}.json"
        try:
            with open(flight_detail_file_path, "w", encoding="utf-8") as f:
                json.dump(pts, f, indent=2)
            logging.debug(
                f"Puntos de posición detallados (simulados) para el vuelo {fid} guardados en: {flight_detail_file_path}"
            )
        except IOError as e:
            logging.error(
                f"Error al guardar los puntos de posición del vuelo {fid} en {flight_detail_file_path}: {e}"
            )

    time.sleep(0.1)  # Pequeña pausa para simular el tiempo de procesamiento

    processed_file_path = out_dir / f"flights_processed_{date_str}.json"
    try:
        with open(processed_file_path, "w", encoding="utf-8") as f:
            json.dump(processed_flights, f, indent=2)
        logging.info(
            f"Datos de vuelos procesados (simulados) guardados en: {processed_file_path}"
        )
    except IOError as e:
        logging.error(
            f"Error al guardar los vuelos procesados en {processed_file_path}: {e}"
        )

    logging.info(
        f"✅ Finalizado el procesamiento de {len(processed_flights)} vuelos para {date_str}."
    )


if __name__ == "__main__":
    today_utc_midnight = datetime.utcnow().replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    days_to_offset_from_today = (
        1  # Para procesar el día de ayer UTC (ej. hoy 04/07, se procesa 03/07)
    )
    start_processing_day = today_utc_midnight - timedelta(
        days=days_to_offset_from_today
    )

    logging.info(
        "Iniciando el script de recolección y procesamiento de datos de FlightRadar24 (Modo Simulación)."
    )
    for day_offset in range(DAYS):
        day = start_processing_day
        logging.info(
            f"\n--- 📅 Iniciando procesamiento (simulado) para el día: {day.strftime('%Y-%m-%d')} ---"
        )
        try:
            process_day(day)
        except Exception as e:
            logging.critical(
                f"❌ Error crítico al procesar el día {day.strftime('%Y-%m-%d')} (simulado): {e}",
                exc_info=True,
            )
    logging.info(
        "Procesamiento de datos de FlightRadar24 finalizado (Modo Simulación)."
    )
