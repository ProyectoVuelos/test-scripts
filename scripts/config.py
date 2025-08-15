import os
import json
import logging
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("PROD_FR24_API_KEY")
if not API_KEY:
    raise RuntimeError("PROD_FR24_API_KEY is not defined in .env")

HEADERS = {
    "Accept": "application/json",
    "Accept-Version": "v1",
    "Authorization": f"Bearer {API_KEY}",
}

SUMMARY_URL = "https://fr24api.flightradar24.com/api/flight-summary/full"
POSITIONS_URL = "https://fr24api.flightradar24.com/api/historic/flight-positions/full"

TOTAL_FLIGHTS_TO_PROCESS = 100
USA_BOUNDS = "49.38,24.52,-124.77,-66.95"

MINIMUM_DATA_POINTS = 5
BASE_OUTPUT_DIR = Path("data/flights")
LOG_DIR = Path("logs")

try:
    with open("data/fuel_profiles.json", "r") as f:
        FUEL_PROFILES = json.load(f)
except (FileNotFoundError, json.JSONDecodeError) as e:
    logging.critical(f"Could not load 'data/fuel_profiles.json': {e}")
    raise


def setup_logging():
    LOG_DIR.mkdir(exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.FileHandler(LOG_DIR / "logs.log"), logging.StreamHandler()],
    )
