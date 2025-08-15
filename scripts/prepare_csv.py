import logging
import argparse
from pathlib import Path
import pandas as pd

SAMPLE_SIZE = 5000
OUTPUT_FILE = Path("flights_to_track.csv")

IATA_TO_ICAO_MAP = {
    "AA": "AAL",
    "DL": "DAL",
    "UA": "UAL",
    "WN": "SWA",
    "B6": "JBU",
    "AS": "ASA",
    "NK": "NKS",
    "F9": "FFT",
    "HA": "HAL",
    "G4": "AAY",
}

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)


def create_tracking_file(input_path: Path, output_path: Path, sample_size: int):
    """
    Lee un archivo CSV del BTS, lo procesa y crea un archivo de seguimiento de vuelos.
    """
    if not input_path.is_file():
        logging.error(f"El archivo de entrada no fue encontrado: {input_path}")
        return

    logging.info(f"Cargando el archivo de datos del BTS desde {input_path}...")
    try:
        df = pd.read_csv(
            input_path,
            usecols=["FL_DATE", "OP_UNIQUE_CARRIER", "OP_CARRIER_FL_NUM"],
            low_memory=False,
        )
    except Exception as e:
        logging.error(f"No se pudo leer el archivo CSV. Error: {e}")
        return

    logging.info(f"Se cargaron {len(df)} registros. Procesando...")

    df.dropna(inplace=True)
    df = df.astype({"OP_CARRIER_FL_NUM": int})

    logging.info("Formateando la columna de fecha a YYYY-MM-DD...")
    df["FL_DATE"] = pd.to_datetime(df["FL_DATE"]).dt.strftime("%Y-%m-%d")

    logging.info("Creando 'callsigns' en formato ICAO...")
    df["callsign"] = (
        df["OP_UNIQUE_CARRIER"]
        .map(IATA_TO_ICAO_MAP)
        .str.cat(df["OP_CARRIER_FL_NUM"].astype(str))
    )

    df.rename(columns={"FL_DATE": "date"}, inplace=True)
    df.dropna(subset=["callsign"], inplace=True)

    if len(df) > sample_size:
        logging.info(f"Tomando una muestra aleatoria de {sample_size} vuelos...")
        df_sample = df.sample(n=sample_size, random_state=42)
    else:
        logging.warning(
            f"El número de vuelos procesables ({len(df)}) es menor que el tamaño de muestra deseado. Usando todos los disponibles."
        )
        df_sample = df

    final_df = df_sample[["callsign", "date"]]
    final_df.to_csv(output_path, index=False)

    logging.info(
        f"✅ ¡Éxito! Se ha creado el archivo '{output_path}' con {len(final_df)} vuelos."
    )


def main():
    parser = argparse.ArgumentParser(
        description="Procesa un archivo CSV del BTS para crear un archivo de seguimiento de vuelos."
    )
    parser.add_argument(
        "--input", required=True, help="Ruta al archivo CSV descargado del BTS."
    )
    args = parser.parse_args()

    create_tracking_file(Path(args.input), OUTPUT_FILE, SAMPLE_SIZE)


if __name__ == "__main__":
    main()
