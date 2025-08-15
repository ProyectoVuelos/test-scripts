# Flight Data Processing Pipeline

Este proyecto proporciona un robusto pipeline de punta a punta para enriquecer una lista de vuelos proporcionada por el usuario con datos detallados de la API de FlightRadar24, calcular métricas de vuelo y almacenar los resultados en una base de datos PostgreSQL para su análisis.

El flujo de trabajo comienza con un archivo CSV de vuelos (que puede ser generado a partir de datos públicos del BTS). Un script principal enriquece esta lista con rutas detalladas y metadatos de la API. Posteriormente, los datos son procesados para calcular fases de vuelo, consumo de combustible y emisiones de CO₂, y finalmente cargados en la base de datos.

---

## Key Features

- **Enriquecimiento de Datos Dirigido**: Utiliza créditos de API de manera ultra eficiente al trabajar sobre una lista predefinida de vuelos que te interesan.
- **Ejecución Flexible**: Controla el número de vuelos a procesar en cada ejecución (`--limit`) y evita volver a procesar datos con el flag (`--skip-processed`).
- **Pipeline Modular y Robusto**: Un proceso limpio que separa la preparación de datos, el enriquecimiento vía API, el procesamiento de cálculos y la carga a la base de datos.
- **Análisis de Vuelo Detallado**: Calcula la distancia de gran círculo vs. la ruta real, detecta la duración de las fases de vuelo y proporciona estimaciones de consumo de combustible y emisiones.
- **Integración con Base de Datos**: Carga todos los datos procesados en una base de datos PostgreSQL bien estructurada, permitiendo consultas y análisis robustos.
- **Datos de Rendimiento Curados**: Utiliza un archivo `fuel_profiles.json` estable y curado manualmente para cálculos de rendimiento fiables.

---

## Project Structure

    .
    ├── data/
    │   ├── airports.txt
    │   ├── fuel_profiles.json
    │   └── flights/
    │       └── run_YYYY-MM-DD_HH-MM-SS/
    │           ├── flight_details_map...json
    │           ├── summaries/
    │           ├── processed/
    │           └── detailed_paths/
    ├── database/
    │   ├── seeder/
    │   │   └── seeder.py
    │   └── schema.sql
    ├── logs/
    ├── scripts/
    │   ├── config.py
    │   ├── prepare_csv.py
    │   ├── enrich_flights.py
    │   └── process_data.py
    ├── .env
    ├── flights_to_track.csv
    └── requirements.txt

---

## Setup

1. **Archivos del Proyecto**: Asegúrate de que todos los archivos estén en sus ubicaciones correctas según la estructura anterior.

2. **Archivo `.env`**: Crea un archivo `.env` en el directorio raíz con tu clave de API y credenciales de la base de datos:

        # API Credentials
        PROD_FR24_API_KEY="your_actual_api_key"

        # Database Credentials
        DB_HOST="your_db_host"
        DB_PORT="5432"
        DB_USER="your_db_user"
        DB_PASSWORD="your_db_password"
        DB_NAME="your_db_name"

3. **Base de Datos PostgreSQL**: Antes de ejecutar el seeder, crea las tablas en tu base de datos usando los comandos de `database/schema.sql`.

4. **Dependencias**: Instala las librerías de Python necesarias.

        pip install -r requirements.txt

---

## Usage / Workflow

El pipeline es un proceso secuencial. Todos los comandos deben ejecutarse desde el directorio raíz del proyecto.

### Paso 1 (Opcional pero Recomendado): Preparar el CSV de Vuelos

Usa el script `prepare_csv.py` para procesar un archivo grande de datos públicos (como los del BTS) y convertirlo en el formato `flights_to_track.csv` que necesitamos.

    # Reemplaza 'nombre_archivo_bts.csv' con el nombre de tu archivo descargado
    python scripts/prepare_csv.py --input nombre_archivo_bts.csv

Esto creará un archivo `flights_to_track.csv` en la raíz de tu proyecto.

### Paso 2: Enriquecer los Datos de Vuelos

Este es el script principal de adquisición. Lee `flights_to_track.csv` y usa la API para descargar los datos detallados.

#### Ejecución Inicial (con límite)

Para tu primera gran extracción, usa `--limit` para controlar tu presupuesto de créditos.

    python scripts/enrich_flights.py --limit 1200

#### Añadir más vuelos (con créditos sobrantes)

Si te sobran créditos, puedes añadir más vuelos. El script ignorará los que ya procesaste y tomará los siguientes de la lista.

    python scripts/enrich_flights.py --limit 100 --skip-processed

### Paso 3: Procesar los Datos

Este script lee los datos brutos de una carpeta `run_...`, realiza los cálculos y guarda los resultados.

    # Asegúrate de que la ruta coincida con la que quieres procesar
    python scripts/process_data.py data/flights/run_YYYY-MM-DD_HH-MM-SS

### Paso 4: Cargar a la Base de Datos

Este script encuentra los datos procesados más recientes y los carga en tu base de datos PostgreSQL.

    python database/seeder/seeder.py
