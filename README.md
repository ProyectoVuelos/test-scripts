# Flight Data Processing Pipeline

Este proyecto proporciona un robusto pipeline de punta a punta para obtener datos de vuelos de la API de FlightRadar24, calcular métricas detalladas y almacenar los resultados en una base de datos PostgreSQL para su análisis.

El pipeline primero recolecta una lista de vuelos candidatos recientes. Después de un período de espera para asegurar que los vuelos hayan finalizado, verifica el estado de cada candidato y descarga los datos completos solo de los vuelos que han aterrizado. Finalmente, procesa estos datos y los carga en la base de datos.

---

## Key Features

- **Credit-Optimized Acquisition**: Evita llamadas costosas a la API utilizando una estrategia inteligente de siembra por aeropuertos y verificación de estado antes de la descarga masiva.
- **Modular & Robust Pipeline**: Un proceso limpio y multifase que separa la recolección de IDs, la adquisición de datos, el procesamiento de cálculos y la carga a la base de datos.
- **Detailed Flight Analysis**: Calcula la distancia de gran círculo vs. la ruta real, detecta la duración de las fases de vuelo y proporciona estimaciones de consumo de combustible y emisiones de CO₂.
- **Database Integration**: Carga todos los datos procesados en una base de datos PostgreSQL bien estructurada, permitiendo consultas y análisis robustos.
- **Curated Performance Data**: Utiliza un archivo `fuel_profiles.json` estable y curado manualmente para cálculos de rendimiento fiables.

---

## Project Structure

    .
    ├── data/
    │   ├── airports.txt
    │   ├── fuel_profiles.json
    │   └── flights/
    │       └── run_YYYY-MM-DD_HH-MM-SS/
    │           ├── candidate_flights.json
    │           ├── flight_details_map_YYYYMMDD.json
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
    │   ├── acquire_ids.py
    │   ├── acquire_data.py
    │   └── process_data.py
    ├── .env
    └── requirements.txt

---

## Setup

1. **Project Files**: Asegúrate de que todos los archivos del proyecto estén en sus ubicaciones correctas según la estructura anterior.

2. **Create `.env` File**: Crea un archivo llamado `.env` en el directorio raíz. Añade tu clave de API y credenciales de PostgreSQL:

        # API Credentials
        PROD_FR24_API_KEY="your_actual_api_key"

        # Database Credentials
        DB_HOST="your_db_host"
        DB_PORT="5432"
        DB_USER="your_db_user"
        DB_PASSWORD="your_db_password"
        DB_NAME="your_db_name"

3. **Set Up PostgreSQL Database**: Antes de ejecutar el seeder, crea las tablas necesarias en tu base de datos utilizando los comandos SQL proporcionados en `database/schema.sql`.

4. **Prepare Input Files**: Popula `data/airports.txt` con los códigos ICAO de los aeropuertos que te interesan. El archivo `data/fuel_profiles.json` está listo para usar.

5. **Install Dependencies**: Instala las librerías de Python necesarias.

        pip install -r requirements.txt

---

## Usage / Workflow

El pipeline es un proceso secuencial. Todos los comandos deben ejecutarse desde el directorio raíz del proyecto.

### Día 1: Recolectar IDs de Vuelos Candidatos

Este primer script lee tu lista de aeropuertos, obtiene los IDs de todos los vuelos recientes (aterrizados, en ruta, etc.) y los guarda. Esto crea una carpeta `run_<timestamp>` con un archivo `candidate_flights.json`.

    python scripts/acquire_ids.py

### Esperar 24 Horas

Es crucial esperar aproximadamente 24 horas. Esto asegura que todos los vuelos de la lista de candidatos hayan tenido tiempo de completar sus viajes.

### Día 2: Verificar y Descargar Datos Completos

Este script lee el archivo `candidate_flights.json` de la ejecución del día anterior. Verifica el estado final de cada vuelo y descarga los datos completos solo de aquellos que han aterrizado.

    # Asegúrate de usar la ruta correcta a la carpeta creada en el Día 1
    python scripts/acquire_data.py data/flights/run_YYYY-MM-DD_HH-MM-SS

### Paso Final 1: Procesar los Datos

Este script lee los datos brutos descargados, realiza todos los cálculos y guarda los resultados finales en la misma carpeta `run_<timestamp>`.

    # Asegúrate de que la ruta coincida con la que estás procesando
    python scripts/process_data.py data/flights/run_YYYY-MM-DD_HH-MM-SS

### Paso Final 2: Cargar a la Base de Datos

Este último script encuentra los datos procesados más recientes y los carga de manera eficiente en tu base de datos PostgreSQL.

    python database/seeder/seeder.py
