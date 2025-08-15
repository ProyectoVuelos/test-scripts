# Flight Data Processing Pipeline

Este proyecto proporciona un robusto pipeline de punta a punta para obtener datos de vuelos de la API de FlightRadar24, calcular métricas detalladas y almacenar los resultados en una base de datos PostgreSQL para su análisis.

El pipeline utiliza un método avanzado de **descubrimiento y reconstrucción**. Primero, descubre IDs de vuelos históricos usando snapshots geográficos. Luego, obtiene los resúmenes de estos vuelos. Finalmente, reconstruye las rutas de vuelo detalladas tomando snapshots en intervalos de tiempo fijos, ensambla los datos y los procesa para calcular fases de vuelo, consumo de combustible y emisiones de CO₂ antes de cargarlos a la base de datos.

---

## Key Features

- **Descubrimiento Controlado**: Utiliza snapshots de `bounds` de forma controlada y económica para descubrir IDs de vuelos históricos reales.
- **Reconstrucción de Rutas**: Implementa una estrategia de "fuerza bruta" inteligente para reconstruir rutas de vuelo de alta resolución a partir de snapshots periódicos.
- **Pipeline Modular y Robusto**: Un proceso limpio y multifase que separa el descubrimiento, la obtención de resúmenes, la reconstrucción, el ensamblaje, el procesamiento y la carga a la base de datos en scripts manejables.
- **Análisis de Vuelo Detallado**: Calcula la distancia de gran círculo vs. la ruta real, detecta la duración de las fases de vuelo y proporciona estimaciones de consumo de combustible y emisiones.
- **Integración con Base de Datos**: Carga todos los datos procesados en una base de datos PostgreSQL bien estructurada.
- **Datos de Rendimiento Curados**: Utiliza un archivo `fuel_profiles.json` estable y curado manualmente para cálculos de rendimiento fiables.

---

## Project Structure

    .
    ├── data/
    │   ├── fuel_profiles.json
    │   └── flights/
    │       └── run_YYYY-MM-DD_HH-MM-SS/
    │           ├── discovered_ids.json
    │           ├── summaries/
    │           ├── flight_timelines.json
    │           ├── raw_positions/
    │           ├── flight_details_map...json
    │           └── processed/
    ├── database/
    │   ├── seeder/
    │   │   └── seeder.py
    │   └── schema.sql
    ├── logs/
    ├── scripts/
    │   ├── config.py
    │   ├── discover_flights.py
    │   ├── get_summaries.py
    │   ├── prepare_timelines.py
    │   ├── reconstruct_paths.py
    │   ├── assemble_flights.py
    │   └── process_data.py
    ├── .env
    └── requirements.txt

---

## Setup

1. **Archivos del Proyecto**: Asegúrate de que todos los archivos estén en sus ubicaciones correctas.
2. **Archivo `.env`**: Crea un archivo `.env` en el directorio raíz con tu clave de API y credenciales de la base de datos.
3. **Base de Datos PostgreSQL**: Antes de ejecutar el seeder, crea las tablas en tu base de datos usando los comandos de `database/schema.sql`.
4. **Dependencias**: Instala las librerías de Python necesarias.

        pip install -r requirements.txt

---

## Usage / Workflow

El pipeline es un proceso secuencial. Todos los comandos deben ejecutarse desde el directorio raíz del proyecto.

### Paso 1: Descubrir IDs de Vuelos

Este script inicia el proceso, creando una nueva carpeta `run_...` y guardando una lista de IDs de vuelos a procesar.

    python scripts/discover_flights.py

### Paso 2: Obtener Resúmenes (Summaries)

Este script toma los IDs del paso anterior y descarga los metadatos de cada vuelo.

    # Usa la ruta a la carpeta creada en el paso 1
    python scripts/get_summaries.py data/flights/run_...

### Paso 3: Preparar Líneas de Tiempo

Prepara los datos para la fase de reconstrucción, creando un mapa de los tiempos de actividad de cada vuelo.

    # Usa la misma ruta de carpeta
    python scripts/prepare_timelines.py data/flights/run_...

### Paso 4: Reconstruir Rutas (Costoso)

Este es el script que consume la mayor parte de los créditos. Toma snapshots periódicos para recolectar los puntos de posición de los vuelos.

    # Usa la misma ruta de carpeta
    python scripts/reconstruct_paths.py data/flights/run_...

### Paso 5: Ensamblar Datos de Ruta

Une todas las piezas de los snapshots crudos para construir el archivo final con las rutas de vuelo completas.

    # Usa la misma ruta de carpeta
    python scripts/assemble_flights.py data/flights/run_...

### Paso 6: Procesar Cálculos Finales

Lee las rutas de vuelo ensambladas y realiza todos los cálculos de fases, combustible y CO₂.

    # Usa la misma ruta de carpeta
    python scripts/process_data.py data/flights/run_...

### Paso 7: Cargar a la Base de Datos

Encuentra los datos procesados más recientes y los carga en tu base de datos PostgreSQL.

    python database/seeder/seeder.py
