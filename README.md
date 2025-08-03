# Flight Data Processing Pipeline

This project provides a robust, end-to-end pipeline for fetching flight data from the FlightRadar24 API, calculating detailed flight metrics, and seeding the results into a PostgreSQL database for analysis.

The pipeline first acquires flight data using a credit-optimized, airport-seeding method. It then processes this raw data to calculate flight path distances, detect distinct flight phases, and estimate fuel consumption and CO₂ emissions. Finally, it uploads the clean, processed data into a relational database.

---
## Key Features

- **Credit-Optimized Acquisition**: Avoids expensive API calls by using a smart airport-seeding strategy to gather flight IDs before fetching their full, high-resolution data.
- **Modular & Robust Pipeline**: A clean multi-stage process separates acquisition, processing, and database seeding into distinct, manageable scripts.
- **Detailed Flight Analysis**: Calculates great-circle vs. actual path distance, detects flight phase durations, and provides estimates for fuel burn and CO₂ emissions.
- **Database Integration**: Seeds all processed data into a well-structured PostgreSQL database, enabling robust querying and analysis.
- **Curated Performance Data**: Utilizes a stable, manually curated `fuel_profiles.json` file for reliable performance calculations.
- **Expandable & Data-Driven**: Includes a workflow for analyzing results to identify and add missing aircraft profiles over time, continuously improving data quality.

---
## Project Structure

    .
    ├── data/
    │   ├── airports.txt
    │   ├── fuel_profiles.json
    │   └── flights/
    │       └── run_YYYY-MM-DD_HH-MM-SS/
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
    │   ├── acquire_data.py
    │   └── process_data.py
    ├── .env
    └── requirements.txt

---
## Setup

1.  **Project Files**: Ensure all project files are in their correct locations as per the structure above.

2.  **Create `.env` File**: Create a file named `.env` in the root directory. Add your API key and PostgreSQL credentials:

        # API Credentials
        PROD_FR24_API_KEY="your_actual_api_key"

        # Database Credentials
        DB_HOST="your_db_host"
        DB_PORT="5432"
        DB_USER="your_db_user"
        DB_PASSWORD="your_db_password"
        DB_NAME="your_db_name"

3.  **Set Up PostgreSQL Database**: Before running the seeder, create the necessary tables in your database. You can use the SQL commands provided in `database/schema.sql`.

4.  **Prepare Input Files**: Populate `data/airports.txt` with ICAO codes. The `data/fuel_profiles.json` file is ready to use but can be customized.

5.  **Install Dependencies**: Install the necessary Python libraries.

        pip install -r requirements.txt

---
## Usage / Workflow

The pipeline is a sequential process. All commands should be run from the project's root directory.

### Step 1: Acquire Raw Data

This script reads your `data/airports.txt` file, fetches recent flight IDs, and then acquires the full data for each flight. It creates a unique `run_<timestamp>` directory inside `data/flights/`.

    python scripts/acquire_data.py

### Step 2: Process the Data

This script reads the raw data generated in Step 1, performs all calculations, and saves the final results into the same `run_<timestamp>` directory.

    python scripts/process_data.py

### Step 3: Seed the Database

This final script finds the latest processed data from Step 2 and efficiently uploads all flight summaries and position data into your PostgreSQL database.

    python database/seeder/seeder.py

### (Optional) Step 4: Analyze & Enhance

To improve the accuracy of your fuel calculations over time, you can systematically expand your `fuel_profiles.json`.

1.  **Run the Pipeline** as described above.
2.  **Find the Gaps**: Inspect your final `flights_processed.json` file and find flights where `"aircraft_model": "default"`.
3.  **Prioritize & Expand**: Note the most common aircraft types that fell back to the default and manually add their profiles to `fuel_profiles.json`.
