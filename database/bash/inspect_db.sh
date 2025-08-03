#!/bin/bash
# =================================================================
#  Inspects the database structure, data samples, and aggregates.
#  Reads configuration from the project's .env file.
# =================================================================
set -e

# --- Load Configuration from .env file ---
if [ -f .env ]; then
  export $(grep -v '^#' .env | sed -e 's/^"//' -e 's/"$//' | xargs)
fi

# --- Configuration (with fallbacks) ---
CONTAINER_NAME="flights-db-container"
DB_USER=${DB_USER:-"postgres"}
DB_NAME=${DB_NAME:-"postgres"}

# --- Pre-flight Check ---
if ! docker ps -f name=^/${CONTAINER_NAME}$ | grep -q $CONTAINER_NAME; then
    echo "❌ Error: The Docker container '$CONTAINER_NAME' is not running."
    exit 1
fi

echo "========================================"
echo "🔍 Inspecting Database: $DB_NAME"
echo "========================================"
echo ""

# --- 1. View Structure: flights table ---
echo "----------------------------------------"
echo "🏛  Structure: flights table"
echo "----------------------------------------"
docker exec $CONTAINER_NAME psql -U $DB_USER -d $DB_NAME -c "\d flights"

echo ""

# --- 2. View Structure: flight_positions table ---
echo "----------------------------------------"
echo "🏛  Structure: flight_positions table"
echo "----------------------------------------"
docker exec $CONTAINER_NAME psql -U $DB_USER -d $DB_NAME -c "\d flight_positions"

echo ""

# --- 3. View Data Sample: flights table ---
echo "----------------------------------------"
echo "📊 Data Sample (Top 5): flights table"
echo "----------------------------------------"
docker exec $CONTAINER_NAME psql -U $DB_USER -d $DB_NAME -c "SELECT flight_id, fr24_id, callsign, departure_icao, arrival_icao, co2_total_kg FROM flights ORDER BY created_at DESC LIMIT 5;"

echo ""

# --- 4. View Data Sample: flight_positions table ---
echo "----------------------------------------"
echo "📊 Data Sample (Top 5): flight_positions table"
echo "----------------------------------------"
docker exec $CONTAINER_NAME psql -U $DB_USER -d $DB_NAME -c "SELECT position_id, flight_id, \"timestamp\", latitude, longitude FROM flight_positions ORDER BY position_id DESC LIMIT 5;"

echo ""

# --- 5. Data Aggregates & Validation ---
echo "----------------------------------------"
echo "📈 Data Aggregates & Validation"
echo "----------------------------------------"
echo "-> Total Flights:"
docker exec $CONTAINER_NAME psql -U $DB_USER -d $DB_NAME -c "SELECT COUNT(*) FROM flights;"
echo "-> Total Flight Positions:"
docker exec $CONTAINER_NAME psql -U $DB_USER -d $DB_NAME -c "SELECT COUNT(*) FROM flight_positions;"
echo "-> Flights with Most Position Points (Top 5):"
docker exec $CONTAINER_NAME psql -U $DB_USER -d $DB_NAME -c "SELECT f.callsign, COUNT(p.position_id) AS position_count FROM flights f JOIN flight_positions p ON f.flight_id = p.flight_id GROUP BY f.callsign ORDER BY position_count DESC LIMIT 5;"


echo ""
echo "✅ Inspection complete."
