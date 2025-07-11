#!/bin/bash

# =================================================================
#  Bash script to inspect the structure and data of the database.
# =================================================================

# --- Configuration ---
# These should match the values in your start_db.sh script
CONTAINER_NAME="postgres-db"
DB_USER="listerineh"
DB_NAME="flights-db"

# --- Pre-flight Check ---
# Check if the Docker container is running
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
echo "🏛 Structure: flights table"
echo "----------------------------------------"
docker exec $CONTAINER_NAME psql -U $DB_USER -d $DB_NAME -c "\d flights"

echo ""

# --- 2. View Structure: flight_positions table ---
echo "----------------------------------------"
echo "🏛 Structure: flight_positions table"
echo "----------------------------------------"
docker exec $CONTAINER_NAME psql -U $DB_USER -d $DB_NAME -c "\d flight_positions"

echo ""

# --- 3. View Data Sample: flights table ---
echo "----------------------------------------"
echo "📊 Data Sample (Top 10): flights table"
echo "----------------------------------------"
docker exec $CONTAINER_NAME psql -U $DB_USER -d $DB_NAME -c "SELECT * FROM flights LIMIT 10;"

echo ""

# --- 4. View Data Sample: flight_positions table ---
echo "----------------------------------------"
echo "📊 Data Sample (Top 10): flight_positions table"
echo "----------------------------------------"
docker exec $CONTAINER_NAME psql -U $DB_USER -d $DB_NAME -c "SELECT * FROM flight_positions LIMIT 10;"

echo ""
echo "✅ Inspection complete."