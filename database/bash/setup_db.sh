#!/bin/bash

# =================================================================
#  Bash script to set up the PostgreSQL database schema.
#  - Enables the PostGIS extension.
#  - Executes a .sql file to create tables.
# =================================================================

# --- Configuration ---
# These should match the values in your start_postgres.sh script
CONTAINER_NAME="postgres-db"
DB_USER="listerineh"
DB_NAME="flights-db"
SQL_FILE="database/sql/generation.sql"

# --- Pre-flight Check ---
# Check if the Docker container is running
if ! docker ps -f name=^/${CONTAINER_NAME}$ | grep -q $CONTAINER_NAME; then
    echo "❌ Error: The Docker container '$CONTAINER_NAME' is not running."
    echo "-> Please run ./start_postgres.sh first."
    exit 1
fi

# Check if the SQL file exists
if [ ! -f "$SQL_FILE" ]; then
    echo "❌ Error: SQL file '$SQL_FILE' not found in the current directory."
    exit 1
fi

# --- Step 1: Enable PostGIS Extension ---
# We execute this as the 'postgres' superuser inside the container for privileges.
echo "-> Enabling PostGIS extension in the database '$DB_NAME'..."
docker exec $CONTAINER_NAME psql -U postgres -d $DB_NAME -c "CREATE EXTENSION IF NOT EXISTS postgis;"


# --- Step 2: Copy the SQL file into the container ---
# The file is copied to the /tmp/ directory inside the container.
echo "-> Copying '$SQL_FILE' into the container..."
docker cp "$SQL_FILE" "${CONTAINER_NAME}:/tmp/generation.sql"


# --- Step 3: Execute the SQL file to create tables ---
# We execute the file using the specific user for our database.
echo "-> Executing '$SQL_FILE' to generate tables..."
docker exec $CONTAINER_NAME psql -U $DB_USER -d $DB_NAME -f /tmp/generation.sql

echo ""
echo "✅ Database schema and extensions configured successfully!"