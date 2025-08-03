#!/bin/bash
# =================================================================
#  Sets up the database schema by executing a .sql file.
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
SQL_FILE="database/sql/generation.sql"

# --- Pre-flight Checks ---
if ! docker ps -f name=^/${CONTAINER_NAME}$ | grep -q $CONTAINER_NAME; then
    echo "❌ Error: The Docker container '$CONTAINER_NAME' is not running."
    echo "-> Please run ./start_postgres.sh first."
    exit 1
fi

if [ ! -f "$SQL_FILE" ]; then
    echo "❌ Error: SQL file '$SQL_FILE' not found."
    exit 1
fi

# --- Execute the SQL file to create tables ---
# We pipe the SQL file directly to psql inside the container.
# This is cleaner than copying the file in first.
echo "-> Executing '$SQL_FILE' to generate tables in database '$DB_NAME'..."
docker exec -i $CONTAINER_NAME psql -U $DB_USER -d $DB_NAME < "$SQL_FILE"

echo ""
echo "✅ Database schema configured successfully!"
