#!/bin/bash
# =================================================================
#  Starts a PostGIS database container using Docker.
#  Reads configuration from the project's .env file.
# =================================================================
set -e

# --- Load Configuration from .env file ---
if [ -f .env ]; then
  # export variables
  source .env
fi

# --- Configuration (with fallbacks if .env is not set) ---
DB_PASSWORD=${DB_PASSWORD:-"your_fallback_password"}
DB_USER=${DB_USER:-"postgres"}
DB_NAME=${DB_NAME:-"postgres"}
CONTAINER_NAME="flights-db-container"
VOLUME_NAME="flights-pgdata"
DB_PORT=${DB_PORT:-"5432"}
IMAGE_NAME="postgres:16"

# --- Main Script ---
# Stop and remove any existing container with the same name
if [ "$(docker ps -a -q -f name=^/${CONTAINER_NAME}$)" ]; then
    echo "-> Stopping and removing existing container named '$CONTAINER_NAME'..."
    docker stop $CONTAINER_NAME
    docker rm $CONTAINER_NAME
fi

# Pull the PostGIS image
echo "-> Pulling latest PostGIS image: $IMAGE_NAME..."
docker pull $IMAGE_NAME

# Start the new PostGIS container
echo "-> Starting PostGIS container '$CONTAINER_NAME'..."
docker run --name $CONTAINER_NAME \
    -e POSTGRES_PASSWORD=$DB_PASSWORD \
    -e POSTGRES_USER=$DB_USER \
    -e POSTGRES_DB=$DB_NAME \
    -p $DB_PORT:5432 \
    -v $VOLUME_NAME:/var/lib/postgresql/data \
    -d \
    $IMAGE_NAME

echo ""
echo "✅ PostGIS container started successfully!"
echo "------------------------------------------"
echo "  Host:         localhost"
echo "  Port:         $DB_PORT"
echo "  Database:     $DB_NAME"
echo "  User:         $DB_USER"
echo "  Password:     (from your .env file)"
echo "------------------------------------------"
