#!/bin/bash

# =================================================================
#  Bash script to start a PostgreSQL container using Docker.
# =================================================================

# --- Configuration ---
# Customize these variables for your project.
DB_PASSWORD="listerineh-test"
DB_USER="listerineh"
DB_NAME="flights-db"
CONTAINER_NAME="postgres-db"
VOLUME_NAME="pgdata"
DB_PORT="5432"

# Stop and remove any existing container with the same name
if [ $(docker ps -a -q -f name=^/${CONTAINER_NAME}$) ]; then
    echo "-> Stopping and removing existing container named '$CONTAINER_NAME'..."
    docker stop $CONTAINER_NAME
    docker rm $CONTAINER_NAME
fi

# Pull the latest official PostgreSQL image
echo "-> Pulling latest postgres image..."
docker pull postgres

# Start the new PostgreSQL container
echo "-> Starting PostgreSQL container '$CONTAINER_NAME'..."
docker run --name $CONTAINER_NAME \
    -e POSTGRES_PASSWORD=$DB_PASSWORD \
    -e POSTGRES_USER=$DB_USER \
    -e POSTGRES_DB=$DB_NAME \
    -p $DB_PORT:5432 \
    -v $VOLUME_NAME:/var/lib/postgresql/data \
    -d \
    postgres

echo ""
echo "✅ PostgreSQL container started successfully!"
echo "------------------------------------------"
echo "  Host:         localhost"
echo "  Port:         $DB_PORT"
echo "  Database:     $DB_NAME"
echo "  User:         $DB_USER"
echo "  Password:     $DB_PASSWORD"
echo "------------------------------------------"