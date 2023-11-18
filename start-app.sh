#! /bin/bash

# Verify that kafka is running
if ! netstat -tunlp 2>/dev/null | grep -q 9092; then
    echo "Kafka is not running. Please start Kafka and try again."
    echo "You can use the following command to start Kafka:"
    echo "docker compose  -f \"docker-compose.yml\" up -d --build broker"
    exit 1
fi

# Verify that schema registry is running
if ! netstat -nlp 2>/dev/null | grep -q 8081; then
    echo "Schema Registry is not running. Please start Schema Registry and try again."
    echo "You can use the following command to start Schema Registry:"
    echo "docker compose  -f \"docker-compose.yml\" up -d --build schema-registry"
    exit 1
fi

# Verify that the DB is running
if ! netstat -nlp 2>/dev/null | grep -q 5432; then
    echo "Postgres is not running. Please start Postgres and try again."
    echo "You can use the following command to start Postgres:"
    echo "docker compose  -f \"docker-compose.yml\" up -d --build database"
    exit 1
fi

# If the first arg is "clean", then clean the database
if [ "$1" == "clean" ]; then
    echo "Cleaning the database..."
    docker exec -it postgres psql -U postgres -c "DROP DATABASE IF EXISTS orderbook"
    docker exec -it postgres psql -U postgres -c "CREATE DATABASE orderbook"
    echo "Database cleaned."
fi

# Kill all xterm windows
killall xterm

# Start the Order Stream
xterm -e "/bin/bash $(pwd)/gradlew runOrderStream" &
sleep 1

# Start the Trade Stream
xterm -e "/bin/bash $(pwd)/gradlew runTradeStream" &
sleep 1

# Start the Market Matcher
xterm -e "/bin/bash $(pwd)/gradlew runMarketMatcher" &
sleep 10

# Start the Order producer
xterm -e "/bin/bash $(pwd)/gradlew runProducer" &
sleep 1

# Start the Trade consumer
xterm -e "/bin/bash $(pwd)/gradlew runConsumer" &

# Wait for the user to press Ctrl+C
while true; do sleep 1; done
