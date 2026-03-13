#!/bin/bash
set -e

echo "========================================="
echo "Payment Gateway Data Generator & Streamer"
echo "========================================="

# Function to check if MySQL is ready
wait_for_mysql() {
    echo "Waiting for MySQL to be ready..."
    while ! mysqladmin ping -h"$MYSQL_HOST" -P"$MYSQL_PORT" -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" --silent; do
        sleep 2
    done
    echo "✓ MySQL is ready"
}

# Wait for MySQL
wait_for_mysql

# Check if data has already been generated
if [ ! -f /app/.data_generated ]; then
    echo ""
    echo "📦 Running initial data generation (one-time setup)..."
    echo "========================================="
    
    # Run the data generator
    python /app/generate_data.py \
        --host "$MYSQL_HOST" \
        --port "$MYSQL_PORT" \
        --user "$MYSQL_USER" \
        --password "$MYSQL_PASSWORD" \
        --database "$MYSQL_DATABASE"
    
    # Check if data generation was successful
    if [ $? -eq 0 ]; then
        touch /app/.data_generated
        echo "✅ Initial data generation completed successfully"
    else
        echo "❌ Data generation failed"
        exit 1
    fi
else
    echo ""
    echo "✓ Initial data already generated, skipping..."
fi

echo ""
echo "🚀 Starting real-time data stream..."
echo "========================================="
echo "Press Ctrl+C to stop"
echo ""

# Run the streamer
exec python /app/stream_data.py \
    --host "$MYSQL_HOST" \
    --port "$MYSQL_PORT" \
    --user "$MYSQL_USER" \
    --password "$MYSQL_PASSWORD" \
    --database "$MYSQL_DATABASE"