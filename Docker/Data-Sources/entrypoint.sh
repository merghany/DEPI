#!/bin/bash
set -e

echo "========================================="
echo "Payment Gateway Data Generator & Streamer"
echo "========================================="

# Function to check if MySQL is ready using Python
wait_for_mysql() {
    echo "Waiting for MySQL to be ready..."
    until python -c "
import mysql.connector
import time
import sys
try:
    conn = mysql.connector.connect(
        host='$MYSQL_HOST',
        port=$MYSQL_PORT,
        user='$MYSQL_USER',
        password='$MYSQL_PASSWORD',
        database='$MYSQL_DATABASE'
    )
    conn.close()
    sys.exit(0)
except Exception as e:
    sys.exit(1)
" 2>/dev/null; do
        echo "   MySQL not ready yet - sleeping..."
        sleep 3
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