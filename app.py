
from flask import Flask, jsonify, render_template
import psycopg2
import pandas as pd
from datetime import datetime
import logging
import os

# Database configuration (same as in your DAG)
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'postgres'),  # Use env var or default to 'postgres'
    'port': os.getenv('DB_PORT', 5432),
    'dbname': os.getenv('DB_NAME', 'maintenance_db'),
    'user': os.getenv('DB_USER', 'admin'),
    'password': os.getenv('DB_PASSWORD', 'password')
}

# Airflow database configuration for resetting variables
AIRFLOW_DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'postgres'),
    'port': os.getenv('DB_PORT', 5432),
    'dbname': os.getenv('DB_NAME', 'maintenance_db'),  # Airflow uses same DB
    'user': os.getenv('DB_USER', 'admin'),
    'password': os.getenv('DB_PASSWORD', 'password')
}

# Initialize Flask app
app = Flask(__name__)

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def fetch_predictions(machine_id=0, limit=100):
    """Fetch the latest predictions from the machine_predictions table."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        query = """
            SELECT datetime, volt, rotate, pressure, vibration
            FROM machine_predictions
            WHERE machine_id = %s
            ORDER BY datetime DESC
            LIMIT %s;
        """
        df = pd.read_sql(query, conn, params=(machine_id, limit))
        conn.close()
        
        # Sort by datetime ascending for charting (oldest first)
        df = df.sort_values('datetime').reset_index(drop=True)
        
        # Convert datetime to string for JSON serialization
        df['datetime'] = df['datetime'].apply(lambda x: x.isoformat())
        return df.to_dict(orient='records')
    except Exception as e:
        logger.error(f"Error fetching predictions: {e}")
        return []

def reset_airflow_variable(machine_id=0):
    """Reset the last_processed_time variable in the Airflow variable table."""
    try:
        conn = psycopg2.connect(**AIRFLOW_DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute("""
            DELETE FROM variable WHERE key = %s;
        """, (f"last_processed_time_machine_{machine_id}",))
        conn.commit()
        cursor.close()
        conn.close()
        logger.info(f"Reset last_processed_time for machine {machine_id}")
        return {"status": "success", "message": f"Reset last_processed_time for machine {machine_id}"}
    except Exception as e:
        logger.error(f"Error resetting Airflow variable: {e}")
        return {"status": "error", "message": str(e)}

@app.route('/')
def index():
    """Render the main page with charts."""
    return render_template('index.html')

@app.route('/api/predictions')
def get_predictions():
    """API endpoint to fetch predictions as JSON."""
    predictions = fetch_predictions(machine_id=0, limit=100)
    return jsonify(predictions)

@app.route('/api/reset/<int:machine_id>', methods=['POST'])
def reset_variable(machine_id):
    """API endpoint to reset the Airflow variable for a given machine_id."""
    result = reset_airflow_variable(machine_id)
    return jsonify(result)

if __name__ == '__main__':
    # Optionally reset variable on startup
    if os.getenv('RESET_ON_STARTUP', 'false').lower() == 'true':
        reset_airflow_variable(machine_id=0)
    app.run(host='0.0.0.0', port=5000, debug=True)
