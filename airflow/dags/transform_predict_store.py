from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import psycopg2
import logging
import joblib
import pandas as pd
import numpy as np
import os

# DB config
DB_CONFIG = {
    'host': 'postgres',
    'port': 5432,
    'dbname': 'maintenance_db',
    'user': 'admin',
    'password': 'password'
}

# Model path
MODEL_PATH = '/opt/airflow/dags/model.joblib'

def load_model():
    """Load the trained random forest model"""
    try:
        if os.path.exists(MODEL_PATH):
            model = joblib.load(MODEL_PATH)
            logging.info(f"Model loaded successfully from {MODEL_PATH}")
            return model
        else:
            logging.error(f"Model file not found at {MODEL_PATH}")
            raise FileNotFoundError(f"Model file not found at {MODEL_PATH}")
    except Exception as e:
        logging.error(f"Error loading model: {e}")
        raise

def fetch_and_predict(**context):
    """
    Fetch the next row based on a counter stored in Airflow Variables,
    make a prediction using the random forest model, and store the result.
    """
    try:
        # Load the model
        model = load_model()
        
        # Get the current offset from Airflow Variables (starts at 0)
        try:
            current_offset = int(Variable.get("row_offset", default_var=0))
        except:
            current_offset = 0
            
        logging.info(f"Current offset: {current_offset}")
        
        # Connect to PostgreSQL
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # First, check if there are any rows available at this offset
        cursor.execute("SELECT COUNT(*) FROM raw_data;")
        total_rows = cursor.fetchone()[0]
        
        if current_offset >= total_rows:
            logging.info(f"No more rows available. Total rows: {total_rows}, Current offset: {current_offset}")
            print(f"No more rows available. Resetting to start. Total rows: {total_rows}")
            # Reset to beginning if we've reached the end
            current_offset = 0
            Variable.set("row_offset", current_offset)
        
        # Fetch the next row using OFFSET (get all columns but we'll exclude ID and failure column)
        cursor.execute("""
            SELECT product_id, air_temperature, process_temperature, rotational_speed, 
                   torque, tool_wear, failure_type 
            FROM raw_data ORDER BY id LIMIT 1 OFFSET %s;
        """, (current_offset,))
        row = cursor.fetchone()
        
        if row:
            product_id, air_temp, process_temp, rotational_speed, torque, tool_wear, actual_failure = row
            
            print(f"Fetched row {current_offset + 1}: Product ID: {product_id}")
            logging.info(f"Successfully fetched row {current_offset + 1}: {row}")
            
            # Prepare features for prediction (excluding Product ID and failure column)
            # Only use the 5 feature columns for prediction
            features = np.array([[air_temp, process_temp, rotational_speed, torque, tool_wear]])
            feature_names = ['Air temperature [K]', 'Process temperature [K]', 
                           'Rotational speed [rpm]', 'Torque [Nm]', 'Tool wear [min]']
            
            # Create DataFrame for better feature handling
            features_df = pd.DataFrame(features, columns=feature_names)
            
            # Make prediction
            prediction = model.predict(features_df)[0]
            prediction_proba = model.predict_proba(features_df)[0]
            
            # Get confidence (probability of predicted class)
            confidence = max(prediction_proba)
            
            print(f"Prediction for {product_id}: {prediction} (Confidence: {confidence:.3f})")
            
            # Log prediction details
            logging.info(f"Features used for prediction: {dict(zip(feature_names, features[0]))}")
            logging.info(f"Prediction: {prediction}, Confidence: {confidence:.3f}")
            logging.info(f"Prediction probabilities: No Failure: {prediction_proba[0]:.3f}, Failure: {prediction_proba[1]:.3f}")
            
            # Store prediction result alongside the original data
            cursor.execute("""
                INSERT INTO predictions (id, air_temp, process_temp, rotational_speed, 
                                       torque, tool_wear, predicted_failure, confidence, prediction_timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
            """, (product_id, air_temp, process_temp, rotational_speed, torque, tool_wear,
                  int(prediction), float(confidence), datetime.now()))
            
            conn.commit()
            
            # Increment the offset for next run
            Variable.set("row_offset", current_offset + 1)
            
            print(f"Prediction stored successfully for {product_id}")
            
        else:
            print(f"No row found at offset {current_offset}")
            logging.warning(f"No row found at offset {current_offset}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logging.error(f"Error in fetch_and_predict: {e}")
        print(f"Error in fetch_and_predict: {e}")
        raise

def initialize_tables(**context):
    """
    Initialize the tables if they don't exist and add some sample data for testing.
    """
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # Create raw_data table if it doesn't exist
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS raw_data (
                id SERIAL PRIMARY KEY,
                product_id VARCHAR(20),
                air_temp FLOAT,
                process_temp FLOAT,
                rotational_speed FLOAT,
                torque FLOAT,
                tool_wear FLOAT,
                actual_failure INTEGER,
                created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        # Create predictions table with only the features and prediction results
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS predictions (
                id SERIAL PRIMARY KEY,
                air_temp FLOAT,
                process_temp FLOAT,
                rotational_speed FLOAT,
                torque FLOAT,
                tool_wear FLOAT,
                predicted_failure INTEGER,
                confidence FLOAT,
                prediction_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        # Check if raw_data table is empty and add sample data
        cursor.execute("SELECT COUNT(*) FROM raw_data;")
        count = cursor.fetchone()[0]
        
        if count == 0:
            # Insert sample data based on your test data format
            sample_data = [
                ('L50177', 300.5, 309.8, 1345, 62.7, 153, 0),
                ('L52051', 303.7, 312.4, 1513, 40.1, 135, 0),
                ('L51038', 302.5, 311.4, 1559, 37.6, 209, 0),
                ('H30365', 295.6, 306.3, 1509, 35.8, 60, 0),
                ('H35877', 300.5, 310.0, 1358, 60.4, 102, 0),
                ('H32678', 301.3, 310.1, 1455, 44.1, 188, 0),
                ('L51688', 302.4, 310.0, 1426, 46.6, 102, 0),
                ('M16960', 299.3, 309.3, 1471, 46.0, 42, 0),
                ('L55065', 300.7, 312.3, 1590, 36.3, 93, 0),
                ('L49601', 298.9, 308.2, 2384, 15.0, 19, 0),
                ('L47308', 299.0, 308.6, 1587, 38.6, 124, 0),
                ('L54897', 300.5, 311.6, 1365, 53.0, 85, 0),
                ('M21064', 300.9, 310.8, 1519, 34.9, 73, 0),
                ('L47469', 298.1, 308.4, 1436, 46.7, 103, 0),
                ('H30335', 295.5, 305.9, 1593, 37.2, 197, 0),
                ('L52266', 304.0, 313.0, 1541, 45.9, 64, 0),
                ('L52286', 304.0, 313.3, 1519, 36.7, 117, 0),
                ('L52031', 303.7, 312.1, 1363, 51.8, 90, 1),
                ('L55929', 297.2, 308.4, 1677, 26.4, 146, 0),
                ('M14992', 298.7, 308.4, 1441, 43.2, 132, 0),
                ('L54561', 299.7, 310.3, 1495, 43.1, 101, 0),
                ('L48571', 298.9, 310.2, 2737, 8.8, 142, 1)
            ]
            
            cursor.executemany("""
                INSERT INTO raw_data (product_id, air_temp, process_temp, rotational_speed, 
                                    torque, tool_wear, actual_failure) 
                VALUES (%s, %s, %s, %s, %s, %s, %s);
            """, sample_data)
            
            print(f"Inserted {len(sample_data)} sample rows into raw_data")
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print("Tables initialization completed")
        
    except Exception as e:
        logging.error(f"Error initializing tables: {e}")
        print(f"Error initializing tables: {e}")
        raise

def view_recent_predictions(**context):
    """
    Optional task to view recent predictions with their data.
    """
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # Get recent predictions with all data
        cursor.execute("""
            SELECT product_id, air_temp, process_temp, rotational_speed, 
                   torque, tool_wear, predicted_failure, confidence, prediction_timestamp 
            FROM predictions 
            ORDER BY prediction_timestamp DESC 
            LIMIT 10;
        """)
        
        results = cursor.fetchall()
        
        if results:
            print("Recent Predictions with Data:")
            print("-" * 100)
            for row in results:
                product_id, air_temp, process_temp, rot_speed, torque, tool_wear, prediction, confidence, timestamp = row
                status = "FAILURE" if prediction == 1 else "NO FAILURE"
                print(f"Product: {product_id} | Temp: {air_temp}K/{process_temp}K | Speed: {rot_speed}rpm | "
                      f"Torque: {torque}Nm | Wear: {tool_wear}min")
                print(f"  → Prediction: {status} (Confidence: {confidence:.3f}) at {timestamp}")
                print("-" * 100)
            
            # Get prediction distribution
            cursor.execute("""
                SELECT predicted_failure, COUNT(*) 
                FROM predictions 
                GROUP BY predicted_failure;
            """)
            
            distribution = cursor.fetchall()
            print("\nPrediction Distribution:")
            for prediction, count in distribution:
                status = "FAILURE" if prediction == 1 else "NO FAILURE"
                print(f"  {status}: {count}")
                
        else:
            print("No predictions found yet")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logging.error(f"Error viewing predictions: {e}")
        print(f"Error viewing predictions: {e}")

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Define the DAG
with DAG(
    dag_id='ml_prediction_pipeline',
    default_args=default_args,
    description='Fetch data and make ML predictions every minute',
    schedule_interval='*/1 * * * *',  # Every 1 minute
    catchup=False,
    max_active_runs=1,  # Ensure only one instance runs at a time
) as dag:
    
    # Task to initialize tables (runs once)
    init_task = PythonOperator(
        task_id='initialize_tables',
        python_callable=initialize_tables,
        provide_context=True
    )
    
    # Task to fetch data and make predictions
    predict_task = PythonOperator(
        task_id='fetch_and_predict',
        python_callable=fetch_and_predict,
        provide_context=True
    )
    
    # Optional task to view recent predictions
    view_task = PythonOperator(
        task_id='view_recent_predictions',
        python_callable=view_recent_predictions,
        provide_context=True,
        trigger_rule='none_failed'  # Only run if previous tasks succeed
    )
    
    # Set task dependencies
    init_task >> predict_task >> view_task