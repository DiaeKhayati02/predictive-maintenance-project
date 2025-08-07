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
import tensorflow as tf

# DB config
DB_CONFIG = {
    'host': 'postgres',
    'port': 5432,
    'dbname': 'maintenance_db',
    'user': 'admin',
    'password': 'password'
}

# Model path
MODEL_PATH = '/opt/airflow/dags/lstm_model.h5'
SCALER_PATH = '/opt/airflow/dags/scaler.pkl'

SEQUENCE_LENGTH = 5  # Past steps to use for prediction

def load_model_and_scaler():
    if not os.path.exists(MODEL_PATH) or not os.path.exists(SCALER_PATH):
        raise FileNotFoundError("Model or scaler files not found")
    model = tf.keras.models.load_model(
        MODEL_PATH,
        custom_objects={'mse': tf.keras.losses.MeanSquaredError()}
    )
    scaler = joblib.load(SCALER_PATH)
    logging.info("Loaded LSTM model and scaler")
    return model, scaler

def fetch_latest_telemetry(machine_id=30):
    """
    Fetch last SEQUENCE_LENGTH + 1 rows (we need the last 5 steps to predict the next 1 step)
    """
    conn = psycopg2.connect(**DB_CONFIG)
    query = f"""
        SELECT datetime, volt, rotate, pressure, vibration
        FROM machine_data
        WHERE machine_id = %s
        ORDER BY datetime DESC
        LIMIT {SEQUENCE_LENGTH + 1};
    """
    df = pd.read_sql(query, conn, params=(machine_id,))
    conn.close()
    if df.shape[0] < SEQUENCE_LENGTH + 1:
        raise ValueError(f"Not enough data to predict for machine {machine_id}")
    # Sort ascending (oldest first) for LSTM input
    df = df.sort_values('datetime').reset_index(drop=True)
    return df

def predict_next_step(**context):
    model, scaler = load_model_and_scaler()
    machine_id = 0
    
    # Get last processed datetime from Airflow variable or use a default (start of data)
    last_processed_time_str = Variable.get(f"last_processed_time_machine_{machine_id}", default_var=None)
    
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    if last_processed_time_str and last_processed_time_str != 'None':
        last_processed_time = datetime.fromisoformat(last_processed_time_str)
    else:
        # Get earliest datetime from data for this machine
        cursor.execute("""
            SELECT MIN(datetime) FROM machine_data WHERE machine_id = %s;
        """, (machine_id,))
        last_processed_time = cursor.fetchone()[0]
        if last_processed_time is None:
            cursor.close()
            conn.close()
            logging.error(f"No data found for machine {machine_id}")
            raise ValueError(f"No data found for machine {machine_id}")
    
    # Fetch 6 rows starting from last_processed_time (including it)
    cursor.execute("""
        SELECT datetime, volt, rotate, pressure, vibration
        FROM machine_data
        WHERE machine_id = %s
          AND datetime >= %s
        ORDER BY datetime ASC
        LIMIT 6;
    """, (machine_id, last_processed_time))
    
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    
    if len(rows) < 6:
        logging.info("No more data to predict. Resetting to beginning.")
        Variable.set(f"last_processed_time_machine_{machine_id}", None)
        return
    
    # Prepare dataframe
    df = pd.DataFrame(rows, columns=['datetime', 'volt', 'rotate', 'pressure', 'vibration'])
    
    # Log the rows used for prediction (first 5 rows)
    logging.info("Rows used for prediction (first 5 rows):")
    logging.info(df.iloc[:-1].to_string(index=False))
    
    # Prepare features
    features = df[['volt', 'rotate', 'pressure', 'vibration']].values
    scaled_features = scaler.transform(features)
    input_seq = np.expand_dims(scaled_features[:-1], axis=0)  # (1,5,4)
    pred_scaled = model.predict(input_seq)[0]
    pred_original = scaler.inverse_transform([pred_scaled])[0]
    
    prediction_time = df['datetime'].iloc[-1] + timedelta(minutes=1)
    
    # Save prediction
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO machine_predictions (machine_id, datetime, volt, rotate, pressure, vibration)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (machine_id, prediction_time, *pred_original))
    conn.commit()
    cursor.close()
    conn.close()
    
    # Update last_processed_time to the second row datetime (move window by 1 step)
    new_last_time = df['datetime'].iloc[1]
    Variable.set(f"last_processed_time_machine_{machine_id}", new_last_time.isoformat())
    
    logging.info(f"Prediction for machine {machine_id} at {prediction_time}: "
                 f"volt={pred_original[0]:.3f}, rotate={pred_original[1]:.3f}, "
                 f"pressure={pred_original[2]:.3f}, vibration={pred_original[3]:.3f}")
    logging.info("Prediction saved to DB")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='lstm_telemetry_prediction',
    default_args=default_args,
    description='LSTM sequence prediction on telemetry data every 5 minutes',
    schedule_interval='*/1 * * * *',
    catchup=False,
    max_active_runs=1,
) as dag:

    predict = PythonOperator(
        task_id='predict_next_step',
        python_callable=predict_next_step,
        provide_context=True
    )
