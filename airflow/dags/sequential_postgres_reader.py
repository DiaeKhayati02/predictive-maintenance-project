from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import psycopg2
import logging

# DB config
DB_CONFIG = {
    'host': 'postgres',
    'port': 5432,
    'dbname': 'maintenance_db',
    'user': 'admin',
    'password': 'password'
}

def fetch_next_row(**context):
    """
    Fetch the next row based on a counter stored in Airflow Variables.
    This ensures we get the next row each time the task runs.
    """
    try:
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
        
        # Fetch the next row using OFFSET
        cursor.execute("SELECT * FROM raw_data ORDER BY id LIMIT 1 OFFSET %s;", (current_offset,))
        row = cursor.fetchone()
        
        if row:
            print(f"Fetched row {current_offset + 1}: {row}")
            logging.info(f"Successfully fetched row {current_offset + 1}: {row}")
            
            # Increment the offset for next run
            Variable.set("row_offset", current_offset + 1)
        else:
            print(f"No row found at offset {current_offset}")
            logging.warning(f"No row found at offset {current_offset}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logging.error(f"Error fetching row: {e}")
        print(f"Error fetching row: {e}")
        raise

def initialize_table(**context):
    """
    Initialize the table if it doesn't exist and add some sample data for testing.
    """
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # Create table if it doesn't exist
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS raw_data (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100),
                value INTEGER,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        # Check if table is empty and add sample data
        cursor.execute("SELECT COUNT(*) FROM raw_data;")
        count = cursor.fetchone()[0]
        
        if count == 0:
            # Insert sample data
            sample_data = [
                ('Sample 1', 100),
                ('Sample 2', 200),
                ('Sample 3', 300),
                ('Sample 4', 400),
                ('Sample 5', 500)
            ]
            
            cursor.executemany(
                "INSERT INTO raw_data (name, value) VALUES (%s, %s);",
                sample_data
            )
            
            print(f"Inserted {len(sample_data)} sample rows")
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print("Table initialization completed")
        
    except Exception as e:
        logging.error(f"Error initializing table: {e}")
        print(f"Error initializing table: {e}")
        raise

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
    dag_id='sequential_postgres_reader',
    default_args=default_args,
    description='Read next row from PostgreSQL every minute',
    schedule_interval='*/1 * * * *',  # Every 1 minute
    catchup=False,
    max_active_runs=1,  # Ensure only one instance runs at a time
) as dag:
    
    # Task to initialize table (runs once)
    init_task = PythonOperator(
        task_id='initialize_table',
        python_callable=initialize_table,
        provide_context=True
    )
    
    # Task to fetch next row
    fetch_task = PythonOperator(
        task_id='fetch_next_row',
        python_callable=fetch_next_row,
        provide_context=True
    )
    
    # Set task dependencies
    init_task >> fetch_task