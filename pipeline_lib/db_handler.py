# pipeline_lib/db_handler.py
import psycopg2
import logging

def get_db_connection(db_config):
    """Establishes and returns a database connection."""
    try:
        conn = psycopg2.connect(
            dbname=db_config['dbname'],
            user=db_config['user'],
            password=db_config['password'],
            host=db_config['host'],
            port=db_config['port']
        )
        logging.info("Database connection established successfully.")
        return conn
    except psycopg2.OperationalError as e:
        logging.error(f"Database connection failed: {e}")
        return None