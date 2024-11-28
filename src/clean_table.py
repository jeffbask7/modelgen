import psycopg2
from datetime import datetime



def pg_conn():
    # Database connection details
    DB_HOST = 'localhost'
    DB_PORT = '5432'
    DB_NAME = 'geoserver_db'
    DB_USER = 'geoserver_user'
    DB_PASS = 'geoserver'

    # Connect to the PostGIS database
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )

    # Create a cursor object
    cur = conn.cursor()
    return conn, cur

def tableClean():
    conn, cur = pg_conn()
    cutoff_time = datetime(2024, 11, 20, 0, 0)  # Replace with your desired datetime

    # SQL query to delete rows
    delete_query = """DELETE FROM model_image.image_meta WHERE "initTime" < %s"""

    # Execute the delete query
    with conn.cursor() as cur:
        cur.execute(delete_query, (cutoff_time,))
        conn.commit()  

    # Close the connection
    conn.close()

tableClean()