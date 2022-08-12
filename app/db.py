from os import getenv
import psycopg2
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()  # Load environment variables from .env file

# Database connection configuration
db_config = {
        "host": getenv("DB_HOST", "http://127.0.0.1"),
        "port": getenv("DB_PORT", "5433"),
        "dbname": getenv("DB_NAME", "yugabyte"),
        "user": getenv("DB_USER", "yugabyte"),
        "password": getenv("DB_PASSWORD", "yugabyte")
    }


def write_to_db(connection, query_values):
    # TODO: write a SQL query to insert data into table
    trade_table_name = "Trade"
    buyers_table_name = "Buyer"
    #insert_query = f"INSERT INTO {table_name}"
    pass


def database_connection():
    """Returns the database connection"""
    try:
        conn = psycopg2.connect(**db_config)
        return conn
    except Exception as e:
        print(">>>>>>> Error connecting to database:", e)
        exit(1)


def init_db():
    """Initialize the database with default data"""

    # Read table schema from .sql file
    drop_sql_schema = Path('schema/drop.sql').read_text()
    insert_sql_schema = Path('schema/schema_postgres.sql').read_text()

    # Create the database connection.
    conn = database_connection()
    print(">>>>>>> Connecting to database...")
    # Open a cursor to perform database operations.
    # The default mode for psycopg2 is "autocommit=false".
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    # Create the table & insert new rows into the database table
    cur.execute(drop_sql_schema)
    cur.execute(insert_sql_schema)
    print("Created table")
    cur.close()


if __name__ == "__main__":
    connection = init_db()
    print(">>>>>>> Connected to database...")
