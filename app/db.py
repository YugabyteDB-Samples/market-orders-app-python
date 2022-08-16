from os import getenv
import psycopg2
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()  # Load environment variables from .env file

# Database connection configuration
db_config = {
        "host": getenv("DB_HOST", "us-west-2.32e262a9-a69d-4a0c-a55f-4d9ede43575b.aws.ybdb.io"),
        "port": getenv("DB_PORT", "5433"),
        "dbname": getenv("DB_NAME", "yugabyte"),
        "user": getenv("DB_USER", "admin"),
        "password": getenv("DB_PASSWORD", "9tz_zVt-2hjCu0STkIMZUmqOEbs80F")
    }



def database_connection():
    """Returns the database connection"""
    try:
        conn = psycopg2.connect(**db_config)
        return conn
    except Exception as e:
        print(">>>>>>> Error connecting to database:", e)
        exit(1)


def write_trade_to_db(connection, message, user_id):
    """Write a trade to the database"""
    order_quantity = message['order_quantity']
    trade_type = message['trade_type']
    symbol = message['symbol']
    # trade_time = message['timestamp']
    bid_price = message['bid_price']
    insert_query = f'insert into public."Trade"(user_id, bid_price, order_quantity, trade_time, trade_type, symbol) values (\'{user_id}\',\'{bid_price}\',\'{order_quantity}\',NOW(),\'{trade_type}\',\'{symbol}\');'
    conn = database_connection()
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    cur.execute(insert_query)
    print(">>>>>>> Inserted trade into database")
    cur.close()


def init_db():
    """Initialize the database with default data"""

    # Read table schema from .sql file
    create_sql_schema = Path('schema/schema.sql').read_text()

    # Create the database connection.
    conn = database_connection()
    print(">>>>>>> Connecting to database...")
    # Open a cursor to perform database operations.
    # The default mode for psycopg2 is "autocommit=false".
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    # Create the table & insert new rows into the database table
    cur.execute(create_sql_schema)
    print("Created schema with default data")
    cur.close()


if __name__ == "__main__":
    connection = init_db()
    print(">>>>>>> Connected to database...")
