from os import getenv
import pathlib

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


def main(db_config):
  print(">>>>>>> Connecting to database...")
  try:
    db_conn = psycopg2.connect(**db_config)
    print(">>>>>>> Connected to database.")
  except Exception as e:
    print(">>>>>>> Error connecting to database:", e)
    exit(1)

  create_table_with_insert()





# Read table schema from .sql file
sql_schema = Path('sample_schema.sql').read_text()

# Create the database connection.

conn = psycopg2.connect(**db_config)

# Open a cursor to perform database operations.
# The default mode for psycopg2 is "autocommit=false".

conn.set_session(autocommit=True)
cur = conn.cursor()

# Create the table. (It might preexist.)

cur.execute(sql_schema)
print("Created table employee")
cur.close()

# Take advantage of ordinary, transactional behavior for DMLs.

conn.set_session(autocommit=False)
cur = conn.cursor()

# Insert a row.

cur.execute(
    "INSERT INTO employee (id, name, age, language) VALUES (%s, %s, %s, %s)",
    (1, "John", 35, "Python"),
)
print("Inserted (id, name, age, language) = (1, 'John', 35, 'Python')")

# Query the row.
cur.execute("SELECT name, age, language FROM employee WHERE id = 1")
row = cur.fetchone()
print("Query returned: %s, %s, %s" % (row[0], row[1], row[2]))

# Commit and close down.

conn.commit()
cur.close()
conn.close()
