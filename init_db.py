"""
init_db.py  —  Creates the 'kafka_demo' database and 'orders' table.
Run once before starting the server:  python init_db.py
"""
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

DB_HOST     = "localhost"
DB_PORT     = 5432
DB_USER     = "amkadian"
DB_PASSWORD = "Myjob@2017"
DB_NAME     = "kafka_demo"

# ── Step 1: create the database if it doesn't exist ───────────────────────────
conn = psycopg2.connect(
    host=DB_HOST, port=DB_PORT,
    user=DB_USER, password=DB_PASSWORD,
    dbname="postgres"          # connect to the default DB first
)
conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
cur = conn.cursor()

cur.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = %s", (DB_NAME,))
if cur.fetchone():
    print(f"[init_db] Database '{DB_NAME}' already exists — skipping creation.")
else:
    cur.execute(f"CREATE DATABASE {DB_NAME}")
    print(f"[init_db] Database '{DB_NAME}' created.")

cur.close()
conn.close()

# ── Step 2: create the orders table ───────────────────────────────────────────
conn = psycopg2.connect(
    host=DB_HOST, port=DB_PORT,
    user=DB_USER, password=DB_PASSWORD,
    dbname=DB_NAME
)
cur = conn.cursor()

cur.execute("""
    CREATE TABLE IF NOT EXISTS orders (
        order_id    VARCHAR(20)     PRIMARY KEY,
        customer    VARCHAR(100)    NOT NULL,
        item        VARCHAR(200)    NOT NULL,
        quantity    INTEGER         NOT NULL DEFAULT 1,
        amount      DECIMAL(10, 2)  NOT NULL,
        status      VARCHAR(50)     NOT NULL DEFAULT 'NEW',
        received_at TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
""")
conn.commit()
print("[init_db] Table 'orders' is ready.")

cur.close()
conn.close()
print("[init_db] Initialisation complete. You can now run: python server.py")
