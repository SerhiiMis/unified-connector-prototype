import os
import psycopg2
from dotenv import load_dotenv
from connectors.jsonplaceholder_connector import JsonPlaceholderConnector

load_dotenv() # Loads DATABASE_URL form .env

def init_db(conn):
    with conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS records (
            source TEXT,
            record_id INTEGER,
            payload JSONB,
            fetched_at TIMESTAMPTZ DEFAULT NOW()
        );
        """)
        conn.commit()

def insert_records(conn, records):
    with conn.cursor() as cur:
        for rec in records:
            cur.execute(
                "INSERT INTO records (source, record_id, payload) VALUES (%s, %s, %s)",
                (rec["source"], rec["record_id"], rec["payload"])
            )
        conn.commit()

def main():
    connector = JsonPlaceholderConnector()
    raw = connector.fetch()
    normalized = connector.normalize(raw)

    conn = psycopg2.connect(os.getenv("DATABASE_URL"))
    init_db(conn)
    insert_records(conn, normalized)
    print(f"Inserted {len(normalized)} records into the database.")

if __name__ == "__main__":
    main()

