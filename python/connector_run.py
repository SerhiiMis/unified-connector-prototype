import os
import psycopg2
from psycopg2.extras import Json
from dotenv import load_dotenv
from connectors.jsonplaceholder_connector import  JSONPlaceholderConnector
import subprocess
import json


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
                (rec["source"], rec["record_id"], Json(rec["payload"]))
            )
    conn.commit()

def run_binary_connector(conn):
    # Path to the C++ binary you just built
    bin_path = os.path.abspath(os.path.join("..", "cpp", "binary_log_reader"))
    sample_file = os.path.abspath(os.path.join("..", "cpp", "sample_logs", "record.bin"))

    # Invoke the C++ reader
    result = subprocess.run(
        [bin_path, sample_file],
        capture_output=True,
        text=True,
        check=True
    )
    # Parse its JSON output
    items = json.loads(result.stdout)

    # Prepare records: use "binarylog" as the source
    records = [
        {"source": "binarylog", "record_id": rec["id"], "payload": rec}
        for rec in items
    ]

    # Insert into the same records table
    with conn.cursor() as cur:
        for rec in records:
            cur.execute(
                "INSERT INTO records (source, record_id, payload) VALUES (%s, %s, %s)",
                (rec["source"], rec["record_id"], Json(rec["payload"]))
            )
    conn.commit()
    print(f"Inserted {len(records)} binary-log records.")

def main():
    connector =  JSONPlaceholderConnector()
    raw = connector.fetch()
    normalized = connector.normalize(raw)

    conn = psycopg2.connect(os.getenv("DATABASE_URL"))
    init_db(conn)
    insert_records(conn, normalized)
    print(f"Inserted {len(normalized)} records into the database.")

        # After the JSONPlaceholder run
    run_binary_connector(conn)


if __name__ == "__main__":
    main()

