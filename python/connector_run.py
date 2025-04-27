import os
import psycopg2
from psycopg2.extras import Json
from dotenv import load_dotenv
from connectors.jsonplaceholder_connector import  JSONPlaceholderConnector
import subprocess
import json
from datetime import datetime, timezone


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
        cur.execute("""
        CREATE TABLE IF NOT EXISTS run_logs (
          id          SERIAL PRIMARY KEY,
          connector   TEXT NOT NULL,
          status      TEXT NOT NULL,
          started_at  TIMESTAMPTZ NOT NULL,
          finished_at TIMESTAMPTZ NOT NULL,
          message     TEXT
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

def run_with_logging(conn, name, func):
    """Run a connector function and log its outcome."""
    started = datetime.now(timezone.utc)
    status = "success"
    message = None

    try:
        func(conn)
    except Exception as e:
        status = "failure"
        message = str(e)
    finally:
        finished = datetime.now(timezone.utc)
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO run_logs (connector, status, started_at, finished_at, message)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (name, status, started, finished, message),
            )
        conn.commit()

    if status == "failure":
        # re-raise after logging so errors aren’t swallowed
        raise Exception(f"{name} run failed: {message}")

def main():
    load_dotenv()
    conn = psycopg2.connect(os.getenv("DATABASE_URL"))
    init_db(conn)

    # JSONPlaceholder connector
    def run_json(conn):
        raw = JSONPlaceholderConnector().fetch()
        recs = JSONPlaceholderConnector().normalize(raw)
        insert_records(conn, recs)

    run_with_logging(conn, "jsonplaceholder", run_json)

    # C++ binary-log connector from earlier
    def run_bin(conn):
        bin_path = os.path.abspath(os.path.join("..", "cpp", "binary_log_reader"))
        sample_file = os.path.abspath(os.path.join("..", "cpp", "sample_logs", "record.bin"))
        result = subprocess.run([bin_path, sample_file], capture_output=True, text=True, check=True)
        items = json.loads(result.stdout)
        records = [
            {"source": "binarylog", "record_id": rec["id"], "payload": rec}
            for rec in items
        ]
        with conn.cursor() as cur:
            for rec in records:
                cur.execute(
                    "INSERT INTO records (source, record_id, payload) VALUES (%s, %s, %s)",
                    (rec["source"], rec["record_id"], Json(rec["payload"]))
                )
        conn.commit()

    run_with_logging(conn, "binarylog", run_bin)

    print("All connectors have run. Check run_logs for details.")


if __name__ == "__main__":
    main()

