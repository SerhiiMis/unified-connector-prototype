import os
import time
from datetime import datetime
import schedule
import psycopg2
from dotenv import load_dotenv
import sys
from pathlib import Path
project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root / "python"))

from connector_run import run_with_logging, init_db

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

def get_conn():
    return psycopg2.connect(DATABASE_URL)

def job_json():
    conn = get_conn()
    try:
        # run_with_logging logs to the DB
        run_with_logging(conn, "jsonplaceholder", lambda c: __import__("connector_run").insert_records(c, __import__("connector_run").JSONPlaceholderConnector().normalize(__import__("connector_run").JSONPlaceholderConnector().fetch())))
        print("Inserted JSONPlaceholder records.")
    finally:
        conn.close()

def job_binary():
    conn = get_conn()
    try:
        run_with_logging(conn, "binarylog", __import__("connector_run").run_binary_connector)
    finally:
        conn.close()

if __name__ == "__main__":
    
    conn = get_conn()
    init_db(conn)
    conn.close()

    job_json()
    job_binary()

    schedule.every(1).minutes.do(job_json)
    schedule.every(1).minutes.do(job_binary)

    print(f"[{datetime.now()}] Scheduler started")
    while True:
        schedule.run_pending()
        time.sleep(1)

