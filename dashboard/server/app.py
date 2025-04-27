from flask_cors import CORS
app = Flask(__name__)
CORS(app)

import os
import sys
from pathlib import Path
import json
import subprocess
from flask import Flask, request, jsonify, abort
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

# The top-level "python/" folder is on the path
project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root / "python"))

# Import the runner utilities
from connector_run import run_with_logging, init_db, run_binary_connector, insert_records
from connectors.jsonplaceholder_connector import JSONPlaceholderConnector

# Bootstrap environment & DB
load_dotenv()
def get_db_conn():
    return psycopg2.connect(os.getenv("DATABASE_URL"))

# Initialize tables immediately at startup
conn = get_db_conn()
init_db(conn)
conn.close()

app = Flask(__name__)

@app.route("/runs", methods=["GET"])
def list_runs():
    limit = int(request.args.get("limit", 20))
    conn = get_db_conn()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            "SELECT id, connector, status, started_at, finished_at, message "
            "FROM run_logs ORDER BY started_at DESC LIMIT %s",
            (limit,)
        )
        rows = cur.fetchall()
    conn.close()
    return jsonify(rows)

@app.route("/runs", methods=["POST"])
def trigger_run():
    data = request.get_json() or {}
    name = data.get("connector")
    if name not in ("jsonplaceholder", "binarylog"):
        abort(400, "connector must be 'jsonplaceholder' or 'binarylog'")

    conn = get_db_conn()
    try:
        if name == "jsonplaceholder":
            def fn(c):
                raw = JSONPlaceholderConnector().fetch()
                recs = JSONPlaceholderConnector().normalize(raw)
                insert_records(c, recs)
            run_with_logging(conn, name, fn)
        else:
            run_with_logging(conn, name, run_binary_connector)
    finally:
        conn.close()

    return jsonify({"status": "started", "connector": name}), 202

if __name__ == "__main__":
    app.run(port=5000, debug=True)
