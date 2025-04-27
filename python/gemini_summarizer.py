import os
import psycopg2
import openai
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

load_dotenv()
DB_URL = os.getenv("DATABASE_URL")
openai.api_key = os.getenv("OPENAI_API_KEY")

def generate_summary_window(hours: int = 24):
    # Connect to the database
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()

    # 1) Aggregate run counts
    window_start = datetime.now(timezone.utc) - timedelta(hours=hours)
    cur.execute(
        """
        SELECT connector, status, COUNT(*) 
          FROM run_logs
         WHERE started_at >= %s
         GROUP BY connector, status;
        """,
        (window_start,)
    )
    counts = cur.fetchall()

    # 2) Top error messages
    cur.execute(
        """
        SELECT message, COUNT(*) 
          FROM run_logs
         WHERE status = 'failure' AND started_at >= %s
         GROUP BY message
         ORDER BY COUNT(*) DESC
         LIMIT 5;
        """,
        (window_start,)
    )
    errors = cur.fetchall()

    conn.close()

    # 3) Build the prompt
    prompt = f"Summarize the connector runs in the last {hours} hours:\n"
    for connector, status, cnt in counts:
        prompt += f"- {cnt} runs of {connector} with status {status}\n"
    if errors:
        prompt += "Top error messages:\n"
        for msg, cnt in errors:
            prompt += f"- \"{msg}\" occurred {cnt} times\n"

    # 4) Call Gemini (via OpenAI API)
    response = openai.ChatCompletion.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "You summarize run log statistics."},
            {"role": "user",   "content": prompt}
        ]
    )
    summary = response.choices[0].message.content.strip()

    # 5) Store into run_summaries
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO run_summaries (summary) VALUES (%s);",
        (summary,)
    )
    conn.commit()
    conn.close()

    return summary
