# Unified Connector Prototype

This project demonstrates a Python + C++ + JS framework for pulling data from multiple sources,
normalizing it into PostgreSQL, and offering a scheduler + dashboard with AI summaries.

## Tech Stack

- Python 3.10+ (connectors, scheduler)
- C++ (binary‑log reader)
- JavaScript (dashboard)
- PostgreSQL
- Gemini API for summaries

## Setup

1. Install Python dependencies:

   ```bash
   pip install -r python/requirements.txt

   ```

2. Build C++ module

   ```bash
   cd cpp && mkdir build && cd build
   cmake .. && make

   ```

3. Start Postgres (Docker recommended):

   ```bash
   docker‑compose up -d

   ```

4. Launch the dashboard:

   ```bash
   cd dashboard && npm install && npm start

   ```
