#!/usr/bin/env python3
"""
PayTrack Web — startup script.
Run:
    pip install flask sqlalchemy
    python run.py
    → http://localhost:5000
"""
from app import app, init_db

if __name__ == "__main__":
    print("Initialising database...")
    init_db()
    print("Starting PayTrack on http://localhost:5000")
    app.run(debug=True, host="0.0.0.0", port=5000)
