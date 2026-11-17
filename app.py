import os
import time
from datetime import datetime, timezone
from flask import Flask, jsonify

app = Flask(__name__)
STARTED_AT = time.time()

@app.get("/")
def home():
    return jsonify({
        "service": "investment-watchdog",
        "message": "Investment Watchdog starter service is running.",
        "health": "/health",
        "status": "/status"
    })

@app.get("/health")
def health():
    return jsonify({"ok": True, "service": "investment-watchdog"}), 200

@app.get("/status")
def status():
    now = datetime.now(timezone.utc)
    return jsonify({
        "service": "investment-watchdog",
        "status": "running",
        "checked_at_utc": now.isoformat(),
        "uptime_seconds": int(time.time() - STARTED_AT),
        "note": "Infrastructure heartbeat only; no brokerage account or ChatGPT task integration is configured."
    }), 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
