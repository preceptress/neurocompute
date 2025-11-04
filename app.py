#!/usr/bin/env python3
# -----------------------------------------------------------------------------
# ForgeOS / NeuroCompute Performance Monitor
# -----------------------------------------------------------------------------
# Measures full-stack latency (Flask → Redis → Render) and exposes lightweight
# JSON endpoints for front-end visualization.
# -----------------------------------------------------------------------------

import os
import time
import subprocess
import re
import redis
from flask import Flask, render_template, jsonify

# -----------------------------------------------------------------------------
# Flask setup
# -----------------------------------------------------------------------------
app = Flask(__name__, template_folder="templates", static_folder="static")
app.config["TEMPLATES_AUTO_RELOAD"] = True
app.jinja_env.cache = {}

# -----------------------------------------------------------------------------
# Redis connection
# -----------------------------------------------------------------------------
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB = int(os.getenv("REDIS_DB", 0))

try:
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
    r.ping()
    print(f"✅ Redis connected at {REDIS_HOST}:{REDIS_PORT}")
except redis.ConnectionError as e:
    print(f"❌ Redis connection failed: {e}")
    r = None

# -----------------------------------------------------------------------------
# Latency cache (to prevent running redis-benchmark too often)
# -----------------------------------------------------------------------------
_last_latency = {"value": 0.0, "timestamp": 0}

# -----------------------------------------------------------------------------
# Routes
# -----------------------------------------------------------------------------
@app.route("/")
def index():
    """Serve main page."""
    return render_template("index.html")

# -----------------------------------------------------------------------------
# Redis latency API
# -----------------------------------------------------------------------------
@app.route("/api/redis-latency")
def redis_latency():
    """Run redis-benchmark and return measured latency (in ms)."""
    global _last_latency
    now = time.time()

    # Return cached value if under 10s old
    if now - _last_latency["timestamp"] < 10:
        return jsonify({
            "latency_ms": _last_latency["value"],
            "cached": True
        })

    try:
        # Primary method: redis-benchmark
        out = subprocess.run(
            ["redis-benchmark", "-q", "-n", "1000", "-c", "10"],
            capture_output=True,
            text=True,
            timeout=5
        )
        match = re.search(r"GET:\s+([\d\.]+)", out.stdout)
        latency = float(match.group(1)) if match else 0.0

        _last_latency.update({"value": latency, "timestamp": now})
        return jsonify({
            "latency_ms": latency,
            "cached": False,
            "method": "redis-benchmark"
        })

    except Exception as e:
        # Fallback: measure direct Redis ping latency
        try:
            start = time.time()
            if r:
                r.ping()
            latency = (time.time() - start) * 1000.0
            _last_latency.update({"value": latency, "timestamp": now})
            return jsonify({
                "latency_ms": latency,
                "cached": False,
                "method": "redis-ping",
                "error": str(e)
            })
        except Exception as inner_e:
            return jsonify({
                "latency_ms": 0.0,
                "cached": False,
                "method": "failed",
                "error": f"{e} | {inner_e}"
            })

# -----------------------------------------------------------------------------
# Full stack response time API
# -----------------------------------------------------------------------------
@app.route("/api/stack-speed")
def stack_speed():
    """
    Measures total time through Flask + Redis + return.
    Useful for displaying full-stack response latency.
    """
    start = time.time()
    try:
        if r:
            r.ping()
    except Exception:
        pass
    duration = (time.time() - start) * 1000.0
    return jsonify({
        "stack_ms": duration,
        "timestamp": time.time()
    })

# -----------------------------------------------------------------------------
# Health endpoint (for monitoring)
# -----------------------------------------------------------------------------
@app.route("/api/health")
def health():
    """Simple service heartbeat."""
    status = "ok"
    try:
        if r:
            r.ping()
    except Exception:
        status = "degraded"
    return jsonify({
        "service": "ForgeOS",
        "status": status,
        "redis_host": REDIS_HOST,
        "redis_port": REDIS_PORT,
        "time": time.strftime("%Y-%m-%d %H:%M:%S")
    })

# -----------------------------------------------------------------------------
# Main entry
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    print("🚀 Launching ForgeOS / NeuroCompute Flask service on port 9054")
    app.run(host="0.0.0.0", port=9054, debug=True)