#!/usr/bin/env python3
"""
Token Server - Storage Queue
Receives tokens from yidun_proxyless.py, gen.py, and ab.py
"""

import os
import time
import threading
import argparse
from collections import deque
from datetime import datetime
from flask import Flask, jsonify, request
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

# ============================================================
#  CONFIG
# ============================================================
TOKEN_TTL = 180  # seconds (3 minutes)

# ============================================================
#  STATE
# ============================================================
_lock = threading.Lock()
_token_queue = deque()
_stats = {
    "received": 0,
    "served": 0,
    "expired": 0,
    "duplicates": 0,
    "flushed": 0,
    "start_time": time.time(),
    "peak_queue": 0,
    "last_received": None,
    "last_served": None,
}

# ============================================================
#  CLEANUP THREAD
# ============================================================
def _purge_expired():
    now = time.time()
    removed = 0
    while _token_queue and (now - _token_queue[0]["ts"]) > TOKEN_TTL:
        _token_queue.popleft()
        removed += 1
    _stats["expired"] += removed
    return removed

def _cleanup_loop():
    while True:
        time.sleep(10)
        with _lock:
            _purge_expired()

_cleaner = threading.Thread(target=_cleanup_loop, daemon=True)
_cleaner.start()

# ============================================================
#  API ENDPOINTS
# ============================================================

@app.route("/api/save-token", methods=["POST"])
def receive_token():
    """Receive token from solver"""
    data = request.get_json(silent=True)
    if not data or "token" not in data:
        return jsonify({"error": "missing 'token' field"}), 400

    token = str(data["token"]).strip()
    if not token:
        return jsonify({"error": "empty token"}), 400

    with _lock:
        _purge_expired()
        now = time.time()
        _token_queue.append({"token": token, "ts": now})
        _stats["received"] += 1
        _stats["last_received"] = datetime.now().isoformat()
        queue_size = len(_token_queue)
        if queue_size > _stats["peak_queue"]:
            _stats["peak_queue"] = queue_size

    return jsonify({
        "status": "ok",
        "queue_size": queue_size,
        "total_received": _stats["received"],
    }), 200


@app.route("/api/get-token", methods=["GET"])
def get_token():
    """Get 1 token (removes from queue)"""
    with _lock:
        _purge_expired()
        if _token_queue:
            entry = _token_queue.popleft()
            _stats["served"] += 1
            _stats["last_served"] = datetime.now().isoformat()
            return jsonify({
                "token": entry["token"],
                "remaining": len(_token_queue),
                "age_seconds": round(time.time() - entry["ts"], 1),
            }), 200
        else:
            return jsonify({"error": "no tokens available", "remaining": 0}), 404


@app.route("/api/token/bulk", methods=["GET"])
def get_tokens_bulk():
    """Get multiple tokens (removes from queue)"""
    n = request.args.get("n", 1, type=int)
    n = max(1, min(n, 100))

    tokens = []
    with _lock:
        _purge_expired()
        for _ in range(n):
            if _token_queue:
                entry = _token_queue.popleft()
                tokens.append(entry["token"])
                _stats["served"] += 1
            else:
                break
        if tokens:
            _stats["last_served"] = datetime.now().isoformat()

    return jsonify({
        "tokens": tokens,
        "count": len(tokens),
        "remaining": len(_token_queue),
    }), 200


@app.route("/api/status", methods=["GET"])
def status():
    """Queue status and statistics"""
    with _lock:
        _purge_expired()
        elapsed = time.time() - _stats["start_time"]
        rate = _stats["received"] / (elapsed / 60) if elapsed > 0 else 0

        recent_tokens = []
        for item in list(_token_queue)[-5:]:
            recent_tokens.append({
                "token": item["token"][:40] + "...",
                "age": round(time.time() - item["ts"], 1)
            })

        return jsonify({
            "queue_size": len(_token_queue),
            "total_received": _stats["received"],
            "total_served": _stats["served"],
            "total_expired": _stats["expired"],
            "total_duplicates": _stats["duplicates"],
            "total_flushed": _stats["flushed"],
            "peak_queue": _stats["peak_queue"],
            "uptime_seconds": round(elapsed, 1),
            "tokens_per_minute": round(rate, 2),
            "token_ttl_seconds": TOKEN_TTL,
            "last_received": _stats["last_received"],
            "last_served": _stats["last_served"],
            "recent_tokens": recent_tokens,
        }), 200


@app.route("/api/tokens", methods=["DELETE"])
def flush_tokens():
    """Delete all tokens from queue"""
    with _lock:
        count = len(_token_queue)
        _token_queue.clear()
        _stats["flushed"] += count
    return jsonify({"status": "flushed", "removed": count}), 200


@app.route("/api/tokens/count", methods=["GET"])
def token_count():
    """Get token count only"""
    with _lock:
        _purge_expired()
        return jsonify({
            "queue_size": len(_token_queue),
            "total_received": _stats["received"],
            "total_served": _stats["served"],
        }), 200


@app.route("/", methods=["GET"])
def index():
    """Simple dashboard"""
    with _lock:
        _purge_expired()
        return jsonify({
            "status": "Token Server Running",
            "version": "2.0",
            "mode": "RAM Only (NO STORAGE)",
            "queue_size": len(_token_queue),
            "total_received": _stats["received"],
            "total_served": _stats["served"],
            "token_ttl": f"{TOKEN_TTL}s",
            "endpoints": {
                "POST /api/save-token": "Send token",
                "GET /api/get-token": "Get 1 token",
                "GET /api/token/bulk?n=5": "Get multiple tokens",
                "GET /api/status": "Status & stats",
                "DELETE /api/tokens": "Flush queue",
                "GET /api/tokens/count": "Token count only"
            }
        })


@app.route("/health", methods=["GET"])
def health():
    """Health check"""
    return jsonify({
        "ok": True,
        "uptime": round(time.time() - _stats["start_time"], 1),
        "queue_size": len(_token_queue),
        "total_received": _stats["received"],
    })


# ============================================================
#  HTML DASHBOARD
# ============================================================

@app.route("/dashboard", methods=["GET"])
def dashboard():
    """HTML dashboard for browser"""
    with _lock:
        _purge_expired()
        elapsed = time.time() - _stats["start_time"]
        rate = _stats["received"] / (elapsed / 60) if elapsed > 0 else 0
        q = len(_token_queue)
        peak = _stats["peak_queue"] or 1
        bar_pct = min(int(q / peak * 100), 100) if peak else 0

        # Build tokens HTML
        tokens_html = ""
        for item in list(_token_queue)[-10:]:
            token_preview = item["token"][:50] if len(item["token"]) > 50 else item["token"]
            tokens_html += f'<div class="item">{token_preview}... <span class="time">{round(time.time() - item["ts"], 1)}s</span></div>'

        if not tokens_html:
            tokens_html = '<div class="text-muted">No tokens in queue</div>'

        html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta http-equiv="refresh" content="3">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Token Server</title>
<style>
*{{margin:0;padding:0;box-sizing:border-box}}
body{{background:#0a0e17;color:#e0e0e0;font-family:'Segoe UI',sans-serif;padding:20px}}
h1{{color:#00e5ff;font-size:24px;margin-bottom:6px}}
.sub{{color:#666;font-size:14px;margin-bottom:24px}}
.grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:14px;margin-bottom:28px}}
.card{{background:#151515;border:1px solid #222;border-radius:10px;padding:14px 16px;text-align:center}}
.card .val{{font-size:28px;font-weight:700;color:#00e5ff}}
.card .lbl{{font-size:12px;color:#888;text-transform:uppercase;letter-spacing:.5px;margin-top:4px}}
.card.green .val{{color:#76ff03}}
.card.orange .val{{color:#ff9100}}
.card.pink .val{{color:#ff6b81}}
.card.purple .val{{color:#d500f9}}
.bar{{background:#151515;border:1px solid #222;border-radius:10px;padding:16px;margin-bottom:20px}}
.bar .fill{{height:24px;background:linear-gradient(90deg,#00e5ff,#76ff03);border-radius:6px;transition:width .5s;min-width:2px}}
.bar-label{{font-size:12px;color:#666;margin-top:4px}}
.tokens{{background:#151515;border:1px solid #222;border-radius:10px;padding:16px;max-height:300px;overflow-y:auto}}
.tokens .item{{padding:6px 0;border-bottom:1px solid #222;font-family:monospace;font-size:12px;color:#00e5ff;word-break:break-all}}
.tokens .item .time{{color:#666;float:right}}
.text-muted{{color:#666;font-size:12px}}
</style>
</head>
<body>
<h1>⚡ Token Server</h1>
<p class="sub">RAM Storage · TTL: {TOKEN_TTL}s · Auto-expire</p>
<div class="grid">
  <div class="card green"><div class="val">{q}</div><div class="lbl">Queue</div></div>
  <div class="card"><div class="val">{_stats["received"]}</div><div class="lbl">Received</div></div>
  <div class="card orange"><div class="val">{_stats["served"]}</div><div class="lbl">Served</div></div>
  <div class="card pink"><div class="val">{_stats["expired"]}</div><div class="lbl">Expired</div></div>
  <div class="card purple"><div class="val">{rate:.1f}</div><div class="lbl">Tok/min</div></div>
  <div class="card"><div class="val">{_stats["peak_queue"]}</div><div class="lbl">Peak Queue</div></div>
</div>
<div class="bar">
  <div class="fill" style="width:{bar_pct}%"></div>
  <div class="bar-label">{q} tokens · Peak {_stats["peak_queue"]}</div>
</div>
<div class="tokens">
  <div class="text-muted" style="margin-bottom:8px">Recent Tokens</div>
  {tokens_html}
</div>
</body>
</html>"""

        return html, 200, {"Content-Type": "text/html"}


# ============================================================
#  MAIN
# ============================================================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Token Server v2")
    parser.add_argument("--host", default="0.0.0.0", help="Bind address")
    parser.add_argument("--port", type=int, default=5050, help="Port (default 5050)")
    parser.add_argument("--ttl", type=int, default=180, help="Token TTL in seconds")
    args = parser.parse_args()

    TOKEN_TTL = args.ttl

    print(f"""
[ Token Server v2.0 ]
  Mode   : RAM Only (NO STORAGE)
  Port   : {args.port}
  TTL    : {args.ttl}s
  URL    : http://{args.host}:{args.port}

  POST   /api/save-token
  GET    /api/get-token
  GET    /api/token/bulk?n=5
  GET    /api/status
  DELETE /api/tokens
  GET    /dashboard
""")

    app.run(host=args.host, port=args.port, debug=False, threaded=True)