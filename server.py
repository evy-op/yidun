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
def dashboard():
    """Premium glass-morphism dashboard"""
    with _lock:
        _purge_expired()
        elapsed = time.time() - _stats["start_time"]
        rate = _stats["received"] / (elapsed / 60) if elapsed > 0 else 0
        q = len(_token_queue)
        peak = _stats["peak_queue"] or 1
        bar_pct = min(int(q / peak * 100), 100) if peak else 0

        # Build token list with colors
        token_html = ""
        for item in list(_token_queue)[-15:]:
            age = round(time.time() - item["ts"], 1)
            if age < 60:
                color = "#34d399"
                status = "Fresh"
            elif age < 120:
                color = "#fbbf24"
                status = "Aging"
            else:
                color = "#f87171"
                status = "Expiring"
            
            token_preview = item["token"][:45] + "..." if len(item["token"]) > 45 else item["token"]
            token_html += f'''
            <div class="token-row">
                <span class="token-text" style="color:{color}">{token_preview}</span>
                <span class="token-meta">
                    <span class="token-age">{age}s</span>
                    <span class="token-status" style="color:{color}">{status}</span>
                </span>
            </div>
            '''

        if not token_html:
            token_html = '<div class="empty-state">🎯 No tokens in queue</div>'

        html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>⚡ Token Server</title>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        
        body {{
            min-height: 100vh;
            display: flex;
            justify-content: center;
            align-items: center;
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: linear-gradient(135deg, #0f172a 0%, #1e293b 50%, #334155 100%);
            padding: 20px;
        }}
        
        /* Main Container - Glass Effect */
        .container {{
            display: flex;
            background: rgba(255, 255, 255, 0.05);
            backdrop-filter: blur(20px);
            -webkit-backdrop-filter: blur(20px);
            border-radius: 24px;
            border: 1px solid rgba(255, 255, 255, 0.1);
            box-shadow: 0 25px 50px rgba(0, 0, 0, 0.4), inset 0 1px 0 rgba(255, 255, 255, 0.1);
            overflow: hidden;
            max-width: 1100px;
            width: 100%;
        }}
        
        /* Left Side - Stats */
        .stats-side {{
            flex: 1.2;
            padding: 48px 40px;
            background: rgba(255, 255, 255, 0.03);
            min-width: 0;
        }}
        
        /* Right Side - Tokens */
        .tokens-side {{
            flex: 1;
            padding: 48px 40px;
            background: rgba(255, 255, 255, 0.02);
            border-left: 1px solid rgba(255, 255, 255, 0.05);
            min-width: 0;
        }}
        
        .logo {{
            font-size: 28px;
            font-weight: 700;
            color: white;
            margin-bottom: 32px;
            letter-spacing: 1px;
        }}
        
        .logo span {{
            color: rgba(255, 255, 255, 0.3);
        }}
        
        .logo .badge {{
            font-size: 12px;
            background: rgba(52, 211, 153, 0.2);
            color: #34d399;
            padding: 2px 12px;
            border-radius: 20px;
            margin-left: 12px;
            font-weight: 400;
        }}
        
        /* Stats Grid */
        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(2, 1fr);
            gap: 12px;
            margin-bottom: 24px;
        }}
        
        .stat-card {{
            background: rgba(255, 255, 255, 0.05);
            border-radius: 12px;
            padding: 16px;
            border: 1px solid rgba(255, 255, 255, 0.05);
            transition: all 0.3s ease;
        }}
        
        .stat-card:hover {{
            background: rgba(255, 255, 255, 0.08);
            transform: translateY(-2px);
        }}
        
        .stat-card .value {{
            font-size: 28px;
            font-weight: 700;
            color: white;
            line-height: 1.2;
        }}
        
        .stat-card .label {{
            font-size: 11px;
            color: rgba(255, 255, 255, 0.4);
            text-transform: uppercase;
            letter-spacing: 1px;
            margin-top: 4px;
        }}
        
        .stat-card .value.green {{ color: #34d399; }}
        .stat-card .value.blue {{ color: #60a5fa; }}
        .stat-card .value.orange {{ color: #fbbf24; }}
        .stat-card .value.pink {{ color: #f472b6; }}
        .stat-card .value.purple {{ color: #a78bfa; }}
        
        /* Progress Bar */
        .progress-section {{
            margin-bottom: 24px;
        }}
        
        .progress-section .label-row {{
            display: flex;
            justify-content: space-between;
            font-size: 12px;
            color: rgba(255, 255, 255, 0.4);
            margin-bottom: 6px;
        }}
        
        .progress-bar {{
            height: 6px;
            background: rgba(255, 255, 255, 0.05);
            border-radius: 10px;
            overflow: hidden;
        }}
        
        .progress-bar .fill {{
            height: 100%;
            background: linear-gradient(90deg, #60a5fa, #a78bfa);
            border-radius: 10px;
            transition: width 0.5s ease;
            width: {bar_pct}%;
        }}
        
        /* Uptime & Rate */
        .info-row {{
            display: flex;
            gap: 24px;
            font-size: 13px;
            color: rgba(255, 255, 255, 0.4);
        }}
        
        .info-row strong {{
            color: rgba(255, 255, 255, 0.7);
            font-weight: 500;
        }}
        
        /* Tokens List */
        .tokens-header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 16px;
        }}
        
        .tokens-header h3 {{
            color: white;
            font-size: 16px;
            font-weight: 600;
        }}
        
        .tokens-header .count {{
            font-size: 12px;
            color: rgba(255, 255, 255, 0.3);
            background: rgba(255, 255, 255, 0.05);
            padding: 4px 12px;
            border-radius: 20px;
        }}
        
        .tokens-list {{
            max-height: 400px;
            overflow-y: auto;
        }}
        
        .tokens-list::-webkit-scrollbar {{
            width: 4px;
        }}
        
        .tokens-list::-webkit-scrollbar-track {{
            background: rgba(255, 255, 255, 0.05);
            border-radius: 10px;
        }}
        
        .tokens-list::-webkit-scrollbar-thumb {{
            background: rgba(255, 255, 255, 0.15);
            border-radius: 10px;
        }}
        
        .token-row {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 10px 0;
            border-bottom: 1px solid rgba(255, 255, 255, 0.03);
            gap: 12px;
        }}
        
        .token-row:last-child {{
            border-bottom: none;
        }}
        
        .token-text {{
            font-family: 'SF Mono', 'Courier New', monospace;
            font-size: 12px;
            color: #34d399;
            word-break: break-all;
            flex: 1;
        }}
        
        .token-meta {{
            display: flex;
            align-items: center;
            gap: 12px;
            flex-shrink: 0;
        }}
        
        .token-age {{
            font-size: 11px;
            color: rgba(255, 255, 255, 0.3);
            font-family: 'SF Mono', monospace;
        }}
        
        .token-status {{
            font-size: 10px;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }}
        
        .empty-state {{
            text-align: center;
            color: rgba(255, 255, 255, 0.2);
            padding: 40px 0;
            font-size: 14px;
        }}
        
        /* API Endpoints */
        .api-endpoints {{
            margin-top: 24px;
            padding-top: 20px;
            border-top: 1px solid rgba(255, 255, 255, 0.05);
        }}
        
        .api-endpoints h4 {{
            color: rgba(255, 255, 255, 0.3);
            font-size: 11px;
            text-transform: uppercase;
            letter-spacing: 1px;
            margin-bottom: 12px;
        }}
        
        .endpoint {{
            display: inline-block;
            font-size: 11px;
            font-family: 'SF Mono', monospace;
            color: rgba(255, 255, 255, 0.3);
            background: rgba(255, 255, 255, 0.03);
            padding: 4px 12px;
            border-radius: 6px;
            margin: 0 6px 6px 0;
            border: 1px solid rgba(255, 255, 255, 0.03);
        }}
        
        .endpoint .method {{
            color: #60a5fa;
            font-weight: 600;
        }}
        
        /* Responsive */
        @media (max-width: 768px) {{
            .container {{
                flex-direction: column;
                max-width: 500px;
            }}
            
            .tokens-side {{
                border-left: none;
                border-top: 1px solid rgba(255, 255, 255, 0.05);
            }}
            
            .stats-side,
            .tokens-side {{
                padding: 32px 24px;
            }}
            
            .stats-grid {{
                grid-template-columns: repeat(2, 1fr);
                gap: 8px;
            }}
            
            .stat-card .value {{
                font-size: 22px;
            }}
            
            .token-row {{
                flex-direction: column;
                align-items: flex-start;
                gap: 4px;
            }}
            
            .token-meta {{
                width: 100%;
                justify-content: space-between;
            }}
        }}
        
        @media (max-width: 480px) {{
            .stats-grid {{
                grid-template-columns: 1fr 1fr;
            }}
            
            .stat-card {{
                padding: 12px;
            }}
            
            .stat-card .value {{
                font-size: 18px;
            }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <!-- Left Side - Stats -->
        <div class="stats-side">
            <div class="logo">
                ⚡ Token<span>Server</span>
                <span class="badge">v2.0</span>
            </div>
            
            <div class="stats-grid">
                <div class="stat-card">
                    <div class="value green">{q}</div>
                    <div class="label">Queue</div>
                </div>
                <div class="stat-card">
                    <div class="value blue">{_stats["received"]}</div>
                    <div class="label">Received</div>
                </div>
                <div class="stat-card">
                    <div class="value orange">{_stats["served"]}</div>
                    <div class="label">Served</div>
                </div>
                <div class="stat-card">
                    <div class="value pink">{_stats["expired"]}</div>
                    <div class="label">Expired</div>
                </div>
            </div>
            
            <div class="progress-section">
                <div class="label-row">
                    <span>Queue Capacity</span>
                    <span>{q} / {peak}</span>
                </div>
                <div class="progress-bar">
                    <div class="fill" style="width: {bar_pct}%"></div>
                </div>
            </div>
            
            <div class="info-row">
                <span>⏱ <strong>{round(elapsed, 1)}s</strong> uptime</span>
                <span>⚡ <strong>{rate:.1f}</strong> tokens/min</span>
                <span>📦 <strong>{_stats["peak_queue"]}</strong> peak</span>
            </div>
            
            <div class="api-endpoints">
                <h4>API Endpoints</h4>
                <span class="endpoint"><span class="method">POST</span> /api/save-token</span>
                <span class="endpoint"><span class="method">GET</span> /api/get-token</span>
                <span class="endpoint"><span class="method">GET</span> /api/token/bulk?n=5</span>
                <span class="endpoint"><span class="method">GET</span> /api/status</span>
                <span class="endpoint"><span class="method">DELETE</span> /api/tokens</span>
            </div>
        </div>
        
        <!-- Right Side - Tokens -->
        <div class="tokens-side">
            <div class="tokens-header">
                <h3>📋 Recent Tokens</h3>
                <span class="count">{q} in queue</span>
            </div>
            <div class="tokens-list">
                {token_html}
            </div>
        </div>
    </div>
</body>
</html>"""

        return html, 200, {"Content-Type": "text/html"}


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
  GET    / (Dashboard)
""")

    app.run(host=args.host, port=args.port, debug=False, threaded=True)