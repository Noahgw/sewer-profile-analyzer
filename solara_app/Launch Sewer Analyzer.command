#!/bin/bash
# ─────────────────────────────────────────────────────────
# Sewer Profile Analyzer — Solara Launcher
# Double-click this file to start the app.
# ─────────────────────────────────────────────────────────

APP_DIR="$(cd "$(dirname "$0")" && pwd)"
PORT=8765
URL="http://localhost:$PORT"

cd "$APP_DIR"

# ── Find Python ──
if command -v python3 &>/dev/null; then
    PY=python3
elif command -v python &>/dev/null; then
    PY=python
else
    osascript -e 'display alert "Python Not Found" message "Install Python 3 from python.org to run this app." as critical'
    exit 1
fi

# ── Check/create venv on first run ──
if [ ! -d ".venv" ]; then
    echo "============================================"
    echo "  First run — setting up environment..."
    echo "  This may take a minute."
    echo "============================================"
    $PY -m venv .venv
    source .venv/bin/activate
    pip install --upgrade pip -q
    pip install -r requirements.txt -q
    echo ""
    echo "  Setup complete."
    echo "============================================"
else
    source .venv/bin/activate
fi

# ── Kill any previous instance on this port ──
lsof -ti:$PORT | xargs kill -9 2>/dev/null

# ── Launch ──
echo ""
echo "============================================"
echo "  Sewer Profile Analyzer"
echo "  Starting at $URL"
echo "  Close this window to stop the server."
echo "============================================"
echo ""

# Open browser after a short delay (server needs a moment)
(sleep 2 && open "$URL") &

# Run Solara (blocks until window is closed)
solara run sol.py --port $PORT --host 0.0.0.0 --no-open
