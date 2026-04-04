FROM python:3.11-slim

WORKDIR /app

# Install system deps for geopandas/pyogrio
RUN apt-get update && apt-get install -y --no-install-recommends \
    libgdal-dev gdal-bin libgeos-dev libproj-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy and install Python deps
COPY solara_app/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY src/ /app/src/
COPY config/ /app/config/
COPY solara_app/ /app/solara_app/
COPY data/ /app/data/

WORKDIR /app/solara_app

ENV PORT=8765
EXPOSE 8765

CMD ["python", "-m", "solara", "run", "sol.py", "--host", "0.0.0.0", "--port", "8765", "--no-open"]
