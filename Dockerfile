FROM python:3.11-slim

# ── Install Chromium + dependencies ──
# curl is needed for the HEALTHCHECK below.
RUN apt-get update && apt-get install -y --no-install-recommends \
    chromium \
    chromium-driver \
    fonts-liberation \
    libnss3 \
    libxss1 \
    libasound2 \
    libatk-bridge2.0-0 \
    libgtk-3-0 \
    libgbm1 \
    curl \
    && rm -rf /var/lib/apt/lists/*

# ── Set Chrome env vars ──
ENV CHROME_BIN=/usr/bin/chromium
ENV CHROMEDRIVER_PATH=/usr/bin/chromedriver
ENV STREAMLIT_SERVER_PORT=8501
ENV STREAMLIT_SERVER_ADDRESS=0.0.0.0
ENV STREAMLIT_SERVER_HEADLESS=true
ENV STREAMLIT_BROWSER_GATHER_USAGE_STATS=false

WORKDIR /app

# ── Install Python deps (cached layer) ──
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ── Copy app files ──
# Legacy POC scripts (legacy/) are intentionally NOT copied into the image.
COPY app.py logo.png config.py ./
COPY utils/    ./utils/
COPY database/ ./database/
COPY exports/  ./exports/
COPY scraper/  ./scraper/
COPY ui/       ./ui/

EXPOSE 8501

HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health || exit 1

ENTRYPOINT ["streamlit", "run", "app.py"]
