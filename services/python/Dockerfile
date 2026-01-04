FROM python:3.11-slim

WORKDIR /app

# Install system dependencies (needed for psycopg2 compilation if not using binary, but binary is fine for dev)
# Also install curl/wget if needed for healthchecks
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "-m", "app.main"]
