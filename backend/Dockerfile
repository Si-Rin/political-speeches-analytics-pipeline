FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    ffmpeg \
    && rm -rf /var/lib/apt/lists/*

COPY backend/requirements.txt .

RUN pip install --no-cache-dir -r backend/requirements.txt

COPY backend ./backend
COPY prefect_flows ./prefect_flows
COPY requirements.txt .

ENV PIPELINE_PYTHON=/usr/local/bin/python

EXPOSE 8000

CMD ["uvicorn", "backend.main:app", "--host", "0.0.0.0", "--port", "8000"]