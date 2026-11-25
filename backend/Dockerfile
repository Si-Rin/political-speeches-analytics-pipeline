FROM python:3.11-slim

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       ffmpeg ca-certificates curl unzip \
    && curl -fsSL https://deno.land/install.sh \
       -o /tmp/install-deno.sh \
    && DENO_INSTALL=/usr/local sh /tmp/install-deno.sh \
    && deno --version \
    && rm -f /tmp/install-deno.sh \
    && rm -rf /var/lib/apt/lists/*


COPY backend/requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY backend ./backend

ENV PIPELINE_PYTHON=/usr/local/bin/python

EXPOSE 8000

CMD ["uvicorn", "backend.main:app", "--host", "0.0.0.0", "--port", "8000"]
