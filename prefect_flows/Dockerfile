FROM prefecthq/prefect:2-latest

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       ca-certificates curl unzip ffmpeg \
    && curl -fsSL https://deno.land/install.sh \
       -o /tmp/install-deno.sh \
    && DENO_INSTALL=/usr/local sh /tmp/install-deno.sh \
    && deno --version \
    && rm -f /tmp/install-deno.sh \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install -r /app/requirements.txt

ENV LD_LIBRARY_PATH=/usr/local/lib/python3.10/site-packages/nvidia/cublas/lib:/usr/local/lib/python3.10/site-packages/nvidia/cudnn/lib
ENV PYTHONUNBUFFERED=1