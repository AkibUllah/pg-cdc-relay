# syntax=docker/dockerfile:1
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app
RUN addgroup --system app && adduser --system --ingroup app app

COPY pyproject.toml README.md ./
COPY cdc_relay ./cdc_relay
COPY examples ./examples
RUN pip install --upgrade pip && pip install ".[redis]"

RUN mkdir -p /data && chown app:app /data
USER app
ENV CDC_CHECKPOINT_PATH=/data/relay.checkpoint.json
EXPOSE 8086
VOLUME ["/data"]

HEALTHCHECK --interval=15s --timeout=3s --start-period=10s \
  CMD python -c "import os,urllib.request,sys; sys.exit(0 if urllib.request.urlopen(f'http://127.0.0.1:{os.environ.get(\"CDC_STATUS_PORT\", \"8086\")}/healthz').status == 200 else 1)"

ENTRYPOINT ["cdc-relay"]
CMD ["run"]
