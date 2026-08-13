FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DATA_DIR=/data

WORKDIR /app

COPY pyproject.toml ./
COPY app ./app
RUN pip install --no-cache-dir . && \
    addgroup --system adapter && \
    adduser --system --ingroup adapter --home /app adapter && \
    mkdir /data && chown adapter:adapter /data

USER adapter
VOLUME ["/data"]

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 CMD python -c "import os,sqlite3; p=os.path.join(os.environ['DATA_DIR'], 'adapter.db'); assert os.path.isfile(p); assert sqlite3.connect('file:'+p+'?mode=ro', uri=True).execute('PRAGMA quick_check').fetchone()[0] == 'ok'"

CMD ["sprotect-telegram-adapter"]
