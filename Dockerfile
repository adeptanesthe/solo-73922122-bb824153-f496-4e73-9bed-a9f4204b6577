FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY sample_data ./sample_data

# The SQLite database lives on a mounted volume so both web processes and the
# one-shot seed job share the same file.
RUN mkdir -p /data
ENV ER_DB_PATH=/data/er.db

EXPOSE 8000

HEALTHCHECK --interval=10s --timeout=3s --start-period=8s --retries=5 \
  CMD python -c "import urllib.request,sys; \
sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8000/healthz', timeout=3).status==200 else 1)"

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
