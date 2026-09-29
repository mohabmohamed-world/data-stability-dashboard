FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    TO_DB_PATH=/data/to_dashboard.db

WORKDIR /app

COPY to_dashboard_app/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY to_dashboard_app/ ./
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 8501

HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
  CMD python -c "import os,urllib.request; p=os.getenv('PORT','8501'); urllib.request.urlopen(f'http://127.0.0.1:{p}/_stcore/health', timeout=3).read()" || exit 1

ENTRYPOINT ["/entrypoint.sh"]
