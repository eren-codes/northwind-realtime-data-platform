FROM python:3.12-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

COPY docker/etl/requirements.txt /tmp/requirements.txt
COPY --chown=appuser:appuser kafka /app/kafka

RUN pip install --no-cache-dir \
    -r /tmp/requirements.txt

RUN useradd \
    --create-home \
    --uid 10001 \
    appuser

COPY --chown=appuser:appuser etl /app/etl

USER appuser

CMD ["python", "-m", "etl.full_load"]
