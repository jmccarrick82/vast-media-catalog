FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    trino \
    vastdb \
    pyarrow

COPY ../../shared /app/shared
COPY main.py /app/services/catalog-reconciler/main.py

WORKDIR /app/services/catalog-reconciler

CMD ["python", "main.py"]
