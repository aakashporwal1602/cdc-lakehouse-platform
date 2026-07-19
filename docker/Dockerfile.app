# Lightweight image for the generators / connector CLIs (seed, simulate).
FROM python:3.11-slim

WORKDIR /opt/app
RUN apt-get update && apt-get install -y --no-install-recommends gcc libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml requirements.txt ./
COPY src ./src
COPY configs ./configs
RUN pip install --no-cache-dir -e ".[ingest]"

ENV PYTHONPATH=/opt/app/src
ENTRYPOINT ["python", "-m"]
CMD ["cdc_platform.generators.seed_source"]
