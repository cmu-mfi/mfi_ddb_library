# MFI DDB Metadata Form - backend
#
# Self-contained: mfi_ddb comes from PyPI. Build context is the
# mfi_ddb_metadata_form/ folder so the default config can be baked in:
#
#   docker build -f backend/Dockerfile -t mfi-metadata-form-backend .
#
# Run the tests in the same environment:
#
#   docker build -f backend/Dockerfile --target test -t mfi-metadata-form-test .
#   docker run --rm mfi-metadata-form-test

# ---------------------------------------------------------------------------
FROM python:3.12-slim AS base

WORKDIR /app

# libpango/harfbuzz + fonts: WeasyPrint (label printing); Liberation Sans stands in for Arial
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpango-1.0-0 libpangoft2-1.0-0 libharfbuzz-subset0 fonts-liberation \
    && rm -rf /var/lib/apt/lists/*

COPY backend/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY backend/app ./app
# Default configuration; mount your own over /app/config.
COPY config/form-config.yaml ./config/form-config.yaml

ENV FORM_CONFIG_PATH=/app/config/form-config.yaml \
    FORM_HISTORY_DB=/app/data/submissions.db \
    PYTHONUNBUFFERED=1

# ---------------------------------------------------------------------------
FROM base AS test

RUN pip install --no-cache-dir pytest httpx pypdf
COPY backend/tests ./tests
COPY backend/pytest.ini ./
CMD ["python", "-m", "pytest", "-v", "-p", "no:cacheprovider"]

# ---------------------------------------------------------------------------
FROM base AS runtime

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health', timeout=4)"

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
