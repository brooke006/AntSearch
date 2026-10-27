FROM python:3.13-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
# The workers run the transit search, so the image needs the science extra.
RUN pip install ".[science]" && useradd --create-home antsearch

USER antsearch
CMD ["python", "-m", "antsearch.worker"]
