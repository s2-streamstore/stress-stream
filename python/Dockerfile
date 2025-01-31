FROM ghcr.io/astral-sh/uv:python3.11-bookworm-slim

LABEL org.opencontainers.image.title="stress-stream" \
    org.opencontainers.image.description="" \
    org.opencontainers.image.version="" \
    org.opencontainers.image.revision="" \
    org.opencontainers.image.created="" \
    org.opencontainers.image.url="" \
    org.opencontainers.image.source="" \
    org.opencontainers.image.documentation="" \
    org.opencontainers.image.vendor="" \
    org.opencontainers.image.licenses="Apache-2.0" \
    org.opencontainers.image.authors=""

WORKDIR /app

COPY uv.lock .
COPY pyproject.toml .
COPY .python-version .

RUN uv sync --no-dev

COPY src/stress_stream src/stress_stream

RUN uv pip install e .

EXPOSE 8000

CMD [ "echo", "Hello! This image has producer and consumer modules that should be invoked accordingly." ]
