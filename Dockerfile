FROM rust:1.93.0-slim-bookworm

WORKDIR /app

COPY Cargo.toml .
COPY Cargo.lock .
COPY src src

RUN cargo install --path .

EXPOSE 9000

CMD [ "echo", "Hello! This image has producer and consumer binaries that should be invoked accordingly." ]
