FROM docker.io/rust:1.90.0-slim-trixie AS builder

ARG PROFILE=release
ARG GIT_REVISION
ENV GIT_REVISION=$GIT_REVISION

WORKDIR work

RUN apt-get update && apt-get -y --no-install-recommends install \
    build-essential \
    libssl-dev \
    libpq-dev \
    pkg-config \
    cmake \
    clang \
    ca-certificates \
    git \
    && rm -rf /var/lib/apt/lists/*

COPY Cargo.lock Cargo.toml ./
COPY diesel.toml ./
COPY pipelines.toml ./
COPY src/ ./src/
COPY migrations/ ./migrations/

ENV PATH="/root/.cargo/bin:${PATH}"

RUN cargo build --profile $PROFILE --config net.git-fetch-with-cli=true

# --- RUNTIME STAGE ---
FROM docker.io/debian:trixie-slim AS runtime

RUN apt-get update && apt-get -y --no-install-recommends install \
    ca-certificates \
    libssl3t64 \
    libpq5 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/indexer/bin

COPY --from=builder /work/target/release/indexer /opt/indexer/bin/indexer
COPY --from=builder /work/pipelines.toml /opt/indexer/bin/pipelines.toml

ARG BUILD_DATE
ARG GIT_REVISION
LABEL build-date=$BUILD_DATE \
      git-revision=$GIT_REVISION

ENTRYPOINT ["/opt/indexer/bin/indexer"]
