FROM rust:1.76-bullseye AS build

ARG GH_USER
ARG GH_TOKEN

RUN apt-get update && apt-get install -y \
  build-essential \
  clang \
  cmake \
  git \
  librdkafka-dev \
  libsasl2-dev \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/gateway
COPY ./ ./


# Setup GitHub credentials for cargo fetch
RUN git config --global credential.helper store \
  && git config --global --replace-all url.https://github.com/.insteadOf ssh://git@github.com/ \
  && git config --global --add url.https://github.com/.insteadOf git@github.com: \
  && mkdir ~/.cargo && echo "[net]\ngit-fetch-with-cli = true" > ~/.cargo/config.toml \
  && (echo url=https://github.com; echo "username=${GH_USER}"; echo "password=${GH_TOKEN}"; echo ) | git credential approve

ENV CC=clang CXX=clang++
RUN cargo build --release --bin graph-gateway --color=always

FROM debian:bullseye-slim

RUN apt-get update && apt-get install -y \
  libsasl2-dev \
  libssl1.1 \
  ca-certificates \
  && rm -rf /var/lib/apt/lists/*

COPY --from=build /opt/gateway/target/release/graph-gateway /opt/gateway/target/release/graph-gateway
COPY GeoLite2-Country.mmdb /opt/geoip/GeoLite2-Country.mmdb

WORKDIR /opt/gateway
ENTRYPOINT [ "target/release/graph-gateway" ]
