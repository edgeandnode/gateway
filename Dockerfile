FROM rust:1-bookworm AS build

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

ENV CC=clang CXX=clang++
RUN cargo build --release --bin graph-gateway --color=always

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y \
  ca-certificates \
  libsasl2-dev \
  libssl-dev \
  && rm -rf /var/lib/apt/lists/*

COPY --from=build /opt/gateway/target/release/graph-gateway /opt/gateway/target/release/graph-gateway
WORKDIR /opt/gateway
ENTRYPOINT [ "target/release/graph-gateway" ]
