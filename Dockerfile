FROM rust:1-slim-bookworm AS build

RUN apt-get update && apt-get install -y \
    clang \
    cmake \
    git \
    libsasl2-dev \
    libssl-dev \
    pkg-config \
    zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/gateway
COPY ./ ./

ENV CC=clang CXX=clang++
RUN cargo build --release --bin graph-gateway --color=always

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y ca-certificates libsasl2-dev libssl-dev \
    && apt-get remove -y libaom3 \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /opt/gateway/target/release/graph-gateway /opt/gateway/target/release/graph-gateway
WORKDIR /opt/gateway
ENTRYPOINT [ "target/release/graph-gateway" ]
