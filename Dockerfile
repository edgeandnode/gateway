FROM rust:1.62-buster AS build

ARG GH_USER
ARG GH_TOKEN

RUN apt-get update && apt-get install -y \
  build-essential \
  git \
  librdkafka-dev \
  libsasl2-dev\
  npm \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/gateway
COPY Cargo.toml .
COPY graphql/ ./graphql/
COPY src/ ./src/

# Setup GitHub credentials for cargo fetch
RUN npm install -g git-credential-env \
  && git config --global credential.helper 'env --username=GH_USER --password=GH_TOKEN' \
  && git config --global --replace-all url.https://github.com/.insteadOf ssh://git@github.com/ \
  && git config --global --add url.https://github.com/.insteadOf git@github.com: \
  && mkdir ~/.cargo && echo "[net]\ngit-fetch-with-cli = true" > ~/.cargo/config.toml

RUN cargo build --release

FROM debian:buster-slim

RUN apt-get update && apt-get install -y \
  libssl1.1 \
  ca-certificates \
  && rm -rf /var/lib/apt/lists/*

COPY --from=build /opt/gateway/target/release/graph-gateway /opt/gateway/target/release/graph-gateway
COPY GeoLite2-Country.mmdb /opt/geoip/GeoLite2-Country.mmdb

WORKDIR /opt/gateway
ENTRYPOINT [ "target/release/graph-gateway" ]
