FROM rust:1.55-alpine3.14 AS build

ARG GH_USER
ARG GH_TOKEN

# TODO: g++, make, and python are only required for neon-sys dependencies
RUN apk update && apk add --no-cache \
  git \
  g++ \
  make \
  musl-dev \
  npm \
  openssl-dev \
  python3

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

FROM alpine:3.14

COPY --from=build /opt/gateway/target/release/graph-gateway /opt/gateway/target/release/graph-gateway

WORKDIR /opt/gateway
ENTRYPOINT [ "target/release/graph-gateway" ]
