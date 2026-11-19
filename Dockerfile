# syntax=docker/dockerfile:1.7
#
# OCI image for the Holochain HTTP push relay (+ the demo client CLI).
#
#   docker build -t hc-push-relay .
#
# Behind a TLS-intercepting proxy, pass its CA as an optional build secret:
#   docker build --secret id=build_ca,src=/path/ca.pem ...

ARG RUST_VERSION=1.91
ARG DEBIAN_RELEASE=bookworm

# Full (non-slim) Rust image: already has gcc, make and perl for the bundled
# SQLite / vendored OpenSSL that Holochain's keystore crates compile.
FROM rust:${RUST_VERSION}-${DEBIAN_RELEASE} AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY crates crates
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    --mount=type=secret,id=build_ca,required=false \
    if [ -s /run/secrets/build_ca ]; then export CARGO_HTTP_CAINFO=/run/secrets/build_ca; fi; \
    cargo build --release --locked -p hc_push_relay -p hc_push_client \
 && install -m 0755 target/release/hc-push-relay target/release/hc-push-client /usr/local/bin/ \
 && mkdir -p /out/data

# Distroless: glibc + libgcc + CA roots, no shell or package manager,
# runs as uid 65532 ("nonroot").
FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=build /usr/local/bin/hc-push-relay /usr/local/bin/hc-push-client /usr/local/bin/
COPY --from=build --chown=65532:65532 /out/data /data
USER 65532:65532
VOLUME ["/data"]
ENV HC_PUSH_BIND=0.0.0.0:8787 \
    HC_PUSH_DATA_DIR=/data \
    HC_PUSH_LOG_FORMAT=json \
    RUST_LOG=info
EXPOSE 8787
HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --retries=3 \
  CMD ["/usr/local/bin/hc-push-relay", "healthcheck"]
# The relay is PID 1, handles SIGTERM itself (drains SSE streams and change
# sources, then exits) and spawns no child processes.
STOPSIGNAL SIGTERM
ENTRYPOINT ["/usr/local/bin/hc-push-relay"]
