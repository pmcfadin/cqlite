# cqlite-flight — Arrow Flight server image.
# Build from the repository root so the cargo workspace is in context:
#   docker build -f cqlite-flight/Dockerfile -t cqlite-flight .
#
# Run co-located with a Cassandra node, mounting its data dir read-only:
#   docker run --rm -p 8815:8815 -v /var/lib/cassandra:/var/lib/cassandra:ro \
#     cqlite-flight --data-dir /var/lib/cassandra/data --listen 0.0.0.0:8815
#
# Build layout (issue #2870): cargo-chef splits dependency compilation from
# workspace compilation so an image rebuild that touches only workspace source
# reuses the expensive `cook` layer instead of recompiling the whole dependency
# tree (per architecture, under the release profile's codegen-units=1 + fat LTO).
#
# #1990 LOCKSTEP: there is a SINGLE `FROM rust:1.97.1-bookworm AS chef` line below
# (planner and builder both `FROM chef`, so they inherit the pin — they carry no
# `rust:` base of their own). That one line MUST move together with
# rust-toolchain.toml's `channel` on a toolchain bump; scripts/ci/check-dockerfile-rust-pin.sh
# mechanizes it (exactly one `FROM rust:` line, matching the pin). See
# docs/development/ci-toolchain-policy.md.

# --- chef: install cargo-chef once; a stable, near-permanently-cached layer. ---
FROM rust:1.97.1-bookworm AS chef
WORKDIR /src
RUN cargo install cargo-chef --locked --version 0.1.77

# --- planner: emit recipe.json. `COPY . .` here is intentional — the planner's
#     only output is recipe.json, which changes solely when a manifest or
#     Cargo.lock changes, so the downstream cook layer survives source-only edits.
FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# --- builder: cook dependencies, then build the workspace binary. The cook flags
#     are character-for-character identical to the final build (release profile,
#     -p cqlite-flight, --features observability); a mismatch would cook artifacts
#     the final build cannot reuse — a silent no-op that recompiles anyway (D3).
FROM chef AS builder
COPY --from=planner /src/recipe.json recipe.json
RUN cargo chef cook --release --locked -p cqlite-flight --features observability --recipe-path recipe.json
COPY . .
RUN cargo build --release --locked -p cqlite-flight --features observability

# --- runtime: unchanged from the pre-#2870 image (same base, user, port,
#     entrypoint). Kept LAST so an untargeted `docker build` still yields it
#     (trino-connector/docker/docker-compose.yml builds with no --target). ---
FROM debian:bookworm-slim
RUN useradd -r -u 10001 flight
COPY --from=builder /src/target/release/cqlite-flight /usr/local/bin/cqlite-flight
USER flight
EXPOSE 8815
ENTRYPOINT ["/usr/local/bin/cqlite-flight"]
