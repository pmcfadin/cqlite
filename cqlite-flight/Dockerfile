# cqlite-flight — Arrow Flight server image.
# Build from the repository root so the cargo workspace is in context:
#   docker build -f cqlite-flight/Dockerfile -t cqlite-flight .
#
# Run co-located with a Cassandra node, mounting its data dir read-only:
#   docker run --rm -p 8815:8815 -v /var/lib/cassandra:/var/lib/cassandra:ro \
#     cqlite-flight --data-dir /var/lib/cassandra/data --listen 0.0.0.0:8815
FROM rust:1.85-bookworm AS build
WORKDIR /src
COPY . .
RUN cargo build --release -p cqlite-flight --features observability

FROM debian:bookworm-slim
RUN useradd -r -u 10001 flight
COPY --from=build /src/target/release/cqlite-flight /usr/local/bin/cqlite-flight
USER flight
EXPOSE 8815
ENTRYPOINT ["/usr/local/bin/cqlite-flight"]
