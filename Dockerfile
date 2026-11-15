# syntax=docker/dockerfile:1
FROM rust:1.95-slim-bookworm AS build
WORKDIR /src
# Build dependencies first so source edits don't invalidate them.
COPY Cargo.toml Cargo.lock ./
RUN mkdir -p src/bin benches \
 && echo 'fn main() {}' > src/bin/ironclad-server.rs \
 && echo 'fn main() {}' > src/bin/ironclad-cli.rs \
 && echo 'fn main() {}' > benches/storage.rs \
 && touch src/lib.rs \
 && cargo build --release --bins \
 && rm -rf src benches
COPY src ./src
COPY benches ./benches
RUN touch src/lib.rs && cargo build --release --bins

FROM debian:bookworm-slim
RUN useradd --system --uid 10001 ironclad && mkdir /data && chown ironclad /data
COPY --from=build /src/target/release/ironclad-server /src/target/release/ironclad-cli /usr/local/bin/
USER ironclad
VOLUME /data
EXPOSE 6001 7001
ENTRYPOINT ["ironclad-server", "--data-dir", "/data"]
