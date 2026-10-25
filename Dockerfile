# The collector and verify in a slim image. TLS roots are compiled in (webpki-roots), so the
# runtime image needs no CA bundle.
#
#   docker build -t collector .
#   docker run --rm -v "$PWD/out:/out" collector --out /out/ob.sbe --max-markets 2000 --duration-secs 120
#   docker run --rm -v "$PWD/out:/out" --entrypoint verify collector /out/ob.sbe
FROM rust:1-bookworm AS build
WORKDIR /src
COPY . .
RUN cargo build --release --bins && strip target/release/collector target/release/verify

FROM debian:bookworm-slim
COPY --from=build /src/target/release/collector /src/target/release/verify /usr/local/bin/
WORKDIR /out
# `docker stop` sends SIGTERM; the collector flushes and closes the file on SIGINT and SIGTERM.
ENTRYPOINT ["collector"]
