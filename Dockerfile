# Shopping Sessions: one native binary, no runtime dependencies.
FROM rust:1.90-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
# Scorers are Cascade Lake or newer Xeons; AVX2 (x86-64-v3) is safe there.
ENV RUSTFLAGS="-C target-cpu=x86-64-v3"
RUN cargo build --release --locked

FROM debian:bookworm-slim
# The scorer runs the container as uid 1000 with HOME=/scratch.
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app
COPY --from=build /src/target/release/sessions /app/sessions
CMD ["/app/sessions"]
