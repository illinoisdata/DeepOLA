### Build container

FROM rust:bookworm as build

RUN USER=root cargo new --bin deepola
WORKDIR /deepola

COPY ./deepola/Cargo.lock ./Cargo.lock
COPY ./deepola/Cargo.toml ./Cargo.toml
COPY ./deepola/wake/Cargo.toml ./wake/Cargo.toml
COPY ./deepola/wake/src ./wake/src
COPY ./deepola/wake/examples ./wake/examples
COPY ./deepola/wake/benches ./wake/benches

RUN cargo build --release
RUN rm src/*.rs

COPY ./deepola/wake/examples ./wake/examples

RUN rm ./target/release/deps/wake*
RUN cargo build --release --example tpch_polars


### Experiment container

FROM python:3.8-bookworm
COPY ./requirements.txt ./requirements.txt
RUN pip install -r requirements.txt

RUN apt-get update
RUN apt-get install vmtouch

COPY --from=build /deepola/target/release/examples/tpch_polars .
COPY ./scripts ./scripts

CMD ["/bin/bash"]
