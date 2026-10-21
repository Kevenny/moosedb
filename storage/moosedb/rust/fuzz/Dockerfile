# Imagem descartavel para o fuzzing: rust nightly + cargo-fuzz (o workspace principal nao precisa de nightly).
FROM rust:1-bookworm
RUN rustup toolchain install nightly --profile minimal && cargo +nightly install cargo-fuzz --locked
