# MooseDB development image.
#
# Contains the Rust toolchain, cbindgen, a C++20 compiler and the MariaDB
# source tree matching the server version we target. The plugin must be
# compiled against the exact same source version as the server that loads it.
#
#   docker build -f docs/docker/dev.Dockerfile -t moosedb-dev .
#
FROM rust:1-bookworm

ARG MARIADB_VERSION=11.4.13

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      cmake ninja-build g++ bison pkg-config \
      libncurses-dev libssl-dev zlib1g-dev libpcre2-dev libaio-dev liburing-dev \
      libsystemd-dev libxml2-dev libcurl4-openssl-dev \
      libgnutls28-dev libpam0g-dev libkrb5-dev libsnappy-dev liblz4-dev libzstd-dev liblzma-dev libbz2-dev \
 && rm -rf /var/lib/apt/lists/*

RUN rustup component add clippy rustfmt \
 && cargo install cbindgen --locked

# Source tarballs from archive.mariadb.org include the git submodules
# (libmariadb, wsrep-lib) that a plain `git clone --depth 1` would miss.
RUN mkdir -p /src \
 && curl -fsSL "https://archive.mariadb.org/mariadb-${MARIADB_VERSION}/source/mariadb-${MARIADB_VERSION}.tar.gz" \
    | tar -xz -C /src \
 && mv "/src/mariadb-${MARIADB_VERSION}" /src/mariadb

ENV MARIADB_SRC=/src/mariadb \
    MARIADB_BUILD=/build/mariadb \
    CARGO_TARGET_DIR=/build/cargo-target

WORKDIR /work
