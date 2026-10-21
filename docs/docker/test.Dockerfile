# Runtime/test image: the official MariaDB 11.4 server plus its test framework (MTR).
#
#   docker build -f docs/docker/test.Dockerfile -t moosedb-test docs/docker/
#
FROM mariadb:11.4

RUN apt-get update \
 && apt-get install -y --no-install-recommends mariadb-test \
 && rm -rf /var/lib/apt/lists/*
