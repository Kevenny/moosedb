#!/usr/bin/env bash
# Runs the MooseDB MTR suite against the official MariaDB server binary.
#
#   docker run --rm -v "$PWD:/work" moosedb-test bash /work/docs/docker/run-mtr.sh [mtr options]
#
# Expects build/ha_moosedb.so (see docs/docker/build-plugin.sh).
set -euo pipefail

MTR_DIR=/usr/share/mariadb/mariadb-test
PLUGIN_DIR=$(dirname "$(find /usr/lib -name ha_archive.so -path '*plugin*' | head -1)")

cp /work/build/ha_moosedb.so "$PLUGIN_DIR/"
ln -sfn /work/mysql-test/suite/moosedb "$MTR_DIR/suite/moosedb"

export MOOSEDB_INSTALL_SQL=/work/storage/moosedb/sql/moosedb_install.sql

cd "$MTR_DIR"
exec ./mariadb-test-run --suite=moosedb --force --max-test-fail=0 \
  --vardir=/tmp/mtr-var "$@"
