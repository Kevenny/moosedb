# MooseDB Engine

Storage engine time-series para MariaDB 11.4+ — handler em C++20, núcleo de
storage em Rust (`#![forbid(unsafe_code)]`) exposto via C ABI.

* Especificação de produto: [moosedb_engine_spec.md](moosedb_engine_spec.md)
* Guia do usuário: [docs/user-guide.md](docs/user-guide.md)
* Arquitetura implementada e divergências da spec: [docs/architecture.md](docs/architecture.md)

## Estado atual (v0.2)

Todas as fases da spec (§18) estão implementadas:

* WAL com CRC32 e replay, MemTable, chunks por intervalo de tempo, MANIFEST
  atômico, recuperação de crash (testada com `kill -9`);
* codificação por tipo (delta-of-delta, Simple8b, Gorilla, RLE) + LZ4 (quente)
  / ZSTD (frio);
* scans em streaming, *k-way merge* ordenado para o índice de timestamp,
  pushdown de filtros por TAG respeitando a collation, Bloom filter por chunk;
* compactação e retenção (OPTIMIZE, `CALL moosedb_*`, threads de fundo);
* INSERT concorrente com SELECT (snapshots imutáveis);
* `INFORMATION_SCHEMA.MOOSEDB_TABLES` e `MOOSEDB_CHUNKS`;
* criptografia AES-256-CTR com chaves do key management do MariaDB;
* replicação row-based.

## Build e testes (Docker)

Todo o toolchain roda em containers; o plugin é compilado contra o source
exato do servidor da imagem `mariadb:11.4` (11.4.13).

```bash
# Imagens
docker build -f docs/docker/dev.Dockerfile  -t moosedb-dev  docs/docker/
docker build -f docs/docker/test.Dockerfile -t moosedb-test docs/docker/

# Rust: testes, clippy
docker run --rm -v "$PWD:/work" -v moosedb-cargo:/usr/local/cargo/registry \
  -v moosedb-build:/build -w /work/storage/moosedb/rust moosedb-dev \
  bash -c 'cargo test --workspace && cargo clippy --workspace --all-targets -- -D warnings'

# Plugin ha_moosedb.so → build/
docker run --rm -v "$PWD:/work" -v moosedb-cargo:/usr/local/cargo/registry \
  -v moosedb-build:/build moosedb-dev bash /work/docs/docker/build-plugin.sh

# Suíte MTR contra o servidor oficial
docker run --rm -v "$PWD:/work" moosedb-test bash /work/docs/docker/run-mtr.sh
```

Antes do primeiro commit: `git config core.hooksPath .githooks` (guardrails
automáticos — ver `CLAUDE.md`).

## Instalação

```ini
[mariadb]
plugin_maturity = experimental   # a engine se declara EXPERIMENTAL em v0.x
plugin_load_add = ha_moosedb
```

Procedures de manutenção (por banco): `SOURCE storage/moosedb/sql/moosedb_install.sql`.
