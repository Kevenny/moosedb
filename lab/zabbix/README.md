# Laboratório Zabbix 7.0 + MooseDB

Laboratório reutilizável para responder: **as tabelas de histórico do Zabbix
podem usar MooseDB? com que ganho/custo frente ao InnoDB?**

* MariaDB **11.4.13** (imagem oficial `mariadb:11.4.13`) com uma cópia de `build/ha_moosedb.so` em `plugin/` (copie com o MariaDB do lab parado: `mkdir -p plugin && cp ../../build/ha_moosedb.so plugin/`; nunca monte `build/` direto — um rebuild sobrescreve o .so em uso e derruba o `mariadbd` com SIGBUS)
  montado e carregado (`--plugin-maturity=experimental --plugin-load-add=ha_moosedb`);
* Zabbix **7.0 LTS** oficial: `zabbix/zabbix-server-mysql:alpine-7.0-latest`
  (7.0.31 na rodada abaixo) e `zabbix/zabbix-web-nginx-mysql:alpine-7.0-latest`;
* contêiner `tools` (python:3.12-slim, só biblioteca padrão) para API, carga e checagens.

O MariaDB 11.4.13 está dentro da faixa suportada do Zabbix 7.0.31
(`ZBX_MARIADB_MAX_VERSION` = 12.03.xx em `include/zbx_dbversion_constants.h`):
**`AllowUnsupportedDBVersions` não é necessário** (fica `0` no compose; mude
`ZBX_ALLOWUNSUPPORTEDDBVERSIONS=1` só se trocar a imagem por uma versão fora da faixa).

Dados ficam em volumes Docker nomeados (`zbxlab-mariadb`, `zbxlab-server`), nunca
no repositório. `results/` (logs e medições) é ignorado pelo git.

## Arquivos

| Arquivo | Para quê |
|---|---|
| `docker-compose.yml` | MariaDB + plugin, Zabbix server, frontend (porta 8088), `tools` (profile) |
| `scripts/create_items.py` | cria via API um host com N itens trapper (padrão 2000 float + 2000 uint + 1000 str) e liga/desliga o housekeeping de histórico |
| `scripts/sender.py` | gerador trapper (protocolo do `zabbix_sender`), 1 valor/item a cada `--interval` s |
| `scripts/api_check.py` | `history.get`, `item.get` (lastvalue) e `history.clear` pela API |
| `scripts/web_check.py` | `chart.php` (gráfico) e *Latest data* pelo frontend |
| `scripts/compat_test.sh` | tenta `ALTER TABLE history* ENGINE=MooseDB` no esquema oficial e isola cada incompatibilidade |
| `scripts/bench.sh` + `sql/bench_setup.sql` | benchmark InnoDB x MooseDB com dados sintéticos idênticos |
| `sql/adapted_history.sql` | variante adaptada v1 (itemid `BIGINT` TAG) |
| `sql/adapted_history_vtag.sql` | variante adaptada v2 (itemid `VARCHAR(20)` TAG) |
| `sql/adapted_history_v3.sql` | variante v3 (itemid `BIGINT` TAG, `clock` como eixo, `INDEX (itemid, clock)`, sem PK) |
| `sql/adapted_history_v4.sql` | **recomendada**: DDL oficial do Zabbix 7.0 com `PRIMARY KEY (itemid,clock,ns)` verificada; só `COMMENT 'TAG'` + `TIMESTAMP_COLUMN='clock'` |
| `sql/revert_history.sql` | volta às tabelas InnoDB oficiais |

## Subir

```bash
cd lab/zabbix
export MSYS_NO_PATHCONV=1          # só no Git Bash do Windows
docker compose up -d               # 1ª vez: o server cria o esquema oficial (~20 s)
docker logs zbxlab-server | grep "current database version"
# frontend: http://localhost:8088  (Admin / zabbix)
```

Portas (variáveis de ambiente): `MARIADB_PORT=13306`, `ZBX_WEB_PORT=8088`,
`ZBX_TRAPPER_PORT=10051`. Senha root do MariaDB: `ROOT_PASSWORD` (padrão `zbxlab`).

## Carregar

```bash
# host "zbxlab-load" com 5.000 itens trapper; desliga o housekeeper de histórico
docker compose --profile tools run --rm tools python scripts/create_items.py \
    --float 2000 --uint 2000 --str 1000 --history-hk off
# 1 valor/item a cada 10 s (500 valores/s) por 10 min
docker compose --profile tools run --rm tools python scripts/sender.py \
    --interval 10 --duration 600
```

### Trocar o histórico para MooseDB (com o server parado)

```bash
docker compose stop zabbix-server
docker exec zbxlab-mariadb mariadb -uroot -pzbxlab zabbix \
    -e "SOURCE /lab/sql/adapted_history_v4.sql"     # recomendada
docker compose start zabbix-server
# voltar ao InnoDB: SOURCE /lab/sql/revert_history.sql (as MooseDB ficam como *_moose;
# apague-as antes de aplicar o v4 de novo)
```

O housekeeper de histórico do Zabbix pode ficar **ligado** (o `DELETE` é suportado);
para expirar só por idade, `RETENTION_PERIOD` da tabela é mais barato (descarta o chunk).
`REPLACE`/`ON DUPLICATE KEY UPDATE` sobre chave existente seguem recusados (sem `UPDATE`).

## Medir

```bash
# compatibilidade (esquema oficial + variantes) — ~5 s
docker exec zbxlab-mariadb bash /lab/scripts/compat_test.sh > results/compat.txt
# funcional pelo Zabbix
docker compose --profile tools run --rm -w /lab/scripts tools python api_check.py --items /lab/results/items.json
docker compose --profile tools run --rm -w /lab/scripts tools python web_check.py
# benchmark (5.000 itens x 7 d x 60 s = 50,4 M linhas por tabela, ~25 min);
# pare o Zabbix antes para não disputar CPU
docker compose stop zabbix-web zabbix-server
docker exec -e ITEMS=5000 -e DAYS=7 -e STEP=60 -e RUNS=5 zbxlab-mariadb \
    bash /lab/scripts/bench.sh > results/bench_5000x7d.txt
# só refazer as consultas sobre as tabelas já carregadas: -e SKIP_LOAD=1
#   (o teste de housekeeping é destrutivo: após uma rodada, recarregue)
```

`bench.sh` cria o banco `zbxbench` com os **mesmos dados** (determinísticos; conferidos por
COUNT e checksum) em `h_innodb` (DDL oficial), `h_moose_v3` (sem PK, `INDEX (itemid, clock)`),
`h_moose_v4` (PK, recomendada) e as variantes antigas `h_moose_int`/`h_moose_str`.
`scripts/bench_delete.sh` mede os DELETEs do housekeeper (`MYSQL="mariadb -uroot -pzbxlab"`,
`ITEMS=5000 DAYS=7 STEP=300`, banco `zbxdel`).

Carga no formato do history syncer: um INSERT…SELECT (autocommit) por minuto com
5.000 linhas, em ordem de clock. Consultas = SQL exato do Zabbix (forma "zabbix")
e, nas MooseDB, a mesma consulta filtrando por `ts` (forma "nativo", o que a
engine faria se o eixo fosse `clock`). Tempo medido no servidor; melhor de 5,
cache quente (InnoDB buffer pool 4 GiB, `moosedb_chunk_cache_size` 1 GiB).

## Derrubar / reconstruir

```bash
docker compose down              # para e remove contêineres; volumes zbxlab-* ficam
docker compose up -d             # reconstrói com os mesmos dados
docker compose down -v           # apaga também os volumes (esquema recriado do zero)
```

## Resultados (2026-10-20, plugin do commit `ee8550c`, Docker Desktop/WSL2, 24 vCPU, 32 GiB)

Compatibilidade: com `sql/adapted_history_v4.sql` (DDL oficial + `COMMENT 'TAG'` +
`TIMESTAMP_COLUMN='clock'`) o Zabbix 7.0.31 roda sem mudança de código, usa o caminho com
PK e o housekeeper/`history.clear` funcionam. Ainda recusados: `REPLACE`/`ON DUPLICATE KEY
UPDATE` sobre chave existente. Histórico dos erros de versões antigas: `results/compat.txt`.

### InnoDB x v3 x v4 (50,4 M linhas por tabela = 5.000 itens × 7 d × 60 s; melhor de 5, cache quente)

| Métrica | InnoDB | MooseDB v3 (sem PK) | MooseDB v4 (PK) |
|---|---:|---:|---:|
| Carga (linhas/s) | 553.505 | 653.615 | 623.602 |
| Disco após carga / após `moosedb_compact` (MiB) | 2.384 / 2.384 | 446,2 / 392,9 | 448,4 / 392,9 (6,1x menor) |
| Gráfico 1 item 1 h / 24 h / 7 d (ms) | 0,49 / 1,89 / 4,69 | 0,55 / 1,89 / 4,41 | 0,63 / 1,88 / 4,62 |
| Último valor de 20 itens, caminho com PK (ms) | 0,91 | 3,20 | 0,96 |
| Agregação por hora, 100 itens, 24 h (ms) | 27,0 | 26,6 | 26,6 |
| Agregação LAST, 1 item, 24 h (ms) | 0,81 | 0,73 | 0,82 |
| Leitura do value cache, 1 h (ms) | 0,43 | 0,35 | 0,34 |
| Expirar 1 dia (6,76 M linhas) por 5.000 DELETEs | 17,3 s | 8,47 s | 8,53 s |
| Expirar o mesmo dia por `RETENTION_PERIOD` | — | 0,6 ms | 0,8 ms |
| **Housekeeper (DELETE por item)**: 1º dia, 1,44 M linhas (`bench_delete.sh`) | 5,69 s | 8,42 s | 8,35 s |
| Housekeeper: 20 % do 2º dia, `LIMIT 5000` | 4,24 s | 8,39 s | 8,48 s |
| `OPTIMIZE` com 5.000 tombstones pendentes | — | 0,24 s | 0,24 s |

Conferência: `COUNT(*)` e `sum(clock)`/`sum(ns)` idênticos nas três tabelas (50.400.000);
`sum(value)` difere na 13ª casa só pela ordem de soma de doubles; depois do housekeeper
`BIT_XOR(CRC32(itemid,clock,value,ns))` é igual (43.645.000 linhas) e no `bench_delete.sh` as
três ficam com 8.350.000 linhas. Leituras com tombstones pendentes ficam iguais às de antes de
apagar (gráfico 0,44 ms, agregação 6,6 ms, `COUNT(*)` 0,2 ms nas MooseDB).

O DELETE do MooseDB custa o `fsync` do WAL por statement (~1,7 ms neste disco, com
`wal_sync_mode=fsync` e `innodb_flush_log_at_trx_commit=1`): ~1,5–2x mais lento que o
InnoDB no housekeeper por item, mas o espaço é devolvido (o `.ibd` não encolhe) e a
expiração por idade via retenção é >10.000x mais rápida.

### Zabbix 7.0.31 real com history* em v4 (`results/zabbix_server_final.log`)

| Teste | Resultado |
|---|---|
| Ingestão trapper, 500 valores/s por 300 s (`sender.py`) | 150.000 enviados = 150.000 processados, 0 falhas |
| Erros SQL no log do server (`query failed`, `[Z3005]`) | 0 |
| Housekeeper ligado, por item (`hk_history_global=0`, 50 itens com histórico de 1 h, 5.000 linhas antigas) | `deleted 5000 hist/trends` em 0,15 s; linhas recentes (1.500) intactas |
| Housekeeper com período global de 1 h (2.000 linhas antigas) | `deleted 2000 hist/trends` em 0,41 s (o server cacheia `min(clock)`: reinicie-o após inserir linhas antigas à mão) |
| API: `history.get` (float/uint/str), `item.get` lastvalue (200 itens) | ok (6,7–69 ms) |
| API: `history.clear` de 1 item | ok em 23,8 ms; 0 linhas restantes do item |
| Frontend: `chart.php` 1 h / 24 h e *Latest data* | HTTP 200 (90 / 29 / 148 ms) |

Observação: o housekeeper não removeu o histórico de um item apagado pela API
(a tabela `housekeeper` recebeu só trends/events; não comparado com InnoDB).
