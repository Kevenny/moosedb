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
    -e "SOURCE /lab/sql/adapted_history_vtag.sql"     # ou adapted_history.sql (v1)
docker compose start zabbix-server
# voltar ao InnoDB: SOURCE /lab/sql/revert_history.sql (as MooseDB ficam como *_moose)
```

Obrigatório com histórico em MooseDB (config do Zabbix, sem mudar código):
*Administration → Housekeeping → History*: desmarcar **Enable internal housekeeping**
(`--history-hk off` faz isso pela API) e, de preferência, marcar **Override item
history period** com o mesmo período do `RETENTION_PERIOD` da tabela (31 d nos scripts).

Cuidado: v1 e v2 usam nomes de trigger diferentes (`*_moose_ts` / `*_vtag_ts`). Uma
tabela MooseDB sem o trigger grava `ts = 1970-01-01` e a retenção apagaria tudo
na próxima varredura — confira com `SELECT COUNT(*) FROM history WHERE ts < '2000-01-01'`.

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

`bench.sh` cria o banco `zbxbench` com três tabelas e os **mesmos dados**
(determinísticos por itemid/clock; conferidos por COUNT e `BIT_XOR(CRC32(...))`):

* `h_innodb` — DDL oficial do Zabbix 7.0 (InnoDB, `PRIMARY KEY (itemid,clock,ns)`);
* `h_moose_int` — variante v1 (itemid `BIGINT UNSIGNED` TAG, `ts DATETIME(6)` + `INDEX(ts)`);
* `h_moose_str` — variante v2 (itemid `VARCHAR(20)` TAG).

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

## Resultados (2026-10-07, Docker Desktop/WSL2, 24 vCPU, 32 GiB)

### Compatibilidade (`results/compat.txt`)

| Tentativa | Resultado |
|---|---|
| `ALTER TABLE history / history_uint / history_str / history_text / history_log / history_bin ENGINE=MooseDB` | **ERROR 1070** `Too many key parts specified; max 1 parts allowed` (PK de 3 colunas) — as 6 tabelas |
| sem PK e sem índice | **ERROR 1478** `a MooseDB table needs a DATETIME/TIMESTAMP column with an index` |
| sem PK, `INDEX(clock)` com `clock INT` | **ERROR 1478** (mesma mensagem: eixo inteiro não é aceito) |
| `PRIMARY KEY`/`UNIQUE (itemid,clock,ns)` + `INDEX(ts)` | **ERROR 1070** |
| `INDEX(itemid)` extra | **ERROR 1478** `index 'itemid' is not supported` |
| `clock TIMESTAMP` + INSERT inteiro do Zabbix | **ERROR 1292** `Incorrect datetime value: '1759843200'`; com `sql_mode=''` grava **0000-00-00** em silêncio |
| `ts` como coluna gerada de `FROM_UNIXTIME(clock)` | **ERROR 1901** (PERSISTENT) / **ERROR 1121** (VIRTUAL não pode ser NOT NULL no índice) |
| **adaptada**: sem PK, itemid TAG, `clock INT` mantido, `ts DATETIME(6)` + trigger | **aceita**; INSERT/SELECT do Zabbix funcionam |
| `delete from history where itemid=X and clock<Y` (housekeeper) | **ERROR 1031** `doesn't have this option` |
| `DELETE … WHERE itemid IN (…)` (history.clear) | **ERROR 1031** |
| duplicata (itemid,clock,ns) | InnoDB: ERROR 1062; MooseDB: **aceita** (2 linhas) |
| `alter table history modify value DOUBLE PRECISION …` (dbupgrade 6050008) | OK (ALTER por cópia) |

> Atualização (índice composto): a tabela acima é o registro da rodada de
> 2026-10-07. Hoje a engine aceita `INDEX (itemid)` e `INDEX (itemid, clock)`
> (TAG + tempo, não únicos); `PRIMARY KEY`/`UNIQUE` seguem recusados. As
> variantes `h_moose_v3` e `adapted_history_v3.sql` passaram a declarar
> `INDEX (itemid, clock)` além de `INDEX (clock)`.

Com a variante adaptada o Zabbix 7.0.31 roda sem mudança de código:
300.000 valores trapper (500/s por 600 s) processados, 0 falhas, todos gravados;
`history.get`, `item.get` (lastvalue), `chart.php` e *Latest data* OK. Log do
server: `database could be upgraded to use primary keys in history tables`
(frontend passa ao caminho sem PK). Com o housekeeper de histórico ligado:
`[Z3005] query failed: [1031] … [delete from history where itemid=… and clock<…]`
(o server segue rodando). `history.clear` → "Database error".

### InnoDB x MooseDB — 5.000 itens × 7 dias × 60 s = 50,4 M linhas por tabela

| Métrica | InnoDB (oficial) | MooseDB v1 (itemid BIGINT) | MooseDB v2 (itemid VARCHAR) |
|---|---:|---:|---:|
| Carga (10.080 INSERTs de 5.000 linhas) | 97,6 s (516 k lin/s) | 129,4 s (390 k lin/s) | 135,7 s (371 k lin/s) |
| Compactação completa (1×, `moosedb_compact`) | — | 19,5 s | 19,9 s |
| Tamanho em disco após carga | 2.384,0 MiB | 640,5 MiB | 649,6 MiB |
| Tamanho após compactação | 2.384,0 MiB | **568,1 MiB (4,2× menor)** | **568,1 MiB** |
| Housekeeping de 1 dia (3,07 M linhas) | 9,08 s (5.000 DELETEs por item) | **0,023 s** (descarta chunk) | **0,019 s** |
| Tamanho após expirar 1 dia | 2.384,0 MiB (não encolhe) | 533,3 MiB | 533,3 MiB |

Trigger de `ts` (só nas MooseDB adaptadas): 1 dia (7,2 M linhas) em 14,4 s com
`ts` no SELECT x 18,2 s pelo trigger (**+27%**).

Latência (ms, melhor de 5, cache quente). "zabbix" = SQL exato do Zabbix;
"nativo" = mesmo SQL filtrando por `ts`:

| Consulta | InnoDB | v1 zabbix | v1 nativo | v2 zabbix | v2 nativo |
|---|---:|---:|---:|---:|---:|
| Gráfico 1 item, 1 h (`getGraphAggregationByWidthFromSql`) | 0,53 | 2.173 | 144 | 1,52 | 0,61 |
| Gráfico 1 item, 24 h | 2,04 | 2.177 | 2.064 | 2,79 | 2,17 |
| Gráfico 1 item, 7 d | 4,66 | 2.150 | 3.606 | 4,92 | 5,22 |
| Último valor de 20 itens — caminho com PK | 1,21 | 52.672 | — | 20,97 | — |
| Último valor de 20 itens — caminho sem PK (usado sem PK) | 1,23 | 87.928 | 876 | 38,30 | 13,17 |
| Agregação por hora, 100 itens, 24 h (`IN` sem aspas) | 28,1 | 2.734 | 2.187 | 4.721 | 76,7 |
| Value cache do server, 1 item, 1 h (`itemid=N` sem aspas) | 0,45 | 2.533 | — | 3.292 | — |
| Agregação LAST por hora, 1 item, 24 h (self-join) | 0,76 | 7.205 | — | 8.947 | — |

Leitura: com **TAG string + itemid entre aspas** (frontend: gráficos, último
valor) a MooseDB fica na mesma ordem do InnoDB. Sempre que o Zabbix manda o
itemid como **número** (server/value cache, `dbConditionInt`) ou usa o itemid
BIGINT como TAG, não há pushdown e cada consulta varre a tabela inteira
(2–9 s em 50 M linhas, crescendo linearmente com o histórico).

## Atualização: PRIMARY KEY aceita — v4 é a variante recomendada

`PRIMARY KEY`/`UNIQUE (itemid,clock,ns)` **passou a ser aceita** (unicidade verificada
de verdade; `ER_DUP_ENTRY`, `INSERT IGNORE`). As linhas "PRIMARY KEY … ERROR 1070" e
"duplicata aceita" da tabela de compatibilidade acima são o registro da rodada de
2026-10-07. A variante **`sql/adapted_history_v4.sql`** é o DDL oficial do Zabbix 7.0
com apenas `COMMENT 'TAG'` em `itemid` e `ENGINE=MooseDB TIMESTAMP_COLUMN='clock'`
(mais as opções de chunk/retenção); sem `INDEX (clock)` nem trigger. Com ela o Zabbix
7.0.31 detecta a PK e usa o caminho com PK (`ORDER BY clock DESC, ns DESC LIMIT 1`
sem filesort).

Uso: `docker exec zbxlab-mariadb mariadb -uroot -pzbxlab zabbix -e "SOURCE /lab/sql/adapted_history_v4.sql"`
(server parado). `bench.sh` agora inclui `h_moose_v4`.

~~O housekeeper de histórico tinha de ficar desligado~~ — superado: ver
"Atualização: DELETE suportado" no fim deste arquivo (o `DELETE … itemid=… AND
clock<…` por item agora funciona; `RETENTION_PERIOD` segue sendo a forma barata de
expirar por idade). `REPLACE`/`ON DUPLICATE KEY UPDATE` sobre chave existente
continuam recusados (sem `UPDATE`).

### InnoDB x v3 x v4 (`results/bench_v4_5000x7d.txt`, 5.000 itens × 7 d × 60 s)

| Métrica | InnoDB | MooseDB v3 (sem PK) | MooseDB v4 (PK) |
|---|---:|---:|---:|
| Carga (linhas/s) | 524.949 | 463.456 | 433.342 |
| Disco (MiB) | 2.384 | 393,1 | 393,1 |
| Último valor, forma com PK (`lastvalue_pk`, ms) | 0,816 | 3,946 | 1,053 |
| Agregação por hora (`agg_hour`, ms) | 27,9 | 35,3 | 37,1 |
| Agregação LAST (`agg_last`, ms) | 0,71 | 1,19 | 1,31 |
| Expirar 1 dia | 16,3 s | 14,5 ms | 14,5 ms |

## Atualização: ingestão e agregação (item 6) — v4 passa o InnoDB na carga

Perfilando a carga (`perf` no `mariadbd`) e a agregação, o tempo estava em: gorilla
bit a bit no flush, SipHash e alocação por linha no `Batch::write`, um push/pop de
heap por linha no merge ordenado (e um grupo `Vec` por linha em tabelas com chave
única), e decodificação repetida das mesmas colunas. As correções (hash rápido com
semente, buffers reutilizados, dica de tamanho do lote, `BitWriter` de 64 bits,
merge por corridas, colunas de desempate sob demanda, cache de colunas decodificadas,
memo de TAG inteira no handler) estão em `docs/architecture.md` §Desempenho. Sem
mudança de formato em disco nem do protocolo de durabilidade (mesmo `fsync` por
statement do `innodb_flush_log_at_trx_commit=1`).

Mesmo container descartável (`mariadb:11.4.13`, mesmos parâmetros do
`docker-compose.yml`: `innodb_buffer_pool_size=4G`, `moosedb_chunk_cache_size=1 GiB`),
5.000 itens × 7 d × 60 s, `bench.sh` completo. `antes` = plugin do commit
`3538347` (`results/bench_item6_antes_5000x7d.txt`); `depois` = working tree
(`results/bench_item6_5000x7d.txt`). Consultas em melhor de 5, cache quente.

| Métrica | InnoDB antes → depois | v3 antes → depois | v4 (PK) antes → depois |
|---|---:|---:|---:|
| Carga (linhas/s) | 497.364 → 527.224 | 408.783 → 652.441 | 370.973 → 643.802 |
| Carga relativa ao InnoDB da mesma rodada | 1,00 | 0,82 → 1,24 | 0,75 → 1,22 |
| `agg_hour` (100 itens × 24 h, ms) | 27,6 → 27,8 | 32,0 → 25,2 | 35,9 → 27,2 |
| `agg_last` (1 item × 24 h, ms) | 0,76 → 0,78 | 1,05 → 0,86 | 1,15 → 0,93 |
| `vc_read_1h` (ms) | 0,36 → 0,40 | 0,42 → 0,33 | 0,37 → 0,43 |
| Disco após `moosedb_compact` (MiB) | 2.384 | 395,0 | 395,0 |

Rodada original do item 5 (outra hora, mesma máquina): InnoDB 524.949, v3 463.456,
v4 433.342 linhas/s; `agg_hour` 27,9 / 35,3 / 37,1 ms; `agg_last` 0,71 / 1,19 / 1,31 ms.

`agg_last` é uma consulta de menos de 1 ms (≈ 50 lookups `(itemid, clock[, ns])`) e
varia ±10 % entre rodadas; reexecutando só as consultas sobre as mesmas tabelas
(`results/bench_item6_agg_15runs.txt`, 3 × melhor de 15): InnoDB 0,69–0,75 ms,
v3 0,73–0,78 ms, v4 0,75–0,80 ms (`agg_hour`: InnoDB 27,4–27,8, v3 25,3–25,7, v4
26,6–27,2 ms). Ou seja, a agregação por hora empata/ganha do InnoDB e o `AGGREGATE_LAST`
ficou a ~5–10 % (antes: +85 %). Com `SKIP_LOAD=1` (`results/bench_item6_consultas_skipload.txt`,
depois do teste destrutivo de housekeeping): `agg_last` 0,77 / 0,93 / 0,85 ms.
Nenhuma outra consulta regrediu além do ruído (gráficos 1 h / 24 h / 7 d, último valor; `vc_read_1h` do v4 mediu 0,43 ms na rodada completa e 0,36–0,37 ms nas repetições).

**Para reproduzir sem tocar nos volumes `zbxlab-*`**: container próprio com
`--plugin-dir` apontando para uma cópia do `ha_moosedb.so` e os mesmos `-v` de `sql/` e
`scripts/`, depois `docker exec -e ITEMS=5000 -e DAYS=7 -e STEP=60 <container> bash /lab/scripts/bench.sh`.

## Atualização: DELETE suportado (item 5) — o housekeeper pode ficar ligado

`DELETE` passou a funcionar em todas as variantes (v3 sem PK, v4 com PK): o
`delete from history where itemid=X and clock<CUT [limit N]` do housekeeper e o
`delete from history where itemid in (...)` do `history.clear` viram **um
tombstone** (intervalo de tempo × série) gravado no WAL, com `ROW_COUNT()` exato;
os chunks só são reescritos pela compactação (≥ 25 % do chunk morto, ou
`OPTIMIZE`), e um chunk inteiramente apagado é descartado sem reescrita. Outros
`WHERE` (`value > x`, `OR`, multi-tabela, `binlog_format=ROW`) apagam linha a
linha, achando as linhas por conteúdo. Detalhes: `docs/architecture.md` §DELETE e
`docs/user-guide.md` §DELETE.

**O housekeeper de histórico pode ser ligado**, mas para expirar só por idade
`RETENTION_PERIOD` continua melhor: descarta o chunk sem um tombstone por item
(`retention_drop_chunk` acima: 14 ms) e sem fsync por item. Use o housekeeper para
o que a retenção não faz: apagar por item (itens removidos, `history.clear`) ou
períodos por item diferentes (*Override item history period*). As linhas
"housekeeper (`delete … itemid=X and clock<Y`) ERROR 1031" e "`DELETE … IN (…)`
ERROR 1031" da tabela de compatibilidade acima são o registro da rodada de
2026-10-07; o log `[Z3005] query failed: [1031]` e o "Database error" do
`history.clear` descritos acima não ocorrem mais.

### 5.000 DELETEs do housekeeper (`scripts/bench_delete.sh`, `results/bench_delete_5000x7d.txt`)

5.000 itens × 7 dias × 300 s = 10,08 M linhas por tabela; um `DELETE` por item
(`delete from t where itemid=X and clock<CUT`, autocommit, via PREPARE/EXECUTE
num procedimento, como `bench_hk_delete`). Container de teste (mesma máquina,
mesmo disco), `wal_sync_mode=fsync` (padrão) e `innodb_flush_log_at_trx_commit=1`:

| Métrica | InnoDB | MooseDB v3 | MooseDB v4 |
|---|---:|---:|---:|
| Apagar o 1º dia: 5.000 DELETEs (1,44 M linhas) | 6,72 s | 8,97 s | 9,14 s |
| 20 % do 2º dia, `LIMIT 5000` (0,29 M linhas) | 4,50 s | 8,81 s | 9,08 s |
| Mesmo com `--moosedb-wal-sync-mode=write` / `innodb_flush_log_at_trx_commit=2` (v4, 576 linhas/item) | 2,98 s | — | **1,41 s** |
| Linhas depois (estatística `ROW_COUNT`, igual ao `COUNT(*)`) | 8.350.000 | 8.350.000 | 8.350.000 |

O custo do MooseDB é o `fsync` do WAL por statement (≈ 1,8 ms neste disco; com
`write` o `DELETE` custa 0,28 ms, dos quais o núcleo gasta ~1,3 µs: um tombstone,
sem tocar nas linhas). O InnoDB também faz fsync por commit, mas no redo log já
alocado (≈ 1,2 ms); o WAL do MooseDB cresce a cada entrada e o fdatasync paga o
metadado do arquivo. Ou seja, em fsync estrito o housekeeper do MooseDB é ~1,35×
**mais lento** que o do InnoDB nesta máquina (no laboratório anterior o InnoDB
levou 16 s; os números absolutos dependem do disco); sem fsync é ~2× mais rápido.

Leitura com 5.000 tombstones pendentes no mesmo chunk, antes de compactar (ms,
melhor de 5, cache quente):

| Consulta | antes de apagar | com tombstones pendentes | depois do `OPTIMIZE` |
|---|---:|---:|---:|
| Gráfico 1 item, 24 h — v4 | 0,45 | 0,44 | 0,49 |
| Último valor de 20 itens — v4 | 0,78 | 0,84 | 0,70 |
| Agregação por hora, 100 itens, 24 h — v4 | 6,82 | 7,01 | 6,92 |
| Último valor de 20 itens — v3 | 3,28 | 2,82 | 3,04 |
| `COUNT(*)` — v3/v4 (estatística exata) | 0,25 | 0,25 | 0,26 |
| (InnoDB, referência: gráfico / último valor / agregação) | 0,43 / 0,75 / 6,8 | 0,47 / 0,77 / 6,6 | 0,52 / 0,90 / 6,6 |

O `OPTIMIZE` que reescreve o chunk com 5.000 tombstones (1,15 M linhas vivas) leva
0,23 s; o descarte do 1º dia (chunk inteiro morto) não reescreve nada.

O caminho direto foi comprovado por `Handler_delete` (0 no direto, N linha a linha) em
`mysql-test/suite/moosedb/t/delete.test`. Observação do item: o servidor só chama o
*direct delete* se `cond_push` devolver NULL para o `WHERE` inteiro; a engine devolve
NULL **apenas** para o alvo de um `DELETE` com `WHERE` totalmente capturado (TAG exata
+ limites de tempo) e qualquer outra condição volta ao servidor (fail-closed).
