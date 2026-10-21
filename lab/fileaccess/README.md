# Laboratório "acesso a arquivos": InnoDB x MooseDB x ClickHouse

Réplica sintética de um relatório de cliente (MariaDB/InnoDB x ClickHouse, 20 M linhas, monitoramento de
acesso a arquivos). Mesmos dados, mesmas 9 consultas, cinco configurações, resultados conferidos por
checksum. Estilo do `lab/zabbix/` (compose, `bench.sh` com melhor-de-N e tempo medido no servidor,
`results/` ignorado, plugin em cópia própria).

## Como rodar

```bash
cd lab/fileaccess
# 1. plugin do HEAD (fluxo da skill moosedb-build) e cópia própria
docker run --rm -v "$PWD/../..:/work" -v moosedb-cargo:/usr/local/cargo/registry -v moosedb-build:/build moosedb-dev bash /work/docker/build-plugin.sh
cp ../../build/ha_moosedb.so plugin/
# 2. dados (determinísticos, ~2,8 GB em 20 arquivos TSV de 1 M linhas) e volume de dados
python scripts/gen_data.py --rows 20000000 --out data/20m     # ~2 min; --rows 2000000 --out data/2m p/ iterar
docker compose up -d --no-start
docker run --rm -v fileacc-data:/d -v "$PWD/data:/src:ro" alpine cp -r /src/20m /d/    # bind do Windows é lento
docker compose up -d
# 3. carga + consultas + checksums + EXPLAIN/Handler_*  (~45 min em 20 M)
DATA=20m bash scripts/bench.sh | tee results/bench_20m.txt
ENGINES="innodb moose moose_h" bash scripts/breakdown.sh | tee results/breakdown_20m.txt   # onde o tempo vai
docker compose down            # -v apaga também os volumes fileacc-* (~20 GB no 20 M)
```

Parâmetros do `bench.sh`: `DATA` (20m/2m), `STAGE` (load|query|explain|all), `ENGINES`, `RUNS` (5 execuções
quentes), `COLD` (1 = reinicia o servidor antes de cada engine, então a "1ª execução" é com buffer pool e cache
da engine frios, mas page cache do SO quente; 2 = também `drop_caches`).

## Esquema e suposições (não temos o esquema real do cliente)

`evento(ts DATETIME(6), host, process, user_name, folder, file, op, bytes BIGINT, latency_us INT)`.

| Item | Suposição |
|---|---|
| Volume | 20 M eventos em 7 dias (2026-10-12 a 2026-10-19 UTC), ordenados por `ts` na carga (ingestão em ordem de relógio). 2.000 hosts, 300 processos, 50 mil pastas, 2,05 M arquivos distintos observados (pool de 2,4 M), 1.500 usuários + 8 contas de serviço. |
| Tempo | Dia útil 8-18 h com peso 1, 6-22 h com 0,25, madrugada 0,05; fim de semana x0,15. Microssegundos uniformes dentro da hora. |
| Hosts | Atividade lognormal (sigma 1,2); 1 em 20 hosts é servidor com 5x o volume. |
| Processos | Cada host roda um perfil de k processos distintos (k lognormal, mediana 35, entre 8 e 150), sorteados por popularidade global Zipf(1,1) e usados com Zipf(1,0) dentro do perfil. Dá ~70 mil séries (host, process) por dia; sem o perfil seriam ~600 mil e a TAG dupla seria irreal. |
| Arquivos | Popularidade Zipf(0,9) sobre 2,4 M; cada arquivo pertence a 1 pasta (tamanhos de pasta enviesados, expoente 0,6). Caminho = `pasta/f<id>.<ext>` (~55 caracteres). O arquivo mais popular tem 587 mil acessos (3%). |
| Operações | read 52%, open 20%, write 18%, rename 4%, delete 3%, create 3%. `bytes` lognormal (mediana 8 KiB) só em read/write (0 nas demais). `latency_us` lognormal por operação (mediana de 120 a 900 us) com 0,5% de outliers x20. |
| Alvos | Dia = 2026-10-15 (quinta); hora = 2026-10-15 10:00-11:00; host = `srv-0677` (100º mais ativo, 34.903 eventos em 7 dias); arquivo = `/srv/dept04/team04/proj30164/logs/f0000201.docx` (200º mais acessado, 4.996 acessos). Ficam em `data/<n>/params.env`. |
| Collation | `utf8mb4_bin` no MariaDB (comparação por bytes, igual ao ClickHouse; dados ASCII): os `ORDER BY ..., chave` desempatam igual e os checksums batem. |

Tabelas:

* **InnoDB** (`evento_innodb`): PK `id` auto-incremento + `(ts)`, `(host,ts)`, `(process,ts)`. Os três índices
  dão 1,7 GB (índice/dados = 0,52; no relatório, 2,1/3,4 = 0,62). Sem índice em `file`, porque os 53 s do
  relatório em "quem acessa um arquivo" só são coerentes com varredura. Variante `evento_innodb_f` = a mesma + `KEY(file)`.
* **MooseDB** (`evento_moose`): `host` e `process` como TAG (pedido), `ts` como eixo, `INDEX(ts)`,
  `(host,ts)`, `(process,ts)`, `(host,process,ts)`, `CHUNK_INTERVAL='1 DAY'`, ZSTD, `HOT_THRESHOLD='1 DAY'`
  (todo chunk vira frio/ZSTD após `moosedb_compact`). `folder` e `file` **não** são TAG: cada valor distinto viraria
  uma série (50 mil e 2 M), o que destrói a compressão por série e estoura o cache de séries; ficam como
  colunas comuns e sem índice (a engine só indexa tempo e TAGs). Variante de controle `evento_moose_h`: só
  `host` como TAG (2 mil séries por chunk em vez de 69 mil).
* **ClickHouse** (`fileacc.evento`): `MergeTree PARTITION BY toDate(ts) ORDER BY (host, ts)`, `LowCardinality`
  em host/process/user/op, `String` em folder/file, `DateTime64(6)`. A ordem `(host, ts)` favorece q06/q07;
  as consultas por janela sem host dependem da partição diária e do paralelismo.

Limitação: a MooseDB recusa `process VARCHAR(64)` utf8mb4 como TAG em índice composto; por isso `process` é
`VARCHAR(63)` nas três engines (ver achado 5).

## Consultas (idênticas nos dialetos; `sql/` = MariaDB, `ch/` = ClickHouse)

| Arquivo | Consulta |
|---|---|
| q01 | Top 10 hosts por `SUM(bytes)` no dia |
| q02 | Top 20 pastas por `COUNT(*)` de `op = 'read'` numa hora |
| q03 | Top 20 processos (todo o ambiente) numa hora: `COUNT`, `SUM(latency_us)`, `MAX(latency_us)` |
| q04 | Top 20 arquivos por `COUNT(*)` no dia |
| q05 / q05b | Quem acessa o arquivo alvo (host, process, `COUNT`), 7 dias / no dia |
| q06 | Top 20 processos do host alvo no dia |
| q07 / q07b | Série por minuto (`COUNT`, `SUM(bytes)`) do host alvo, no dia (1.440 pontos) / numa hora |

Diferenças deliberadas: **p95 não está no q03** (o MariaDB não tem agregado de percentil; `PERCENTILE_CONT` é
função de janela e exigiria subconsulta, não trivial) e **AVG virou `SUM(latency_us)`** (AVG = soma/n, mesmo
custo), para o checksum não depender de arredondamento decimal x float. A única diferença de dialeto é
`DATE_FORMAT` x `formatDateTime`. Todo `ORDER BY` desempata pela chave; `LIMIT` fixo.

## Recursos

Contêineres iguais: **8 CPUs e 10 GiB cada** (host: 24 CPUs e 30 GiB; uma engine por vez sob carga).
MariaDB 11.4.13: `innodb_buffer_pool_size=6G` (dados + índices do InnoDB = 5 GB: tudo em memória, cache
quente), `innodb_log_file_size=2G`, `moosedb_chunk_cache_size=1G`, `tmp_table_size = max_heap_table_size = 512M`;
cada consulta usa **1 thread**. ClickHouse 26.9, `--max_threads 8`, configuração padrão (sem cache de
blocos descomprimidos: "quente" = page cache). Tempo do MariaDB: `NOW(6)` antes e depois no servidor;
ClickHouse: `clickhouse-client --time` (resolução de 1 ms; q06/q07 estão no piso de ~3 ms).

## Resultado final (20 M linhas; melhor de 5 execuções quentes, em segundos; entre parênteses, a 1ª execução)

Os result sets das 9 consultas são **idênticos nas 5 configurações** (md5 por consulta, 0 divergências).

| Consulta | InnoDB | InnoDB + KEY(file) | MooseDB (host+process TAG) | MooseDB (host TAG) | ClickHouse |
|---|---:|---:|---:|---:|---:|
| q01 top 10 hosts por volume, dia | 3,18 (11,94) | 3,18 (11,28) | 3,35 (6,50) | 2,03 (2,23) | 0,016 |
| q02 top 20 pastas lidas, hora | 0,497 (0,516) | 0,513 (0,620) | 0,680 (1,59) | 0,472 (0,715) | 0,028 |
| q03 top processos + latência, hora | 0,297 (0,298) | 0,303 (0,296) | 0,398 (0,473) | 0,214 (0,328) | 0,012 |
| q04 top 20 arquivos, dia | 14,87 (14,63) | 14,81 (15,10) | 17,73 (19,02) | 14,41 (15,11) | 0,070 |
| q05 quem acessa o arquivo, 7 dias | 1,53 (17,64) | 0,0118 (1,05) | 2,22 (4,60) | 3,04 (3,59) | 0,115 |
| q05b quem acessa o arquivo, dia | 2,30 (2,34) | 0,0095 (0,0097) | 3,26 (3,92) | 1,63 (1,76) | 0,034 |
| q06 top processos de um host, dia | 0,0136 (0,0184) | 0,0132 (0,0169) | 0,0048 (0,037) | 0,0038 (0,0059) | 0,003 |
| q07 série por minuto de um host, dia | 0,0150 (0,0158) | 0,0142 (0,0142) | 0,0059 (0,0061) | 0,0047 (0,0052) | 0,003 |
| q07b série por minuto de um host, hora | 0,0018 (0,0021) | 0,0017 (0,0017) | 0,0012 (0,0013) | 0,0009 (0,0010) | 0,003 |

Carga e disco:

| | InnoDB | InnoDB + KEY(file) | MooseDB (host+process) | MooseDB (host) | ClickHouse |
|---|---:|---:|---:|---:|---:|
| Carga (20 partes de 1 M; LOAD DATA / INSERT FORMAT TSV) | 105 s (190 k linhas/s) | 163 s (123 k/s) | 136 s (147 k/s) | 58 s (346 k/s) | 9,1 s (2,2 M/s) |
| Pós-carga (compactação / OPTIMIZE FINAL) | - | - | +48 s | +22 s | +9 s |
| Disco após a carga | 5.028 MiB | 6.680 MiB | 1.126 MiB | 632 MiB | 877 MiB |
| Disco final | 5.028 MiB (3.260 dados + 1.690 índices) | 6.680 MiB (3.260 + 3.321) | 778 MiB | 555 MiB | 519 MiB |

ClickHouse por coluna (comprimido): file 211 MiB, folder 127, ts 76, bytes 43, latency_us 36, process 16, op 6, user_name 3.

A rodada de 20 M está em `results/bench_20m.txt`; a de 2 M (usada para iterar e validar, com os mesmos checksums
iguais) em `results/bench_2m.txt`. `results/` é ignorado pelo git.

Comparação com o relatório do cliente (InnoDB / ClickHouse): carga 9,9x lá, 11,6x aqui; disco 13,8x lá, 9,7x aqui;
q01 150x lá, 200x aqui; q07, InnoDB mais rápido que o CH lá (0,16 x 0,22 s), aqui empatam no piso de poucos ms.
As ordens de grandeza se reproduzem; os tempos absolutos do InnoDB são menores aqui (SSD local, buffer pool com
tudo em memória).

## Agregação na engine (group_by_handler, working tree pós-5c771db, NÃO commitado)

`SET moosedb_group_by_pushdown=ON` (sessão; padrão OFF), `moosedb_agg_threads` (global). Mesmos dados, 20 M linhas,
melhor de 5 quentes (s) e, entre parênteses, 1ª execução (servidor reiniciado). `bench.sh` ganhou `GBH=1` e `AGG_THREADS=N`
(`gbh_engine=sim|nao` por consulta via `Moosedb_gbh_queries`). Resultados em `results/gbh_off.txt`, `results/gbh_on_{1,4,8}.txt`.
Checksums (md5) das 9 consultas **idênticos** a InnoDB e ClickHouse em OFF e ON (0 divergências). Plugin compilado do
working tree em 2026-10-20 (container de 8 CPUs).

| Consulta | InnoDB | MooseDB OFF | MooseDB ON 4 thr | MooseDB ON 8 thr | MooseDB-h OFF | MooseDB-h ON 4 thr | MooseDB-h ON 8 thr | ClickHouse | Na engine? |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| q01 | 3,07 (7,80) | 3,28 (4,18) | 0,125 (0,487) | 0,148 (0,448) | 1,98 | 0,0039 (0,032) | 0,0048 | 0,014 | sim |
| q02 | 0,510 | 0,675 (1,44) | 0,672 | 0,691 | 0,447 | 0,445 | 0,552 | 0,028 | não (op='read') |
| q03 | 0,289 | 0,368 (0,483) | 0,206 (0,255) | 0,235 | 0,215 | 0,0083 (0,068) | 0,0133 | 0,012 | sim |
| q04 | 14,82 | 17,54 (18,18) | 1,16 (1,38) | 1,22 | 14,57 | 1,15 (1,44) | 1,30 | 0,074 | sim |
| q05 | 1,53 (13,4) | 2,15 | 2,03 | 2,14 | 2,97 | 3,06 | 3,42 | 0,105 | não (file=) |
| q05b | 2,22 | 3,19 | 3,29 | 3,54 | 1,65 | 1,58 | 1,86 | 0,030 | não (file=) |
| q06 | 0,0130 | 0,0047 | 0,0046 | 0,0051 | 0,0038 | 0,0008 | 0,0009 | 0,003 | sim |
| q07 | 0,0138 | 0,0056 | 0,0058 | 0,0057 | 0,0047 | 0,0046 | 0,0047 | 0,004 | não (GROUP BY de expressão) |
| q07b | 0,0017 | 0,0012 | 0,0012 | 0,0014 | 0,0009 | 0,0010 | 0,0010 | 0,003 | não (idem) |

Com 1 thread: q01 0,085 (moose) / 0,0045 (moose_h); q03 0,089 / 0,018; q04 1,44 / 1,19. Mais threads não ajudaram: o
tempo restante não é paralelizável por este caminho (ver abaixo). O ClickHouse continua 15x mais rápido no q04 e ~9x no q01 (moose).
Os "não" das q02/q05/q05b são recusas por predicado em coluna comum; q07/q07b ficam no caminho normal (GROUP BY de
`DATE_FORMAT(ts)`), por isso não mudam.

Onde o tempo vai agora: q04 inteiro é engine (EXPLAIN "Storage engine handles GROUP BY", sem tabela temporária no MariaDB; 17,5 s -> 1,16 s,
15x), dominado por 3,7 M grupos distintos de `file` (insensível a threads: 1,44 / 1,16 / 1,22 s com 1/4/8). q02 (caminho normal; `ANALYZE`):
0,49 s na engine (316.990 linhas lidas por `ix_ts`, 52% passam no filtro) + 0,44 s no executor. q05/q05b: varredura de coluna comum sem índice.

## Passo 6 do group_by_handler: WHERE em coluna comum, filtro exato, top-N (working tree, NÃO commitado)

Plugin recompilado do working tree em 2026-10-21 (inclui paralelismo por partição e cache de blocos fragmentado).
`bench.sh` agora imprime, por consulta, os deltas de `Moosedb_gbh_*` (`queries`, `exact_filters`, `topn`, `dict_us`,
`agg_us`, `post_us`, `eval_us`) e aceita `ENGINES=moose_hn`. Resultados: `results/p6_off.txt`, `results/p6_on_{1,4,8}.txt`.
Melhor de 5 quentes, em segundos (entre parênteses, nº de threads do melhor ON). Checksums das 9 consultas **idênticos**
(InnoDB, ClickHouse, moose, moose_h, moose_hn; OFF e ON 1/4/8; 0 divergências).

| Consulta | InnoDB | MooseDB OFF | MooseDB-h OFF | MooseDB ON | MooseDB-h ON | MooseDB nopad ON (`moose_hn`) | ClickHouse |
|---|---:|---:|---:|---:|---:|---:|---:|
| q01 | 3,00 | 3,33 | 1,99 | 0,0345 (8) | 0,0022 (8) | 0,0022 (8) | 0,0150 |
| q02 | 0,496 | 0,693 | 0,460 | 0,0643 (8) | 0,0110 (8) | 0,0059 (8) | 0,0290 |
| q03 | 0,286 | 0,401 | 0,222 | 0,0396 (8) | 0,0038 (8) | 0,0039 (8) | 0,0120 |
| q04 | 14,63 | 17,75 | 14,73 | 0,281 (8) | 0,265 (8) | 0,143 (8) | 0,0760 |
| q05 | 1,56 | 2,22 | 3,16 | 1,02 (8) | 0,873 (8) | 0,212 (8) | 0,104 |
| q05b | 2,14 | 3,30 | 1,66 | 0,190 (8) | 0,134 (8) | 0,0083 (8) | 0,0320 |
| q06 | 0,0132 | 0,0048 | 0,0038 | 0,0045 (8) | 0,0007 (4) | 0,0007 (8) | 0,0030 |
| q07 | 0,0141 | 0,0056 | 0,0047 | 0,0054 (4) | 0,0045 (4) | 0,0046 (8) | 0,0030 |
| q07b | 0,0017 | 0,0012 | 0,0009 | 0,0011 (8) | 0,0008 (8) | 0,0009 (4) | 0,0030 |

Pela engine: q01-q06 (q07/q07b ficam no caminho normal: GROUP BY de expressão). Filtro exato (`Moosedb_gbh_exact_filters=1`):
só em `moose_hn` (q02, q05, q05b). Top-N (`Moosedb_gbh_topn`): **0 em todas as 9 consultas** (ver nota).
`moose_hn` = cópia de `moose_h` com `folder`, `file` e `op` em `utf8mb4_nopad_bin` (`INSERT...SELECT` por dia, `moosedb_compact`);
muda a semântica de comparação nessas colunas (espaços finais passam a contar; `utf8mb4_bin` é PAD SPACE, `'a'='a '`), o
gerador não produz valores que difiram só por espaço final, e os checksums batem.

Tempos por fase da engine, 8 threads (ms; `dict` = máscara de dicionário, `agg` = agregação, `post` = ordenação/emissão):

| | dict | agg | post |
|---|---:|---:|---:|
| q04 moose / moose_h / moose_hn | 0 / 0 / 0 | 120 / 116 / 99 | 160 / 189 / 39 |
| q05 moose / moose_h / moose_hn | 801 / 775 / 0,003 | 284 / 196 / 291 | 7 / 3 / 0,7 |
| q05b moose / moose_h / moose_hn | 155 / 131 / 0,002 | 28 / 9 / 9 | 5 / 2 / 0,2 |
| q02 moose / moose_h / moose_hn | 26 / 2,6 / 0,001 | 35 / 4,9 / 4,7 | 3,7 / 3,4 / 0,5 |

`eval_us` fica em 4-86 ms (avaliação do predicado sobre o dicionário, só nas consultas com filtro não exato).

Nota: quando valem os atalhos.

* **Filtro exato** (`col = const` / `col IN (...)` comparado por bytes no core, sem montar o dicionário de valores): só para colunas
  inteiras ou VARCHAR com collation **binária NO PAD** (`utf8mb4_nopad_bin`). Com `utf8mb4_bin` (PAD SPACE, o padrão do lab) o handler
  monta a máscara por dicionário: q05 gasta 0,8 s em `dict_us` (2,05 M valores distintos de `file`) e q05b 0,13 s.
* **Top-N no core**: só `ORDER BY <um agregado> [DESC] LIMIT K` com uma chave de grupo, sem HAVING, sem TAG, sem string de collation
  que precise de merge (PAD SPACE), sem AVG, sem BIGINT UNSIGNED, e **sem segundo termo no ORDER BY**. As 9 consultas do lab têm o
  desempate `ORDER BY n DESC, chave`, então nunca disparam. Teste avulso (8 threads, `moose_hn`, sem o desempate): q04 0,084 s
  (era 0,143; `post` 39 ms para 0,008 ms) e q02 0,0091 s (era 0,0059 com desempate: sem ganho, o `post` já era 0,5 ms). Em `moose_h`
  (PAD SPACE) o top-N não dispara mesmo sem desempate (0,250 s); resultado desse teste avulso não é conferido por checksum (empates).

## Onde o tempo vai na MooseDB

`EXPLAIN` (MooseDB host+process TAG; saída completa em `results/bench_20m.txt`):

```
q01, q04, q05b  type=range key=ix_ts      rows=3.771.678  Using where; Using temporary; Using filesort
q05             type=ALL                  rows=20.000.000 Using where; Using temporary; Using filesort
q02, q03        type=range key=ix_ts      rows=157.153    Using where; Using temporary; Using filesort
q06, q07, q07b  type=range key=ix_host_ts rows=2.161 (91 na hora)
```

`ANALYZE FORMAT=JSON`, em ms: tempo na engine (`r_table_time`) / no executor (`r_other_time`):

| Consulta | MooseDB host+process | MooseDB host | InnoDB |
|---|---:|---:|---:|
| q01 | 2.751 / 1.059 | 1.149 / 1.007 | 2.301 / 901 |
| q02 | 816 / 333 | 421 / 317 | 192 / 316 |
| q04 | não medido | 2.269 / 12.327 | não medido |
| q05 (varredura de 20 M) | 3.982 / 299 | 3.614 / 301 | 1.670 / 305 |
| q05b | 3.868 / 102 | 1.612 / 92 | 2.260 / 86 |

O `ANALYZE` instrumenta cada linha e infla os tempos de varredura (~1,5x) em relação ao `bench.sh`.

`scripts/breakdown.sh` (s, melhor de 3) isola a leitura:

| | InnoDB | MooseDB host+process | MooseDB host |
|---|---:|---:|---:|
| COUNT(*) do dia (3,77 M linhas, via ix_ts) | 0,28 | 1,92 | 0,86 |
| SUM(latency_us) do dia | 1,44 | 2,30 | 1,05 |
| SUM(LENGTH(file)) do dia | 1,41 | 3,04 | 1,45 |
| SUM(LENGTH(file)+LENGTH(folder)) do dia | 1,60 | 3,83 | 1,91 |
| GROUP BY file do dia (3,7 M grupos) | 16,7 | 19,0 | 17,7 |
| SUM(latency_us) nos 7 dias, sem WHERE (20 M linhas) | 1,42 | 1,13 | 0,81 |
| SUM(LENGTH(file)) nos 7 dias | 1,39 | 1,94 | 2,37 |

`Handler_*` do q01 (MooseDB host+process): `Handler_read_key` 3.771.679, `Handler_read_next` 3.771.678,
`Handler_tmp_update` 3.769.678 (uma atualização de tabela temporária por linha). q05: `Handler_read_rnd_next` 20.003.674.

## Achados (ranqueados por impacto medido)

1. **[alto impacto, esforço médio, risco médio] O range por `ix_ts` é um k-way merge por tempo entre as séries e
   custa 1,6-3,6 M linhas/s, contra 17,7-25 M linhas/s da varredura sem ordem.** O `SUM` dos 20 M de linhas (sem WHERE,
   `rnd_next`) leva 1,13 s (17,7 M linhas/s) com host+process TAG, mas as 3,77 M linhas de um dia por `ix_ts`
   levam 2,30 s (1,6 M/s); com host TAG, 1,05 s (3,6 M/s). Com 69 mil séries por chunk o merge é 2,2x mais lento que
   com 2 mil (COUNT(*) do dia: 1,92 x 0,86 s). As agregações q01-q05b não dependem de ordem; uma leitura por faixa
   de tempo sem ordenação (poda por chunk + `rnd_next` filtrado) deveria levar a parte de engine do dia a
   ~0,2-0,3 s (estimativa por extrapolação do `SUM` de 7 dias, **não medida**). É o ganho mais barato para igualar
   ou passar o InnoDB nos filtros por dia (hoje 1,05-1,4x mais lento nos q01-q03 com host+process).
2. **[alto impacto, esforço alto, risco médio] q04 (GROUP BY de 3,7 M arquivos distintos) é 83% executor do MariaDB,
   nas três engines (14,4-17,7 s).** `ANALYZE`: 2,27 s na engine e 12,3 s em `r_other_time` (tabela temporária: 3,69 M
   `Handler_tmp_write` + 2,8 M `tmp_update`). A engine entrega as linhas em ~15% do tempo; só agregação empurrada
   para a engine (o ClickHouse faz em 0,07 s) resolve. Alternativa de aplicação: tabela de resumo diária
   `(dia, file, n)` (esforço baixo, risco baixo; não medida).
3. **[alto impacto, esforço baixo, risco baixo] Modelagem: TAG em `process` além de `host` custou +40% de disco
   (778 x 555 MiB), 2,4x de carga (136 x 58 s) e 1,2-1,9x nas consultas de janela** (q01 3,35 x 2,03 s;
   q02 0,68 x 0,47; q03 0,40 x 0,21; q04 17,7 x 14,4). Exceção: q05/q05b, varredura sem filtro de TAG (q05 2,22 x 3,04 s é ruído de
   varredura). Só use TAG dupla se a consulta filtrar pela segunda (q06: 4,8 x 3,8 ms, quase igual). Com só `host`,
   a MooseDB empata com o ClickHouse em disco (555 x 519 MiB).
4. **[alto impacto para q05, esforço alto, risco médio] Sem índice secundário em coluna não-TAG, "quem acessa um
   arquivo" é varredura de 20 M linhas (1,6-3,3 s):** 170-340x mais lento que o InnoDB com `KEY(file)` (0,0118 e
   0,0095 s) e 1,4-2,0x mais lento que o InnoDB sem esse índice (1,53 e 2,30 s). `file` como TAG foi descartado
   (2 M de séries): **não medido**. O ClickHouse resolve o caso em 0,115 s varrendo a coluna comprimida com 8 threads.
5. **[médio impacto, esforço baixo, risco baixo] Bug de validação: `process VARCHAR(64)` utf8mb4 (256 bytes de chave)
   como TAG é recusado em índice composto** ("index prefixes are not supported"), enquanto `VARCHAR(63)` e
   `VARCHAR(100)` latin1 passam. A checagem `kp.length != f->key_length()` em `storage/moosedb/ha_moosedb.cc:723`
   falha exatamente quando a chave passa de 255 bytes (causa provável, não confirmada). Contornado aqui com
   `VARCHAR(63)`. Na prática, um `VARCHAR(255)` utf8mb4 comum não pode ser TAG indexada.
6. **[médio impacto, esforço baixo, risco baixo] A 1ª execução (servidor reiniciado) é 1,9-11,5x pior em varreduras
   grandes no InnoDB e na MooseDB:** q01 11,9 x 3,2 s (InnoDB) e 6,5 x 3,35 s (MooseDB); q05 17,6 x 1,5 s (InnoDB), 4,6 x 2,2 s
   (MooseDB). ClickHouse: 0,019 x 0,016 s. Aquecer o buffer pool / cache de chunks no start resolve; não medido.
7. **[médio impacto, esforço baixo, risco baixo] Carga:** a MooseDB carrega 147 mil linhas/s (346 mil com TAG só de
   host), 0,8x o InnoDB sem `KEY(file)` (190 mil), com 6,5-9x menos disco. Rode `moosedb_compact` depois (1.126 para
   778 MiB, -31%, 48 s). O ClickHouse carrega 2,2 M linhas/s (6-15x mais rápido que a MooseDB).
8. **[baixo impacto] Consultas por host (q06, q07, q07b) já são favoráveis:** MooseDB 0,001-0,006 s contra
   0,002-0,015 s do InnoDB e 0,003 s do ClickHouse (piso de medição); `ix_host_ts` faz pushdown de TAG + faixa de tempo.

## Não medido

* `perf` / flame graph da engine (sem `perf` no contêiner; kernel do WSL2 não casa). A decomposição usa
  `ANALYZE FORMAT=JSON`, `Handler_*` e as consultas de `breakdown.sh`.
* p95 de latência; `file` como TAG; tabela de resumo; ajustes de `tmp_table_size`, `moosedb_chunk_cache_size`.
* Concorrência (várias consultas simultâneas) e leitura durante ingestão.
* Variância entre rodadas: uma rodada de 20 M por configuração (melhor de 5 execuções por consulta).

## Versão medida

Plugin compilado do HEAD `5c771db` (árvore limpa naquele momento) pelo fluxo da skill `moosedb-build`, MariaDB
11.4.13, ClickHouse 26.9.23.15. As tabelas acima desta seção são do HEAD; a seção "Agregação na engine" é do working tree com group-by handler (não commitado).
