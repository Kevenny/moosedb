# MooseDB x InnoDB para monitoramento — estado atual (v0.2)

> Resumo para decidir **se e onde** usar a MooseDB numa base de monitoramento
> (Zabbix ou similar). Todos os números vêm dos labs do repositório
> ([lab/zabbix](../lab/zabbix/README.md), [lab/fileaccess](../lab/fileaccess/README.md))
> e da [arquitetura](architecture.md); o que não foi medido está marcado como
> **não medido**. Detalhes de uso: [user-guide.md](user-guide.md).

## 1. O que a engine é hoje

Storage engine **time-series** para MariaDB 11.4+, carregada como plugin
(`INSTALL SONAME` / `plugin_load_add`). Handler em C++20, núcleo em Rust sem
`unsafe`. A versão v0.2 se declara **EXPERIMENTAL** (`plugin_maturity=experimental`).

| Área | Estado |
|---|---|
| Escrita | `INSERT` em lote, atômico por statement; WAL com CRC + fsync; recuperação de crash testada com `kill -9` e perda de energia simulada (LazyFS) |
| Leitura | varredura em streaming, poda por chunk (zone maps), Bloom por chunk, pushdown de `=`/`IN` em colunas TAG, índices compostos `(TAGs…, tempo)` |
| Chave única | `PRIMARY KEY`/`UNIQUE` no formato `(TAGs…, tempo[, extras])` — a PK do histórico do Zabbix funciona |
| `DELETE` | suportado (tombstones); caminho direto por série + faixa de tempo em µs–ms |
| `UPDATE` | suportado (fase 1): `UPDATE`, `REPLACE` e `ON DUPLICATE KEY UPDATE` sobre chave existente, atômicos por statement; colunas da chave única não mudam; até ~150 k linhas por statement |
| Retenção | `RETENTION_PERIOD` por tabela: apaga chunks inteiros |
| Compressão | delta-of-delta, Simple8b, Gorilla, RLE + LZ4 (quente) / ZSTD (frio) |
| Agregação na engine | `group_by_handler` opcional (`moosedb_group_by_pushdown`, padrão ON) |
| Operação | `INFORMATION_SCHEMA.MOOSEDB_TABLES/CHUNKS`, `OPTIMIZE`, procedures `moosedb_*`, compactação em background |
| Segurança | criptografia AES-256-GCM (autenticada) com o key management do MariaDB |
| Replicação | row-based |

## 2. Onde a MooseDB ganha do InnoDB (medido)

### Lab Zabbix — 50,4 M linhas (5.000 itens × 7 dias × 60 s), cache quente, 2026-10-20

| Métrica | InnoDB | MooseDB (PK) | Ganho |
|---|---:|---:|---:|
| Disco após compactação | 2.384 MiB | 392,9 MiB | **6,1x menor** |
| Carga (linhas/s) | 553.505 | 623.602 | 1,13x |
| Expirar 1 dia (6,76 M linhas) | 17,3 s (5.000 DELETEs) | **0,8 ms** (`RETENTION_PERIOD`) | >10.000x |
| Gráfico 1 item, 7 d | 4,69 ms | 4,62 ms | empate |
| Agregação por hora, 100 itens, 24 h | 27,0 ms | 26,6 ms | empate |
| Último valor de 20 itens (PK) | 0,91 ms | 0,96 ms | empate |

Com o Zabbix 7.0.31 real (DDL oficial + `COMMENT 'TAG'` + `TIMESTAMP_COLUMN`):
150.000 valores ingeridos sem falha, 0 erros SQL, housekeeper, `history.get`,
`history.clear` e frontend funcionando **sem mudança de código no Zabbix**.

### Lab acesso a arquivos — 20 M linhas de eventos

| Métrica | InnoDB | MooseDB (TAG host) |
|---|---:|---:|
| Disco final | 5.028 MiB | **555 MiB (9x menor)** |
| Carga | 190 k linhas/s | **346 k linhas/s** |
| Série por minuto de 1 host, dia | 15,0 ms | **4,7 ms** |
| Top processos de 1 host, dia | 13,6 ms | **3,8 ms** (0,8 ms com agregação na engine) |
| Top 10 hosts por volume, dia | 3,07 s | 1,98 s (**3,9 ms** com agregação na engine) |
| Top 20 arquivos, dia (GROUP BY 3,7 M distintos) | 14,8 s | 14,6 s (**1,15 s** com agregação na engine) |

### Contagem/agrupamento por série (10 M linhas, 2.000 séries, 2026-10-30)

| Consulta | InnoDB | MooseDB |
|---|---:|---:|
| `COUNT(DISTINCT itemid)` | 705 ms | **27 ms** |
| `COUNT(*) WHERE clock > …` | ~500 ms | 440–480 ms |

### Por que ganha

1. **Disco 6–9x menor**: codecs de série temporal; o histórico de
   monitoramento (valores numéricos que variam pouco, timestamps regulares)
   é o melhor caso.
2. **Expiração quase grátis**: retenção apaga arquivos de chunk inteiros; o
   InnoDB apaga linha a linha e o `.ibd` não encolhe.
3. **Leitura por série**: dados de uma série ficam contíguos no chunk; um
   gráfico ou "últimos valores de um host" lê só aquela série.
4. **Ingestão em lote** igual ou melhor que o InnoDB, com escrita concorrente
   a leituras (snapshots imutáveis, sem bloqueio de leitor).
5. **Menos I/O de buffer pool**: menos páginas para manter quentes num
   servidor que também roda o InnoDB das tabelas de configuração.

## 3. Onde o InnoDB continua melhor

| Situação | Impacto | Números |
|---|---|---|
| Filtro em coluna **não-TAG** (ex.: `file = 'x'`) | sem índice secundário: Bloom por série poda ~98 % e a engine filtra as linhas, mas ainda lê a coluna das séries restantes | 20 M linhas: 0,048 s vs **0,011 s** do InnoDB com `KEY(file)` (com 1 dia: 0,012 vs 0,009 s); valor popular (587 mil linhas) 0,64 s vs 28 s do InnoDB |
| **Milhões de séries com poucas linhas** cada | custo por série | 1 M séries: `COUNT(*) WHERE` 1,4–1,6 s vs 0,07 s; `COUNT(DISTINCT)` ~1,45 s vs ~0,2 s |
| Housekeeper **por item** (DELETE por itemid) | fsync por statement | 8,4 s vs 5,7 s (1,5x mais lento) — use `RETENTION_PERIOD` |
| `INSERT … ON DUPLICATE KEY UPDATE` de linhas existentes | acha cada linha por conteúdo | 5.000 linhas: 102 ms vs **20 ms** (5x mais lento) |
| `UPDATE` grande ou frequente | statement inteiro precisa caber no buffer do lote; trava a tabela para escrita; linhas antigas ficam mortas até a compactação | teto ~150 k linhas/statement (64 MiB); repetir UPDATE de 100 k nas mesmas linhas sobe de ~255 para ~350 ms (linhas recém-atualizadas ficam no MemTable) |
| `UPDATE` que muda a chave única, ou dentro de trigger/função de outro statement | **recusado** com erro | — |
| Transação com rollback | `ROLLBACK` **não desfaz** statements MooseDB já terminados (como MyISAM); sem XA real | — |
| Backup com `mariadb-backup` | **não copia** tabelas MooseDB (a engine faz o backup falhar em vez de ficar incompleto) | use `BACKUP STAGE` + snapshot de volume ou `mariadb-dump --lock-all-tables` |
| Particionamento | não suportado | retenção/chunks substituem o caso de uso |
| Maturidade | v0.x **experimental** | — |

## 4. O que ajuda num monitoramento

### Encaixa bem

* Tabelas de **histórico e tendências** (`history*`, `trends*` do Zabbix, métricas
  de agente, logs de eventos com timestamp): append-only, consultadas por
  série + janela de tempo, expiradas por idade.
* Cargas com **centenas de milhares de linhas/s** em lote.
* Bases onde o **disco** do histórico domina o custo (SSD, backup, réplicas).

### Não encaixa

* Tabelas de configuração, inventário, eventos que mudam de status, filas,
  qualquer coisa com `UPDATE` **frequente** → **mantenha em InnoDB** (a engine
  convive no mesmo servidor; só as tabelas de série mudam de engine). `UPDATE`
  ocasional (correção de valores, recálculo de um período) funciona: por faixa de
  tempo foi 3–11x mais rápido que o InnoDB sem índice em `clock` (10–100 k linhas:
  62–245 ms vs 681–796 ms).
* Consultas "quem/onde" por coluna livre sem TAG → InnoDB com índice, ou tabela
  de resumo.

### Recomendações de modelagem (ranqueadas por impacto)

| # | Recomendação | Impacto medido | Esforço | Risco |
|---|---|---|---|---|
| 1 | Usar `RETENTION_PERIOD` em vez de DELETE do housekeeper para expiração global | 17,3 s → 0,8 ms por dia expirado | baixo | baixo |
| 2 | TAG só nas colunas que as consultas filtram por igualdade (`itemid`, `host`) | TAG extra custou +40% de disco, 2,4x carga, 1,2–1,9x consultas | baixo | baixo |
| 3 | PK `(itemid, clock, ns)` com `TIMESTAMP_COLUMN='clock'` no histórico do Zabbix | "último valor" igual ao InnoDB (0,96 vs 0,91 ms) | baixo | baixo |
| 4 | Rodar `CALL moosedb_compact` / `OPTIMIZE` após cargas grandes | −31% de disco (1.126 → 778 MiB) | baixo | baixo |
| 5 | Manter `moosedb_group_by_pushdown=ON` (agora o padrão) em relatórios/dashboards de agregação | 14,8 s → 1,15 s; 3,07 s → 0,004–0,13 s | baixo | médio (ON por padrão desde esta versão; só GROUP BY de topo) |
| 6 | Pré-aquecer o cache (`moosedb_chunk_cache_size`) após restart | 1ª execução até 2x pior | baixo | baixo (não medido) |
| 7 | Inserir em lote (`INSERT … SELECT`/multi-row) e, em transações, agrupar statements | 1.179 → 7.617 linhas/s com `BEGIN` + 100 INSERTs (InnoDB 7.204) | baixo | baixo |

### Checklist antes de produção

- [ ] Ambiente aceita plugin `EXPERIMENTAL`.
- [ ] `UPDATE`/upsert nas tabelas candidatas é ocasional, com menos de ~150 k linhas por statement e sem mudar a chave única.
- [ ] Nenhuma escrita depende de `ROLLBACK` de transação multi-statement.
- [ ] Backup ajustado (`BACKUP STAGE` + snapshot ou `mariadb-dump --lock-all-tables`).
- [ ] Consultas principais filtram por TAG + tempo (conferir com `EXPLAIN`).
- [ ] Número de séries dimensionado (~295 B de RAM por série no catálogo).

## 5. Não medido / limites deste documento

* Concorrência de muitas consultas simultâneas durante ingestão; p95 de latência.
* Volumes acima de 50 M linhas por tabela e meses de retenção.
* Ambiente do cliente: os labs rodam em Docker Desktop/WSL2 (24 vCPU, 32 GiB,
  SSD local) com tudo em memória; tempos absolutos mudam, as proporções tendem a
  se manter (o lab reproduziu as ordens de grandeza de um relatório de cliente).
* Medidas de 1 M de séries do commit `c956438` (2026-10-30), com variação
  alta entre rodadas.
