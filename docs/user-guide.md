# MooseDB — Guia do usuário

MooseDB é uma storage engine time-series para MariaDB 11.4+: dados
append-only, agrupados em arquivos por intervalo de tempo, comprimidos por
tipo de coluna e expirados automaticamente.

## Instalação

```ini
# my.cnf — v0.x se declara EXPERIMENTAL
[mariadb]
plugin_maturity = experimental
plugin_load_add = ha_moosedb
```

ou, com `plugin_maturity = experimental` já no my.cnf (a variável é somente
leitura), em tempo de execução:

```sql
INSTALL SONAME 'ha_moosedb';
SHOW ENGINES;                                   -- MooseDB | YES
```

Para `CALL moosedb_compact(...)` e `CALL moosedb_apply_retention(...)`,
instale as procedures em cada banco que tiver tabelas MooseDB:

```sql
USE metricas;
SOURCE /caminho/para/storage/moosedb/sql/moosedb_install.sql;
```

## Criando uma tabela

```sql
CREATE TABLE metricas (
    ts        DATETIME(6) NOT NULL,
    host      VARCHAR(64) NOT NULL COMMENT 'TAG',
    metric    VARCHAR(64) NOT NULL COMMENT 'TAG',
    value     DOUBLE,
    value_int BIGINT,
    INDEX ts_idx (ts)
) ENGINE=MooseDB
  CHUNK_INTERVAL    = '1 DAY'
  RETENTION_PERIOD  = '90 DAYS'
  COMPRESSION       = 'ZSTD'
  COMPRESSION_LEVEL = 3
  HOT_THRESHOLD     = '7 DAYS';
```

Regras:

* uma coluna `DATETIME` ou `TIMESTAMP` **NOT NULL** é o eixo do tempo, com um
  índice simples (`INDEX (ts)`, obrigatório); além dele só se aceitam os
  índices de TAG descritos em [Índices compostos](#índices-compostos).
  `UNIQUE`/`PRIMARY KEY` e `AUTO_INCREMENT` não são aceitos, nem `PARTITION BY`
  (o tempo já é particionado em chunks);
* colunas com `COMMENT 'TAG'` formam a identidade da **série** (host, sensor,
  métrica…). Filtros `=`/`IN` sobre TAGs leem só as séries correspondentes;
* `DATETIME` é armazenado como relógio de parede (UTC); `TIMESTAMP`, como
  instante Unix;
* **Eixo de tempo inteiro** (ex.: `clock INT` do Zabbix): com
  `TIMESTAMP_COLUMN='clock'`, uma coluna `INT` ou `BIGINT` (com ou sem sinal,
  `NOT NULL`, armazenada, com o índice simples `INDEX (clock)`) é o eixo, lida
  como **segundos desde 1970 UTC**. A engine guarda segundos × 10⁶ µs, então
  `CHUNK_INTERVAL`, retenção e poda por faixa funcionam como no `DATETIME`
  (fronteiras de chunk à meia-noite UTC). Uma coluna inteira **só** vira eixo
  quando nomeada em `TIMESTAMP_COLUMN` — um índice sobre `INT` sozinho não basta
  (o `CREATE` falha pedindo a coluna de tempo). Valores fora de
  ±9.223.372.036.854 s (acima disso, segundos × 10⁶ estoura 64 bits) são
  **recusados** com erro claro, nunca truncados — inclusive `BIGINT UNSIGNED`
  acima de 2⁶³. A coluna de tempo não pode ser TAG. Uma coluna `ns`
  (nanossegundos) é comum e **não** entra no eixo: `ORDER BY clock, ns` é
  ordenado por filesort e fica correto. `clock > N`, `BETWEEN`,
  `ORDER BY clock DESC LIMIT n` e `MIN/MAX` usam o índice e a poda por tempo.
  Ex.: `clock INT NOT NULL, INDEX (clock)` + `TIMESTAMP_COLUMN='clock'`;
* **TAG inteira** (`TINYINT`..`BIGINT`, com ou sem sinal, ex.:
  `itemid BIGINT UNSIGNED NOT NULL COMMENT 'TAG'`): `=`, `IN` e igualdades
  múltiplas (`AND`) com constantes numéricas — **sem aspas** (`itemid = 123`,
  `itemid IN (1,2,3)`) ou com — leem só as séries correspondentes. A constante
  vira o texto decimal canônico da coluna: inteira, decimal integral (`123.0`)
  e `DOUBLE` integral abaixo de 2⁵³ são empurrados; fracionária (`123.5`) ou
  fora do intervalo da coluna não casa série alguma; string só é empurrada se
  for um inteiro "limpo" (`'123'`; não `'0123'`, `' 12'`, `'12abc'`, `'1e3'`),
  e tudo que for duvidoso fica para o servidor, que sempre reavalia o `WHERE`
  completo (o pushdown só poda séries). Em coluna `BIGINT` o próprio servidor
  já converte constantes string em inteiras antes de a engine ver a condição.
  **`ZEROFILL` desliga o pushdown da coluna** (o texto gravado é preenchido com
  zeros). Uma TAG `VARCHAR` comparada com número **não** é empurrada (o
  servidor compara como `DOUBLE`: `'0123' = 123`). `IN` com mais de
  `in_predicate_conversion_threshold` (1000) valores vira subquery no servidor
  e não é empurrado — para listas maiores,
  `SET in_predicate_conversion_threshold = 0`.

| Opção | Valores | Padrão |
|---|---|---|
| `CHUNK_INTERVAL` | `1 HOUR`, `6 HOUR`, `12 HOUR`, `1 DAY`, `1 WEEK`, `1 MONTH` | `1 DAY` |
| `RETENTION_PERIOD` | `N HOURS/DAYS/WEEKS/MONTHS/YEARS`, `FOREVER` | `FOREVER` |
| `COMPRESSION` | `ZSTD`, `LZ4`, `NONE` (codec dos chunks frios) | `ZSTD` |
| `COMPRESSION_LEVEL` | 1–19 | 3 |
| `HOT_THRESHOLD` | período; mais novos = LZ4 | `7 DAYS` |
| `MEMTABLE_SIZE` | bytes (4096 – 96 MiB) | `moosedb_memtable_flush_threshold` |
| `TIMESTAMP_COLUMN` | nome da coluna (`DATETIME`/`TIMESTAMP`/`INT`/`BIGINT`) | coluna `DATETIME`/`TIMESTAMP` do índice temporal |
| `ENCRYPTION` | `YES`/`NO` | `NO` |
| `ENCRYPTION_KEY_ID` | id no key management | 1 |

### Índices compostos

Além do índice simples sobre o tempo (obrigatório), a tabela aceita até 8
índices **não únicos** destas formas:

```sql
CREATE TABLE history (
    itemid BIGINT UNSIGNED NOT NULL COMMENT 'TAG',
    clock  INT NOT NULL,
    value  DOUBLE NOT NULL,
    ns     INT NOT NULL,
    INDEX clock_idx (clock),              -- só o tempo: obrigatório
    INDEX item_clock (itemid, clock)      -- TAG(s) + tempo (tempo por último)
) ENGINE=MooseDB TIMESTAMP_COLUMN='clock' CHUNK_INTERVAL='1 DAY';
-- também: INDEX (hostid, itemid, clock)   várias TAGs, qualquer subconjunto/ordem
--         INDEX (itemid)                   só TAGs: ref em joins por TAG
```

Regras: cada parte TAG é uma coluna `COMMENT 'TAG'`, `NOT NULL`, inteira
(`TINYINT`..`BIGINT`) ou `CHAR`/`VARCHAR`, indexada inteira (sem `col(8)`),
sem repetição e sem `DESC`; o tempo, se entrar, é a última parte; as TAGs não
precisam ser um prefixo da ordem das colunas do schema. `UNIQUE` e `PRIMARY KEY`
continuam recusados. O índice não ocupa espaço extra: a engine já sabe quais
séries casam cada valor de TAG.

O otimizador passa a usar o índice (`ref`/`range` em `EXPLAIN`) para:

* `itemid = X`, `itemid IN (…)`, e `+ clock` com `=`, `<`, `<=`, `>`, `>=`,
  `BETWEEN` — lê só as séries pedidas, dentro da faixa de tempo;
* joins/self-joins por `(itemid, clock)` — um *lookup* de ~20 µs por linha
  externa (o self-join `AGGREGATE_LAST` do Zabbix caiu de 1,4 s para
  ~1 ms numa tabela de 8,6 M linhas);
* `WHERE itemid = X ORDER BY clock [DESC] LIMIT n` sem *filesort*;
* `SELECT MAX(clock)` / `MIN(clock)` `WHERE itemid = X [AND clock > N]` — uma
  chamada de índice (`Select tables optimized away`);
* `hostid = H` num índice `(hostid, itemid, clock)`: as séries do prefixo
  saem mescladas por tempo;
* `MIN(itemid)`, `MAX(itemid) WHERE itemid <= N`, etc.

O que o índice **não** faz: não ordena por valor de TAG (`ORDER BY itemid`,
`GROUP BY itemid` e `itemid > 5` continuam sendo *filesort* / outro índice) e
não serve `ORDER BY clock DESC, ns DESC LIMIT 1` por inteiro (`ns` não está no
índice: lê a faixa do item e ordena; para "último valor" use
`SELECT MAX(clock) …` seguido de `… WHERE itemid = X AND clock = <max>
ORDER BY ns DESC LIMIT 1`, como o frontend do Zabbix sem PK). O tempo sozinho
(`INDEX (clock)`) continua sendo o índice das consultas sem TAG.


## Escrevendo e lendo

```sql
INSERT INTO metricas VALUES (NOW(6), 'srv01', 'cpu', 42.5, NULL);

SELECT host, AVG(value) FROM metricas
 WHERE metric = 'cpu' AND ts >= NOW() - INTERVAL 1 HOUR
 GROUP BY host;

SELECT * FROM metricas WHERE host = 'srv01' ORDER BY ts DESC LIMIT 10;
```

* `UPDATE` e `DELETE ... WHERE` retornam `ER_ILLEGAL_HA`: os dados são
  imutáveis. Use `RETENTION_PERIOD` para expirar e `TRUNCATE TABLE` (ou
  `DELETE` sem `WHERE`) para esvaziar.
* Cada statement é **atômico para leitura e para crash**: outras sessões veem
  todas as linhas de um INSERT de uma vez, e um crash no meio dele não deixa
  nada. Não há transações (`BEGIN`/`ROLLBACK` não desfazem) e, se o statement
  falhar no meio, as linhas inseridas antes do erro permanecem (como no MyISAM).
* Uma linha está durável quando o statement retorna OK
  (`moosedb_wal_sync_mode=fsync`). Com `write`, sobrevive a um crash do
  `mysqld`, mas não a uma queda do sistema operacional.

## Manutenção

```sql
FLUSH TABLES metricas;     -- sela a MemTable em chunks
OPTIMIZE TABLE metricas;   -- flush + retenção (exige DELETE) + compactação de tudo
CALL moosedb_compact('metricas', '2026-01-01', '2026-06-30');
CALL moosedb_apply_retention('metricas');
CHECK TABLE metricas;      -- verifica o CRC32 de todos os chunks
```

Privilégios: `OPTIMIZE TABLE` só aplica a retenção se o usuário tiver
`DELETE` na tabela (senão compacta e avisa "retention skipped");
`moosedb_apply_retention` exige `DELETE` na tabela e
`moosedb_compact`, `ALTER`. As procedures funcionam com qualquer charset de
cliente (instale o script com o cliente que preferir).

Em background, threads de manutenção aplicam a retenção e compactam os
intervalos de tempo que acumulam chunks, em tabelas abertas.

## Diagnóstico

Requer o privilégio `PROCESS`.

```sql
SELECT * FROM information_schema.MOOSEDB_TABLES;

SELECT TS_MIN, TS_MAX, `ROWS`, SERIES, DATA_MB, COMPRESSED_MB, RATIO,
       STATUS, COMPRESSION, CHUNK_FILE
  FROM information_schema.MOOSEDB_CHUNKS
 WHERE TABLE_NAME = 'metricas' ORDER BY TS_MIN DESC;
```

`STATUS`: `HOT` (recente, LZ4), `WARM` (esfriou, aguardando recodificação),
`COLD` (codec final), `COMPACTING`, `EXPIRED` (aguardando a próxima varredura).

## Criptografia

```ini
[mariadb]
plugin_load_add = file_key_management
file_key_management_filename = /etc/mysql/keys.txt   # chaves de 256 bits
```

```sql
CREATE TABLE segura (ts DATETIME(6) NOT NULL, v DOUBLE, INDEX(ts))
  ENGINE=MooseDB ENCRYPTION='YES' ENCRYPTION_KEY_ID=1;
```

Chunks e WAL são cifrados com AES-256-CTR. Rotacionar a chave no key
management é transparente: dados novos usam a versão mais recente e os
antigos continuam legíveis.

A criptografia protege a **confidencialidade**, não a integridade: quem pode
escrever no datadir consegue adulterar dados cifrados sem ser detectado. Se a
chave configurada não decifra a tabela, ela não abre (`Got error 192 ... from
MooseDB`, erro de decriptação) — nada é descartado; corrija a configuração
de chaves e reabra.

## Backup

> **Atenção: `mariadb-backup` NÃO copia tabelas MooseDB.** Ele termina com
> sucesso (rc=0), mas ignora os diretórios `<banco>/<tabela>/` da engine; na
> restauração as tabelas aparecem em `SHOW TABLES` e falham com
> `ERROR 1932 ... doesn't exist in engine`. O mariabackup só copia arquivos de
> uma lista fixa de extensões e pula subdiretórios — não há API para uma engine
> de terceiros participar.

Métodos suportados hoje:

| Método | Consistência | Observação |
|---|---|---|
| `mariadb-dump --lock-all-tables` | consistente entre tabelas | bloqueia escritas durante todo o dump; dump em texto claro (mesmo de tabela cifrada) |
| `BACKUP STAGE` + cópia do diretório (abaixo) | consistente | escritas pausadas só durante a cópia |

Não use `mariadb-dump --single-transaction` nem `--skip-lock-tables`: a
engine não é transacional, e tabelas diferentes saem de instantes diferentes.
`FLUSH TABLES ... FOR EXPORT` não é suportado.

Cópia física manual (testada com carga concorrente, inclusive cifrada):

```sql
-- sessão A, mantida aberta durante toda a cópia
BACKUP STAGE START;
BACKUP STAGE FLUSH;          -- a partir daqui INSERTs esperam
BACKUP STAGE BLOCK_DDL;      -- espera statements em andamento terminarem inteiros
BACKUP STAGE BLOCK_COMMIT;
-- (fora do SQL) copie <datadir>/<banco>/<tabela>/ de cada tabela MooseDB,
-- e rode `mariadb-backup --backup --no-lock` para InnoDB/Aria, se houver
BACKUP STAGE END;
```

**Risco residual da cópia manual**: a manutenção em background não é pausada
pelo `BACKUP STAGE` e pode substituir ou apagar chunks no meio da cópia
(compactação, recodificação de chunks que esfriaram, retenção) — o backup
fica desencontrado e a restauração falha com `table is marked as crashed`.
Reduza o risco com `SET GLOBAL moosedb_retention_check_interval = 0` e
`moosedb_compaction_trigger_chunks = 10000` antes (restaure depois), e
**sempre** valide a cópia restaurada com `CHECK TABLE`. A recodificação de
chunks frios não tem como ser desligada hoje. Até a integração com
`BACKUP STAGE` (pausa da manutenção) ser implementada, prefira
`mariadb-dump --lock-all-tables` para backups de produção.

Na restauração, copie os diretórios de volta com o mesmo
`file_key_management`; o WAL é reaplicado na abertura.

## Variáveis globais

| Variável | Padrão | Efeito |
|---|---|---|
| `moosedb_wal_sync_mode` | `fsync` | `fsync` ou `write` no fim do statement |
| `moosedb_memtable_flush_threshold` | 64 MiB | MemTable padrão por tabela (máx. 96 MiB) |
| `moosedb_compaction_trigger_chunks` | 10 | chunks por intervalo que disparam compactação |
| `moosedb_compaction_threads` | 2 | threads de manutenção (somente leitura) |
| `moosedb_retention_check_interval` | 3600 | segundos entre varreduras de retenção (0 = desliga) |
| `moosedb_bloom_filter_false_positive_rate` | 0.01 | Bloom filter dos chunks novos |
| `moosedb_chunk_cache_size` | 128 MiB | cache de blocos e de séries decodificadas (reduzir despeja na hora) |
| `moosedb_batch_memory_budget` | 1 GiB | memória total de statements em andamento; acima disso, os grandes fazem spill antecipado |
| `moosedb_max_open_chunks` | 100 | descritores de chunk mantidos abertos |

## Replicação

Replicação por linha (RBR) e por statement funcionam para INSERT e TRUNCATE;
cada réplica constrói os próprios chunks. Galera não é suportado.

A **retenção é local a cada servidor**: a expiração apaga chunks inteiros
pelo relógio de cada servidor e não gera eventos de binlog. Mestre e réplica
podem, por um intervalo, diferir nas linhas já expiradas (por exemplo, um
`OPTIMIZE` replicado aplica a retenção na réplica — cuja thread SQL tem todos
os privilégios — mesmo quando o usuário do mestre não tinha `DELETE`). Com
`moosedb_retention_check_interval > 0` os dois convergem; não compare
mestre e réplica por contagem em tabelas com `RETENTION_PERIOD`.
