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
  `AUTO_INCREMENT` não é aceito, nem `PARTITION BY` (o tempo já é particionado em
  chunks); uma `PRIMARY KEY`/`UNIQUE (TAGs, tempo[, ns])` é aceita, ver
  [PRIMARY KEY / UNIQUE](#primary-key--unique-chave-do-histórico-do-zabbix);
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
precisam ser um prefixo da ordem das colunas do schema. Chaves únicas têm regras
próprias (seção seguinte). O índice não ocupa espaço extra: a engine já sabe quais
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
só serve `ORDER BY clock DESC, ns DESC LIMIT 1` por inteiro numa chave única que
inclua `ns` (veja abaixo); num índice não único `ns` não está no
índice: lê a faixa do item e ordena; para "último valor" use
`SELECT MAX(clock) …` seguido de `… WHERE itemid = X AND clock = <max>
ORDER BY ns DESC LIMIT 1`, como o frontend do Zabbix sem PK). O tempo sozinho
(`INDEX (clock)`) continua sendo o índice das consultas sem TAG.

### PRIMARY KEY / UNIQUE (chave do histórico do Zabbix)

A tabela aceita **uma** `PRIMARY KEY` ou `UNIQUE` na forma

```
(todas as colunas TAG, a coluna de tempo [, 1 ou 2 colunas inteiras NOT NULL])
```

— a série mais o tempo (mais um desempate, como o `ns` do Zabbix) identificam a
linha. Os DDL oficiais do Zabbix 7.0 (`history`, `history_uint`, `history_str`,
`history_log`, `history_text`, `history_bin`) rodam sem adaptação além de
`COMMENT 'TAG'` em `itemid` e `ENGINE=MooseDB TIMESTAMP_COLUMN='clock'`; a PK
dispensa o `INDEX (clock)` (ela fornece o eixo de tempo):

```sql
CREATE TABLE history (
  itemid bigint unsigned NOT NULL COMMENT 'TAG',
  clock  integer DEFAULT '0' NOT NULL,
  value  DOUBLE PRECISION DEFAULT '0.0000' NOT NULL,
  ns     integer DEFAULT '0' NOT NULL,
  PRIMARY KEY (itemid,clock,ns)
) ENGINE=MooseDB TIMESTAMP_COLUMN='clock' CHUNK_INTERVAL='1 DAY' RETENTION_PERIOD='31 DAYS';
```

Formas aceitas / recusadas (a mensagem do `CREATE` diz o motivo):

* a chave lista **todas** as TAGs da tabela (em qualquer ordem), depois a coluna
  de tempo, depois 0–2 colunas `TINYINT`..`BIGINT` `NOT NULL` (sem prefixo, sem
  `DESC`). Uma chave que não cobre todas as TAGs (`UNIQUE (itemid)` numa tabela
  com `hostid` e `itemid`) exigiria checar várias séries e é recusada;
* TAG de texto numa chave única precisa de collation binária *NO PAD*
  (`utf8mb4_nopad_bin`, `VARBINARY`...): numa collation `_ci` ou *PAD SPACE*,
  `'A'` e `'a '` seriam a mesma chave mas séries diferentes;
* tabela **sem** TAG: só `UNIQUE (coluna_de_tempo)`;
* uma só chave única por tabela; `INDEX (itemid, clock, ns)` (não único)
  continua recusado.

Semântica:

* `INSERT` de chave existente → `ER_DUP_ENTRY` (`Duplicate entry '42-1700000000-7'
  for key 'PRIMARY'`); `INSERT IGNORE` pula a linha com um warning; duplicata
  dentro do mesmo statement, contra a MemTable, chunks selados (quentes, frios,
  compactados, cifrados), após restart e após `kill -9` + replay do WAL: todos
  detectados. Como a engine não é transacional, um `INSERT` de várias linhas que
  falha **mantém as linhas anteriores à duplicata** (o InnoDB desfaz o statement);
* `REPLACE` e `INSERT … ON DUPLICATE KEY UPDATE` funcionam quando a chave **não**
  existe (inclusive depois de um `DELETE` dela); quando existe, falham com
  `ER_ILLEGAL_HA` (a engine não tem `UPDATE`) — nunca com semântica errada;
* duas sessões inserindo a mesma chave: exatamente uma vence. O primeiro
  statement a escrever numa série a "reserva" até terminar; outro que precise da
  mesma série **espera** (até `moosedb_unique_lock_wait_timeout` s, padrão 50,
  respeitando `KILL QUERY`; `ER_LOCK_WAIT_TIMEOUT`) e depois confere contra o já
  confirmado. Statements que escrevem séries *diferentes* não se atrapalham (o
  caso do Zabbix, onde cada syncer cuida de itens distintos). Dois statements que
  escrevem várias séries em ordens opostas e se seguram mutuamente recebem
  `ER_LOCK_DEADLOCK` num deles;
* `SET unique_checks = 0` pula a verificação (carga em massa, como no InnoDB —
  mas aqui vale para a chave primária também): duplicatas inseridas assim são
  gravadas e **não são detectadas depois**; as chaves gravadas continuam sendo
  conhecidas pelas verificações seguintes;
* `ALTER TABLE … ADD PRIMARY KEY` numa tabela com duplicata falha com
  `ER_DUP_ENTRY`; `DROP PRIMARY KEY` exige que exista (ou seja adicionado no
  mesmo `ALTER`) um `INDEX` sobre a coluna de tempo;
* **Falhas da reserva deixam as linhas anteriores do statement.** `ER_LOCK_DEADLOCK`
  e `ER_LOCK_WAIT_TIMEOUT` (e `KILL`) da espera por uma série saem, como qualquer erro
  de um statement, **com as linhas já aceitas mantidas** (a engine não desfaz;
  contrato de atomicidade por statement). O InnoDB desfaria o statement todo. Um
  cliente (ou réplica) que re-executa o mesmo `INSERT` recebe `ER_DUP_ENTRY` nas
  linhas que já entraram: no retry use `INSERT IGNORE`, ou escreva cada statement
  numa só série / numa ordem de séries consistente (o que o Zabbix já faz);
* **Memória e desempate.** O estado de unicidade (~70 B por série) fica em memória,
  fora do `moosedb_batch_memory_budget`. Em varreduras ordenadas por tempo de uma
  tabela inteira com PK, o desempate por `ns` custa um passo extra por instante
  (coleta e ordena o grupo de cada `clock`); varreduras sem ordem (`rnd_next`) e
  tabelas sem PK não pagam isso;
* retenção/`TRUNCATE`: a chave de uma linha expirada pode ser inserida de novo.

Custo: o caso comum (série em ordem de `clock`) se resolve em memória, sem ler
dados — mede-se ~+10 % no tempo de carga do *history syncer* (ver
`architecture.md`). Chaves fora de ordem ou repetidas lêem a série no instante
pedido (~dezenas de µs).

Leitura pela chave: é um índice composto `(itemid, clock, ns)`. `WHERE itemid=X
AND clock>N ORDER BY clock DESC, ns DESC LIMIT 1` (e `clock<=N`, `BETWEEN`,
ascendente) sai na ordem do índice, **sem filesort**: dentro de um mesmo `clock`
as linhas vêm ordenadas por `ns`. A chave completa dá `const` / `eq_ref` em joins.


## Escrevendo e lendo

```sql
INSERT INTO metricas VALUES (NOW(6), 'srv01', 'cpu', 42.5, NULL);

SELECT host, AVG(value) FROM metricas
 WHERE metric = 'cpu' AND ts >= NOW() - INTERVAL 1 HOUR
 GROUP BY host;

SELECT * FROM metricas WHERE host = 'srv01' ORDER BY ts DESC LIMIT 10;
```

* `UPDATE` retorna `ER_ILLEGAL_HA` (os dados não são alterados no lugar). `DELETE`
  é suportado — ver [DELETE](#delete).
* Cada statement é **atômico para leitura e para crash**: outras sessões veem
  todas as linhas de um INSERT de uma vez, e um crash no meio dele não deixa
  nada. Não há transações (`BEGIN`/`ROLLBACK` não desfazem) e, se o statement
  falhar no meio, as linhas inseridas antes do erro permanecem (como no MyISAM).
* Uma linha está durável quando o statement retorna OK
  (`moosedb_wal_sync_mode=fsync`). Com `write`, sobrevive a um crash do
  `mysqld`, mas não a uma queda do sistema operacional.

## DELETE

`DELETE` funciona com qualquer `WHERE`, `LIMIT`, `ORDER BY` e `DELETE` de várias
tabelas (`DELETE a FROM a JOIN b ...`). A exclusão é **lógica**: a engine grava um
*tombstone* (série(s) + intervalo de tempo, ou as linhas exatas) no WAL, as linhas
somem para todas as leituras no mesmo instante, e o espaço só volta quando uma
compactação reescreve os chunks afetados (`OPTIMIZE TABLE`, ou o background quando
um quarto de um chunk está morto) — um chunk inteiramente apagado é simplesmente
descartado. Existem dois caminhos:

| Forma do `WHERE` | Caminho | Custo |
|---|---|---|
| só igualdade/`IN` em colunas TAG e limites (`< <= > >= = BETWEEN`) na coluna de tempo (`INT` via `TIMESTAMP_COLUMN`, ou `DATETIME`), sem `ORDER BY`, sem `LIMIT` (ou com um `LIMIT` que o intervalo não alcança), sem triggers, sem `binlog_format=ROW` | **direto** (*direct delete*): um tombstone, `ROW_COUNT()` exato | O(chunks tocados), independe do número de linhas; ~µs–ms |
| qualquer outro (`value > 10`, `OR`, `LIMIT` menor que o intervalo, `ORDER BY`, subconsulta, multi-tabela, `DELETE` sem `WHERE` com binlog em ROW, coluna `TIMESTAMP` como eixo) | **linha a linha**: o servidor lê as linhas e a engine as acha por conteúdo ao fim do statement | O(linhas apagadas) |

`DELETE FROM t` sem `WHERE` (sem binlog em ROW) continua sendo `TRUNCATE`.

* **Atomicidade e crash**: igual ao `INSERT`. O statement vira *uma* entrada do
  WAL sincronizada antes de as linhas sumirem; um `kill -9` deixa a tabela como
  antes ou como depois, nunca no meio. Um `DELETE` linha a linha que apague mais de
  ~32 MiB de linhas é aplicado em etapas (cada uma atômica e durável); uma falha no
  meio mantém as etapas já aplicadas, como um `INSERT` que falha no meio.
* **Concorrência**: um `DELETE` com `WHERE` roda ao lado de `INSERT`s da mesma
  tabela e da mesma série (o *syncer* não espera o *housekeeper*); só afeta linhas
  que já estavam gravadas quando ele terminou. Dois `DELETE`s da mesma tabela se
  serializam. Leituras já abertas não veem o resultado parcial nem quebram.
* **Chave única**: uma chave apagada pode ser inserida de novo (também por `REPLACE`
  e `ON DUPLICATE KEY UPDATE`, que passam a ver "chave inexistente").
* **`LIMIT`**: no caminho direto só é usado se o intervalo apagaria menos linhas que
  o `LIMIT`; senão o servidor apaga linha a linha, exatamente `N` linhas (a escolha
  de quais, sem `ORDER BY`, é arbitrária, como em qualquer engine).
* **Leitura com apagados pendentes**: custo proporcional ao número de tombstones da
  série lida (≈ 0 para o housekeeper, que gera um por item). Estatísticas
  (`COUNT(*)`, `information_schema.MOOSEDB_TABLES.ROW_COUNT`) já descontam as linhas
  apagadas.
* **Zabbix**: o housekeeper de histórico pode ficar **ligado** (o `DELETE ... WHERE
  itemid=X AND clock<CUT [LIMIT n]` por item é o caminho direto). Para expirar só
  por idade, `RETENTION_PERIOD` continua melhor — descarta o chunk inteiro sem um
  tombstone por item — e o housekeeper de histórico do Zabbix pode ser desligado
  (`HousekeepingFrequency`/`Data storage` → *Override item history period* desativado).
  Use o housekeeper apenas para apagar por item (`history.clear`, itens removidos).
* **Limites**: o statement direto com milhões de séries selecionadas é recusado
  (`Unsupported`, o tombstone caberia mal numa entrada de log de 8 MiB); `UPDATE`
  segue sem suporte; `mariadb-backup` segue não copiando tabelas MooseDB (mas agora falha em vez de ignorar).

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

Chunks e WAL são cifrados com AES-256-GCM, que também **autentica**: bit
alterado, bloco trocado de lugar ou de arquivo e entrada de WAL movida são
detectados na leitura (o dado adulterado nunca é devolvido; a consulta falha
com erro de corrupção, e a abertura da tabela com erro de decriptação).
Rotacionar a chave no key management é transparente: dados novos usam a versão
mais recente e os antigos continuam legíveis.

Tabelas criadas por versões anteriores usavam AES-256-CTR, que cifra mas não
autentica. Elas continuam abrindo (o log avisa "encrypted without
authentication"); `OPTIMIZE TABLE` reescreve os chunks antigos já autenticados
e o WAL antigo some no próximo flush. Depois da migração, um binário anterior
não abre mais a tabela (recusa a versão do formato).

A autenticação não cobre apagar ou truncar arquivos, restaurar uma cópia antiga
válida nem alterar o `MANIFEST`; proteja o datadir com permissões do sistema
de arquivos. Se a chave configurada não decifra a tabela, ela não abre
(`Got error 192 ... from MooseDB`, erro de decriptação) — nada é descartado;
corrija a configuração de chaves e reabra. Adulteração e chave errada não se
distinguem.

## Backup

> **Atenção: `mariadb-backup` NÃO copia tabelas MooseDB.** Ele só copia uma
> lista fixa de extensões e pula os diretórios `<banco>/<tabela>/` da engine;
> não há API para uma engine de terceiros participar. Para não gerar um backup
> silenciosamente incompleto, a engine **faz o `mariadb-backup --backup`
> falhar** (rc=1, `FATAL ERROR ... MooseDB tables are not copied by
> mariadb-backup; use mariadb-dump --lock-all-tables`) quando existe ao menos
> uma tabela MooseDB. Isso vale também para `FLUSH ENGINE LOGS` durante um
> `BACKUP STAGE` ativo. **`--no-lock` não é protegido**: ele pula esse
> statement e sai com rc=0 sem os dados MooseDB — não o use com tabelas
> MooseDB. Sem tabelas MooseDB o backup roda normalmente.

Métodos suportados hoje:

| Método | Consistência | Observação |
|---|---|---|
| **`BACKUP STAGE` + snapshot atômico do volume (recomendado)** | consistente entre engines (MooseDB + InnoDB no mesmo instante) | escritas pausadas só durante o snapshot (segundos); exige LVM/ZFS/btrfs/EBS com snapshot atômico do volume inteiro do datadir |
| `mariadb-dump --lock-all-tables` | consistente entre tabelas | bloqueia escritas durante todo o dump; dump em texto claro (mesmo de tabela cifrada) |
| `BACKUP STAGE` + cópia do diretório (abaixo) | consistente | escritas pausadas só durante a cópia; risco da manutenção em background (ver abaixo) |

### Snapshot atômico do volume

Um snapshot atômico equivale a uma queda de energia congelada: o protocolo
de durabilidade da MooseDB (`.tmp` → fsync → rename → troca do MANIFEST) já
recupera desse estado, e o `BACKUP STAGE BLOCK_COMMIT` pausa as escritas de
MooseDB e InnoDB para os dois saírem do mesmo instante. A manutenção em
background não atrapalha, porque o snapshot congela o volume inteiro de uma
vez. Validado com INSERT/DELETE/compactação concorrentes: 10/10 restaurações
com checksums idênticos aos do `BLOCK_COMMIT`.

```sql
-- sessão A, mantida aberta até o fim
BACKUP STAGE START;
BACKUP STAGE FLUSH;
BACKUP STAGE BLOCK_DDL;
BACKUP STAGE BLOCK_COMMIT;   -- INSERT/DELETE ficam em "Waiting for backup lock"
-- (fora do SQL) snapshot do volume do datadir, ex.:
--   lvcreate --snapshot -n mdb_snap -L 10G /dev/vg/mariadb
--   zfs snapshot pool/mariadb@backup
BACKUP STAGE END;
-- copie o snapshot para o destino com calma e depois descarte-o
```

Restauração: suba um servidor sobre a cópia (mesmo `file_key_management`
se houver tabelas cifradas); a recuperação roda na abertura das tabelas.
Valide com `CHECK TABLE`. O snapshot precisa cobrir o datadir inteiro num
único instante: snapshots separados por arquivo ou por disco não servem.

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
chunks frios não tem como ser desligada hoje. Para produção, prefira o
snapshot atômico do volume ou `mariadb-dump --lock-all-tables`.

Na restauração, copie os diretórios de volta com o mesmo
`file_key_management`; o WAL é reaplicado na abertura.

## Variáveis

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
| `moosedb_unique_lock_wait_timeout` | 50 | segundos que um INSERT espera outro statement que escreve a mesma série (tabelas com PK/UNIQUE); 0 = falha na hora |
| `moosedb_group_by_pushdown` | `OFF` | (sessão) `ON` = a engine calcula o GROUP BY de TAG/VARCHAR/INT com COUNT/SUM/AVG/MIN/MAX (ver architecture.md); o resultado não muda |
| `moosedb_agg_max_memory` | 1 GiB | (sessão) memória máxima dos grupos de um GROUP BY empurrado (mín. 16 MiB) |
| `moosedb_agg_threads` | min(4, CPUs) | threads de um GROUP BY empurrado (1 a 64) |

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
