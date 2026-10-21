# MooseDB — Arquitetura implementada (v0.2)

Este documento descreve o que **está implementado** e registra cada decisão que
diverge de `moosedb_engine_spec.md`, com a justificativa. A spec continua sendo
a referência de produto; este arquivo é a referência de implementação.

## Camadas

```
mysqld ──► ha_moosedb.so
             ├── ha_moosedb.cc   (C++20)  Handler API, pushdown de TAG, IS plugins, UDFs, sysvars
             └── libmoosedb.a    (Rust)   linkada estaticamente, símbolos ocultos
                   ├── moosedb-ffi   C ABI: só marshalling + catch_unwind
                   └── moosedb-core  #![forbid(unsafe_code)] — toda a lógica de storage
```

| Módulo (`moosedb-core`) | Responsabilidade |
|---|---|
| `table` | estado da tabela, protocolo de durabilidade, recuperação |
| `wal` | segmentos v2 (`ROW`/`COMMIT`/`ROW_COMMIT` com CRC), cifrados opcionalmente; v1 legível |
| `batch` | lotes por statement: buffer privado, commit atômico, spill para chunks em estágio |
| `manifest` | MANIFEST v2/v3 (`wal_seq`, `replay_seq`, chunks vivos com `born`, tombstones), troca atômica |
| `tomb`, `delete` | tombstones (exclusão lógica por faixa ou por linhas), `DELETE` atômico e durável, replay |
| `fsutil` | `write_atomic`, `sync_dir`, fsync que marca falha (poison) |
| `memtable` | buffer em segmentos imutáveis (snapshot O(1)) |
| `chunk`, `chunk_writer`, `chunk_reader`, `block` | formato `.tfl` v2, blocos por coluna |
| `compression/` | delta-of-delta, delta, Simple8b, Gorilla, RLE, LZ4/ZSTD |
| `scan` | snapshots, scan em streaming, k-way merge ordenado |
| `compaction` | merge de chunks por intervalo, recodificação fria |
| `maintenance` | registro de tabelas abertas + pool de threads de fundo |
| `crypto` | AES-256-CTR, provedor de chaves |
| `cache` | LRU de blocos decodificados e de descritores de arquivo |
| `inspect` | diagnóstico para o INFORMATION_SCHEMA |

* `moosedb-ffi` é o único crate com `unsafe`. Toda entrada valida ponteiros,
  roda sob `catch_unwind` (um panic vira `TF_ERR_INTERNAL`, nunca derruba o
  `mysqld`) e registra a mensagem em `moosedb_last_error()`.
* `moosedb_ffi.h` é gerado por cbindgen e **versionado**; o build roda
  `cbindgen --verify` e falha se o header estiver desatualizado.
* Os símbolos de `libmoosedb.a` (std do Rust, zstd, lz4) são ocultados com
  `--exclude-libs,libmoosedb.a`, para nunca interporem com outra biblioteca
  carregada no `mysqld`. Só essa lib: os ponteiros de serviço de
  `libmysqlservices.a` precisam continuar visíveis ao `dlsym` do servidor.

## Layout em disco

Cada tabela é um diretório `<datadir>/<schema>/<tabela>/`:

| Arquivo | Conteúdo |
|---|---|
| `MANIFEST` | lista de chunks vivos + checkpoint do WAL (`wal_seq`) + início do replay (`replay_seq`) (v2; v3 quando há ou houve `DELETE`: + `born` por chunk, `tseq` e os tombstones; CRC32, troca atômica) |
| `OPTIONS` | TABLE OPTIONS normalizadas (texto), lidas pelo INFORMATION_SCHEMA |
| `wal_NNNNNN.tfl.wal` | segmentos do WAL |
| `chunk_<início>_<fim>_<id>.tfl` | chunk selado (formato v2, `chunk.rs`) |
| `*.tfl.corrupt` | chunk em quarentena (CRC inválido na recuperação) |

### Formato do chunk (v2)

Header de 64 bytes (texto claro) → header de criptografia de 32 bytes (se
cifrado) → blocos de coluna → índice de séries → Bloom filter → footer de 24
bytes. Cada **bloco de coluna** tem codificação por tipo, codec (LZ4/ZSTD,
aplicado só se reduzir o tamanho) e CRC próprio; o arquivo inteiro tem um CRC
sobre os bytes armazenados (o `CHECK TABLE` funciona sem a chave).

| Tipo | Codificação |
|---|---|
| TIMESTAMP | delta-of-delta + Simple8b |
| INT64 | delta + Simple8b |
| FLOAT64 / FLOAT32 | Gorilla XOR |
| BOOL e mapa de NULLs | RLE |
| VARCHAR / DECIMAL | bytes com prefixo de tamanho (+ codec) |

Chunks **quentes** (intervalo mais novo que `HOT_THRESHOLD`) usam LZ4; os
**frios** usam o codec da tabela (`COMPRESSION`, ZSTD por padrão, nível
`COMPRESSION_LEVEL`). A compactação recodifica chunks que esfriaram.

### Protocolo de durabilidade

Toda mudança estrutural (flush, compactação, retenção, truncate) segue:

1. escrever os arquivos novos (`.tmp` → fsync → rename);
2. **trocar o MANIFEST atomicamente** — ponto de commit;
3. marcar como obsoletos os chunks que saíram do conjunto vivo.

Um chunk obsoleto só é **apagado quando o último snapshot que o referencia é
liberado** (`ChunkFile::drop`). Na recuperação:

* arquivos fora do MANIFEST (órfãos de um crash) e `.tmp` são apagados;
* chunks com header/rodapé em claro corrompidos vão para quarentena
  (`.tfl.corrupt`) e saem do MANIFEST; o chunk mais novo tem o CRC completo
  verificado;
* um chunk **listado no MANIFEST e ausente** faz a abertura falhar (nunca é
  descartado em silêncio), exceto quando existe o `.tfl.corrupt`
  correspondente — quarentena interrompida por crash, que é concluída;
* segmentos de WAL com header incompleto (0–7 bytes, zeros, parâmetros de
  cifra truncados) nunca receberam entradas e são removidos — e não contam ao
  decidir qual é o último segmento: a cauda incompleta do último segmento
  *com entradas* é cortada (e o corte, sincronizado), enquanto uma cauda
  inválida seguida de entradas reais continua sendo corrupção; o restante do
  WAL não coberto pelo checkpoint é reaplicado.
* durabilidade validada com perda de energia simulada (LazyFS: descarta tudo
  que não teve fsync): 76 ciclos com 6–10 clientes, 4.341 statements
  confirmados, nenhum perdido, parcial ou duplicado. Com `wal_sync_mode=write`
  statements confirmados podem sumir (≈20% no teste), mas sempre inteiros.

Se uma falha deixa memória e disco possivelmente divergentes — inclusive
**qualquer erro de fsync** (WAL, chunk, MANIFEST, diretório) — a tabela entra
em modo **read-only** até ser reaberta; um fsync nunca é "tentado de novo"
(após falha, o kernel pode ter descartado as páginas). A reabertura resolve
pela recuperação. Erros de validação (linha acima de 1 GiB no WAL) só falham
o statement.

### Atomicidade por statement (lotes)

Cada statement escreve num **lote** (`batch.rs`): as linhas ficam num buffer
privado e entram na MemTable **de uma vez**, sob o mutex, no commit — um
snapshot nunca vê parte de um statement. O handler faz commit no fim de todo
statement (`external_lock(F_UNLCK)`, `end_bulk_insert`, e `reset()`/`close()`
como rede de segurança; INSERT de uma linha sob `LOCK TABLES` usa a escrita
de linha única — atômica — seguida de sync do WAL), **inclusive
quando o statement falha**: a engine é não transacional para o servidor e o
binlog já registra as linhas inseridas; descartá-las divergiria a réplica.

Casos específicos:

* **Triggers, funções e `CALL`** (modos *prelocked*): o servidor não chama
  `start_bulk_insert`, mas a escrita continua em lote; o commit ocorre no
  `external_lock(F_UNLCK)` do statement externo ou, dentro de `CALL`/`LOCK
  TABLES`, no `reset()` ao fim da sub-instrução que usou a tabela.
* **Ler as próprias escritas**: linhas do statement corrente são invisíveis
  também para a própria sessão até o commit — um trigger ou rotina não vê as
  linhas que o statement externo acabou de inserir.
* **Réplica (RBR)**: o servidor aplica um statement em vários eventos de
  linhas; o commit é adiado até `STMT_END_F` (fim do statement, antes de a
  posição do relay log avançar) — um commit e um fsync por statement.
* **`LOCK TABLES`**: INSERT de uma linha fora de lote é gravado e sincronizado
  na hora; multi-linha commita em `end_bulk_insert`.
* Erro de commit em `reset()` é reportado ao cliente (`print_error`); em
  `close()`/destrutor (caminhos anormais) só vai para o log.

WAL v2 (segmentos v1 continuam legíveis, cada entrada = linha commitada):

| kind | corpo | significado |
|---|---|---|
| ROW | `batch_id` + linha | linha de lote ainda não commitado |
| COMMIT | `batch_id` | todas as ROW do lote passam a valer |
| ROW_COMMIT | linha | lote de uma linha |

Replay e checkpoint (granularidade de segmento):

* o flush grava `wal_seq` (até onde a MemTable foi selada) e
  `replay_seq = min(wal_seq + 1, segmento da 1ª linha de qualquer lote aberto)`;
  só segmentos `< replay_seq` são apagados — um lote aberto nunca perde linhas
  por causa de um flush de outro escritor;
* o replay lê a partir de `replay_seq`, agrupa por lote e aplica um lote só ao
  achar seu COMMIT **depois** de `wal_seq` (os anteriores já estão em chunks —
  sem duplicação); lote sem COMMIT é descartado (crash no meio do statement
  não deixa linhas);
* ids de lote nunca se repetem (`max visto + 1` na abertura); TRUNCATE
  invalida lotes abertos por época.

**Spill**: quando o buffer de um lote passa de `MEMTABLE_SIZE`, ele grava
chunks *em estágio* (protocolo `.tmp` → fsync → rename, fora do MANIFEST) e
para de logar no WAL. O commit grava o restante em estágio e faz **um** swap
de MANIFEST com todos — commit durável e atômico; sem COMMIT no WAL, as
linhas logadas antes do spill são descartadas no replay. Abort/crash: os
chunks em estágio são órfãos e são removidos.

Memória: cada lote é limitado a `MEMTABLE_SIZE`, e **todos** os lotes do
processo (linhas + séries novas) a `moosedb_batch_memory_budget` (1 GiB por
padrão): acima do orçamento, um lote com ≥ 1 MiB faz spill antecipado. Pior
caso: orçamento + 1 MiB por lote aberto.

WAL retido: um lote aberto que logou no WAL segura os segmentos desde a sua
primeira linha. Se passar de 64 segmentos retidos, ele é marcado e faz spill
no próximo write (deixa de logar e libera os segmentos). Um lote totalmente
parado (sem novos writes) continua retendo até commit/abort — descartá-lo
divergiria do binlog; o flush registra um warning.

### Limites de dados não confiáveis

Arquivos em disco são tratados como entrada não confiável: nenhum campo lido
controla uma alocação sem teto (um OOM aborta o `mysqld` e não é capturado
por `catch_unwind`). Bloco descomprimido ≤ 256 MiB e plausível em relação ao
tamanho armazenado; série ≤ 2²⁷ linhas por chunk, soma das séries = contagem
do header; RLE e decodificação crescem sob demanda; nenhuma decodificação
materializa mais de 512 MiB. O replay do WAL é em streaming e em duas passadas
(a 1ª só anota COMMITs; a 2ª guarda, para cada linha de lote que vai
commitar, só a posição no WAL — 16 B — e relê a linha, conferindo CRC e
decifrando, na hora de aplicar o COMMIT). Assim o volume em bytes de lotes
pendentes nunca impede a abertura, qualquer que tenha sido a configuração na
escrita. Tetos só para o que o writer não produz: 16 M linhas pendentes,
8 M COMMITs e 512 MiB de MemTable aplicada. Violação → `Corrupt`.
Aritmética sobre ids/seqs lidos usa `checked_*` (overflow → `Corrupt`).

Na escrita: valor ≤ **64 MiB** por coluna e linha ≤ **128 MiB** (erro
explícito, sem envenenar a tabela). Flush, spill e compactação dividem uma
série em várias partes/chunks quando um bloco passaria de 256 MiB, então
nenhum dado aceito gera estado irrecuperável. Uma linha de WAL antigo acima
de 128 MiB não entra na tabela — e, para manter o tudo-ou-nada, **o lote
inteiro dela também não**. Os segmentos que contêm esse lote são copiados de
forma atômica (`.tmp` → fsync → rename) para `wal_NNNNNN.tfl.wal.oversized`
antes de qualquer checkpoint, e um warning lista lote, linhas, bytes e
arquivos. A engine nunca lê esses arquivos: são cópias byte a byte de
segmentos WAL v2 (ainda cifrados com a versão original da chave, se a tabela
for cifrada — aposentar essa versão os torna ilegíveis). O operador pode
extrair e reinserir as linhas com valores dentro do limite, ou apagá-los.

Se uma thread entra em panic segurando o estado da tabela, toda operação
(leitura inclusive) falha na hora com "table must be reopened" — nunca
devolve estado possivelmente inconsistente; reabrir resolve.

### Coleta do WAL

Segmentos não reutilizados na abertura (keystream CTR único), mas também não
acumulam: a abertura de uma tabela com MemTable vazia grava o checkpoint e
apaga os segmentos antigos; FLUSH/OPTIMIZE com MemTable vazia e o commit de
um lote com spill coletam entradas mortas (respeitando lotes abertos).
Segmento cifrado só com header não exige a chave no replay.

## Concorrência

* `store_lock` faz como o InnoDB: INSERTs rodam concorrentes entre si e com
  SELECTs (`TL_WRITE_ALLOW_WRITE`); TRUNCATE/OPTIMIZE/ALTER e `DELETE` sem WHERE (vira `delete_all_rows`) continuam
  exclusivos.
* Escritores acumulam linhas em lotes privados (sem mutex) e só tomam o mutex
  no commit; leitores só seguram o mutex para
  pegar um **snapshot**: lista de chunks + segmentos congelados da MemTable
  (sem cópia). O snapshot é compartilhado enquanto a tabela não muda.
* `position()` guarda o snapshot do scan; `rnd_pos()` resolve posições por
  ele. Por isso o filesort continua correto mesmo com flush/compactação
  concorrentes (testado em `concurrency.test` e `moosedb-tests`).
* Existe **uma instância por diretório** no processo (`maintenance::open_shared`).
  DROP/RENAME aposentam a instância e esperam o job de manutenção em curso.

## Leitura

* **`rnd_pos`** (filesort por rowid, etc.): a série é decodificada uma vez em
  colunas compactas e guardada num LRU que divide o orçamento de
  `moosedb_chunk_cache_size` com o cache de blocos; cada fetch custa O(colunas)
  (~300 ns numa série de 200k linhas; antes, ~22 ms). Uma série cujo formato
  compacto não cabe no cache (ou com cache 0) é redecodificada a cada fetch —
  correto, mas O(série); dimensione `moosedb_chunk_cache_size` para as maiores
  séries consultadas com filesort.
* **Índice de séries**: log append-only compartilhado (`Arc`), lido sem cópia e
  fora do mutex, com versão (contador do processo, nunca repete) e *epoch*
  (identidade do log; muda em TRUNCATE/reabertura). A lista da FFI vem em
  ordem de registro.
* **Cache do pushdown de TAG** (handler, por tabela): por (coluna TAG,
  collation do predicado), as séries são agrupadas por valor em buckets de
  `CHARSET_INFO::hash_sort` — o mesmo hash que o servidor usa em GROUP BY e
  hash join — e cada candidato é confirmado com `strnncollsp`, então o
  resultado é idêntico à comparação série a série (PAD SPACE, NO PAD, `_ci`,
  expansões como ß = ss, ignoráveis). Custo O(valores do `IN`) em vez de
  O(séries × valores): 200k séries e `IN` de 900 valores caíram de ~1,6 s
  para ~20 ms. Indexação incremental enquanto a epoch não muda; teto de
  256 MiB e de 32 índices por tabela (acima disso, comparação direta).
  Para não ver um statement pela metade, o handler relê a versão depois de
  abrir o scan e refaz se ela mudou; após 16 tentativas (ou erro), abre o
  scan sem filtro de séries — correto, pois o servidor avalia o WHERE inteiro.
* **Leitura colunar, projetada e em lote**: o scan recebe as colunas
  pedidas (o handler deriva do `read_set`) e só elas são decodificadas; cada
  série vira colunas tipadas e as linhas são emitidas em lotes colunares
  reutilizáveis (`moosedb_scan_next_batch` / `prev_batch`), sem alocação por
  linha. Dentro da série (ordenada por ts — fora de ordem é `Corrupt`), o
  intervalo de tempo é achado por busca binária e as demais colunas são
  decodificadas só até o fim dele. Medido no core (2M linhas): full scan de
  135 → 5 ns/linha; range de 1 h em chunk de 1 dia, 236 → 6 ms.
  Consequência: uma consulta só detecta corrupção nos blocos que lê (ex.:
  `SUM(v)` não acusa um bloco de `ts` danificado); `CHECK TABLE` continua
  verificando o CRC de todos os blocos. Colunas virtuais, comandos que não
  são SELECT e `read_set` ampliado após a abertura usam a linha inteira;
  `rnd_pos` sempre devolve a linha inteira.
* **Full scan**: chunk por chunk, série por série (blocos lidos sob demanda,
  via cache LRU), e por fim a MemTable.
* **Scan por índice** (`index_read_map`, `index_first/last`, `ORDER BY ts`):
  *k-way merge* ordenado por `(ts, origem, ordinal)`, ascendente ou
  descendente. Cada série só é decodificada quando chega ao topo do heap, então
  `ORDER BY ts DESC LIMIT n` lê apenas os dados mais novos. Se a direção muda
  no meio (`index_next` após `index_prev`), o intervalo é materializado.
* **Índices compostos `(TAG…, tempo)`** (`index_read_map`, `read_range_*`,
  `records_in_range`, `info`). A chave do índice (formato *key image* do
  campo) é convertida, pelo próprio campo, no texto canônico que a TAG grava
  (`val_str`: decimal para inteiros, bytes na charset da coluna para strings) e
  vira um predicado de igualdade no **mesmo cache de séries por TAG** do
  pushdown (`hash_sort` + `strnncollsp`, invalidado por *epoch*), então a busca
  de um valor é O(1) e usa a collation da coluna (`'ALPHA'` casa as séries
  `alpha`, `Alpha` e `ALPHA ` de uma coluna `_ci`). O scan é o k-way merge
  ordenado por tempo sobre essas séries, com a faixa de tempo da chave
  (`=`, `>`, `>=`, `<`, `<=`; o `end_range` aperta o limite superior) — dentro de um prefixo
  fixo de tags as linhas saem em ordem de tempo, crescente ou decrescente
  (`ORDER BY clock DESC LIMIT n`, `MAX/MIN(clock)` por item: uma chamada de
  índice). Prefixo parcial de tags (`hostid = 2` num `(hostid, itemid, clock)`)
  mistura as séries por tempo. O scan usa o lote/snapshot privado do statement
  (as linhas do `INSERT … SELECT` da própria tabela não são vistas) e refaz a
  resolução de séries se uma série nova apareceu entre a resolução e o
  snapshot (sem o *fallback* sem filtro do pushdown: aqui as séries são o
  significado da leitura; após 64 tentativas falha com `HA_ERR_LOCK_WAIT_TIMEOUT`).
  As colunas do índice entram sempre na projeção do scan (o servidor as lê do
  registro em `compare_key`/`key_cmp_if_same`).
  **O que a engine promete sobre ordem.** Tags fixas + tempo: ordem de tempo.
  **Ordem entre valores diferentes de TAG** só em *leituras ordenadas*, pedidas
  pelo servidor quando ele depende dela: `MIN/MAX(tag)`, `MIN/MAX(tag) WHERE
  tag <op> const` (otimização de `opt_sum`, que pergunta `index_flags(idx,
  parte, 0)`), `index_first/last` e `index_read_map` só com tags e
  `KEY_OR_NEXT/AFTER_KEY/KEY_OR_PREV/BEFORE_KEY`. Elas usam uma visão
  ordenada das séries (tuplas de tag ordenadas pela comparação da coluna —
  numérica para TAG inteira com ou sem sinal, collation para string —, séries
  de tupla igual fundidas), reconstruída só quando a versão das séries muda
  (O(S log S), fora do mutex da tabela; teto de 512 MiB de heap para todas as visões do processo, com estimativa conservadora e cobrança em dobro durante a construção; acima disso a leitura falha com erro). A versão comparada é `moosedb_series_version()`, o mesmo contador que `moosedb_series_list_version()` informa para a lista usada na construção, e `index_next/prev` passam à tupla
  vizinha quando a atual acaba. **Cada tupla é lida num snapshot próprio** (não há um corte único no statement para leituras ordenadas que atravessam tuplas: linhas commitadas por outra sessão no meio podem aparecer só nas tuplas seguintes; cada tupla individualmente é consistente e atômica por statement). Se a tupla da
  chave existe (o caso comum: `MAX(clock)` de um item) a leitura não toca a
  visão ordenada. `index_flags(idx, parte, 1)` — a pergunta de `ORDER BY` /
  `GROUP BY` — **não** inclui `HA_READ_ORDER` para parte TAG, e nenhuma parte
  TAG tem `HA_READ_RANGE`: `ORDER BY itemid`, `GROUP BY itemid` e
  `itemid > 5` nunca usam o índice (filesort / outro índice), então nenhuma
  ordem errada pode ser devolvida. `index_next_same` de leitura exata não
  recompara as partes (todas as linhas já casam).
  **Estimativas.** `records_in_range` = linhas da janela de tempo (estimativa
  por chunk, já existente) × séries casadas ÷ séries da tabela (as séries
  casadas vêm do mesmo cache); nunca 0. `rec_per_key` das partes (`info`):
  linhas ÷ tuplas distintas, onde tuplas = `min(séries, produto dos
  distintos por TAG)` (contagens do cache de TAG, recalculadas só quando a
  versão das séries muda) e a parte de tempo = séries por tupla — daí o
  `ref` por `(itemid, clock)` em joins é custeado como ~1 linha.
  Medido (3000 itens × 2 dias × 60 s = 8,6 M linhas, MemTable com 300 k
  linhas): lookup por `(itemid, clock)` ≈ 20 µs, `MAX(clock)` de um item
  ≈ 15 µs; self-join `AGGREGATE_LAST` de 1 item/24 h 1,40 s → 0,97 ms
  (InnoDB 0,90 ms). Para isso o core passou a achar as séries pedidas dentro
  de um chunk por índice (`ChunkMeta::entries_of`, busca binária) e as
  linhas da MemTable por índice por segmento (`MemSnapshot::rows_of_series`)
  em vez de varrer todas as entradas/linhas a cada abertura de scan.
  O índice por segmento da MemTable só existe em segmentos de ≥ 64 linhas com no máximo 1 série por 4 linhas (senão o segmento é percorrido), e seu custo (24 B por linha) entra no `row_bytes` da MemTable, então flush e o orçamento global dos lotes continuam honestos. Se o cache de TAG passa de 32 índices ou 256 MiB, o pushdown e o lookup caem na comparação direta, O(séries) por sondagem (correto, só mais lento).
* **Pushdown de TAG** (`cond_push`): igualdades, `IN` e igualdades múltiplas
  sobre colunas TAG com constantes string viram um conjunto de `series_id`,
  calculado comparando os valores de cada série **com a collation da
  comparação** (`strnncollsp`). Só essas séries são lidas; os Bloom filters
  podam chunks. O servidor continua avaliando o WHERE inteiro.
  **TAG inteira** (`TINYINT`..`BIGINT`, signed/unsigned, sem `ZEROFILL`): a
  série guarda o texto de `val_str()` (decimal canônico) e o predicado é
  comparado **byte a byte** (`my_charset_bin`, também no cache — o mesmo
  `hash_sort` + `strnncollsp` do caminho string, então cache e fallback
  coincidem). A constante é convertida ao mesmo texto só quando isso é
  exatamente o que o servidor decide (`numeric_const()`): `INT` e `DECIMAL`
  são exatos; `DOUBLE` só abaixo de 2⁵³ (acima, `2⁵³+1` compara igual a `2⁵³`
  como double); string só se for inteiro limpo (`-?(0|[1-9]\d*)`, ≤ 16
  dígitos, charset não binário e de 1 byte por caractere); fracionária ou
  fora do intervalo do campo → o valor não casa série alguma e sai da lista;
  qualquer valor duvidoso (`'0123'`, `' 12'`, `'1e3'`, hex/binário, ≥ 2⁵³) →
  o predicado inteiro **não** é empurrado. `ZEROFILL` nunca é empurrado (o
  texto gravado é preenchido). TAG `VARCHAR` comparada com número também não
  (o servidor compara como `DOUBLE`). Em coluna `BIGINT` o servidor converte
  constantes string em inteiras antes de `cond_push` (`convert_const_to_int`),
  então o handler já recebe a constante `INT`.

## Desempenho: ingestão e leitura em série

Medido com o benchmark do laboratório Zabbix (`lab/zabbix/scripts/bench.sh`:
10.080 statements `INSERT … SELECT` de 5.000 linhas em autocommit, em ordem de
clock; depois agregações por hora sobre 100 itens × 24 h). Nenhuma mudança de
ABI (`moosedb_ffi.h` intacto), de formato em disco ou do protocolo de
durabilidade: o COMMIT do WAL continua sendo gravado e sincronizado antes de a
MemTable publicar as linhas, e o `fsync` por statement continua (o mesmo que o
`innodb_flush_log_at_trx_commit=1` do InnoDB no laboratório).

**Perfil da ingestão** (`perf record --call-graph dwarf` no `mariadbd`, tabela
com `PRIMARY KEY`; ~85 % das amostras no thread do statement): avaliação das
expressões do `SELECT` pelo servidor 18 % (fora do engine), `batch_write` 26 %,
flush em linha na hora do commit (MemTable cheia → chunk, sob o mutex) 23 %,
commit sem flush 10 %, compactação em background 15 % (outro thread). Por
função: `gorilla::encode` bit a bit 8,8 %, `malloc/free` ≈ 15 % (tags clonadas,
chave da série, payload do WAL e vetor de ids **por linha**), SipHash 6 %,
CRC32 3 %, ZSTD de blocos pequenos 6 %, `Arc<Segment>::drop` 2,7 %.

O que mudou (ganho medido no reprodutor, 5.000 itens, v4 com PK, ~485 → ~628
k linhas/s no conjunto; InnoDB ~570 k na mesma sessão):

* **Hash rápido só para chaves internas** (`fasthash.rs`): multiplicação
  dobrada com uma semente por processo, sem garantia contra colisões forjadas,
  por isso usada apenas em mapas cujas chaves o cliente não escolhe (ids de
  série, uids de chunk, posições): `tags_by_id`, estado de unicidade, índice por
  segmento da MemTable, LRU do cache. Mapas com chaves vindas do cliente
  (`SeriesIndex::by_key`, `Batch::local_ids`, `Batch::own`, agrupamento do
  flush) usam `std::collections::HashMap` (SipHash, `RandomState` por mapa);
  o custo medido dessa escolha na carga foi ~4 % (v4: ~650 → ~625 k linhas/s).
* **`Batch::write` sem alocação por linha**: a chave da série e o payload do
  WAL são codificados em buffers reutilizados do lote (`wal::row_entry_into`);
  as tags só são materializadas quando a série é nova (a série guarda a
  posição da primeira linha em `local_first`, em vez de uma cópia das tags).
  No commit, tabelas com chave única reaproveitam os ids globais resolvidos em
  `admit`; as demais resolvem por chave já codificada, na ordem de chegada
  (os ids continuam sendo atribuídos como antes).
* **Dica de tamanho do lote** (`Table::batch_hint`): o statement seguinte
  reserva linhas/séries do anterior (teto 4096, `try_reserve`: se o alocador recusar, cresce normalmente), fora do orçamento dos lotes por ser < 1 MiB.
* **Gorilla com acumulador de 64 bits** (`BitWriter`), byte a byte idêntico ao
  formato anterior (teste contra o escritor bit a bit); `group_rows` do flush
  em duas passadas (contagem e preenchimento com capacidade exata).

**Leitura** (agregação de 100 itens × 24 h e self-join `AGGREGATE_LAST`):

* **Merge por corridas** (`Scan::merge_fill`, `merge_fill_tie`): em varredura
  ascendente, as linhas da série no topo que ordenam antes da melhor linha das
  outras corridas saem juntas (busca binária no `ts` da janela + um
  `push_window` de k linhas), em vez de um push/pop de heap por linha. Em
  tabelas com chave única, uma linha cujo timestamp difere do próximo da
  corrida e é menor que o das outras é um grupo de um elemento (sem
  desempate, sem `Vec<Pend>`); só timestamps repetidos usam o caminho de
  grupos, e as colunas extras da chave são **decodificadas sob demanda**
  (`chunk_reader::load_more_columns`) em vez de forçadas na projeção. Descendente
  segue linha a linha.
* **Cache de colunas decodificadas** (`cache::get_column/put_column`, compartilha
  o orçamento de `moosedb_chunk_cache_size`): leitura **estreita** (≤ 1/8 de uma
  série de até 65.536 linhas: lookup por `(itemid, clock)`, última hora de um
  dia) decodifica a coluna inteira uma vez e a guarda; leitura larga só guarda
  a partir da 2ª vez que vê a mesma (arquivo, série, coluna)
  (`cache::touched_before`, tabela direta de 64 K slots) — uma varredura que
  passa por cada série uma vez não polui o cache. O LRU não reordena entradas
  entre as ~6 % mais recentes.
* **Handler**: coluna TAG inteira usa o mesmo memo de `fill_from_batch` que as
  TAGs string (o texto da série é parseado uma vez por corrida, não por linha).

Resultados (mesmo container, 5.000 itens × 7 d × 60 s; `lab/zabbix/README.md`):

| | InnoDB | v3 antes → depois | v4 antes → depois |
|---|---:|---:|---:|
| carga (linhas/s) | 497 k → 527 k | 409 k → 652 k | 371 k → 644 k |
| `agg_hour` (ms) | 27,6 → 27,8 | 32,0 → 25,2 | 35,9 → 27,2 |
| `agg_last` (ms) | 0,76 → 0,78 | 1,05 → 0,86 | 1,15 → 0,93 (0,75–0,85 em 15 repetições) |

**O que sobrou.** (1) O flush ainda é síncrono, sob o mutex da tabela, no commit
que enche a MemTable: ~19 % do tempo do statement, dos quais ZSTD de blocos
pequenos (~570 B por série/coluna) é a maior parte e a liberação das 350 mil
linhas (`Arc<Segment>::drop`) ~3 %. Escrever o flush de dados "frios por idade" com
LZ4 e deixar a recodificação para a compactação cortaria ~5 %, mas muda o codec
observável logo após o flush (testes MTR de compressão) — não feito. (2)
`ha_moosedb::fill_from_batch` custa ~14 % da agregação (um `Field::store` virtual
por coluna e linha). (3) `agg_last` com PK segue ~5–10 % acima do InnoDB: ~50
lookups `(itemid, clock[, ns])` por consulta, cada um abre um scan (alocação de
um `ScanBatch` por scan); reaproveitar os buffers entre scans é o próximo passo.

## Manutenção em background

`moosedb_compaction_threads` workers + um agendador (tick de 1 s) percorrem
as tabelas **abertas**:

* retenção a cada `moosedb_retention_check_interval` s (a primeira um
  intervalo após a abertura);
* compactação quando um intervalo acumula `moosedb_compaction_trigger_chunks`
  chunks, ou quando um chunk esfria e ainda não está no codec frio.

A compactação monta o chunk novo fora do lock (uma série por vez) e confirma
só se as entradas ainda estiverem vivas.

## Criptografia

`ENCRYPTION='YES' ENCRYPTION_KEY_ID=n` cifra chunks e WAL com AES-256-CTR. As
chaves vêm do plugin de key management do servidor (`file_key_management`,
`aws_key_management`, …) através de um callback; cada arquivo guarda id e
versão da chave e um IV aleatório, então a rotação de chaves funciona
(arquivos antigos seguem legíveis; novos usam a versão mais recente). A chave
precisa ter 256 bits. Ficam em claro: os headers (contagens, intervalo de
tempo, ids), o MANIFEST e o OPTIONS.

**Modelo de ameaça: só confidencialidade.** AES-CTR não tem MAC e os CRC32
não são autenticação — quem escreve no datadir consegue alterar bits do texto
claro sem a chave (testado). Integridade autenticada (AEAD) exigiria formato
v3. Salvaguardas atuais:

* abertura **fail-closed**: falha de decriptação do índice (chave errada ou
  índice corrompido sob cifra) recusa a abertura sem quarentena e sem
  reescrever o MANIFEST; para descartar um chunk de fato corrompido, o
  operador o renomeia para `.tfl.corrupt`;
* tabela cifrada não aceita chunk em claro, e vice-versa;
* sem chave disponível, criação, abertura e flush falham — não há fallback
  para texto claro.

## Mapeamento de tipos

| MariaDB | Core | Observação |
|---|---|---|
| DATETIME | TIMESTAMP (µs) | relógio de parede tratado como UTC — fronteiras de chunk independem de `time_zone` |
| TIMESTAMP | TIMESTAMP (µs) | tempo Unix real |
| TINYINT..BIGINT, YEAR, BIT | INT64 | BIGINT UNSIGNED preservado bit a bit |
| INT/BIGINT nomeado em `TIMESTAMP_COLUMN` | TIMESTAMP (µs) | segundos desde 1970 UTC × 10⁶ (eixo inteiro); fora de ±9223372036854 s o INSERT é recusado |
| FLOAT / DOUBLE | FLOAT32 / FLOAT64 | |
| DECIMAL | DECIMAL | string canônica (exata) |
| `COMMENT 'TAG'` | TAG | identidade da série |
| demais (VARCHAR, TEXT, BLOB, DATE, TIME, ENUM, SET, JSON…) | VARCHAR | via `val_str`/`store` |
| GEOMETRY | — | rejeitado no CREATE |

## Divergências da spec (e por quê)

| # | Spec | Implementado | Motivo |
|---|---|---|---|
| 1 | `struct ha_moosedb_table_option_struct` | `struct ha_table_option_struct` | O servidor tipa `TABLE_SHARE::option_struct` com esse nome exato. |
| 2 | `records_in_range(inx, min, max)` | `+ page_range *` | Assinatura do 11.4. |
| 3 | `compression_level` como `uint` | `ulonglong` | `HA_TOPTION_NUMBER` exige `ulonglong`. |
| 4 | `MYSQL_ADD_PLUGIN(... DEFAULT)` | `MODULE_ONLY` | `DEFAULT` compila a engine *dentro* do servidor; a spec exige `INSTALL SONAME`. |
| 5 | Glob em `DEPENDS` do `add_custom_command` | `FILE(GLOB_RECURSE … CONFIGURE_DEPENDS)` | CMake não expande globs em `DEPENDS`. |
| 6 | TINYINT(1) → BOOL | TINYINT → INT64 | `TINYINT(1)` aceita -128..127; a codificação delta comprime igualmente. |
| 7 | `max_supported_key_parts = 4` | `16`; formas de índice: o tempo sozinho, `(tag_1..tag_k, tempo)` e `(tag_1..tag_k)` (não únicos, #29) e **uma** chave única `(todas as TAGs, tempo[, 1-2 inteiras])` (#30) | Originalmente `1` (só o tempo): o otimizador remove do `WHERE` as condições usadas em `ref`, e um índice composto só vale se a engine honrar igualdade exata nas TAGs; passou a honrar (#29). `UNIQUE`/`PRIMARY` passaram a ser aceitos na forma do #30. |
| 8 | `HA_CAN_INDEX_BLOBS` | removido; `+HA_BINLOG_*_CAPABLE`, `HA_NO_AUTO_INCREMENT`, `HA_READ_ORDER`, `HA_CAN_TABLE_CONDITION_PUSHDOWN` | Binlog (§13), `ORDER BY ts DESC` pelo índice, pushdown de TAG. |
| 9 | TFValue com `bool`/enum/união anônima | `uint8_t bool_val`, `uint8_t kind`, união `data` | Ler `bool`/enum inválido vindo do C é UB no Rust; cbindgen não gera união anônima. |
| 10 | — | `TF_ERR_INTERNAL`, `TF_ERR_UNSUPPORTED`, `TF_ERR_CRYPTO` (10) | Panic capturado / recurso indisponível / falha de decriptação ou chave indisponível (vira `HA_ERR_DECRYPTION_FAILED`). |
| 11 | API FFI mínima | + sync do WAL, truncate, scans filtrados/bidirecionais, snapshots, séries, check, estimativa, inspeção, manutenção, chaves, settings | Necessárias para a Handler API completa e os recursos acima. |
| 12 | fsync por `write_row` | commit do **lote** no fim do statement (`external_lock(F_UNLCK)`, `end_bulk_insert`; sob `LOCK TABLES`, cada INSERT de uma linha) com fsync conforme `wal_sync_mode` | Linha confirmada = statement retornou OK; fsync por linha inviabiliza 1M linhas em 30 s. Ver §Atomicidade por statement. |
| 13 | Entrada do WAL `[CRC][len][ts][series_id][valores]` | header de segmento + `[CRC][len][linha]` | Header guarda os parâmetros de criptografia; `series_id` é reconstruído no replay. |
| 14 | Header de 64 bytes com os campos listados | mesmos campos + `column_count`, `chunk_id`, `wal_seq`, `schema_fingerprint`; codec nas flags | Os campos listados somam 40 bytes; o resto valida schema e ajuda diagnóstico. |
| 15 | Checkpoint implícito | `MANIFEST` explícito | Um flush gera vários chunks; sem commit único, um crash duplicaria ou perderia linhas. |
| 16 | `.tfl.idx` (índice de séries) | não existe | Índice reconstruído dos chunks + WAL na abertura. |
| 17 | `ENCRYPTION='YES'` | + `ENCRYPTION_KEY_ID` (padrão 1) | Escolher a chave do key management. |
| 18 | `CALL moosedb_compact(...)` global | procedures instaladas por banco (`sql/moosedb_install.sql`) sobre UDFs do `.so` | Plugins não criam procedures; `CALL` sem prefixo só procura no banco corrente. |
| 19 | Colunas IS `SCHEMA`, `TABLE`, `COLD_CHUNK` | `TABLE_SCHEMA`, `TABLE_NAME`, `COLD_CHUNKS` + colunas extras | Convenção do INFORMATION_SCHEMA; `ROWS` é palavra reservada (use `` `ROWS` ``). |
| 20 | Deps `byteorder`, `ahash`, `probabilistic-collections` | `std`, Bloom próprio, `getrandom` | Menos dependências; nada que a std não resolva. |
| 21 | — (particionamento não mencionado) | `HTON_NO_PARTITION`: `PARTITION BY` é recusado | `ha_partition` reorganiza partições com `delete_row`, que a engine append-only não suporta: `ADD`/`COALESCE PARTITION` duplicavam e perdiam linhas. O tempo já é particionado por chunks. |
| 22 | Procedures sem modelo de privilégio | UDFs exigem `DELETE` (retenção) e `ALTER` (compactação) na tabela; tabela inexistente e sem privilégio dão o mesmo erro | MariaDB não aplica ACL a UDFs; sem a checagem, qualquer usuário podia expirar dados alheios. |
| 23 | `RETENTION_PERIOD` / `HOT_THRESHOLD` sem limite | ≤ 10 000 anos; cálculo de cutoff saturante | Períodos absurdos estouravam a aritmética e geravam cutoff no futuro (retenção apagaria tudo). |
| 24 | — (isolamento não especificado) | atomicidade por statement: linhas de um statement aparecem todas de uma vez; crash no meio não deixa nada; sem MVCC nem transações multi-statement | Leituras nunca veem statements parciais; MVCC completo não se justifica com dados imutáveis. Erro no meio do statement mantém as linhas já inseridas (coerente com o binlog de engine não transacional). |
| 25 | Entrada do WAL única | WAL v2 com `ROW`/`COMMIT`/`ROW_COMMIT`; MANIFEST v2 com `replay_seq` | Necessário para a atomicidade por statement com escritores concorrentes. v1 continua legível. |
| 26 | Leitura linha a linha | leitura colunar projetada e em lote pela FFI (`TFBatch`), poda por ts dentro da série | O custo dominante era montar linhas (4 mallocs/linha), não decodificar. |
| 27 | Eixo de tempo só `DATETIME`/`TIMESTAMP` | + `INT`/`BIGINT` (signed/unsigned, `NOT NULL`, armazenada) **somente** via `TIMESTAMP_COLUMN='col'`, lido como segundos epoch UTC (`ts_us = secs × 10⁶`); índice simples sobre a coluna continua obrigatório; `ns` não é dobrado no eixo | Zabbix guarda `clock INT` + `ns INT`. Exigir `TIMESTAMP_COLUMN` evita que um índice qualquer sobre um inteiro mude o significado de tabelas existentes. Produto que estoura `i64` (ou `BIGINT UNSIGNED` > 2⁶³) é erro explícito, não truncamento; limites de range vindos do índice saturam. O core não mudou (mesmo `TF_COL_TIMESTAMP`, sem mudança de FFI); o IS exibe o eixo como DATETIME (clampa em 0000/9999 nos extremos). |
| 28 | Pushdown de TAG só para strings | + TAG inteira (`TINYINT`..`BIGINT`) com constantes `INT`/`DECIMAL`/`DOUBLE`/string-inteira-limpa em `=`, `IN`, igualdade múltipla; comparação binária do texto canônico | O Zabbix consulta `itemid = 123` sem aspas; cada query varria todas as séries. Regra de correção: o servidor reavalia o WHERE, então só se poda uma série quando é certo que o servidor não a casaria (ver §Leitura). Tipos mistos: em `IN`, uma constante de módulo ≥ 2⁵³ junto de constante `DOUBLE`/string (a lista pode ser comparada como DOUBLE) recusa o pushdown; em igualdade múltipla, todos os membros precisam ser colunas inteiras (membro `DOUBLE`/`DECIMAL`/string recusa). |
| 29 | Índice só no timestamp | Índices compostos não-UNIQUE `(tag_1..tag_k [, tempo])`, até 8 por tabela. TAG do índice: `COMMENT 'TAG'`, `NOT NULL`, inteira (`TINYINT`..`BIGINT`) ou `CHAR`/`VARCHAR` inteira (sem prefixo), sem repetição, sem `DESC`; qualquer subconjunto/ordem das TAGs (não precisa ser prefixo da ordem do schema); o tempo, se presente, é a **última** parte. O índice só no tempo continua obrigatório. `index_flags`: parte de tempo = `NEXT\|PREV\|ORDER\|RANGE`; parte TAG = `NEXT\|PREV`, e `+ORDER` só para `all_parts=0` (pergunta do `MIN/MAX(col)`); **nenhuma** parte TAG anuncia `RANGE`. Detalhes em §Leitura (índices compostos) | Zabbix lê `WHERE itemid=X AND clock BETWEEN …`, `MAX(clock) … itemid=X`, e faz self-join em `(itemid, clock)`; sem índice cada linha do join varria a série inteira (4,1 s vs 0,7 ms no InnoDB). A engine não tem estrutura de índice: a leitura é "quais séries casam as TAGs da chave" (cache de TAG, O(1)) + faixa de tempo (poda por busca binária), servida pelo k-way merge que já existia. Por isso a ordem que o servidor pode assumir é só a que isso dá de graça (ver §Leitura), e **não** se anuncia ordem nem faixa entre valores de TAG. |
| 30 | Índices só não-UNIQUE ("linhas não são únicas por tag+tempo") | **Uma** `PRIMARY KEY`/`UNIQUE (todas as TAGs em qualquer ordem, tempo[, 1-2 colunas INT NOT NULL])`, com unicidade verificada de verdade (`ER_DUP_ENTRY`, `INSERT IGNORE`, errkey = a chave). Sem TAGs: só `UNIQUE(tempo)`. TAG de texto na chave exige collation binária NO PAD (ou inteira); `REPLACE`/`ODKU` sobre chave existente → `ER_ILLEGAL_HA` (sem UPDATE; depois de um `DELETE` da chave inserem normalmente); `unique_checks=0` pula a checagem. Detalhes em §Chave única | O Zabbix 7.0 declara `PRIMARY KEY (itemid,clock,ns)`; antes só rodava com a PK removida. Subconjunto de TAGs foi recusado: unicidade sobre parte da identidade da série exigiria checar várias séries a cada INSERT. Collation `_ci`/PAD SPACE foi recusada porque duas séries (bytes diferentes) seriam a mesma chave. O índice de tempo isolado deixa de ser obrigatório quando existe a chave única (ela contém o tempo). |
| 31 | Engine append-only: sem `DELETE` (a retenção por chunk era a única remoção) | `DELETE` suportado por **tombstones** duráveis (faixa de tempo × séries, ou linhas exatas), com a atomicidade e a visibilidade do `INSERT` e remoção física na compactação. MANIFEST v3 (`born` por chunk, `tseq`, tombstones) só depois do primeiro `DELETE`; entradas WAL 4 e 5. `UPDATE` segue recusado. `DELETE` com `WHERE` roda concorrente com `INSERT`. Detalhes em §DELETE | O housekeeper e `history.clear` do Zabbix apagam por item/intervalo. Reescrever chunks a cada `DELETE` custaria O(chunk) por statement (5.000 por dia); o tombstone custa O(1) e o espaço volta na compactação. |

## DELETE (tombstones)

`DELETE` é **exclusão lógica**: nenhum chunk é alterado (são imutáveis); um
*tombstone* esconde as linhas até uma compactação (ou a retenção) removê-las
fisicamente. Implementação em `tomb.rs` (modelo, MANIFEST), `delete.rs` (operações,
replay), com ganchos em `scan`, `unique`, `compaction`, `table`, `batch`, `wal`.

**Dois tipos de tombstone.**

* *Faixa* (`RangeTomb`): `seq`, `[lo, hi]` de tempo, séries (`Sel::All` ou ids) e
  `hits` (linhas escondidas por chunk, para as estatísticas). Custa O(séries) de
  memória e nada por linha. É o que o housekeeper do Zabbix gera (um por item).
* *Linhas* (`RowsTomb`): `(chunk, entrada de série, ordinais)` em corridas
  `(início, tamanho)`. Chunks são imutáveis e ids não se repetem, então o par
  chunk/ordinal nomeia a linha para sempre. Usado pelo `WHERE` arbitrário, em que as
  linhas são achadas por **conteúdo** (série + `ts` + todas as colunas, floats bit a
  bit; linhas iguais são apagadas uma de cada vez).

**Ordem em relação aos inserts (`born`).** O tombstone de faixa esconde só linhas que
já existiam. Cada tombstone tem `seq` (por tabela, estritamente crescente, nunca
reutilizado: o MANIFEST guarda o maior, `tseq`); cada chunk tem `born`, o maior `seq`
já refletido nas linhas que ele contém. Um tombstone se aplica a um chunk **sse
`seq > born`**. Chunk de flush e chunk em estágio (spill) nascem com o `tseq` do
momento em que são publicados; a saída de uma compactação nasce com o `tseq` do
início dela (os deletes posteriores ainda a escondem, os anteriores foram aplicados
fisicamente). Linhas da MemTable são **removidas na hora** do `DELETE` (só os
segmentos que perdem linhas são reconstruídos; os de snapshots abertos ficam
intactos), então não precisam de `born`. Reinserir uma chave apagada funciona
porque a linha nova entra depois do tombstone (MemTable, ou chunk com `born` maior).

**Protocolo de um `DELETE`** (mesma regra do COMMIT de lote):

1. sob `del_gate` (um `DELETE` por vez por tabela, para as contagens ficarem exatas) e
   **sem** o mutex da tabela, conta/localiza o que ele esconde nos chunks de um
   snapshot (blocos de `ts` só são decodificados nas entradas cortadas pelo
   intervalo; entrada inteira dentro dele usa `row_count`);
2. toma o mutex; se o conjunto de chunks mudou (`layout_gen`: flush, compactação...)
   refaz o 1; senão
3. **apenda uma entrada DELETE ao WAL e a sincroniza** (conforme `wal_sync_mode`) —
   ponto de commit; falha aqui não muda nada em memória (e envenena a tabela como
   qualquer falha de WAL);
4. só então aplica: remove da MemTable, adiciona o tombstone, invalida o estado de
   unicidade das séries tocadas, publica o snapshot.

O `DELETE` **não reescreve o MANIFEST**: o tombstone vive no WAL até a próxima
mudança estrutural (flush, compactação, retenção, truncate, batch com spill, ou o
checkpoint da abertura), que o grava no MANIFEST v3. **Todo MANIFEST escrito carrega
os tombstones** (`Inner::manifest_for`), senão um checkpoint que apaga os segmentos
do WAL perderia os deletes.

**Formatos.** WAL (a versão do segmento continua 2): `4 DELETE_RANGE = seq lo hi
(0 | 1 n tags*)` — as séries vão **pelos valores das TAGs**, não por id, porque ids
de séries só presentes no WAL podem mudar entre execuções; `5 DELETE_ROWS = seq
n_mem rows* n_hits RowsTomb*` (linhas removidas da MemTable por conteúdo + linhas de
chunk já resolvidas em ordinais). Entradas de DELETE têm teto de 8 MiB (maior = log
adulterado = `Corrupt`; o escritor recusa ou quebra antes). MANIFEST v3: ver
`manifest.rs`. **Compatibilidade**: arquivos antigos (MANIFEST v1/v2, WAL v1/v2)
abrem sem mudança (`born = 0`, nenhum tombstone). Enquanto a tabela nunca teve um
`DELETE` (`tseq = 0`) o MANIFEST continua sendo gravado em v2, byte a byte como
antes; depois do primeiro é v3, e um binário antigo recusa abrir ("MANIFEST version 3
not supported") em vez de mostrar linhas apagadas. Uma entrada DELETE no WAL faz um
binário antigo falhar com "unknown kind" (nunca ignora em silêncio). Não há volta
automática a v2.

**Recuperação.** Abertura: tombstones do MANIFEST + replay das entradas DELETE do WAL
com `seq > tseq` do MANIFEST, na ordem do log, entre as linhas commitadas. Para
`seq <= tseq` só a parte da MemTable é reaplicada (o chunk já tem o efeito). A
posição no log é a ordem em que a tabela viu as coisas porque DELETE e COMMIT são
apendados e sincronizados sob o mutex. Idempotente: a faixa é idempotente por
natureza; a de linhas reaplica a MemTable por conteúdo e os ordinais gravados (o
MANIFEST anterior ao delete é, por construção, o conjunto de chunks do momento
dele). `kill -9` entre o fsync do WAL e a publicação = o delete vale (está no WAL),
nunca meio aplicado; antes do fsync = não vale.

**Leitura.** `Snapshot` leva o `TombSet`. Por entrada de série de chunk o scan corta o
intervalo de tempo pedido nos subintervalos que nenhum tombstone de faixa cobre (um
*run* por subintervalo no merge ordenado, um `ChunkCursor` por subintervalo no
stream) e os ordinais mortos viram buracos na janela decodificada (trechos
separados). Sem tombstones o caminho é o antigo (custo zero); com eles, O(tombstones
da série) por entrada. Também respeitam os tombstones: o lookup de unicidade
(`existing_extras`), a compactação, `stats()` (`row_count` exato, que o servidor usa
para `COUNT(*)` por `HA_STATS_RECORDS_IS_EXACT`) e o INFORMATION_SCHEMA.
`UniqueState` perde a lista "extras no maior `ts`" das séries afetadas (o limite
`max_ts` continua válido): sem `ER_DUP_ENTRY` falso depois de apagar nem falso aceite.

**Remoção física.** A compactação filtra as linhas escondidas (`build_merged`), grava
a saída com `born` = `tseq` do início, move os `hits` dos tombstones mais novos para a
saída e solta o resto; recusa o commit se surgiu um tombstone de linhas sobre suas
entradas (tenta no próximo ciclo). Dispara quando ≥ 25 % de um chunk está morto
(sempre no `OPTIMIZE`). Chunk cujas linhas estão **todas** escondidas é descartado
sem reescrita (conferido linha a linha antes; `dead_rows` é só estatística).
Tombstones que nenhum chunk vivo (com `born < seq` e faixa de tempo sobreposta) ainda
precisa são descartados (`TombSet::prune`), o que cobre a retenção; TRUNCATE zera
tudo (`tseq` não volta).

**Handler.** `HA_CAN_DIRECT_UPDATE_AND_DELETE`: `cond_push` (só no alvo de um `DELETE`, `SQLCOM_DELETE`
com lock de escrita) captura o `WHERE` INTEIRO e devolve NULL para o servidor, que só então
chama `direct_delete_rows_init` (com `WHERE` não capturado inteiro: recusa, fail-closed);
`direct_delete_rows_init` usa a captura. A análise do
`WHERE` (`capture_delete_cond`: conjunções de `TAG = const`/`IN` exatos e limites
`< <= > >= = BETWEEN` na coluna de tempo `INT`/`DATETIME`; nada é aproximado, ao
contrário do pushdown de leitura) e `direct_delete_rows` chama
`moosedb_delete_range` (`ROW_COUNT()` exato). `ORDER BY`, `WHERE` que não casa,
`LIMIT` menor que o intervalo, triggers, `binlog_format=ROW` e `RETURNING` fazem o
servidor apagar linha a linha: `delete_row` converte o registro como o `write_row` e
acumula em `DeleteBatch`; o fim do statement (`external_lock(F_UNLCK)`, `reset`,
`close`) o aplica por conteúdo — mesmo com erro no meio, como o `INSERT` (o binlog já
registrou as linhas). `delete_row` também atende `DELETE` multi-tabela e a réplica RBR (o applier compara o
registro lido com a imagem anterior do mestre byte a byte nos bytes de NULL, então os
bits não usados desses bytes agora vêm de `default_values` em vez de zero, e o applier
lê linhas inteiras).

**Limites e decisões do endurecimento.** Seleção de mais de 20.000 séries: cai para linha a linha
(a entrada do log tem teto de 8 MiB). Uma única linha cujo conteúdo codificado passa de 8 MiB e que
está na MemTable não cabe numa entrada: o `DELETE` falha com erro claro (`Unsupported`), sem efeito.
O handler avisa (nota) quando o núcleo apagou menos linhas do que o servidor contou. Acima de 4.096
tombstones de faixa pendentes a compactação reescreve os chunks tocados mesmo abaixo de 25 % mortos.
Leitura de faixa sobre a chave inteira `(itemid, clock, ns)` com `ns` fixado (`clock BETWEEN a AND b AND
ns = 1`): a engine começa em `clock = a` sem filtrar `ns` (superconjunto, o servidor reavalia o `WHERE`).

## Chave única (`PRIMARY KEY` / `UNIQUE`)

Forma aceita (`build_layout`): `(todas as TAGs, coluna de tempo [, até 2 inteiras
NOT NULL])`; a coluna de tempo e as extras entram no core como `UniqueSpec`
(passada em `TFTableConfig`, não persistida: o DDL a fornece a cada abertura).
A engine não tem estrutura de índice, então a unicidade é checada contra os dados,
com três peças em memória (`unique.rs`, estado sob o mutex da tabela):

* **por série, o maior tempo já aceito** (`max_ts`, confirmado *ou* em andamento) e,
  quando conhecido (`complete`), o conjunto das colunas extras das linhas
  exatamente nesse tempo. Linha com tempo `> max_ts` não pode ser duplicata
  (caso comum do *history syncer*: nada é lido); no `max_ts` responde o
  conjunto; mais antigo (ou conjunto desconhecido) faz um *lookup*: a série é
  lida naquele único instante nos chunks (Bloom + busca binária, cifrados ou
  frios incluídos), nos chunks em estágio (*spill*) do statement, na MemTable e
  nas linhas ainda no buffer do próprio statement. `max_ts` é só um limite
  superior (retenção e statements abortados nunca o baixam): estado perdido ou
  velho torna a checagem mais lenta, nunca errada. Reconstruído na abertura a
  partir das entradas de série dos chunks (`max_ts`, `complete=false`) e das
  linhas reaplicadas do WAL; `TRUNCATE` o zera;
* **reserva de séries (`claims`).** Linhas de um statement são invisíveis aos
  outros até o commit, então duas sessões veriam "a chave não existe". A primeira
  linha que um statement escreve numa série a reserva até o commit/abort; outro
  statement que precise dela espera (`Error::Busy`, em fatias de 50 ms numa
  condvar; o handler repete respeitando `KILL` e `moosedb_unique_lock_wait_timeout`)
  e depois confere contra o confirmado. Resultado: de duas inserções da mesma
  chave, exatamente uma vence (`Duplicate` para a outra). Ciclos de espera
  (A segura s1 e quer s2, B o inverso) são detectados e o que fecha o ciclo falha
  com `Error::Deadlock` → `ER_LOCK_DEADLOCK`. Statements em séries disjuntas não
  se tocam; statements que varrem as mesmas séries na mesma ordem se serializam.
  Um statement que falha/aborta invalida o estado das séries que segurava;
* **`unique_checks=0`**: linhas gravadas sem checar e sem reservar, mas ainda
  registradas em `max_ts`/conjunto (as seguintes continuam corretas para o que foi
  registrado). Chaves gravadas assim podem estar duplicadas e isso não é detectado
  depois — o mesmo risco do InnoDB em carga em massa, que aqui inclui a PK.

Durabilidade/atomicidade: nada muda (a checagem acontece antes de gravar a linha no
WAL/buffer; a linha recusada não entra no lote, que continua utilizável). Um erro
no meio do statement mantém as linhas já aceitas (§Atomicidade, #24): ao contrário
do InnoDB, `INSERT (a),(b),(a)` deixa `a` e `b`.

**Leitura pela chave.** A PK é um índice composto `(TAGs, tempo, extras)`: as
partes extras só aceitam igualdade (sem `HA_READ_RANGE`); o `index_read_map` com a
chave inteira (`HA_WHOLE_KEY`, `const`/`eq_ref`/ODKU/REPLACE) filtra as colunas
extras entre as linhas do instante. **Ordem:** nos scans ordenados de uma tabela com
chave única, o *merge* do core põe as linhas de mesmo `ts` em ordem das colunas extras
(coleta o grupo de um instante entre todos os runs, ordena, entrega — crescente ou
decrescente; um grupo pode atravessar lotes), então a parte `ns` anuncia
`HA_READ_NEXT|PREV|ORDER` e `WHERE itemid=X AND clock>N ORDER BY clock DESC, ns DESC
LIMIT 1` não faz filesort. Partes TAG continuam sem ordem (`all_parts=1`).

**Custo medido** (carga no formato do syncer: 100 statements de 5000 itens, 500 k
linhas, autocommit, em ordem de clock; MooseDB sem PK com `INDEX(clock)` +
`INDEX(itemid,clock)` contra a PK `(itemid,clock,ns)`): 0,75 s contra 0,81 s em média
de 6 execuções (+8 %, de −8 % a +20 % entre execuções; o InnoDB leva ~21 s).

## Agregação pela engine (group_by_handler)

`hton->create_group_by` aponta para `moosedb_create_group_by`
(`moosedb_gbh.cc`). Com `moosedb_group_by_pushdown=OFF` (padrão) devolve
`nullptr` e nada muda. Com `ON`, `analyze()` decide se a consulta está no escopo
e monta o plano; se estiver, o handler `moosedb_gbh` assume o GROUP BY (zera
`group_by` e `order_by`), senão devolve `nullptr` e a consulta segue o caminho
normal. Com `moosedb_gbh_debug=ON` o veredito sai como nota (`gbh: accepted` /
`gbh: refused: <motivo>`). Contador global `Moosedb_gbh_queries` = consultas
executadas pelo handler.

**Escopo aceito**: SELECT de nível superior, uma tabela MooseDB, GROUP BY de
colunas TAG (string ou inteira), VARCHAR ou INT comuns; select list só com
essas colunas, constantes e `COUNT(*)`/`COUNT(col)`/`SUM`/`AVG` de inteiro (menos
`BIGINT UNSIGNED`, que não cabe nos agregados de 64 bits com sinal do core) e
`MIN`/`MAX` de inteiro ou do eixo de tempo; WHERE = conjunção de igualdades/IN
de TAG e limites do eixo (`capture_delete_cond`, o mesmo reconhecimento exato do
DELETE direto; constantes string contra o eixo `DATETIME` valem quando o
comparador é datetime). HAVING, ORDER BY (chave ou agregado, ASC/DESC), LIMIT e
OFFSET entram. Fora: DISTINCT, ROLLUP, window, WITH TIES, SQL_CALC_FOUND_ROWS,
subconsulta/derivada/view/CTE/UNION, rotinas, expressões, predicado em coluna
comum, eixo `TIMESTAMP`.

**Execução** (`init_scan`): mapeia `all_fields` para os campos da tmp table,
resolve as séries do WHERE, abre a agregação (`moosedb_agg_open`, reconferindo
`moosedb_series_version` até 64 vezes) e roda com `moosedb_agg_threads` threads
e `moosedb_agg_max_memory` (o mesmo orçamento vale para a parte em C++, que
fica sozinha com os grupos depois do `agg_close`). Chave TAG = uma chave de
série; os valores vêm de `moosedb_series_list`. Grupos cujas chaves são iguais
pela collation (séries com o mesmo valor, `a`/`A` em `_ci`) são fundidos com
`hash_sort` + `strnncollsp`; o valor mostrado é o menor pelos bytes
(determinístico: o servidor mostra o primeiro que viu). A ordenação é a do
ORDER BY (NULL primeiro no ASC, strings por `strnncollsp`, AVG depois da
divisão com `my_decimal`), desempate pelos bytes da chave; sem HAVING e com
LIMIT usa `partial_sort` (top-N). `next_row` grava COUNT direto, SUM como
DECIMAL a partir do `i128`, AVG por `my_decimal_div(prec_increment)`, MIN/MAX
do eixo via `micros_to_time`. KILL/`max_statement_time` chegam pelo callback
`thd_killed`; estouro de orçamento vira o erro `MooseDB: full: aggregation
exceeds moosedb_agg_max_memory ...`. `end_scan` grava `thd->limit_found_rows`
(o servidor não o faz no caminho do gbh).

Pontos de atenção (já medidos ou assumidos):
- `max_sort_length`: o servidor pode agrupar/ordenar strings truncadas por
  `max_sort_length` (blobs/strings muito longas na ordenação); o handler compara
  a string inteira com `strnncollsp`. Para as chaves aceitas (TAG e VARCHAR até
  255 caracteres) a diferença só aparece com valores acima de `max_sort_length`
  (1024 bytes por padrão), que o escopo praticamente não admite.
- A lista de séries (`moosedb_series_list`) usada para obter os valores de TAG
  dos grupos é compartilhada e materializa todas as séries da tabela; ela não
  entra no orçamento `moosedb_agg_max_memory` e não pode ser restringida aos
  grupos do resultado.
- A reconferência de `moosedb_series_version` só acontece quando há filtro de
  séries (WHERE em TAG); sem filtro, séries novas durante a abertura são vistas
  inteiras ou não, nunca parcialmente.
- Nenhuma exceção C++ sai do handler: falta de memória vira `HA_ERR_OUT_OF_MEM`
  (ou o caminho normal, no planejamento).

Diferenças visíveis de metadata (não do dado): as flags `BINARY`/`NUM`/
`NO_DEFAULT_VALUE` dos campos do resultado vêm do campo da tmp table em vez do
item; `NOT_NULL`/`UNSIGNED` são iguais. Testes: `gbh_scope`, `gbh_basic`,
`gbh_collation`, `gbh_scale`.

Comportamento do servidor (11.4.13) medido com um handler de teste que devolvia
linhas fixas fora de ordem; o que a implementação de verdade precisa respeitar:

- **O servidor nunca ordena por cima do handler.** `Query::order_by` é o
  ORDER BY, ou a própria lista do GROUP BY quando não há ORDER BY (e também com
  `ORDER BY NULL`, que só se distingue pelo item NULL auxiliar no início de
  `all_fields`). Mesmo deixando `order_by` preenchido, o servidor não ordena (e
  com LIMIT devolve linhas erradas): o handler deve sempre zerar `group_by` **e**
  `order_by` e entregar as linhas já ordenadas (ORDER BY, top-N, LIMIT/OFFSET
  são aplicados pelo servidor sobre a ordem emitida: o handler faz o top-N).
  Quando ORDER BY coincide com GROUP BY, as duas listas são o mesmo `ORDER*`
  (a direção DESC vem nele).
- **HAVING é avaliado pelo servidor** sobre a tmp table, incluindo os agregados
  auxiliares (`n_aux` itens ocultos no início de `all_fields`: colunas de GROUP
  BY fora do SELECT, agregados de HAVING/ORDER BY, NULL de `ORDER BY NULL`).
  Todos os campos da tmp table precisam ser preenchidos.
- **Mapeamento `all_fields` -> `table->field`**: constantes não ocupam campo;
  agregados e colunas ocupam um campo cada, em ordem; campos de agregados
  auxiliares não têm nome. Expressões (`SUM(x)/2`) são calculadas pelo
  servidor, mas `COUNT(*)+1` (sem tabela em `used_tables`) ganha um campo
  que o handler não deve usar. `Item_sum::result_field` é a ponte segura; a
  tmp table só existe depois de `create_group_by` (o handler recebe `table`
  antes de `init_scan`).
- **`FOUND_ROWS()` fica desatualizado** (`do_select` retorna antes de gravar
  `limit_found_rows`), com ou sem `SQL_CALC_FOUND_ROWS`. Consultas com
  `SQL_CALC_FOUND_ROWS` são recusadas; para `FOUND_ROWS()` simples o handler
  precisa gravar `thd->limit_found_rows`.
- `create_group_by` é chamado **também em EXPLAIN** (duas vezes no formato
  tabular, uma no JSON), sem `init_scan`/`end_scan`: não pode abrir snapshot nem
  reservar recurso no construtor. É recriado a cada `EXECUTE` de PREPARE, com
  os `Item_param` já ligados (`CONST_ITEM`, `const_item()`), e o objeto é
  apagado pelo servidor com `delete` (`~Pushdown_query`): criar com
  `std::make_unique<...>().release()`.
- É chamado para qualquer SELECT não constante na tabela, **sem GROUP BY**
  (`group_by == NULL`), em subconsulta, derivada/CTE/view com GROUP BY, partes de
  UNION, ROLLUP, window e dentro de rotinas — o reconhecedor recusa tudo isso.
  `SELECT DISTINCT col` chega como GROUP BY sem `distinct`. View mesclada e
  JOIN com outra engine não chamam o hook; `COUNT(*)` puro e tabela de ≤1 linha
  (const table) também não. `GROUP BY` sobre constante some (`distinct=1`,
  `group_by=NULL`).
- **WHERE**: nunca traz `Item_equal` (já substituído por `=` simples, com
  constantes propagadas e conjuntos reordenados; `NOT (v>3)` vira `v<=3`).
  Constantes de tempo chegam como `Item_string`/`Item_param` (varchar/longblob),
  mas o comparador é `datetime`: o valor tem de ser obtido por `get_date()`,
  nunca por `val_str()`. `BETWEEN` e expressões constantes chegam como
  `Item_cache` já tipados.

### Motor de agregação (`moosedb-core/src/agg.rs`, FFI `moosedb_agg_*`)

Usado por `moosedb_gbh.cc` (ver acima).
`Table::agg_open(lo, hi, séries)` segura **um** `Snapshot` (mesmo corte de um
scan não ordenado: só o commitado; o lote privado do statement é invisível).
`Agg::run(spec, cancel)` faz hash aggregation e deixa um resultado colunar
(`moosedb_agg_result(start, max)`, ordem não especificada). Deletes passam por
`scan::visible_stretches`/`Visible` (a mesma lógica de `plan`/`split_by_holes`/
`load_window` do scan em stream; não há segunda cópia).

- **Chaves**: `Series` (um estado por série, laço apertado na janela) e
  `Column(i)` (Int64/Timestamp/Bool como inteiro, Varchar/Decimal como bytes;
  NULL é valor próprio). Imagem da chave em bytes num `std::HashMap` (SipHash:
  o valor vem do cliente; o `FastMap` é só para ids internos), com cache da
  última chave e do "mesmo valor da linha anterior".
- **Funções**: `CountStar`, `Count(i)`, `Sum(i)` (Int64, acumulador `i128` com
  `checked_add`; estouro = erro `Full`), `Min/Max(i)` (Int64/Timestamp).
- **Filtros** por máscara de dicionário: uma execução com a única chave
  `Column(c)`, sem funções nem filtros, grava o dicionário de `c` (valores na
  ordem do resultado); filtros seguintes passam `mask[i]` por posição do
  dicionário (vários = AND; máscara de tamanho errado ou sem dicionário =
  `InvalidArg`).
- **Paralelismo**: `std::thread::scope`; unidades de ~256 entradas de chunk e
  de 64 Ki linhas da MemTable via `AtomicUsize`; um mapa parcial por worker,
  merge sequencial; resultado idêntico para qualquer nº de threads. O callback
  de cancelamento só roda na thread chamadora (~10 ms); workers leem um
  `AtomicBool`. Pânico em worker é repropagado na thread chamadora (vira
  `TF_ERR_INTERNAL` na FFI). Cancelado = `Error::Cancelled`/`TF_ERR_CANCELLED`.
- **Memória**: um orçamento atômico compartilhado (chave + estados + overhead
  por grupo em cada mapa parcial, conjuntos dos filtros, resultado, dicionário);
  estourar = `Error::Full("aggregation exceeds moosedb_agg_max_memory ...")`.
  Cada worker decodifica uma janela por vez, então a decodificação simultânea
  é limitada pelo nº de threads.

## Limitações conhecidas

* A manutenção de background só alcança tabelas **abertas** (no table cache).
  `OPTIMIZE TABLE` e as procedures funcionam sempre.
* `DELETE FROM t` sem `WHERE` com `binlog_format=ROW` é executado linha a linha
  pelo servidor (`delete_row`, por conteúdo); `TRUNCATE` é o caminho rápido.
* `DELETE`: ver §DELETE. Limites: `UPDATE` recusado; `LIMIT` sem `ORDER BY` apaga
  linhas arbitrárias (as mais antigas só se o intervalo cabe no `LIMIT`); o caminho
  direto exige o `WHERE` exato descrito no handler (eixo `TIMESTAMP` e funções vão
  linha a linha); seleção de séries que não cabe em 8 MiB de log é recusada; `DELETE`
  linha a linha de mais de 32 MiB de linhas é aplicado em etapas atômicas; dois
  `DELETE`s concorrentes sobre as mesmas linhas fazem o `ROW_COUNT()` do servidor
  contar linhas que o outro levou (estado final correto); leitura com muitos
  tombstones sobre a mesma série custa O(tombstones) por entrada até a compactação;
  chunks com tombstones só são reescritos quando ≥ 25 % mortos ou no `OPTIMIZE`.
* Pushdown de TAG só em `=`, `IN` (não negado) e igualdades múltiplas ligadas
  por `AND` (OR, `NOT IN`, `<>`, `<=>` são avaliados só pelo servidor), com
  constantes string (colunas string) ou numéricas (TAG inteira, ver §Leitura).
  Sem pushdown: TAG `ZEROFILL`, TAG `VARCHAR` comparada com número, TAG
  `DECIMAL`/`DOUBLE`/`YEAR`/`BIT`, strings inteiras ≥ 2⁵³ ou não limpas e
  `DOUBLE` ≥ 2⁵³.
* Chave única, falhas da reserva: `ER_LOCK_DEADLOCK`, `ER_LOCK_WAIT_TIMEOUT` e `KILL`
  durante a espera por uma série saem com as linhas anteriores do statement já
  mantidas (#24), ao contrário do InnoDB; quem re-executa recebe `ER_DUP_ENTRY` nessas
  linhas. Recomendação: `INSERT IGNORE` no retry, ou statements de uma série / ordem
  de séries consistente. O estado de unicidade (~70 B/série, em `Inner`) não entra no
  `moosedb_batch_memory_budget`. O desempate por `ns` em varredura ordenada de tabela
  inteira com PK coleta e ordena o grupo de cada instante (custo extra por linha;
  não medido isoladamente). `Table::write_shared` usa um prazo fixo de 50 s
  (o handler não o usa: respeita `moosedb_unique_lock_wait_timeout`).
* Chave única: uma só por tabela, forma fixa (ver §Chave única); statements que
  escrevem várias séries em ordens opostas ao mesmo tempo podem receber
  `ER_LOCK_DEADLOCK`; uma sessão que tem dois statements abertos na mesma tabela
  e espera por si mesma só sai pelo timeout; `REPLACE`/`ODKU` sobre chave
  existente e `UPDATE` segue recusado; `unique_checks=0` abre mão da
  checagem sem aviso; ao ler a PK, `ns` com intervalo (`ns > X`) não vira faixa de
  índice (o servidor filtra).
* Índices compostos não únicos: TAG nullable, `DECIMAL`/`DOUBLE`/`YEAR`/
  `ENUM`/`BLOB` ou prefixo (`col(8)`) não são aceitos em índice; o tempo é sempre
  a última parte. Uma faixa sobre TAG (`itemid > 5`) não vira faixa de índice, e
  `ORDER BY`/`GROUP BY` por TAG não são servidos pelo índice. `ORDER BY clock
  DESC, ns DESC LIMIT 1` (índice não único: o `ns` não está nele; com a PK `(itemid,clock,ns)` sai do índice sem sort, ver §Chave única) lê a faixa do item e ordena
  (≈ 1440 linhas/dia a 60 s): ~210 µs por item contra ~55 µs no InnoDB (PK com `ns`);
  a forma do frontend sem PK (`MAX(clock)` + `clock = X ORDER BY ns DESC LIMIT 1`)
  fica em ~60 µs por item, incluindo o PREPARE/EXECUTE. A visão ordenada de tuplas (leituras `MIN/MAX(tag)`) é refeita quando
  surge série nova e custa O(S log S); acima de 512 MiB a leitura falha com erro
  em vez de devolver ordem errada. O estimador supõe séries de tamanho
  parecido.
* Eixo de tempo inteiro: só `INT`/`BIGINT` e só por `TIMESTAMP_COLUMN`; intervalo
  ±9223372036854 s (≈ ano 292277); `SMALLINT`/`MEDIUMINT`/`TINYINT` não são
  aceitos como eixo. `ns` do Zabbix é coluna comum (não há `TIMESTAMP_NS_COLUMN`);
  `ORDER BY clock, ns` usa filesort. As procedures de manutenção continuam
  recebendo limites como `'YYYY-MM-DD[ HH:MM:SS]'` (UTC) também para eixo inteiro.
* A compactação mantém um intervalo de tempo em um chunk; com
  `CHUNK_INTERVAL='1 MONTH'` e muitas séries, o chunk resultante pode ser grande.
  Uma série que não cabe nos limites de bloco é dividida em vários chunks do
  mesmo intervalo; um grupo que falha ou precisa de divisão não é reenfileirado
  pelo background até o conjunto de chunks mudar (só `OPTIMIZE` força) — isso
  inclui a recodificação quente→fria e falhas transitórias (ENOSPC/EIO), e
  um `OPTIMIZE` com parte dos grupos falhando retorna OK (falhas só no log).
* Retenção é física e local a cada servidor (sem eventos de binlog): mestre e
  réplica podem diferir temporariamente nas linhas já expiradas — inclusive
  quando um `OPTIMIZE` sem `DELETE` no mestre é replicado e a thread SQL da
  réplica, com todos os privilégios, aplica a retenção.
* 1 MiB ≤ `MEMTABLE_SIZE` ≤ 96 MiB (garante blocos de flush < 256 MiB); tabelas antigas
  com valor maior abrem com o valor limitado.
* O plugin se declara `EXPERIMENTAL`: requer `plugin_maturity=experimental`.
* Criptografia sem integridade autenticada (ver §Criptografia).
* **`mariadb-backup` não copia tabelas MooseDB**: ele pula subdiretórios do
  banco (`extra/mariabackup/backup_copy.cc:379`) e só copia extensões fixas
  (`backup_copy.cc:926-943`, `common_engine.cc:352-361`). Os hooks
  `hton->prepare_for_backup`/`end_backup` (`sql/handler.cc`) são `void` e não
  podem falhar o `BACKUP STAGE`; mas `hton->flush_logs` retorna erro, e o
  mariadb-backup executa `FLUSH NO_WRITE_TO_BINLOG ENGINE LOGS` em
  BLOCK_COMMIT com `die_on_error`. A engine liga um flag atômico em
  `prepare_for_backup`/desliga em `end_backup` e `flush_logs` falha só com o
  flag ligado e ao menos uma tabela MooseDB no datadir (mesma varredura do
  I_S) — rc=1 em vez de backup incompleto. Limitação: `--no-lock` pula esse
  statement (nem emite `BACKUP STAGE`, então nenhum hook roda) e continua
  silencioso. Backup suportado: `BACKUP STAGE BLOCK_COMMIT` + snapshot
  atômico do volume (crash-consistente pelo protocolo de durabilidade) ou
  `mariadb-dump --lock-all-tables` — ver user-guide §Backup.
* Tabelas não podem ser particionadas.
* `HANDLER ... OPEN` não é suportado (`ER_ILLEGAL_HA`); `IN` com mais de
  `in_predicate_conversion_threshold` (1000) valores vira subquery no
  servidor e não é empurrado.
