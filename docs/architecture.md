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
| `manifest` | MANIFEST v2 (`wal_seq`, `replay_seq`, chunks vivos), troca atômica |
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
| `MANIFEST` | lista de chunks vivos + checkpoint do WAL (`wal_seq`) + início do replay (`replay_seq`) (v2; CRC32, troca atômica) |
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
  SELECTs (`TL_WRITE_ALLOW_WRITE`); TRUNCATE/OPTIMIZE/DELETE/ALTER continuam
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
| 7 | `max_supported_key_parts = 4` | `16`, mas só 3 formas de índice, todas **não-UNIQUE**: o tempo sozinho (obrigatório), `(tag_1..tag_k, tempo)` e `(tag_1..tag_k)` — ver #29 | Originalmente `1` (só o tempo): o otimizador remove do `WHERE` as condições usadas em `ref`, e um índice composto só vale se a engine honrar igualdade exata nas TAGs; passou a honrar (#29). `UNIQUE`/`PRIMARY` seguem recusados (linhas não são únicas por tag+tempo; é outro item). |
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

## Limitações conhecidas

* A manutenção de background só alcança tabelas **abertas** (no table cache).
  `OPTIMIZE TABLE` e as procedures funcionam sempre.
* `DELETE FROM t` sem `WHERE` com `binlog_format=ROW` é executado linha a linha
  pelo servidor e falha (`ER_ILLEGAL_HA`); `TRUNCATE` funciona sempre.
* Pushdown de TAG só em `=`, `IN` (não negado) e igualdades múltiplas ligadas
  por `AND` (OR, `NOT IN`, `<>`, `<=>` são avaliados só pelo servidor), com
  constantes string (colunas string) ou numéricas (TAG inteira, ver §Leitura).
  Sem pushdown: TAG `ZEROFILL`, TAG `VARCHAR` comparada com número, TAG
  `DECIMAL`/`DOUBLE`/`YEAR`/`BIT`, strings inteiras ≥ 2⁵³ ou não limpas e
  `DOUBLE` ≥ 2⁵³.
* Índices compostos: sem `UNIQUE`/`PRIMARY`; TAG nullable, `DECIMAL`/`DOUBLE`/`YEAR`/
  `ENUM`/`BLOB` ou prefixo (`col(8)`) não são aceitos em índice; o tempo é sempre
  a última parte. Uma faixa sobre TAG (`itemid > 5`) não vira faixa de índice, e
  `ORDER BY`/`GROUP BY` por TAG não são servidos pelo índice. `ORDER BY clock
  DESC, ns DESC LIMIT 1` (o `ns` não está no índice) lê a faixa do item e ordena
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
* **`mariadb-backup` não copia tabelas MooseDB** (sai com rc=0): ele pula
  subdiretórios do banco (`extra/mariabackup/backup_copy.cc:379`) e só copia
  extensões fixas (`backup_copy.cc:926-943`, `common_engine.cc:352-361`).
  A engine ainda não implementa `hton->prepare_for_backup`/`end_backup`
  (pausar manutenção durante `BACKUP STAGE`). Backup suportado:
  `mariadb-dump --lock-all-tables` — ver user-guide §Backup.
* Tabelas não podem ser particionadas.
* `HANDLER ... OPEN` não é suportado (`ER_ILLEGAL_HA`); `IN` com mais de
  `in_predicate_conversion_threshold` (1000) valores vira subquery no
  servidor e não é empurrado.
