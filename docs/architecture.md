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
| `chunk`, `chunk_writer`, `chunk_reader`, `block` | formato `.tfl` v2/v3, blocos por coluna |
| `compression/` | delta-of-delta, delta, Simple8b, Gorilla, RLE, LZ4/ZSTD |
| `scan` | snapshots, scan em streaming, k-way merge ordenado |
| `compaction` | merge de chunks por intervalo, recodificação fria |
| `maintenance` | registro de tabelas abertas + pool de threads de fundo |
| `crypto` | AES-256-GCM autenticado (AES-CTR legado só para leitura), provedor de chaves |
| `format` | números de versão de todos os formatos em disco |
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
| `chunk_<início>_<fim>_<id>.tfl` | chunk selado (formato v2 ou v3 cifrado, `chunk.rs`) |
| `*.tfl.corrupt` | chunk em quarentena (CRC inválido na recuperação) |

### Formato do chunk (v2 e v3)

Header de 64 bytes (texto claro) → header de criptografia de 32 bytes (se
cifrado) → blocos de coluna → índice de séries → Bloom filter → footer de 24
bytes. Cada **bloco de coluna** tem codificação por tipo, codec (LZ4/ZSTD,
aplicado só se reduzir o tamanho) e CRC próprio; o arquivo inteiro tem um CRC
sobre os bytes armazenados (o `CHECK TABLE` funciona sem a chave). No v3
(cifrado) cada bloco e a seção índice+Bloom são selados com AES-256-GCM e
seguidos de uma tag de 16 bytes (ver §Criptografia).

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

#### Zone maps e Bloom por coluna (poda por valor)

Além de tempo e TAG (índice de séries + Bloom de séries), o chunk guarda, por
**bloco** (uma entrada de série × uma coluna de dados; a entrada é a unidade
que o scan pula, com todos os blocos dela), um resumo de valores
(`moosedb-core/src/zone.rs`):

| Coluna | Resumo | Bytes por bloco |
|---|---|---|
| INT64 / TIMESTAMP / BOOL | min, max, nº de NULLs | 17 (+4 se há NULLs) |
| FLOAT64 / FLOAT32 | min, max dos não-NaN, "tem NaN", NULLs | 17 (+4) |
| VARCHAR / DECIMAL | Bloom de valores do bloco (1% FPR, hash FNV-1a 64 do valor exato e do sem espaços finais), NULLs | ≥14 (+4); ~1,2 B por valor distinto; sem Bloom se o bloco tem >65536 distintos (≈80 KiB), sem teto de linhas |
| coluna de tempo da tabela | nenhum (os limites da entrada já cobrem) | 1 |

Layout: seção opcional **no fim da unidade índice+Bloom**, depois do Bloom de
séries: `ndata:u32 len:u32 registros crc32:u32`, um registro por (entrada,
coluna) em ordem de entrada. A flag `ZONES` (bit 3 de `flags` do header) a
anuncia; **sem bump de versão**: v2 continua v2 e v3 continua v3, pois leitores
anteriores param após o Bloom e ignoram bytes e flag (por leitura do código; não testado com um
binário antigo de verdade). Só é gravada se algum resumo for útil. Em v3 a seção está dentro da
unidade selada com AES-GCM (nonce por posição, AAD = header, incluindo a flag, +
tipo de unidade `1`/`bloom_offset`): adulterar, truncar ou remover a seção ou
limpar a flag falha a autenticação (`Error::Crypto`, fail-closed); não há tipo
de unidade novo porque não há unidade nova. Em chunk v2 em claro um CRC32 próprio
da seção a protege: seção malformada ou com CRC errado vira "sem resumos"
(nunca erro, nunca poda errada). Chunks antigos não têm a seção e não são
podados; `OPTIMIZE`/compactação regrava com resumos (tudo passa por
`ChunkBuilder::add_series`). Custo em memória: 32 B por bloco em `ChunkMeta`, mais o Bloom
(≥~60 B de estruturas + ~1,2 B por distinto) em blocos VARCHAR/DECIMAL; fica
residente para todo chunk vivo e não entra em nenhum orçamento (medido no lab
fileaccess, 20 M linhas: +75 MB de RSS com 14 mil séries×dia, +332 MB com 485
mil; disco +6% e +10%). Chunks gravados antes da seção (ou já compactados) só
ganham resumos quando regravados — `OPTIMIZE` não reescreve chunk já compactado.

Poda (sempre conservadora; `Pruner::entry_pruned`):

* só descarta a entrada se **nenhuma** linha dela pode casar; NULL nunca casa
  os predicados empurrados; resumo ausente/desconhecido ⇒ não poda;
* FLOAT: comparação IEEE; chunk com NaN no bloco nunca poda; ±0.0 são iguais;
  limite NaN não poda; em FLOAT32 o limite é alargado para cobrir o
  arredondamento a `float` e o valor `double`;
* VARCHAR/DECIMAL: **só igualdade exata em bytes** (collation binária NO PAD,
  ou PAD SPACE de charset com espaço 0x20 com `pad_space`, como
  `TFAggFilter`); sem min/max (a ordem de bytes não é a ordem SQL);
* DELETE: o resumo vem das linhas gravadas, ou seja, só pode ser mais largo que
  as linhas vivas; tombstones continuam aplicados depois;
* MemTable/WAL: sem resumo, nunca podadas; resumos de chunk com `schema_fp` ou
  nº de colunas diferente do schema atual são ignorados.

Onde liga: (1) **scan** — `ScanFilter::cols` (`ColPred`: faixa inteira, faixa
float, conjunto de bytes), testado junto de tempo/série nos modos stream e
merge; é só dica (linhas das entradas lidas voltam mesmo sem casar; o servidor
reavalia o WHERE); C ABI `moosedb_scan_open_pruned`. (2) **agregação** — os
`filters` já existentes (`TF_AGG_FILTER_VALUES` e máscara de dicionário, incl.
`null_ok`) viram `Pruner` em `Plan::new`, aplicado em `Plan::units`; sem mudança
de ABI. Além deles, `AggSpec::preds` / `TFAggSpec::preds` (`TFColPred`, mesma
semântica do scan: dica, NULL nunca casa) entram no mesmo `Pruner`; o gbh passa
os limites de faixa do WHERE (`INT`/`BIGINT`; para `FLOAT`/`DOUBLE` sem `(M,D)` a dica `TF_PRED_FLOAT` também é gerada, mas hoje o gbh recusa colunas DOUBLE como filtro, então não chega ao core por ele) (`col > K`, `BETWEEN`, ...) extraídos
por `collect_value_predicates` (a mesma extração do scan normal: `FLOAT(M,D)`/
`DOUBLE(M,D)`, `BIGINT UNSIGNED`, `YEAR`, `BIT`, string e `OR`/`NOT`/`IS NULL`
não geram dica) tanto na **fase de dicionário** quanto na execução final. É na
fase de dicionário que a faixa importa: a máscara só vira conjunto exato
(`null_ok` incluso) depois que o dicionário percorreu todos os blocos da coluna;
com a dica, as entradas fora da faixa nem são lidas. A avaliação exata
(máscara/valores) não muda. Contadores globais (`moosedb_prune_stats`): entradas testadas, podadas
por zone map, podadas por Bloom, linhas e blocos pulados, blocos lidos do disco e ns gastos lendo/decifrando/descomprimindo (`read_block`). Não há poda no nível do chunk
inteiro além da que já vem de tempo/Bloom de séries (um chunk cujas entradas
são todas podadas não decodifica nada, mas ainda é percorrido).

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

### Fuzzing dos decodificadores de disco

`storage/moosedb/rust/fuzz/` (cargo-fuzz, **fora** do workspace: exige nightly e
não afeta o build do plugin nem o clippy). O core expõe, só com a feature
`fuzz` (`#[doc(hidden)] pub mod fuzz_api`), um ponto de entrada por parser:

| Alvo | Cobre |
|---|---|
| `column_block` | corpo de bloco por tipo (delta, delta-of-delta, Gorilla, RLE, simple8b, plain; `decode`, `decode_compact`, `decode_compact_prefix`) |
| `block_envelope` | envelope do bloco (`unpack`, CRC) + wrapper LZ4/ZSTD (`decompress`) |
| `codec_stream` | cada codec isolado (simple8b, delta, dod, Gorilla 32/64, RLE, varint) |
| `chunk_file` | arquivo `.tfl` inteiro (CRC do header reparado): header, índice de séries, bloom, `decode_series`, `fetch_row`, `read_block` |
| `small_records` | entrada de série, valor/linha, bloom, tombstones, cabeçalho de cifra |
| `manifest` | `MANIFEST` v1/v2/v3 (com e sem CRC reparado) |
| `wal_segment` | segmento WAL (CRC das entradas reparado), replay em todos os modos |
| `wal_payload` | corpo de entrada WAL (DELETE_RANGE/DELETE_ROWS/linha) |

Rodar (imagem descartável com nightly + cargo-fuzz; target e corpus em volumes):

```bash
docker build -t moosedb-fuzz-img storage/moosedb/rust/fuzz
R='docker run --rm -v "$PWD:/work" -v moosedb-target-fuzz:/fuzz-target -e CARGO_TARGET_DIR=/fuzz-target -w /work/storage/moosedb/rust/fuzz moosedb-fuzz-img'
eval $R cargo +nightly fuzz build
eval $R cargo run --release --bin gen_seeds /fuzz-target/corpus      # corpus inicial (arquivos reais)
eval $R /fuzz-target/x86_64-unknown-linux-gnu/release/chunk_file /fuzz-target/corpus/chunk_file \
        -max_total_time=600 -timeout=10 -rss_limit_mb=2048
```

Critério: entrada arbitrária devolve `Err` — nunca panic, travamento
(`-timeout`) ou alocação acima do teto (`-rss_limit_mb`). Todo crash achado
vira teste de regressão em `moosedb-tests`. O harness limita `n` (linhas) a 1 M
por execução; o teto real (512 MiB materializados por decode) está acima disso.

### Coleta do WAL

Segmentos não reutilizados na abertura (nonce GCM único por IV de arquivo), mas também não
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

`ENCRYPTION='YES' ENCRYPTION_KEY_ID=n` cifra chunks e WAL com **AES-256-GCM**
(autenticado). As chaves vêm do plugin de key management do servidor
(`file_key_management`, `aws_key_management`, …) através de um callback; cada
arquivo guarda id e versão da chave e um IV aleatório de 128 bits, então a
rotação de chaves funciona (arquivos antigos seguem legíveis; novos usam a
versão mais recente). A chave precisa ter 256 bits. Ficam em claro: os headers
(contagens, intervalo de tempo, ids), o MANIFEST e o OPTIONS.

**Desenho** (`crypto.rs`). Cada *unidade* é selada separadamente e portanto lida
e verificada separadamente: um bloco de coluna do chunk, a seção
índice+Bloom do chunk, uma entrada do WAL. Unidade armazenada = texto cifrado +
tag de 16 bytes.

| | Chunk v3 | WAL v3 |
|---|---|---|
| nonce (128 bits) | IV do arquivo com a posição da unidade no arquivo (u64 BE, relativa ao início dos dados) XOR nos 8 bytes baixos | idem, posição = início da entrada |
| AAD | os 64 bytes do header do chunk (versão, flags, `chunk_id`, intervalo, contagens, `wal_seq`, fingerprint) + tipo da unidade (`0` bloco; `1` índice, com `bloom_offset`) | os 40 bytes do header do segmento (versão, flags, chave, IV) + número do segmento |
| `len`/`BlockRef.len` | `BlockRef.len` não conta a tag; `offset` conta | `len` da entrada = payload + 16 |

Nonce único: o IV é aleatório por arquivo (128 bits) e a posição é única dentro
do arquivo (blocos e índice nunca se sobrepõem; entradas do WAL nunca são
reescritas — segmento novo a cada abertura, falha de escrita invalida o
segmento). Como o header entra no AAD e o `chunk_id`/número do segmento também,
trocar blocos entre arquivos, reordenar blocos, mover entradas para outra
posição/segmento, editar o índice (que nomeia os blocos) ou o header falha a
verificação **antes** de qualquer decodificação.

**Resultado de uma falha**: bloco → `Error::Corrupt` (o `read_block` nunca
devolve dado); índice do chunk e entrada do WAL → `Error::Crypto`
(*fail-closed*: chave errada e adulteração não se distinguem, então não há
quarentena nem reescrita do MANIFEST). Dano visível sem a chave (CRC do header
ou do footer) segue o caminho de antes: quarentena do chunk.

**Compatibilidade**: chunk v2 e WAL v2 *com* a flag ENCRYPTED são o formato
antigo AES-256-CTR sem MAC; continuam legíveis (um aviso vai ao log na
abertura do chunk) mas **sem autenticação**. Toda escrita nova é v3.
`OPTIMIZE TABLE` reescreve chunks antigos como v3 (mesmo com um só chunk no
intervalo); segmentos de WAL antigos somem no próximo flush. Chunks e WAL em
claro continuam v2 e legíveis por binários antigos; um binário antigo diante
de arquivo v3 recusa explicitamente ("version 3 not supported").

**Limites do modelo de ameaça**: a autenticação é por unidade, não por tabela:
a detecção de troca, reordenação e truncamento vale **dentro de uma
unidade/arquivo** (blocos de um chunk, entradas de um segmento WAL). Quem
escreve no datadir ainda consegue (a) apagar um chunk ou um segmento WAL
inteiro, ou cortar o fim do último segmento (tratado como torn tail; o CRC é
recalculável sem a chave), (b) substituir um arquivo por uma cópia antiga
válida do mesmo `chunk_id`/segmento (rollback), (c) copiar um chunk GCM de
outra tabela com a mesma chave (o AAD não amarra a tabela), (d) alterar o MANIFEST
(CRC32, em claro) — por exemplo trocar a lista de chunks vivos ou os
tombstones. O `CHECK TABLE` continua sem chave e só confere CRC32. Salvaguardas:

* abertura **fail-closed**: falha de decriptação/autenticação do índice
  recusa a abertura sem quarentena e sem reescrever o MANIFEST; para descartar
  um chunk de fato corrompido, o operador o renomeia para `.tfl.corrupt`;
* tabela cifrada não aceita chunk nem segmento WAL (com entradas) em claro, e vice-versa — o replay compara a flag do segmento com a configuração da tabela antes de ler;
* sem chave disponível, criação, abertura e flush falham — não há fallback
  para texto claro.

## Formato em disco v1.0

Todos os números de versão estão em `moosedb-core/src/format.rs`. Regras
comuns a todos os arquivos:

1. **Escrita**: emite a *menor* versão que expressa o conteúdo. Uma tabela
   sem criptografia e sem `DELETE` continua gravando exatamente os formatos
   v0.x (chunk v2, WAL v2, MANIFEST v2); binários antigos seguem abrindo-a.
2. **Leitura**: cada leitor aceita só as versões da tabela abaixo. Qualquer
   outra (ou flag/combinação impossível, como chunk v3 sem `ENCRYPTED`) vira
   `Error::Corrupt("... version N not supported")` — recusa explícita, sem
   leitura parcial nem reinterpretação.
3. **Binário antigo, arquivo novo**: chunk v3, WAL v3 e MANIFEST v3 são
   recusados pela mensagem acima (já valia para o MANIFEST v3 dos tombstones).
4. **Binário novo, arquivo antigo**: lê tudo que um binário v0.x gravou. Os
   arquivos CTR antigos são lidos sem autenticação (aviso no log do chunk);
   a migração é preguiçosa e não precisa de passo de upgrade: o WAL antigo é
   consumido no flush, chunks antigos são reescritos por `OPTIMIZE TABLE`
   (ou por qualquer compactação que os junte).
5. **Rebaixar** uma tabela cifrada para um binário v0.x não é possível (os
   arquivos v3 são recusados); exporte os dados (`INSERT ... SELECT`).
6. Uma versão nova de formato exige: constante em `format.rs`, leitor que
   recusa as desconhecidas, **fixture binária nova** em
   `moosedb-tests/fixtures/` e entrada nesta tabela. As fixtures antigas nunca
   são regeradas.

| Arquivo | Magic | Versão | Conteúdo | Lido por |
|---|---|---|---|---|
| `chunk_*.tfl` | `TFLW` … `TFLE` | **2** | em claro; ou, com a flag `ENCRYPTED`, AES-256-CTR sem tags (legado, só leitura) | v0.x e v1.0 |
| | | **3** | sempre `ENCRYPTED`: blocos e índice+Bloom selados com AES-256-GCM + tag de 16 B | v1.0 |
| | | flag `ZONES` (v2 e v3) | bit 3 de `flags`: seção de zone maps no fim da unidade índice+Bloom (§Zone maps); sem versão nova, leitores anteriores a ignoram | v1.0 com zone maps; ignorada por anteriores |
| `wal_*.tfl.wal` | `TFWL` | **1** | linhas puras, tudo confirmado (só leitura) | v0.x e v1.0 |
| | | **2** | entradas `ROW`/`COMMIT`/`ROW_COMMIT`/`DELETE_*`; em claro ou, com `ENCRYPTED`, AES-256-CTR (legado) | v0.x e v1.0 |
| | | **3** | idem v2, sempre `ENCRYPTED`, entradas seladas com AES-256-GCM (`len` inclui a tag) | v1.0 |
| `MANIFEST` | `TFLM` | **1** | sem `replay_seq` (só leitura) | v0.x e v1.0 |
| | | **2** | `wal_seq`, `replay_seq`, `next_chunk_id`, chunks vivos, CRC32 | v0.x e v1.0 |
| | | **3** | + `born` por chunk, `tseq`, tombstones; gravado a partir do primeiro `DELETE` | v0.x com DELETE e v1.0 |
| `OPTIONS` | — | sem versão | `chave=valor` em texto; chaves desconhecidas são ignoradas | todos |
| `*.tmp`, `*.tfl.corrupt` | — | — | temporário de escrita atômica (apagado na abertura) / chunk em quarentena | — |

O MANIFEST **não** é cifrado nem autenticado (CRC32). O header de criptografia
(32 B: `key_id`, `key_version`, IV, reservado, CRC32) é igual em chunk e WAL,
v2 e v3; é a *versão do contêiner* que decide se há tags.

**Testes de compatibilidade** (`moosedb-tests/src/compat_tests.rs`, fixtures
em `moosedb-tests/fixtures/`, cada uma um diretório de tabela pequeno):

| Fixture | Gerada por | Cobre |
|---|---|---|
| `plain_v2` | código v0.x (HEAD `b37d555`) | chunk em claro v2, WAL v2, MANIFEST v2 |
| `ctr_v2` | idem | chunk v2 e WAL v2 AES-256-CTR (chave 7, versão 1); migração por `OPTIMIZE` |
| `wal_v1` | idem, WAL reescrito no layout v1 | WAL v1 |
| `deleted_v3` | idem | MANIFEST v3 com tombstone |
| `aead_v3` | v1.0 | chunk v3 e WAL v3 (trava o formato novo) |
| `zones_v2`, `zones_v3` | v1.0 + zone maps | chunk com a flag `ZONES`, em claro (v2) e selado (v3); `gen_fixture_zones` |

Cada teste copia a fixture, abre com o código atual e exige as mesmas linhas e
`CHECK TABLE` limpo. Os geradores são testes `#[ignore]`
(`gen_fixture_legacy`, `gen_fixture_aead`); só se rodam de propósito.

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
| 32 | AES-256-CTR sem MAC (§14 da spec) | AES-256-**GCM** (crate `aes-gcm` 0.10 da RustCrypto, mesma família de `aes`/`ctr`); arquivos CTR antigos continuam legíveis, sem autenticação | CTR não detecta adulteração (o CRC32 é linear e recomputável por quem escreve no datadir). GCM é um AEAD pronto, com AAD; CTR+HMAC exigiria duas crates novas (`hmac`+`sha2`) e composição manual. Formato novo: chunk v3 / WAL v3 (§Formato em disco v1.0). Resolve a limitação "criptografia sem integridade autenticada". |

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
colunas TAG (string ou inteira), VARCHAR ou INT comuns (ou **nenhum GROUP BY**,
ver §Agregação escalar); select list só com
essas colunas, constantes e `COUNT(*)`/`COUNT(col)`/`SUM`/`AVG` de inteiro (menos
`BIGINT UNSIGNED`, que não cabe nos agregados de 64 bits com sinal do core) e
`MIN`/`MAX` de inteiro ou do eixo de tempo; WHERE = conjunção de igualdades/IN
de TAG e limites do eixo (`capture_delete_cond`, o mesmo reconhecimento exato do
DELETE direto; constantes string contra o eixo `DATETIME` valem quando o
comparador é datetime). Os demais conjuntos do WHERE entram se cada um referencia
**uma única coluna comum** (VARCHAR/CHAR de qualquer collation ou INT/BIGINT,
inclusive `BIGINT UNSIGNED`; nunca TAG nem o eixo) só com constantes: `=`, `<=>`,
`<>`, `<`, `<=`, `>`, `>=`, `BETWEEN`, `IN`, `NOT IN`, `LIKE`/`NOT LIKE`,
`IS [NOT] NULL`, e `OR`/`NOT`/`AND` aninhados desses sobre a MESMA coluna (os
conjuntos de uma coluna formam o predicado dela). HAVING, ORDER BY (chave ou
agregado, ASC/DESC), LIMIT e OFFSET entram. Fora: DISTINCT, ROLLUP, window, WITH
TIES, SQL_CALC_FOUND_ROWS, subconsulta/derivada/view/CTE/UNION, rotinas,
expressões, `OR` entre colunas diferentes, conjunto com duas colunas, função da
coluna (`UPPER(op)='X'`), função não constante/não determinística, `Item_param`
não ligado, comparação string x número (o servidor converte e avisa linha a
linha), predicado em TAG que não seja `=`/`IN`, eixo `TIMESTAMP`.

**Predicado em coluna comum** (`init_scan`, `build_mask`): para cada coluna
filtrada `c` a engine roda a fase de dicionário no MESMO snapshot já aberto com a
faixa de tempo e as séries do WHERE (`Column(c)` como única chave, sem funções
nem filtros), de modo que o dicionário só tem os valores relevantes. Cada valor
distinto (NULL inclusive) é gravado no próprio `Field` de `c` do `record[0]` da
tabela (restaurado ao fim) e o `Item` do predicado do **próprio servidor** é
avaliado (`val_int`, NULL = falso, como no WHERE); o resultado vira a máscara
(`TFAggFilter`) da execução final. É isso que garante a exatidão, já que o
servidor não reavalia o WHERE sobre os grupos: collation, PAD SPACE, `LIKE`,
lógica de NULL e conversões são as do servidor, não uma reimplementação. A
máscara entra no orçamento `moosedb_agg_max_memory` (o dicionário, dentro do
core). Custo: uma avaliação de `Item` por valor DISTINTO (colunas com milhões de
valores distintos pagam isso; `=`/`IN` byte-exatos nem passam por aqui, ver atalhos abaixo). Limites do core (16 chaves, 64 funções, 16 filtros) e `moosedb_agg_max_memory` esgotado antes de uma execução: recusa no planejamento / erro de orçamento. O `parallel_sort` do pós-processamento não checa KILL (≈1 s por milhão de grupos string). Medição do
tempo (status cumulativos `Moosedb_gbh_dict_us`, `_agg_us`, `_dict_values`,
`_eval_us`, `_post_us` = pós-processamento em C++).

**Atalhos para o core.** (1) *Conjunto de valores*: um filtro cujo predicado é
exatamente `col = const` / `col IN (consts)` (sem NOT/OR/NULL na lista) vai ao core
como `TF_AGG_FILTER_VALUES`, sem fase de dicionário, quando a comparação é
byte a byte: inteiro com constantes inteiras (constante fora do range da coluna
é descartada; `BIGINT UNSIGNED` vai como padrão de bits) ou VARCHAR com collation
binária NO PAD (`utf8mb4_nopad_bin`) ou binária PAD SPACE (`utf8mb4_bin`, `latin1_bin`; só em utf8mb4/utf8mb3/latin1/ascii, com `pad_space=1`: espaços finais 0x20 ignorados dos dois lados), cuja
collation de comparação seja a da coluna e cujas constantes estejam no charset da
coluna. Qualquer outro caso fica no dicionário + máscara. (2) *Top-N*:
uma chave não-TAG, `ORDER BY` de um único agregado (COUNT/SUM/MIN/MAX, não AVG)
com `LIMIT` (ou `ORDER BY agregado, a própria chave ASC`: ordem SQL total, 2º termo = desempate do core), sem HAVING, chave inteira (não `BIGINT UNSIGNED`), string
binária NO PAD ou string **binária PAD SPACE** (`utf8mb4_bin`, `utf8mb3_bin`, `latin1_bin`, `ascii_bin`; ver abaixo); o desempate do core (chave crescente, na
ordem da collation) é o do handler, então os K grupos são os mesmos. Chave TAG nunca usa (o core
desempata por id de série, o handler pelo valor); `_ci`/`_ai` e demais collations
também não (exigiriam os pesos da collation no core; o handler continua agrupando
por `strnncollsp` e ordenando todos os grupos). Status `Moosedb_gbh_exact_filters`
e `Moosedb_gbh_topn`. O pós-processamento ordena em paralelo
(`moosedb_agg_threads`) e o merge por collation ordena pares (hash, grupo).
Testes: `gbh_shortcuts`, `gbh_pad_scalar`. Dicas de faixa para a poda por zone map (§Zone maps e Bloom): `gbh_prune`.

**Chave string binária PAD SPACE** (`TF_AGG_KEY_COLUMN_PAD` / `KeySpec::ColumnPad`).
Valores que diferem só por 0x20 finais são o MESMO grupo no servidor. Em vez de
fundir os grupos depois (`merge_groups`, que exige materializar todos), o core
agrupa pela forma sem espaços finais e guarda por grupo o menor comprimento bruto
visto (uma função `Min` oculta por chave PAD, fora do `AggResult`); a chave
devolvida é o aparado mais espaços até esse comprimento, isto é, o menor valor
do grupo por bytes (o mesmo que o merge do handler escolhia). Sem `need_merge`, o
top-N roda no core. O desempate é `pad_cmp` (= `strnncollsp`: bytes sem sinal até a
primeira diferença; se um é prefixo do outro, o resto do maior comparado com
espaços, então `'a' < 'a' < 'a	'`), que NÃO é a ordem dos bytes quando o resto
tem byte < 0x20. Só charsets com espaço de 1 byte e `mbminlen = 1` (`cs_pad_bin`).
Qual valor do grupo o servidor mostra (`'a'` ou `'a  '`) depende do plano dele; os
testes comparam sem o `Max_length` do metadata nesses casos (`$nomaxlen`).

**Agregação escalar (sem GROUP BY)**. O servidor chama `create_group_by` também
para consultas com agregado e sem `group_list` (`JOIN::make_aggr_tables_info`,
`Query.group_by == NULL`; confirmado no fonte do 11.4 e medido). `analyze()` aceita
quando há `Item_sum` na lista (`with_sum_func`) e trata como um único grupo
(chaves vazias; o core já devolve um grupo mesmo sem linhas). Semântica de tabela
vazia: se o core devolve 0 grupos (saída antecipada "nada casa": TAG inexistente,
conjunto exato vazio, máscara vazia) o handler sintetiza a linha (COUNT = 0,
SUM/MIN/MAX/AVG = NULL); a checagem "grupo com COUNT(*) = 0" não vale no escalar.
HAVING, LIMIT/OFFSET, ORDER BY seguem no servidor. Agregados que o servidor já
dobrou (`opt_sum_query`: `COUNT(*)` sem WHERE pelo nº de linhas da tabela, MIN/MAX
por índice) são `const_item()` e não têm campo na tabela temporária: viram `CONST`
no plano (sem isso o mapeamento de campos falha); se só sobrar isso, a consulta
fica com o servidor. Continuam fora: expressão sobre agregado (`COUNT(*)+1`),
`COUNT(DISTINCT)`, coluna fora de agregado.

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
`gbh_collation`, `gbh_scale`, `gbh_where`.

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
  NULL é valor próprio). Imagem da chave em bytes numa tabela própria
  (arena de chaves + índice de endereçamento aberto com `u32`), hash
  SipHash-1-3 com chave aleatória por execução (`RandomState`: o valor vem do
  cliente; o `FastMap` é só para ids internos), hash guardado por grupo (merge
  e crescimento nunca rehasheiam). Linhas consecutivas com a mesma chave
  reaproveitam a imagem, e as imagens distintas de um trecho são resolvidas em
  lotes de 256 (`Groups::warm` toca slots/hashes/chaves em laços independentes
  para sobrepor cache misses). Grupos sem nenhuma linha aceita (filtros) não
  aparecem no resultado (exceto o grupo único da agregação sem chave).
- **Funções**: `CountStar`, `Count(i)`, `Sum(i)` (Int64, acumulador `i128` com
  `checked_add`; estouro = erro `Full`), `Min/Max(i)` (Int64/Timestamp). Estado
  de 24 bytes por (grupo, função). `FuncCol` só preenche `sum_*` para `Sum` e
  `val` para `Min/Max`.
- **Filtros**: (1) máscara de dicionário: uma execução com a única chave
  `Column(c)`, sem funções nem filtros, grava o dicionário de `c` (valores na
  ordem do resultado); filtros seguintes passam `mask[i]` por posição do
  dicionário (vários = AND; máscara de tamanho errado ou sem dicionário =
  `InvalidArg`). (2) **Valores exatos** (`TFAggFilter.mode = TF_AGG_FILTER_VALUES`,
  `ValueFilter`): conjunto de inteiros ou de strings comparadas byte a byte,
  aplicado direto na agregação, sem fase de dicionário; NULL nunca é aceito;
  conjunto vazio rejeita tudo; com um só valor compara sem hash. O handler só
  pode usar quando a comparação SQL é exata (inteiros ou collation binária /
  NO PAD).
- **Top-N** (`TopN`, `TFAggSpec.top_*`): com uma única chave, devolve só os K
  melhores grupos por uma função (COUNT, SUM, MIN, MAX; ASC/DESC), melhor
  primeiro. NULL (grupo sem valores) é o menor valor; empate resolvido SEMPRE
  pela chave crescente (id da série, inteiro com sinal, ou bytes sem sinal
  lexicográfico, prefixo menor primeiro; chave NULL primeiro; `KeySpec::ColumnPad`:
  `pad_cmp`, ver acima). Seleção por
  partição (`select_nth_unstable`) e depois global; `limit = 0` devolve nenhum
  grupo. Só vale quando essa ordem é a que o SQL pede (chave inteira, bytes
  em collation binária NO PAD ou chave `KeySpec::ColumnPad`, cujos grupos já são os do servidor).
- **Paralelismo**: `std::thread::scope`; unidades de ~32 Ki linhas (entradas de
  chunk inteiras, no máximo 1024) e de 64 Ki linhas da MemTable via
  `AtomicUsize`; uma tabela de grupos por worker. Com mais de um worker o
  merge é **por partição de hash** (4 partições por thread, potência de 2, até
  256): cada worker agrupa seus grupos por partição ao terminar, as partições
  são reivindicadas pelos workers, cada uma é fundida (hash guardado, tabela
  pré-dimensionada) e já vira colunas de resultado (ou candidatos do top-N) na
  mesma thread; os pedaços são concatenados na thread chamadora. Menos de
  `DEFAULT_MIN_PARALLEL_ROWS` (128 Ki) linhas estimadas ou menos de 8192
  grupos: sem threads de fundo / merge na thread chamadora
  (`Agg::set_min_parallel_rows` muda o piso; o resultado não depende dele).
  Resultado idêntico para qualquer nº de threads. O callback de cancelamento
  só roda na thread chamadora (~10 ms); workers leem um `AtomicBool`. Pânico em
  worker é repropagado na thread chamadora (vira `TF_ERR_INTERNAL` na FFI).
  Cancelado = `Error::Cancelled`/`TF_ERR_CANCELLED`.
- **Memória**: um orçamento atômico compartilhado (chave + estados + overhead
  por grupo em cada tabela, cobrado em lotes de até 32 KiB por worker para não
  disputar o contador; conjuntos dos filtros, resultado, dicionário);
  estourar = `Error::Full("aggregation exceeds moosedb_agg_max_memory ...")`.
  As tabelas dos workers ficam vivas durante o merge, então uma execução
  paralela com muitos grupos chega a ~2x a memória dos grupos vistos pelos
  workers; a concatenação final cobra uma cópia a mais do resultado.
  Cada worker decodifica uma janela por vez, então a decodificação simultânea
  é limitada pelo nº de threads.
- **Cache de blocos** (`cache.rs`): o LRU de blocos/colunas/séries é dividido em
  16 shards (chave de uma série = mesmo shard) sob um orçamento global
  (`total` atômico): cada shard expulsa primeiro seus próprios mais antigos
  (LRU amostrado) e só depois os dos outros, um lock por vez; `used_bytes`
  é ≤ `moosedb_chunk_cache_size` em repouso (a soma é feita antes do trim, então
  com N inserções concorrentes pode passar transitoriamente em até N entradas;
  o trim começa por um shard rotativo). `touched_before` é lock-free. Antes
  a agregação de 70 mil séries piorava com mais threads por causa do mutex
  único.
- **Orçamento subestimado (limite conhecido)**: `GROUP_OVERHEAD` (80 B) não
  cobre a folga de capacidade dos `Vec` (até 2x na arena e nos vetores), o
  `grow()` do índice (slots velho + novo ao mesmo tempo, 4 B por slot) nem o
  `presize` das partições (reserva pelo teto soma-dos-workers, mesmo com
  muitos duplicados; não é cobrado). Somado ao ~2x das tabelas dos workers
  durante o merge, o pico real pode passar de `moosedb_agg_max_memory` por uma
  fração; dimensione o limite com folga.
- **Dicionário de varchar**: os blocos de varchar são `ENC_PLAIN` (sem
  dicionário nem RLE por bloco), então não há como agregar por códigos; o custo
  da fase de dicionário é o da própria agregação por chave `Column`.

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
  de séries consistente. O estado de unicidade (em `Inner`) tem teto próprio por
  tabela: `settings::unique_state_bytes`, padrão ¼ de `moosedb_batch_memory_budget`
  (256 MiB), sem sysvar própria. Custo medido: ~146 B de RSS por série (era ~224 antes
  de encaixotar o `Top::Many`; cobrança de 150 B/série + 32 B por extra além do
  primeiro; conjunto de extras no mesmo `ts` limitado a 1024, acima vira
  "desconhecido" e cada linha naquele instante faz lookup). Ao estourar, o quarto mais
  frio das séries (menor `max_ts`) é descartado e o maior `max_ts` descartado vira um
  piso (`evicted`): série sem entrada só é aceita sem lookup acima do piso, e entradas
  recriadas abaixo dele herdam o piso como limite e nascem incompletas — a unicidade
  continua exata (testado com modelo, restart e WAL), só a checagem de linhas
  antigas/recentes das séries descartadas fica mais lenta (lookup nos chunks). Com
  orçamento menor que o conjunto quente, o piso sobe até "agora" e toda linha vira
  lookup (correto, lento). Memória do `SeriesIndex` (não limitável: é o catálogo de
  séries): ~270 B/série com TAG de 20 bytes (`TableStats::series_bytes`, estimativa;
  `unique_bytes` mostra o estado de unicidade); `moosedb_series_list` já é um
  snapshot compartilhado (O(1), sem cópia), a cópia ordenada `Table::series()` custa
  ~96 B/série e não é usada pela FFI. Ainda não exposto no INFORMATION_SCHEMA
  (precisa de campo FFI + handler). O desempate por `ns` em varredura ordenada de tabela
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
* Autenticação da criptografia é por unidade (bloco/entrada), não da tabela: apagar ou truncar arquivos, restaurar cópia antiga válida e alterar o MANIFEST não são detectados (ver §Criptografia).
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
