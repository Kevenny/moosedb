# MooseDB Engine — Contexto Operacional (Claude Code)

> Harness de agents/skills para trabalhar no MooseDB Engine: storage engine
> time-series para MariaDB 11.4+, handler em **C++20**, núcleo de storage em
> **Rust** (`#![forbid(unsafe_code)]`) exposto via **C ABI**.

## Fontes de verdade

| Arquivo | Conteúdo |
|---|---|
| `moosedb_engine_spec.md` | Especificação de produto. Não editar levianamente. |
| `docs/architecture.md` | O que está **implementado de fato** + tabela de divergências da spec (com justificativa). Em caso de conflito com a spec, este arquivo vence. |
| `docs/user-guide.md` | Guia do usuário final (SQL, opções de tabela, procedures). |

Leia `docs/architecture.md` antes de tocar em qualquer módulo — ele tem o mapa
de camadas, o layout em disco, o protocolo de durabilidade e a tabela de
divergências. Não repita conteúdo dele aqui; ele é a referência.

## Arquitetura (resumo)

```
mysqld ──► ha_moosedb.so (C++20, Handler API)
             └── libmoosedb.a (Rust, linkada estaticamente)
                   ├── moosedb-ffi   único crate com `unsafe` — só marshalling + catch_unwind
                   └── moosedb-core  #![forbid(unsafe_code)] — toda a lógica de storage
```

## Princípio inegociável — "delegate large reads/runs"

**O agente principal nunca lê diretamente** saída bruta de:

- `docker build` / `cmake --build` (configuração do servidor MariaDB é verbosa)
- `cargo test --workspace` / `cargo clippy --all-targets`
- a suíte MTR (`run-mtr.sh`, todos os `mysql-test/suite/moosedb/t/*.test`;
  diffs `.reject` podem ser longos)
- benchmarks (encode/decode, compressão, scan)
- testes de crash-recovery (`kill -9` + replay de WAL)

Para qualquer um desses, **delegue ao agent `moosedb-build-runner`** (builds e
testes funcionais) ou **`perf-benchmark`** (medições de performance). Eles
rodam no contexto isolado deles e devolvem só um resumo estruturado
(pass/fail, erros, métricas). Isso protege a janela de contexto do agente
principal — essencial porque logs de build do MariaDB (`cmake_release`) e da
suíte MTR facilmente passam de milhares de linhas.

**Exceção**: ler um único arquivo fonte pequeno (`.rs`, `.cc`, `.h`) para
editar é direto, sem delegação.

## Mapeamento sintoma/pedido → quem executa

| Pedido do usuário | Skill | Agent |
|---|---|---|
| "builda o plugin" / "compila" | `moosedb-build` | `moosedb-build-runner` |
| "roda os testes Rust" / "roda o clippy" | `moosedb-rust-check` | `moosedb-build-runner` |
| "roda a suíte MTR" | `moosedb-mtr` | `moosedb-build-runner` |
| "testa crash recovery" / "kill -9" | `moosedb-crash-recovery` | `moosedb-build-runner` |
| Mudar `moosedb-core` / `moosedb-ffi` | — | `rust-core-engineer` |
| Mudar `ha_moosedb.cc/.h`, `CMakeLists.txt` | — | `cpp-handler-engineer` |
| "está lento" / comparar codecs / taxa de compressão / latência de scan | `moosedb-bench` | `perf-benchmark` |
| Antes de comitar mudança em core/handler | `moosedb-safety-review` | `storage-code-reviewer` |
| Código divergiu da spec | `moosedb-doc-sync` | agente principal |

## Regras de segurança obrigatórias (não negociáveis)

### Rust

- `#![forbid(unsafe_code)]` em **todos** os crates exceto `moosedb-ffi`.
- `moosedb-ffi` contém **apenas** `extern "C"` de entrada: valida ponteiros,
  roda sob `catch_unwind`, registra erro em `moosedb_last_error()`. **Zero**
  lógica de negócio ali — isso vive em `moosedb-core`.
- `panic = "unwind"` no profile release é deliberado: um panic no core deve
  virar `TF_ERR_INTERNAL` na fronteira FFI, nunca derrubar o `mysqld`. Nunca
  trocar para `panic = "abort"`.
- `moosedb_ffi.h` é gerado por `cbindgen` e **versionado no git**. Nunca
  editar manualmente — regenerar e `cbindgen --verify`.

### C++

- Proibido ponteiro bruto (`new`/`delete`) — só smart pointers.
- Todo acesso a buffer/array é bounds-checked antes do acesso.
- Todo ponteiro recebido da API do MariaDB é verificado contra `nullptr`
  antes de uso.

Qualquer diff que viole essas regras deve ser bloqueado — ver skill
`moosedb-safety-review` / agent `storage-code-reviewer`.

**Enforcement automático**: o hook versionado `.githooks/pre-commit` bloqueia
`unsafe` fora do FFI, perda de `forbid(unsafe_code)`, `panic = "abort"`,
plugin fora de `MODULE_ONLY` e `new`/`delete` crus no handler; com Docker,
roda também clippy e `cbindgen --verify`. Ative uma vez por clone:
`git config core.hooksPath .githooks`. O CMake falha se não houver
`cbindgen` (header não verificável). O hook não substitui a revisão — ele
não cobre durabilidade, concorrência, bounds-check nem nullptr.

### Protocolo de durabilidade (não violar silenciosamente)

`.tmp` → fsync → rename → **troca atômica do MANIFEST** (ponto de commit) →
chunk obsoleto só é apagado quando o último snapshot que o referencia é
liberado. Qualquer mudança em `wal.rs`, `manifest.rs`, `chunk_writer.rs`,
`compaction.rs` ou `table.rs` deve preservar esse protocolo — ver
`docs/architecture.md` §Protocolo de durabilidade. Se não tiver certeza,
rode `moosedb-safety-review` antes de concluir.

## Como apresentar resultados de performance

- **Sempre quantificar**: MB/s, registros/s, taxa de compressão, ns/op,
  % do tempo. Nunca "rápido"/"lento" sem número.
- Toda recomendação tem **esforço** (baixo/médio/alto) e **risco**
  (baixo/médio/alto).
- Ranquear findings por impacto medido, não por curiosidade técnica.
- Se não tem medição, diga **"não medido"** — não invente números.

## Cautela em build/CI

- Nunca compilar a engine como `DEFAULT` (vira parte do servidor, viola a
  decisão registrada em `docs/architecture.md` — a spec exige
  `INSTALL SONAME`). Mantenha `STORAGE_ENGINE MODULE_ONLY`.
- Nunca remover `catch_unwind` da fronteira FFI.
- Nunca commitar mudança em `storage/moosedb/rust/**` ou
  `storage/moosedb/*.cc/.h` sem rodar ao menos `moosedb-rust-check`.
- O plugin se declara `EXPERIMENTAL` (`plugin_maturity=experimental`) — isso
  é intencional em v0.x, não "corrigir" sem o usuário pedir.

## Quando despachar agents em paralelo

Se a tarefa precisa de **dois ou mais** agents (ex.: mudar `moosedb-core` E
validar com MTR, ou revisar segurança E medir performance), despache em
paralelo numa única mensagem com múltiplas tool calls. Série desperdiça
tempo.

## graphify

This project has a knowledge graph at graphify-out/ with god nodes, community structure, and cross-file relationships.

Rules:
- When graphify-out/graph.json exists, run `graphify query "<question>"` before your first grep, search or source-file read for a codebase question, and put this rule in every subagent prompt that explores code. Read raw files only for lines the graph or an earlier read already located. Use `graphify path "<A>" "<B>"` for relationships and `graphify explain "<concept>"` for focused concepts. These return a scoped subgraph, usually much smaller than GRAPH_REPORT.md or raw grep output.
- If graphify-out/wiki/index.md exists, use it for broad navigation instead of raw source browsing.
- Read graphify-out/GRAPH_REPORT.md only for broad architecture review or when query/path/explain do not surface enough context.
- After modifying code, run `graphify update .` to keep the graph current (AST-only, no API cost).
- A committed graphify-out/graph.json is rewritten whole on every build, so a large diff to it in a pull request is expected. Do not flag it as churn or suggest skipping the graph rebuild or its hooks.
