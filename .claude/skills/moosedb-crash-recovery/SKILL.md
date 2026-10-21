---
name: moosedb-crash-recovery
description: Valida o protocolo de durabilidade do MooseDB — kill -9 do servidor seguido de replay do WAL e verificação do MANIFEST, e os testes de fault-injection do crate moosedb-tests. Use quando o usuário pedir para "testar crash recovery"/"kill -9", ou após mudanças em wal.rs, manifest.rs, chunk_writer.rs, compaction.rs, table.rs.
---

# Validação de crash recovery

## Quando usar

- Depois de qualquer mudança em `wal.rs`, `manifest.rs`, `chunk_writer.rs`,
  `compaction.rs` ou `table.rs`.
- Quando o usuário pedir explicitamente para testar recuperação de crash.

## Duas camadas de verificação

### 1. Fault-injection em nível de unidade (rápido, sem Docker completo)

`moosedb-tests/src/lib.rs` simula cenários de crash **sem** matar um
processo: dropa um `Table` sem shutdown limpo e adultera os arquivos
exatamente como um processo interrompido deixaria (chunk sem seal, WAL sem
checkpoint, MANIFEST apontando para arquivo ausente). Roda como parte de
`cargo test --workspace` — ver skill `moosedb-rust-check`.

### 2. Crash real via MTR (`kill -9` de verdade)

`mysql-test/suite/moosedb/t/crash_recovery.test` (spec §12): insere linhas,
sela parte em chunk via `FLUSH TABLES`, deixa o resto só no WAL, mata o
`mysqld` com `kill -9` (`include/kill_mysqld.inc`), reinicia
(`include/start_mysqld.inc`), e verifica que **toda linha confirmada antes
do kill sobreviveu** — inclusive um segundo ciclo de kill/restart depois da
recuperação, para garantir que a tabela recuperada continua gravável e
corrompível corretamente.

```bash
docker run --rm -v "$PWD:/work" moosedb-test bash /work/docs/docker/run-mtr.sh --do-test=crash_recovery
```

Requer `build/ha_moosedb.so` atualizado (rode `moosedb-build` antes se o
código mudou).

## Contrato de saída

**Delegue execução e leitura ao agent `moosedb-build-runner`.** Ele reporta
o status de cada etapa do protocolo (não só pass/fail do teste MTR):

```json
{
  "fault_injection_unitario": "ok|falhou",
  "kill_e_restart": "ok|falhou",
  "linhas_confirmadas_sobreviveram": "ok|perdeu N linhas",
  "manifest_consistente": "ok|inconsistente",
  "chunks_fora_do_manifest_removidos": "ok|sobrou lixo",
  "writes_continuam_apos_recovery": "ok|falhou"
}
```

Qualquer item diferente de "ok" é um **bloqueador** — não é uma falha de
teste cosmética, é perda de dado ou corrupção de estado.

## Quem executa

Agente principal (ou `rust-core-engineer` ao terminar uma mudança nos
módulos listados) despacha; **`moosedb-build-runner`** executa e lê.
