---
name: moosedb-mtr
description: Roda a suíte MTR (todos os testes de mysql-test/suite/moosedb) do MooseDB contra o servidor MariaDB oficial via Docker. Use para validar DDL, tipos, chunk queries, manutenção e recuperação de crash end-to-end.
---

# Suíte MTR — testes funcionais end-to-end

## Quando usar

- Depois de `moosedb-build` ter gerado `build/ha_moosedb.so` com sucesso.
- Antes de considerar qualquer mudança em `ha_moosedb.cc/.h` ou em
  `moosedb-core` (comportamento observável) pronta.
- Quando o usuário pedir "roda a suíte MTR" / "testa DDL" / "testa tipos".

Testes disponíveis: `ls mysql-test/suite/moosedb/t/*.test` (não fixe a
contagem aqui — ela cresce). Grupos úteis para iterar: atomicidade
(`statement_atomicity`, `statement_crash`, `statement_errors`), crash
(`crash_recovery`, `restart`, `restart_encrypted`), segurança (`security`,
`udf_privilege`), concorrência (`concurrency`).

## Pré-requisito

Requer `build/ha_moosedb.so` já compilado — rode `moosedb-build` primeiro
se ainda não existir ou se o código mudou desde o último build.

## Como executar

Suíte completa:
```bash
docker run --rm -v "$PWD:/work" moosedb-test bash /work/docs/docker/run-mtr.sh
```

Um teste específico (mais rápido ao iterar):
```bash
docker run --rm -v "$PWD:/work" moosedb-test bash /work/docs/docker/run-mtr.sh ddl statement_atomicity
```

## Contrato de saída

**Delegue execução e leitura ao agent `moosedb-build-runner`.** Ele devolve:

```json
{
  "total": 0, "passed": 0, "failed": 0,
  "falhas": [{"teste": "ddl", "diff_resumido": "..."}]
}
```

Para cada falha, o `diff_resumido` deve ser só o trecho divergente entre
`.result` e `.reject` — nunca o arquivo `.result` inteiro.

## Quem executa

Agente principal (ou `cpp-handler-engineer`/`rust-core-engineer` ao terminar
uma mudança) despacha; **`moosedb-build-runner`** executa e lê.
