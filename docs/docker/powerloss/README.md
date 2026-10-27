# Queda de energia simulada (LazyFS)

Harness que mede se a MooseDB perde dado confirmado numa queda de energia.
[LazyFS](https://github.com/dsrhaslab/lazyfs) é um sistema de arquivos FUSE que
guarda toda escrita no próprio cache até o `fsync`. Ele pode descartar o que não
teve `fsync` quando recebe um comando (`lazyfs::clear-cache`), ou derrubar a si
mesmo numa syscall escolhida (`lazyfs::crash::...`). O datadir inteiro do
MariaDB fica sobre o ponto de montagem LazyFS.

| Arquivo | Papel |
|---|---|
| `Dockerfile` | `mariadb:11.4` + LazyFS (commit fixado) + `python3-pymysql` |
| `run.sh` | lado do host: builda a imagem se faltar e roda o container |
| `powerloss.py` | dentro do container: ciclos, carga, queda, verificação |

## Rodar

```bash
# 1. plugin (volume de build próprio; no Git Bash use MSYS_NO_PATHCONV=1)
MSYS_NO_PATHCONV=1 docker run --rm -v "$PWD:/work" -v moosedb-build-pl:/build \
  -v moosedb-cargo:/usr/local/cargo/registry -w /work moosedb-dev bash docs/docker/build-plugin.sh
# 2. N ciclos com uma seed (saída FORA do repositório)
docs/docker/powerloss/run.sh -o /c/TEMP/powerloss -n 30 -s 1 -c 4
#    opções: --sync-mode write (controle: deve perder), --phases insert,flush,...
```

O FUSE exige `--device /dev/fuse --cap-add SYS_ADMIN --security-opt apparmor:unconfined`
(é o padrão do `run.sh`). Com `PL_PRIVILEGED=1` ele usa `--privileged`.
O `.so` é copiado de `/repo/build` para o diretório de plugins quando o container
sobe, antes de o `mariadbd` iniciar. Ele nunca é montado direto num servidor
em execução.

## O que um ciclo faz

1. monta o LazyFS (o diretório de apoio `/pl/root` faz o papel do disco) e sobe o `mariadbd`;
2. verifica a queda anterior (ver abaixo) e faz um INSERT de sonda por tabela
   (uma tabela que ficou read-only aparece aqui);
3. carga aleatória de 4 clientes em `t_plain`, `t_enc` (`ENCRYPTION='YES'`,
   `file_key_management` com `mysql-test/suite/moosedb/std_data/keys.txt`,
   MANIFEST v4) e `t_pk` (`PRIMARY KEY (itemid TAG, clock)`). A carga mistura:
   INSERT de 1 linha, multi-row (2–60) e grande (3–8 mil linhas); `DELETE`
   direto (TAG + faixa de tempo) e linha a linha (`MOD(id,k)`); `FLUSH TABLES`;
   `OPTIMIZE TABLE`; `CALL moosedb_compact(...)`; `CHECK TABLE`; `SELECT`. Cada
   statement é registrado em `ops.jsonl` (fora do datadir) antes do envio e de
   novo no ack. Cada linha tem `id` único, `stmt` e conteúdo derivado do `id`;
4. a queda, mirando uma **fase** (rodízio embaralhado pela seed):
   * fases de statement (`insert`, `insert_multi`, `insert_big`, `delete_direct`,
     `delete_rows`, `flush`, `optimize`, `compact`, `random`): um atraso aleatório
     depois do envio de um statement daquele tipo, `SIGSTOP` no mysqld, depois
     `lazyfs::clear-cache`, depois `kill -9`, depois `clear-cache` de novo. O disco
     fica exatamente com o que teve `fsync` até o instante da queda;
   * fases de syscall (`manifest_rename`, `chunk_fsync`, `chunk_unlink`,
     `wal_fsync`, com timing `before`/`after`): o LazyFS cai sozinho na syscall
     (o cache, ou seja todo dado sem `fsync`, morre com ele), depois `kill -9`.
     Assim a queda cai exatamente na troca do MANIFEST, no selo de um chunk, na
     remoção de chunk compactado e no `fsync` do WAL;
5. desmonta (como um reboot) e guarda uma cópia do disco (`/pl/snap_cur`).

## Verificação (após cada reinício)

* toda tabela abre: `SELECT` completo, `COUNT(*)` igual ao número de linhas e `CHECK TABLE` = OK.
  Se `t_enc` não abre, o erro sai marcado como GRAVE;
* todo INSERT confirmado está presente, uma vez só e com o conteúdo certo;
* todo DELETE confirmado continua aplicado, sem linha ressuscitada;
* nenhuma linha nunca enviada aparece;
* statement em voo é tudo ou nada. Um statement que *retornou erro* antes da
  queda (ex.: `ER_LOCK_DEADLOCK` em `t_pk`) pode ficar parcial, porque é o
  contrato documentado;
* nenhum `.tmp` sobra depois da abertura (só conta se persistir por 3 s) e
  nenhum `.tfl.corrupt` aparece;
* linhas `[ERROR]`/MooseDB do log do servidor são classificadas, e `[ERROR]` da MooseDB conta como falha.

A verdade de cada tabela, ou seja o conjunto de ids vivos, passa a ser o que foi lido.
Statements em voo ficam resolvidos e uma violação não se propaga para os ciclos seguintes.

## Saída

`<out>/seed<S>/`: `results.jsonl` (um registro por ciclo), `summary.json`,
`ops.jsonl`. Num ciclo com falha, `fail/cycleNNN/` guarda:

* `datadir_before_recovery/`: o disco logo após a queda;
* `moose_after_recovery/`;
* `mysqld.err`;
* `ops.jsonl`;
* `failure.json`.

Para reproduzir, suba um `mariadbd` com o `my.cnf` do harness sobre uma cópia
de `datadir_before_recovery` (não precisa de LazyFS). A seed fixa as fases e a
carga de cada cliente. O instante exato da queda depende de timing, então a
reprodução fiel é pelo snapshot.

## O que não cobre

* metadados: o LazyFS aplica `create`/`rename`/`unlink` direto no disco. Um
  `rename` sem `fsync` do diretório nunca se perde aqui, mas pode se perder num
  ext4/xfs real;
* escrita rasgada dentro de uma página/setor (`torn-op`/`torn-seq` do LazyFS não são usados);
* falha de `fsync` com o processo vivo (EIO): coberto pelos testes Rust de injeção;
* replicação, binlog, `mariadb-backup`, `ALTER TABLE` e retenção (as tabelas não têm `RETENTION_PERIOD`) durante a queda.
