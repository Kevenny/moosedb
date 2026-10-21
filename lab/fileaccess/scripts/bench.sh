#!/usr/bin/env bash
# Benchmark InnoDB x MooseDB x ClickHouse x ColumnStore (monitoramento de acesso a arquivos).
# Roda NO HOST (Git Bash/Linux), chamando `docker exec` nos contêineres do compose.
#
#   DATA=20m bash scripts/bench.sh | tee results/bench_20m.txt
#
# Variáveis:
#   DATA     subpasta de data/ (e do volume fileacc-data) com events_*.tsv e params.env   (20m)
#   STAGE    load | query | explain | all                                                  (all)
#   ENGINES  innodb innodb_f moose moose_h moose_hn ch cs   (tabelas evento_innodb[_f], evento_moose[_h], fileacc.evento,
#            evento_cs no contêiner fileacc-columnstore; cs não entra no padrão)
#   RUNS     execuções quentes por consulta (melhor de)                                    (5)
#   COLD     0 = não reinicia; 1 = reinicia o contêiner antes das consultas (BP/cache do
#            servidor frios, 1ª execução ~ "fria"); 2 = idem + drop_caches do page cache   (1)
#   GBH      1 = SET moosedb_group_by_pushdown=ON antes de cada consulta nas tabelas MooseDB   (0)
#   AGG_THREADS  se definido, SET GLOBAL moosedb_agg_threads=N antes das consultas MooseDB
# Tempos: servidor (MariaDB: NOW(6) antes/depois; ClickHouse: clickhouse-client --time).
set -uo pipefail
export MSYS_NO_PATHCONV=1
cd "$(dirname "$0")/.."
DATA=${DATA:-20m}; STAGE=${STAGE:-all}; RUNS=${RUNS:-5}; COLD=${COLD:-1}; GBH=${GBH:-0}; AGG_THREADS=${AGG_THREADS:-}
TAG="gbh=$GBH${AGG_THREADS:+ thr=$AGG_THREADS}"
ENGINES=${ENGINES:-"innodb innodb_f moose moose_h ch"}
PW="${ROOT_PASSWORD:-fileacc}"
MC=fileacc-mariadb; CC=fileacc-clickhouse; XC=fileacc-columnstore
MCON=$MC   # contêiner MariaDB alvo de M(): $XC para a engine cs
M() { docker exec -i $MCON mariadb -uroot -p"$PW" -N -B "$@"; }
C() { docker exec -i $CC clickhouse-client --max_threads 8 "$@"; }
RS="results/rs_$DATA"; mkdir -p "$RS"
set -a; . <(grep -v '^#' "data/$DATA/params.env"); set +a
table_of() { case $1 in innodb) echo evento_innodb;; innodb_f) echo evento_innodb_f;; moose) echo evento_moose;; moose_h) echo evento_moose_h;; moose_hn) echo evento_moose_hn;; ch) echo fileacc.evento;; cs) echo evento_cs;; esac; }
render() {   # render <arquivo.sql> <tabela>
  sed -e "s|{{T}}|$2|g" -e "s|{{DAY0}}|$DAY0|g" -e "s|{{DAY1}}|$DAY1|g" -e "s|{{HOUR0}}|$HOUR0|g" \
      -e "s|{{HOUR1}}|$HOUR1|g" -e "s|{{HOST0}}|$HOST0|g" -e "s|{{FILE0}}|$FILE0|g" "$1"
}
now() { date +%s.%N; }
mb() { awk -v b="$1" 'BEGIN{printf "%.0f", b/1048576}'; }
wait_healthy() { until [ "$(docker inspect -f '{{.State.Health.Status}}' $1)" = healthy ]; do sleep 2; done; }
pre() { [ "$GBH" = 1 ] && case "$1" in evento_moose*) echo "SET SESSION moosedb_group_by_pushdown=ON;";; esac; }
mtime() {   # tempo do statement no servidor (s): mtime "<sql>" [pre]
  local o; o=$(M fileacc -e "$2 SET @t0=NOW(6); $1; SELECT TIMESTAMPDIFF(MICROSECOND,@t0,NOW(6))/1e6" 2>&1) || { echo "ERRO: $(echo "$o" | tail -1)"; return; }
  echo "$o" | tail -1
}
dsize() { docker exec $MCON du -sb "$1" | cut -f1; }
oneline() { render "$1" "$2" | grep -v '^--' | tr '\n' ' ' | sed 's/;[[:space:]]*$//'; }

echo "# fileaccess: InnoDB x MooseDB x ClickHouse | DATA=$DATA ROWS=$ROWS | $(date -u +%FT%TZ)"
echo "# janela dia=[$DAY0,$DAY1) hora=[$HOUR0,$HOUR1) host=$HOST0 arquivo=$FILE0"
echo " $ENGINES " | grep -q ' cs ' && echo "# ColumnStore $(MCON=$XC M -e "SELECT PLUGIN_AUTH_VERSION FROM information_schema.PLUGINS WHERE PLUGIN_NAME='Columnstore'") (contêiner $XC)"
echo "# MariaDB $(M -e 'SELECT VERSION()') | innodb_buffer_pool=$(mb $(M -e 'SELECT @@innodb_buffer_pool_size')) MiB moosedb_chunk_cache=$(mb $(M -e 'SELECT @@moosedb_chunk_cache_size')) MiB | ClickHouse $(C -q 'SELECT version()')"

# ------------------------------------------------------------------ carga
if [ "$STAGE" = load ] || [ "$STAGE" = all ]; then
  echo; echo "## Carga ($ROWS linhas em partes de 1 M, 1 statement/parte)"
  if echo " $ENGINES " | grep -q -e ' innodb ' -e ' innodb_f ' -e ' moose ' -e ' moose_h '; then
    M -e "DROP DATABASE IF EXISTS fileacc; CREATE DATABASE fileacc"
    M fileacc -e "SOURCE /lab/sql/schema.sql"
    M fileacc -e "SOURCE /moosedb-sql/moosedb_install.sql"
    M -e "SET GLOBAL moosedb_retention_check_interval=0"
  fi
  for e in innodb innodb_f moose moose_h; do
    echo " $ENGINES " | grep -q " $e " || continue
    t=$(table_of $e)
    { echo "SET unique_checks=0, foreign_key_checks=0; SET @t0=NOW(6);"
      for f in $(docker exec $MC sh -c "ls /lab/data/$DATA/events_*.tsv"); do
        echo "LOAD DATA INFILE '$f' INTO TABLE $t (ts,host,process,user_name,folder,file,op,bytes,latency_us);"
      done
      echo "SELECT TIMESTAMPDIFF(MICROSECOND,@t0,NOW(6))/1e6;"; } > "$RS/load_$e.sql"
    s=$(M fileacc < "$RS/load_$e.sql" 2>&1 | tail -1)
    echo "load	$e	${s}s	$(awk -v r=$ROWS -v s="$s" 'BEGIN{printf "%.0f linhas/s", r/s}')"
    case $e in
      innodb*) M fileacc -e "ANALYZE TABLE $t" >/dev/null
        echo "disco	$e	ibd=$(mb $(dsize /var/lib/mysql/fileacc/$t.ibd)) MiB	$(M -e "SELECT CONCAT('dados=',ROUND(DATA_LENGTH/1048576),' MiB indices=',ROUND(INDEX_LENGTH/1048576),' MiB') FROM information_schema.TABLES WHERE TABLE_SCHEMA='fileacc' AND TABLE_NAME='$t'")";;
      *) echo "flush	$e	$(mtime "FLUSH TABLES $t")s"
        echo "disco_pos_flush	$e	$(mb $(dsize /var/lib/mysql/fileacc/$t)) MiB"
        echo "compact	$e	$(mtime "CALL moosedb_compact('$t', NULL, NULL)")s"
        echo "disco_pos_compact	$e	$(mb $(dsize /var/lib/mysql/fileacc/$t)) MiB	$(M -e "SELECT CONCAT(COUNT(*),' chunks, ',SUM(\`ROWS\`),' linhas, ',ROUND(SUM(COMPRESSED_MB)),' MiB comprimidos, series/chunk~',ROUND(AVG(SERIES))) FROM information_schema.MOOSEDB_CHUNKS WHERE TABLE_SCHEMA='fileacc' AND TABLE_NAME='$t'")";;
    esac
    echo "conferencia	$e	$(M fileacc -e "SELECT CONCAT(COUNT(*),' linhas sum(bytes)=',SUM(bytes),' sum(lat)=',SUM(latency_us)) FROM $t")"
  done
  if echo " $ENGINES " | grep -q ' cs '; then
    MCON=$XC
    M -e "DROP DATABASE IF EXISTS fileacc; CREATE DATABASE fileacc"; M fileacc < cs/schema.sql
    d0=$(dsize /var/lib/columnstore/data1)
    { echo "SET @t0=NOW(6);"
      for f in $(docker exec $XC sh -c "ls /lab/data/$DATA/events_*.tsv"); do
        echo "LOAD DATA INFILE '$f' INTO TABLE evento_cs (ts,host,process,user_name,folder,file,op,bytes,latency_us);"
      done
      echo "SELECT TIMESTAMPDIFF(MICROSECOND,@t0,NOW(6))/1e6;"; } > "$RS/load_cs.sql"
    s=$(M fileacc < "$RS/load_cs.sql" 2>&1 | tail -1)
    echo "load	cs	${s}s	$(awk -v r=$ROWS -v s="$s" 'BEGIN{printf "%.0f linhas/s", r/s}')"
    echo "disco	cs	$(mb $(( $(dsize /var/lib/columnstore/data1) - d0 ))) MiB (data1, descontado o catálogo vazio)"
    echo "conferencia	cs	$(M fileacc -e "SELECT CONCAT(COUNT(*),' linhas sum(bytes)=',SUM(bytes),' sum(lat)=',SUM(latency_us)) FROM evento_cs")"
    MCON=$MC
  fi
  if echo " $ENGINES " | grep -q ' ch '; then
    C -q "DROP DATABASE IF EXISTS fileacc"; C -n < ch/schema.sql
    t0=$(now)
    docker exec $CC sh -c "cat /lab/data/$DATA/events_*.tsv | clickhouse-client --max_threads 8 -q \"INSERT INTO fileacc.evento FORMAT TSV\""
    s=$(awk -v a=$t0 -v b=$(now) 'BEGIN{printf "%.2f", b-a}')
    echo "load	ch	${s}s	$(awk -v r=$ROWS -v s="$s" 'BEGIN{printf "%.0f linhas/s", r/s}')"
    echo "disco_pos_carga	ch	$(C -q "$(cat ch/size.sql)")"
    t0=$(now); C -q "OPTIMIZE TABLE fileacc.evento FINAL"; s=$(awk -v a=$t0 -v b=$(now) 'BEGIN{printf "%.2f", b-a}')
    echo "optimize_final	ch	${s}s"
    echo "disco_pos_optimize	ch	$(C -q "$(cat ch/size.sql)")"
    echo "colunas	ch	$(C -q "$(cat ch/columns.sql)")"
    echo "conferencia	ch	$(C -q "$(cat ch/check.sql)")"
  fi
fi

restart_cold() {   # restart_cold <container>
  [ "$COLD" = 0 ] && return 0
  docker restart "$1" >/dev/null; wait_healthy "$1"
  [ "$COLD" = 2 ] && docker run --rm --privileged alpine sh -c "sync; echo 3 > /proc/sys/vm/drop_caches" >/dev/null 2>&1
  return 0
}
run_once() {   # run_once <engine> <arquivo.sql>  -> segundos
  local e=$1 f=$2 t o; t=$(table_of $e)
  if [ "$e" = ch ]; then
    o=$(render "$f" "$t" | C --format Null --time 2>&1)
    echo "$o" | tail -1 | grep -E '^[0-9.]+$' || echo "ERRO: $(echo "$o" | tail -1)"
  else
    mtime "$(oneline "$f" "$t")" "$(pre $t)"
  fi
}

# ------------------------------------------------------------------ consultas
if [ "$STAGE" = query ] || [ "$STAGE" = all ]; then
  echo; echo "## Consultas (s): melhor de $RUNS quentes | primeira execução (COLD=$COLD)"
  for e in $ENGINES; do
    MCON=$MC; [ $e = cs ] && MCON=$XC
    if [ "$e" = ch ]; then restart_cold $CC; else restart_cold $MCON; fi
    d=sql; [ $e = ch ] && d=ch
    [ -n "$AGG_THREADS" ] && case $e in moose*) M -e "SET GLOBAL moosedb_agg_threads=$AGG_THREADS";; esac
    for f in sql/q*.sql; do
      q=$(basename $f .sql)
      first=$(run_once $e $d/$q.sql); best=""
      case "$first" in ERRO*) echo "q	$e	$q	$first"; continue;; esac
      for ((i = 0; i < RUNS; i++)); do
        s=$(run_once $e $d/$q.sql); best=$(awk -v a="${best:-$s}" -v c="$s" 'BEGIN{print (c<a)?c:a}')
      done
      g=""
      case $e in moose*) [ "$GBH" = 1 ] && g="	$(M fileacc -e "$(pre $(table_of $e)) SHOW STATUS LIKE 'Moosedb_gbh%'; $(oneline $d/$q.sql $(table_of $e)); SHOW STATUS LIKE 'Moosedb_gbh%'" | grep '^Moosedb_gbh' | awk '{n=NR; if(!($1 in a)){a[$1]=$2; o[++k]=$1} else {b[$1]=$2}} END{for(i=1;i<=k;i++){d=b[o[i]]-a[o[i]]; if(d!=0){x=o[i]; sub("Moosedb_gbh_","",x); printf "%s=%d ", x, d}} if(!(b["Moosedb_gbh_queries"]>a["Moosedb_gbh_queries"])) printf "gbh_engine=nao"}')";; esac
      echo "q	$e	$q	$best	$first	[$TAG]$g"
    done
  done
  echo; echo "## Conferência dos result sets (md5 por consulta; igual entre engines = OK)"
  for f in sql/q*.sql; do
    q=$(basename $f .sql); ref=""; line="chk	$q"
    for e in $ENGINES; do
      t=$(table_of $e); MCON=$MC; [ $e = cs ] && MCON=$XC
      if [ $e = ch ]; then render ch/$q.sql $t | C --format TSV > "$RS/${e}_$q.tsv" 2>"$RS/${e}_$q.err"
      else { pre $t; render $f $t; } | M fileacc > "$RS/${e}_$q.tsv" 2>"$RS/${e}_$q.err"; fi
      h=$(md5sum < "$RS/${e}_$q.tsv" | cut -c1-8); [ -z "$ref" ] && ref=$h
      line="$line	$e=$h($(wc -l < "$RS/${e}_$q.tsv")l)"; [ "$h" != "$ref" ] && line="$line DIFERE"
    done
    echo "$line"
  done
fi

# ------------------------------------------------------------------ EXPLAIN + Handler_*
if [ "$STAGE" = explain ] || [ "$STAGE" = all ]; then
  MCON=$MC
  echo; echo "## EXPLAIN e contadores Handler_* por consulta (MariaDB)"
  for e in $ENGINES; do
    case $e in ch|cs) continue;; esac   # cs: EXPLAIN/Handler_* não refletem o ColumnStore
    t=$(table_of $e)
    for f in sql/q*.sql; do
      q=$(basename $f .sql); sqltxt=$(oneline $f $t)
      echo "### $e $q"
      M fileacc --table -e "$(pre $t) EXPLAIN $sqltxt" 2>&1
      M fileacc -e "FLUSH STATUS; $sqltxt; SHOW SESSION STATUS WHERE Variable_name RLIKE '^Handler_(read|tmp|write)' AND Value <> '0'" 2>&1 | grep -E '^Handler_'
    done
  done
fi
