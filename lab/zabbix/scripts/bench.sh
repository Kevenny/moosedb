#!/usr/bin/env bash
# Benchmark InnoDB x MooseDB no formato de histórico do Zabbix 7.0 (mesmos dados).
#
# Roda DENTRO do container MariaDB do laboratório:
#   docker exec -e ITEMS=5000 -e DAYS=7 -e STEP=60 zbxlab-mariadb \
#       bash /lab/scripts/bench.sh | tee results/bench.txt
#
# Parâmetros (variáveis de ambiente):
#   ITEMS  itens (séries)                       5000
#   DAYS   dias de histórico sintético          7
#   STEP   segundos entre valores de um item    60
#   RUNS   repetições por consulta (melhor de)  5
#   NLAST  itens no teste "último valor"        20
#   NAGG   itens na agregação por hora          100
#   SKIP_LOAD=1 reaproveita as tabelas já carregadas (só consultas + housekeeping)
#
# Saída: linhas "métrica<TAB>tabela<TAB>forma<TAB>valores" no stdout (o
# contêiner MariaDB monta scripts/ e sql/ só para leitura) — redirecione para
# results/ no host. Pré-requisito: DAYS >= 2 (o teste de housekeeping expira o 1º dia).
set -uo pipefail

ITEMS=${ITEMS:-5000}; DAYS=${DAYS:-7}; STEP=${STEP:-60}; RUNS=${RUNS:-5}
NLAST=${NLAST:-20}; NAGG=${NAGG:-100}; SKIP_LOAD=${SKIP_LOAD:-0}
PW="${ROOT_PASSWORD:-zbxlab}"
DB=zbxbench
DATADIR=/var/lib/mysql
TABLES="h_innodb h_moose_int h_moose_str h_moose_v3 h_moose_v4"
NATIVE="h_moose_int h_moose_str"   # tabelas com a coluna ts (forma "nativo(ts)")

q()  { mariadb -uroot -p"$PW" -N -s "$DB" -e "$1"; }
qt() { mariadb -uroot -p"$PW" --table "$DB" -e "$1"; }
# tempo do statement medido no servidor (s); "ERRO: ..." se falhar
timeit() {
  local out
  if out=$(q "SET @t0=NOW(6); $1; SELECT TIMESTAMPDIFF(MICROSECOND,@t0,NOW(6))/1e6" 2>&1); then
    echo "$out" | tail -1
  else
    echo "ERRO: $(echo "$out" | tail -1)"
  fi
}
# melhor (mínimo) de RUNS execuções após 1 aquecimento; imprime "melhor 1a_execucao"
best() {
  local first b t i
  first=$(timeit "$1")
  case "$first" in ERRO*) echo "$first"; return;; esac
  b=$first
  for ((i = 1; i < RUNS; i++)); do
    t=$(timeit "$1")
    b=$(awk -v a="$b" -v c="$t" 'BEGIN{print (c<a)?c:a}')
  done
  echo "$b $first"
}
dtu() { date -u -d "@$1" '+%Y-%m-%d %H:%M:%S'; }   # epoch -> DATETIME UTC
size_of() {
  case "$1" in
    h_innodb) du -sb "$DATADIR/$DB/h_innodb.ibd" | cut -f1 ;;
    *)        du -sb "$DATADIR/$DB/$1" | cut -f1 ;;
  esac
}
mb() { awk -v b="$1" 'BEGIN{printf "%.1f", b/1048576}'; }

if [ "$SKIP_LOAD" != 1 ]; then
  END=$(( $(date +%s) / STEP * STEP ))
  START=$(( END - DAYS * 86400 ))
  NSTEPS=$(( DAYS * 86400 / STEP ))
  mariadb -uroot -p"$PW" -e "DROP DATABASE IF EXISTS $DB; CREATE DATABASE $DB CHARACTER SET utf8mb4 COLLATE utf8mb4_bin"
  # retenção = DAYS-1 dias: o teste de housekeeping expira exatamente o 1º dia (parcial)
  sed "s/RETENTION_PERIOD='6 DAYS'/RETENTION_PERIOD='$(( DAYS - 1 )) DAYS'/" /lab/sql/bench_setup.sql > /tmp/bench_setup.sql
  q "SOURCE /tmp/bench_setup.sql"
  q "SOURCE /moosedb-sql/moosedb_install.sql"
  q "CREATE TABLE bench_meta (k VARCHAR(32) PRIMARY KEY, v BIGINT) ENGINE=InnoDB"
  q "INSERT INTO bench_meta VALUES ('start',$START),('end',$END),('items',$ITEMS),('step',$STEP),('days',$DAYS)"
else
  START=$(q "SELECT v FROM bench_meta WHERE k='start'"); END=$(q "SELECT v FROM bench_meta WHERE k='end'")
  ITEMS=$(q "SELECT v FROM bench_meta WHERE k='items'"); STEP=$(q "SELECT v FROM bench_meta WHERE k='step'")
  NSTEPS=$(( (END - START) / STEP ))
fi
OLD_RET=$(q "SELECT @@GLOBAL.moosedb_retention_check_interval")
q "SET GLOBAL moosedb_retention_check_interval=0"
ROWS=$(( ITEMS * NSTEPS ))

echo "# Benchmark Zabbix history: InnoDB x MooseDB"
echo "MariaDB $(q 'SELECT VERSION()') | itens=$ITEMS passo=${STEP}s dias=$(( (END-START)/86400 )) linhas/tabela=$ROWS"
echo "janela: $(dtu $START) .. $(dtu $END) UTC | innodb_buffer_pool=$(mb $(q 'SELECT @@innodb_buffer_pool_size')) MiB moosedb_chunk_cache=$(mb $(q 'SELECT @@moosedb_chunk_cache_size')) MiB"
echo

# ---------------------------------------------------------------- carga
if [ "$SKIP_LOAD" != 1 ]; then
  echo "## Carga ($NSTEPS statements de $ITEMS linhas, autocommit, em ordem de clock)"
  for spec in "h_innodb i 0" "h_moose_int i 1" "h_moose_str CAST(i&AS&CHAR) 1" "h_moose_v3 i 0" "h_moose_v4 i 0"; do
    set -- $spec; t=$1; expr=${2//&/ }; wts=$3
    s=$(timeit "CALL bench_load('$t','$expr',$wts,$START,$NSTEPS,$STEP,$ITEMS)")
    echo "load	$t	${s}s	$(awk -v r=$ROWS -v s="$s" 'BEGIN{printf "%.0f linhas/s", r/s}')"
  done
  echo
  echo "## Pós-carga"
  sf=$(timeit "FLUSH TABLES h_moose_int, h_moose_str, h_moose_v3, h_moose_v4"); echo "flush_moose	${sf}s"
  q "ANALYZE TABLE h_innodb" >/dev/null
  for t in $TABLES; do echo "size_after_load	$t	$(mb $(size_of $t)) MiB"; done
  for t in h_moose_int h_moose_str h_moose_v3 h_moose_v4; do
    s=$(timeit "CALL moosedb_compact('$t', NULL, NULL)")
    echo "compact_full	$t	${s}s	-> $(mb $(size_of $t)) MiB"
  done
  echo
fi

echo "## Tamanho e conferência dos dados"
for t in $TABLES; do
  echo "size	$t	$(mb $(size_of $t)) MiB	$(q "SELECT CONCAT(COUNT(*),' linhas | sum(value)=',ROUND(SUM(value),2),' | sum(ns)=',SUM(ns),' | sum(clock)=',SUM(clock)) FROM $t")"
done
q "SELECT CONCAT('innodb information_schema: data=',ROUND(DATA_LENGTH/1048576,1),' MiB index=',ROUND(INDEX_LENGTH/1048576,1),' MiB') FROM information_schema.TABLES WHERE TABLE_SCHEMA='$DB' AND TABLE_NAME='h_innodb'"
qt "SELECT TABLE_NAME, COUNT(*) chunks, SUM(\`ROWS\`) linhas, ROUND(SUM(DATA_MB),1) data_mb, ROUND(SUM(COMPRESSED_MB),1) comp_mb, GROUP_CONCAT(DISTINCT STATUS) status, GROUP_CONCAT(DISTINCT COMPRESSION) codec FROM information_schema.MOOSEDB_CHUNKS WHERE TABLE_SCHEMA='$DB' GROUP BY TABLE_NAME"
echo

# ---------------------------------------------------------------- consultas
ITEM=100003                      # item com senóide+ruído
W=1000                           # largura do gráfico (pixels)
echo "## Consultas (melhor de $RUNS, cache quente; colunas: melhor_s primeira_s)"
for win in 3600 86400 $(( END - START )); do
  S=$(( END - win )); P=$win
  case $win in 3600) wl=1h;; 86400) wl=24h;; *) wl=$(( win / 86400 ))d;; esac
  for t in $TABLES; do
    # CHistoryManager::getGraphAggregationByWidthFromSql — exatamente como o frontend
    zq="SELECT itemid,COUNT(*) AS count,AVG(value) AS avg,MIN(value) AS min,MAX(value) AS max,round($W.0*(clock-$S)/$P,0) AS i,MAX(clock) AS clock FROM $t WHERE itemid='$ITEM' AND clock>='$S' AND clock<='$END' GROUP BY itemid,round($W.0*(clock-$S)/$P,0) ORDER BY itemid,i"
    echo "graph_$wl	$t	zabbix	$(best "$zq")"
    if [[ " $NATIVE " == *" $t "* ]]; then
      nq="SELECT itemid,COUNT(*) AS count,AVG(value) AS avg,MIN(value) AS min,MAX(value) AS max,round($W.0*(clock-$S)/$P,0) AS i,MAX(clock) AS clock FROM $t WHERE itemid='$ITEM' AND ts>='$(dtu $S)' AND ts<'$(dtu $((END + 1)))' GROUP BY itemid,round($W.0*(clock-$S)/$P,0) ORDER BY itemid,i"
      echo "graph_$wl	$t	nativo(ts)	$(best "$nq")"
    fi
  done
done

SINCE=$(( END - 86400 ))
for t in $TABLES; do
  for mode in 1 0 2; do
    [ "$mode" = 2 ] && [[ " $NATIVE " != *" $t "* ]] && continue
    label=$([ $mode = 1 ] && echo "zabbix_pk" || ([ $mode = 0 ] && echo "zabbix_sem_pk" || echo "nativo(ts)"))
    r=$(best "CALL bench_lastvalue('$t',$mode,100001,$NLAST,$SINCE)")
    echo "lastvalue_${NLAST}itens	$t	$label	$r	($(awk -v r="${r%% *}" -v n=$NLAST 'BEGIN{printf "%.4f s/item", r/n}'))"
  done
done

IDS=$(seq -s, 100001 $(( 100000 + NAGG ))); QIDS="'$(seq -s"','" 100001 $(( 100000 + NAGG )))'"
S=$(( END - 86400 ))
for t in $TABLES; do
  # CHistoryManager::getAggregationByIntervalFromSql (AVG por hora, dbConditionInt => IN sem aspas)
  aq="SELECT itemid,CAST(clock AS SIGNED)-MOD(CAST(clock AS SIGNED),3600) AS tick,AVG(value) AS value,MAX(clock) AS clock,COUNT(*) AS num FROM $t WHERE itemid IN ($IDS) AND clock BETWEEN $S AND $END GROUP BY itemid,CAST(clock AS SIGNED)-MOD(CAST(clock AS SIGNED),3600)"
  echo "agg_hour_${NAGG}itens_24h	$t	zabbix	$(best "$aq")"
  if [[ " $NATIVE " == *" $t "* ]]; then
    nq="SELECT itemid,CAST(clock AS SIGNED)-MOD(CAST(clock AS SIGNED),3600) AS tick,AVG(value) AS value,MAX(clock) AS clock,COUNT(*) AS num FROM $t WHERE itemid IN ($QIDS) AND ts>='$(dtu $S)' AND ts<'$(dtu $((END + 1)))' GROUP BY itemid,CAST(clock AS SIGNED)-MOD(CAST(clock AS SIGNED),3600)"
    echo "agg_hour_${NAGG}itens_24h	$t	nativo(ts)	$(best "$nq")"
  fi
  # history_sql.c:db_read_values_by_time (value cache do server, itemid sem aspas, 1 h)
  vq="select clock,ns,value from $t where itemid=$ITEM and clock>$(( END - 3600 )) and clock<=$END"
  echo "vc_read_1h	$t	zabbix	$(best "$vq")"
  # getAggregationByIntervalFromSql com AGGREGATE_LAST (self-join em itemid,clock[,ns]), 1 item, 24 h
  lq="SELECT h.itemid,h.value,h.clock,h.ns,s.tick FROM $t h JOIN (SELECT h2.itemid,h2.clock,MAX(h2.ns) AS ns,s2.tick FROM $t h2 JOIN (SELECT itemid,CAST(clock AS SIGNED)-MOD(CAST(clock AS SIGNED),3600) AS tick,MAX(clock) AS clock FROM $t WHERE itemid=$ITEM AND clock BETWEEN $S AND $END GROUP BY itemid,CAST(clock AS SIGNED)-MOD(CAST(clock AS SIGNED),3600)) s2 ON h2.itemid=s2.itemid AND h2.clock=s2.clock WHERE h2.clock BETWEEN $S AND $END GROUP BY h2.itemid,h2.clock,s2.tick) s ON h.itemid=s.itemid AND h.clock=s.clock AND h.ns=s.ns WHERE h.clock BETWEEN $S AND $END"
  echo "agg_last_hour_1item_24h	$t	zabbix	$(best "$lq")"
done
echo

echo "## EXPLAIN (gráfico 1h, forma do Zabbix)"
for t in $TABLES; do
  qt "EXPLAIN SELECT itemid,COUNT(*),AVG(value) FROM $t WHERE itemid='$ITEM' AND clock>='$(( END - 3600 ))' AND clock<='$END' GROUP BY itemid"
done
echo

# ---------------------------------------------------------------- housekeeping
CUT=$(( (START / 86400 + 1) * 86400 ))   # 1ª meia-noite UTC após o início = fronteira do 1º chunk
echo "## Housekeeping: expirar dados com clock < $CUT ($(dtu $CUT) UTC)"
for t in $TABLES; do echo "rows_to_expire	$t	$(q "SELECT COUNT(*) FROM $t WHERE clock<$CUT")"; done
s=$(timeit "CALL bench_hk_delete('h_innodb',100001,$ITEMS,$CUT)")
echo "hk_delete_per_item	h_innodb	${s}s	restantes_antigas=$(q "SELECT COUNT(*) FROM h_innodb WHERE clock<$CUT") size=$(mb $(size_of h_innodb)) MiB (o .ibd não encolhe)"
echo "hk_delete_moose	h_moose_int	$(timeit "delete from h_moose_int where itemid=100001 and clock<$CUT")"
for t in h_moose_int h_moose_str h_moose_v3 h_moose_v4; do
  s=$(timeit "CALL moosedb_apply_retention('$t')")
  echo "retention_drop_chunk	$t	${s}s	restantes_antigas=$(q "SELECT COUNT(*) FROM $t WHERE clock<$CUT") total=$(q "SELECT COUNT(*) FROM $t") size=$(mb $(size_of $t)) MiB"
done
echo "rows_after	h_innodb	$(q "SELECT COUNT(*) FROM h_innodb")"

q "SET GLOBAL moosedb_retention_check_interval=$OLD_RET"
echo
echo "fim: $(date -u '+%F %T') UTC"
