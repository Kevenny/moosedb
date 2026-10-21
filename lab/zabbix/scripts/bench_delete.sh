#!/usr/bin/env bash
# Benchmark do DELETE do housekeeper do Zabbix: InnoDB x MooseDB (v3 sem PK, v4 com PK).
#
# Roda contra QUALQUER servidor com o plugin MooseDB (variavel MYSQL = linha de
# comando do cliente, p.ex. "mariadb -uroot -S /tmp/m.sock"). Cria o banco
# zbxdel, carrega ITEMS x DAYS x STEP segundos, mede:
#   * leitura (grafico de 1 item/24 h, ultimo valor, agregacao por hora de 100
#     itens/24 h) ANTES de apagar;
#   * housekeeper: 1 DELETE por item "delete from t where itemid=X and clock<CUT"
#     (autocommit, como housekeeper_server.c): o 1o dia inteiro (o chunk morre) e depois
#     20% do 2o dia com LIMIT 5000 (o chunk fica com 5.000 tombstones pendentes);
#   * as mesmas leituras com os tombstones ainda pendentes (sem compactar);
#   * OPTIMIZE (a compactacao aplica os deletes) e as leituras de novo.
# Saida: linhas "metrica<TAB>tabela<TAB>valor" no stdout.
set -uo pipefail
MYSQL=${MYSQL:-"mariadb -uroot"}
ITEMS=${ITEMS:-5000}; DAYS=${DAYS:-7}; STEP=${STEP:-60}; RUNS=${RUNS:-5}
START=${START:-1767225600}                       # 2026-01-01 00:00 UTC
DB=zbxdel
q()  { $MYSQL -N -s "$DB" -e "$1"; }
timeit() {
  local out
  if out=$(q "SET @t0=NOW(6); $1; SELECT TIMESTAMPDIFF(MICROSECOND,@t0,NOW(6))/1e6" 2>&1); then echo "$out" | tail -1; else echo "ERRO: $(echo "$out" | tail -1)"; fi
}
best() { local b t i; b=$(timeit "$1"); for ((i = 1; i < RUNS; i++)); do t=$(timeit "$1"); b=$(awk -v a="$b" -v c="$t" 'BEGIN{print (c<a)?c:a}'); done; echo "$b"; }
PER_DAY=$(( 86400 / STEP ))
END=$(( START + DAYS * 86400 - STEP ))
CUT=$(( START + 86400 ))
IDS100=$(seq -s, 1000 1099)

echo "# inicio $(date -u '+%F %T') UTC  ITEMS=$ITEMS DAYS=$DAYS STEP=$STEP (linhas=$(( ITEMS * DAYS * PER_DAY )))"
$MYSQL -e "DROP DATABASE IF EXISTS $DB; CREATE DATABASE $DB"
q "
CREATE TABLE h_innodb (itemid bigint unsigned NOT NULL, clock integer NOT NULL DEFAULT 0, value double NOT NULL DEFAULT 0, ns integer NOT NULL DEFAULT 0, PRIMARY KEY (itemid,clock,ns)) ENGINE=InnoDB;
CREATE TABLE h_v3 (itemid bigint unsigned NOT NULL COMMENT 'TAG', clock integer NOT NULL DEFAULT 0, value double NOT NULL DEFAULT 0, ns integer NOT NULL DEFAULT 0, INDEX (clock), INDEX (itemid,clock)) ENGINE=MooseDB TIMESTAMP_COLUMN='clock' CHUNK_INTERVAL='1 DAY' HOT_THRESHOLD='1 DAY';
CREATE TABLE h_v4 (itemid bigint unsigned NOT NULL COMMENT 'TAG', clock integer NOT NULL DEFAULT 0, value double NOT NULL DEFAULT 0, ns integer NOT NULL DEFAULT 0, PRIMARY KEY (itemid,clock,ns)) ENGINE=MooseDB TIMESTAMP_COLUMN='clock' CHUNK_INTERVAL='1 DAY' HOT_THRESHOLD='1 DAY';
DELIMITER //
CREATE PROCEDURE bench_hk_delete(IN tbl VARCHAR(64), IN first_item INT, IN n INT, IN cut INT, IN lim INT)
BEGIN
  DECLARE k INT DEFAULT 0;
  SET @hk_deleted = 0;
  WHILE k < n DO
    SET @bench_sql = CONCAT('delete from ', tbl, ' where itemid=', first_item + k, ' and clock<', cut, IF(lim > 0, CONCAT(' limit ', lim), ''));
    PREPARE s1 FROM @bench_sql; EXECUTE s1;
    SET @hk_deleted = @hk_deleted + ROW_COUNT();
    DEALLOCATE PREPARE s1;
    SET k = k + 1;
  END WHILE;
END //
DELIMITER ;
" 2>&1 | grep -v "^$" || true
# carga: um INSERT ... SELECT por 'tick' de ITEMS linhas, em ordem de clock (como o syncer)
for t in h_innodb h_v3 h_v4; do
  s=$(timeit "INSERT INTO $t SELECT 1+a.seq, $START + b.seq*$STEP, (a.seq*7+b.seq) MOD 1000, b.seq MOD 2 FROM seq_0_to_$(( ITEMS - 1 )) a, seq_0_to_$(( DAYS * PER_DAY - 1 )) b")
  echo "carga	$t	${s}s	linhas=$(q "SELECT COUNT(*) FROM $t")"
done
q "FLUSH TABLES h_v3, h_v4" >/dev/null
q "OPTIMIZE TABLE h_v3" >/dev/null; q "OPTIMIZE TABLE h_v4" >/dev/null

reads() {  # $1 = rotulo
  for t in h_innodb h_v3 h_v4; do
    echo "$1_grafico_1item_24h	$t	$(best "SELECT clock,value FROM $t WHERE itemid=77 AND clock BETWEEN $((END-86400)) AND $END")"
    echo "$1_lastvalue_20itens	$t	$(best "CALL bench_last('$t',20)")"
    echo "$1_agg_hora_100itens_24h	$t	$(best "SELECT itemid,clock-MOD(clock,3600),AVG(value),COUNT(*) FROM $t WHERE itemid IN ($IDS100) AND clock BETWEEN $((END-86400)) AND $END GROUP BY 1,2")"
    echo "$1_count_total	$t	$(best "SELECT COUNT(*) FROM $t")"
  done
}
q "
DELIMITER //
CREATE PROCEDURE bench_last(IN tbl VARCHAR(64), IN n INT)
BEGIN
  DECLARE k INT DEFAULT 0;
  WHILE k < n DO
    SET @bench_sql = CONCAT('SELECT clock,ns,value INTO @c,@n,@v FROM ', tbl, ' WHERE itemid=', 100 + k, ' ORDER BY clock DESC, ns DESC LIMIT 1');
    PREPARE s1 FROM @bench_sql; EXECUTE s1; DEALLOCATE PREPARE s1;
    SET k = k + 1;
  END WHILE;
END //
DELIMITER ;
" 2>&1 | grep -v "^$" || true
reads antes

echo "## housekeeper: 1 DELETE por item, clock < $CUT (1o dia = $(( ITEMS * PER_DAY )) linhas)"
for t in h_innodb h_v3 h_v4; do
  n0=$(q "SELECT COUNT(*) FROM $t")
  s=$(timeit "CALL bench_hk_delete('$t',1,$ITEMS,$CUT,0)")
  echo "hk_delete_por_item	$t	${s}s	apagadas=$(( n0 - $(q "SELECT COUNT(*) FROM $t") ))  restantes_antigas=$(q "SELECT COUNT(*) FROM $t WHERE clock<$CUT")"
done
echo "## 20% do 2o dia, com LIMIT 5000 (MaxHousekeeperDelete): os chunks seguem vivos, com 1 tombstone por item pendente"
CUT2=$(( START + 86400 + 17280 ))
for t in h_innodb h_v3 h_v4; do
  n0=$(q "SELECT COUNT(*) FROM $t")
  s=$(timeit "CALL bench_hk_delete('$t',1,$ITEMS,$CUT2,5000)")
  echo "hk_delete_limit5000	$t	${s}s	apagadas=$(( n0 - $(q "SELECT COUNT(*) FROM $t") ))"
done
for t in h_v3 h_v4; do
  echo "tombstones	$t	$(q "SELECT CONCAT('chunks=',CHUNKS,' rows=',ROW_COUNT) FROM information_schema.MOOSEDB_TABLES WHERE TABLE_SCHEMA='$DB' AND TABLE_NAME='$t'")"
done
echo "## leituras com os deletes pendentes (sem compactar)"
reads pendentes
echo "## OPTIMIZE (compacta e remove as linhas apagadas)"
for t in h_v3 h_v4; do
  echo "optimize	$t	$(timeit "OPTIMIZE TABLE $t")s"
done
reads depois
for t in h_innodb h_v3 h_v4; do echo "linhas_final	$t	$(q "SELECT COUNT(*) FROM $t")"; done
echo "# fim $(date -u '+%F %T') UTC"
