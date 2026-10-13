#!/usr/bin/env bash
# Teste de compatibilidade: tabelas de histórico do Zabbix 7.0 x MooseDB.
#
# Roda DENTRO do container MariaDB do laboratório:
#   docker exec zbxlab-mariadb bash /lab/scripts/compat_test.sh > results/compat.txt
#
# Parte A tenta `ALTER TABLE <history*> ENGINE=MooseDB` no esquema oficial
# (banco `zabbix`, sem alterar nada quando falha). Parte B aplica adaptações
# progressivas numa CÓPIA (banco `zbxcompat`) para isolar cada
# incompatibilidade. Parte C testa, na variante adaptada, as instruções que o
# Zabbix 7.0 realmente emite (INSERT multi-linha, SELECTs do value cache e do
# frontend, DELETEs do housekeeper e do history.clear, ALTER do dbupgrade).
set -uo pipefail

PW="${ROOT_PASSWORD:-zbxlab}"
SQL() { mariadb -uroot -p"$PW" --table "$@"; }

run() { # run <db> <sql>
  local db="$1"; shift
  echo ">>> [$db] $*"
  local out rc
  out=$(SQL "$db" -e "$*" 2>&1); rc=$?
  if [ -n "$out" ]; then echo "$out" | sed 's/^/    /'; fi
  if [ $rc -eq 0 ]; then echo "    => OK"; else echo "    => FALHOU (rc=$rc)"; fi
  echo
}

HIST_TABLES="history history_uint history_str history_text history_log history_bin"

echo "== Versões"
run zabbix "SELECT VERSION() AS mariadb, (SELECT CONCAT(mandatory,'/',optional) FROM dbversion) AS zabbix_dbversion, (SELECT PLUGIN_STATUS FROM information_schema.PLUGINS WHERE PLUGIN_NAME='MooseDB') AS moosedb"

echo "== A. ALTER TABLE ... ENGINE=MooseDB no esquema oficial (zabbix)"
for t in $HIST_TABLES; do
  run zabbix "SHOW CREATE TABLE $t"
  run zabbix "ALTER TABLE $t ENGINE=MooseDB"
done

echo "== B. Adaptações progressivas numa cópia (zbxcompat)"
SQL -e "DROP DATABASE IF EXISTS zbxcompat; CREATE DATABASE zbxcompat CHARACTER SET utf8mb4 COLLATE utf8mb4_bin"
for t in $HIST_TABLES; do SQL zbxcompat -e "CREATE TABLE $t LIKE zabbix.$t"; done

echo "-- B1. sem PRIMARY KEY, sem índice algum"
run zbxcompat "CREATE TABLE b1 (itemid BIGINT UNSIGNED NOT NULL, clock INT NOT NULL DEFAULT 0, value DOUBLE NOT NULL DEFAULT 0, ns INT NOT NULL DEFAULT 0) ENGINE=MooseDB"
echo "-- B2. sem PK, INDEX(clock) com clock INT (eixo de tempo inteiro)"
run zbxcompat "CREATE TABLE b2 (itemid BIGINT UNSIGNED NOT NULL, clock INT NOT NULL DEFAULT 0, value DOUBLE NOT NULL DEFAULT 0, ns INT NOT NULL DEFAULT 0, INDEX (clock)) ENGINE=MooseDB"
echo "-- B3. PRIMARY KEY (itemid,clock,ns) + coluna ts DATETIME(6) com INDEX(ts)"
run zbxcompat "CREATE TABLE b3 (itemid BIGINT UNSIGNED NOT NULL, clock INT NOT NULL DEFAULT 0, value DOUBLE NOT NULL DEFAULT 0, ns INT NOT NULL DEFAULT 0, ts DATETIME(6) NOT NULL, PRIMARY KEY (itemid,clock,ns), INDEX (ts)) ENGINE=MooseDB"
echo "-- B4. UNIQUE (itemid,clock,ns) + INDEX(ts)"
run zbxcompat "CREATE TABLE b4 (itemid BIGINT UNSIGNED NOT NULL, clock INT NOT NULL DEFAULT 0, value DOUBLE NOT NULL DEFAULT 0, ns INT NOT NULL DEFAULT 0, ts DATETIME(6) NOT NULL, UNIQUE KEY (itemid,clock,ns), INDEX (ts)) ENGINE=MooseDB"
echo "-- B5. índice secundário (itemid) além de INDEX(ts)"
run zbxcompat "CREATE TABLE b5 (itemid BIGINT UNSIGNED NOT NULL, clock INT NOT NULL DEFAULT 0, value DOUBLE NOT NULL DEFAULT 0, ns INT NOT NULL DEFAULT 0, ts DATETIME(6) NOT NULL, INDEX (ts), INDEX (itemid)) ENGINE=MooseDB"
echo "-- B6. índice composto (itemid, ts)"
run zbxcompat "CREATE TABLE b6 (itemid BIGINT UNSIGNED NOT NULL, ts DATETIME(6) NOT NULL, value DOUBLE NOT NULL DEFAULT 0, INDEX (itemid, ts)) ENGINE=MooseDB"
echo "-- B7. clock como TIMESTAMP NOT NULL + INDEX(clock) (aceito pela engine?) e INSERT inteiro como o Zabbix faz"
run zbxcompat "CREATE TABLE b7 (itemid BIGINT UNSIGNED NOT NULL COMMENT 'TAG', clock TIMESTAMP NOT NULL, value DOUBLE NOT NULL DEFAULT 0, ns INT NOT NULL DEFAULT 0, INDEX (clock)) ENGINE=MooseDB"
run zbxcompat "INSERT INTO b7 (itemid,clock,ns,value) VALUES (10001,1759843200,123,1.5)"
run zbxcompat "SET SESSION sql_mode=''; INSERT INTO b7 (itemid,clock,ns,value) VALUES (10001,1759843200,123,1.5); SHOW WARNINGS; SELECT * FROM b7"
run zbxcompat "SELECT itemid,clock,ns,value FROM b7 WHERE itemid=10001 AND clock>1759800000"
echo "-- B8. variante ADAPTADA mínima (aceita): clock INT mantido, itemid TAG, ts DATETIME(6) preenchido por trigger"
run zbxcompat "CREATE TABLE b8 (itemid BIGINT UNSIGNED NOT NULL COMMENT 'TAG', clock INT NOT NULL DEFAULT 0, value DOUBLE NOT NULL DEFAULT 0, ns INT NOT NULL DEFAULT 0, ts DATETIME(6) NOT NULL DEFAULT '1970-01-01 00:00:01', INDEX (ts)) ENGINE=MooseDB"
run zbxcompat "CREATE TRIGGER b8_ts BEFORE INSERT ON b8 FOR EACH ROW SET NEW.ts = FROM_UNIXTIME(NEW.clock) + INTERVAL (NEW.ns DIV 1000) MICROSECOND"
echo "-- B9. itemid BIGINT TAG x coluna gerada como eixo de tempo (sem trigger)"
run zbxcompat "CREATE TABLE b9 (itemid BIGINT UNSIGNED NOT NULL COMMENT 'TAG', clock INT NOT NULL DEFAULT 0, value DOUBLE NOT NULL DEFAULT 0, ns INT NOT NULL DEFAULT 0, ts DATETIME(6) AS (FROM_UNIXTIME(clock)) PERSISTENT, INDEX (ts)) ENGINE=MooseDB"
run zbxcompat "CREATE TABLE b9b (itemid BIGINT UNSIGNED NOT NULL COMMENT 'TAG', clock INT NOT NULL DEFAULT 0, value DOUBLE NOT NULL DEFAULT 0, ns INT NOT NULL DEFAULT 0, ts DATETIME(6) AS (FROM_UNIXTIME(clock)) VIRTUAL, INDEX (ts)) ENGINE=MooseDB"

echo "== C. O que o Zabbix 7.0 emite, contra a variante adaptada (b8)"
echo "-- C1. history syncer: INSERT multi-linha dentro de transação (history_sql.c:sql_writer_flush)"
run zbxcompat "BEGIN; INSERT INTO b8 (itemid,clock,ns,value) VALUES (10001,1759843200,100,1.5),(10002,1759843200,200,2.5),(10001,1759843260,300,1.7); COMMIT; SELECT * FROM b8 ORDER BY ts"
echo "-- C2. duplicata (itemid,clock,ns): InnoDB rejeita com ER_DUP_ENTRY; MooseDB aceita (sem unicidade)"
run zbxcompat "INSERT INTO b8 (itemid,clock,ns,value) VALUES (10001,1759843200,100,9.9); SELECT itemid,clock,ns,COUNT(*) c FROM b8 GROUP BY itemid,clock,ns HAVING c>1"
run zabbix "INSERT INTO zbxcompat.history (itemid,clock,ns,value) VALUES (10001,1759843200,100,1.5); INSERT INTO zbxcompat.history (itemid,clock,ns,value) VALUES (10001,1759843200,100,9.9)"
echo "-- C3. ROLLBACK não desfaz (engine não transacional)"
run zbxcompat "BEGIN; INSERT INTO b8 (itemid,clock,ns,value) VALUES (10003,1759843300,1,3.3); ROLLBACK; SHOW WARNINGS; SELECT COUNT(*) FROM b8 WHERE itemid=10003"
echo "-- C4. value cache (history_sql.c:db_read_values_by_time / _by_count)"
run zbxcompat "select clock,ns,value from b8 where itemid=10001 and clock>1759800000"
run zbxcompat "select clock,ns,value from b8 where itemid=10001 and clock<=1759900000 and clock>1759800000 order by clock desc limit 2"
run zbxcompat "EXPLAIN select clock,ns,value from b8 where itemid=10001 and clock<=1759900000 and clock>1759800000 order by clock desc limit 2"
echo "-- C5. frontend (CHistoryManager.php): last value com e sem PK, gráfico, agregação"
run zbxcompat "SELECT * FROM b8 h WHERE h.itemid='10001' AND h.clock>1759800000 ORDER BY h.clock DESC,h.ns DESC LIMIT 1"
run zbxcompat "SELECT MAX(h.clock) FROM b8 h WHERE h.itemid='10001' AND h.clock>1759800000"
run zbxcompat "SELECT itemid,COUNT(*) AS count,AVG(value) AS avg,MIN(value) AS min,MAX(value) AS max,round(900.0*(clock-1759840000)/3600,0) AS i,MAX(clock) AS clock FROM b8 WHERE itemid='10001' AND clock>='1759840000' AND clock<='1759850000' GROUP BY itemid,round(900.0*(clock-1759840000)/3600,0) ORDER BY itemid,i"
run zbxcompat "SELECT itemid,CAST(clock AS SIGNED)-MOD(CAST(clock AS SIGNED),3600) AS tick,AVG(value) AS value,MAX(clock) AS clock,COUNT(*) AS num FROM b8 WHERE itemid IN (10001,10002) AND clock BETWEEN 1759800000 AND 1759900000 GROUP BY itemid,CAST(clock AS SIGNED)-MOD(CAST(clock AS SIGNED),3600)"
echo "-- C6. housekeeper (housekeeper_server.c:housekeeping_history_and_trends / hk_history_prepare / DBdelete_from_table)"
run zbxcompat "select itemid,min(clock) from b8 group by itemid"
run zbxcompat "delete from b8 where itemid=10001 and clock<1759843260"
run zbxcompat "delete from b8 where itemid=10002 limit 500"
echo "-- C7. frontend history.clear (CHistoryManager.php:deleteHistoryFromSql)"
run zbxcompat "DELETE FROM b8 WHERE itemid IN (10001,10002)"
echo "-- C8. TRUNCATE / DELETE sem WHERE (únicas formas de apagar)"
run zbxcompat "CREATE TABLE b8t LIKE b8"
run zbxcompat "INSERT INTO b8t (itemid,clock,ns,value,ts) VALUES (1,1,1,1,'2026-01-01'); TRUNCATE TABLE b8t; SELECT COUNT(*) FROM b8t"
echo "-- C9. detecção de PK pelo server (db.c:zbx_db_pk_exists) => history_pk=0"
run zbxcompat "show index from b8 where key_name='PRIMARY'"
echo "-- C10. dbupgrade 6050008 (DBmodify_field_type history.value DOUBLE) — ALTER por cópia"
run zbxcompat "alter table b8 modify value DOUBLE PRECISION default '0.0000' not null"
run zbxcompat "SELECT COUNT(*) FROM b8"
echo "-- C11. dbupgrade 6050013-like: CREATE TABLE history_bin com PK (como o Zabbix faria numa engine padrão = InnoDB)"
run zbxcompat "SET SESSION default_storage_engine=MooseDB; create table hb (itemid bigint unsigned not null, clock integer default '0' not null, ns integer default '0' not null, value longblob not null, primary key (itemid,clock,ns))"

echo "== D. Variante adaptada para as 6 tabelas (sql/adapted_history.sql)"
run zbxcompat "SOURCE /lab/sql/adapted_history.sql"
run zbxcompat "SELECT TABLE_NAME, ENGINE FROM information_schema.TABLES WHERE TABLE_SCHEMA='zbxcompat' AND TABLE_NAME LIKE 'history%' ORDER BY 1"
