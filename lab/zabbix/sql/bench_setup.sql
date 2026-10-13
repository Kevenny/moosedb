-- Benchmark InnoDB x MooseDB com o formato de histórico do Zabbix 7.0.
-- Executado por scripts/bench.sh no banco zbxbench (recriado a cada rodada).
--
-- Tabelas (mesmos dados nas três):
--   h_innodb     DDL oficial do Zabbix 7.0 (InnoDB, PRIMARY KEY (itemid,clock,ns))
--   h_moose_int  variante adaptada usada com o Zabbix (itemid BIGINT TAG, clock INT, ts DATETIME(6))
--   h_moose_str  itemid VARCHAR TAG (pushdown só com constante entre aspas)
--   h_moose_v3   esquema v3: itemid BIGINT TAG (pushdown numérico) e clock INT
--                como eixo de tempo (TIMESTAMP_COLUMN='clock'), sem ts/trigger;
--                INDEX (clock) + INDEX (itemid, clock): o índice composto dá ref
--                por item, self-join e MAX(clock) por item sem varrer a série
--
-- RETENTION_PERIOD='6 DAYS' nas MooseDB: o teste de housekeeping descarta o
-- 1º dia (parcial) dos 7 carregados. A varredura automática é desligada
-- durante o benchmark (moosedb_retention_check_interval=0) e a retenção roda
-- só via CALL moosedb_apply_retention.

CREATE TABLE h_innodb (
  itemid bigint unsigned NOT NULL,
  clock  integer DEFAULT '0' NOT NULL,
  value  DOUBLE PRECISION DEFAULT '0.0000' NOT NULL,
  ns     integer DEFAULT '0' NOT NULL,
  PRIMARY KEY (itemid,clock,ns)
) ENGINE=InnoDB;

CREATE TABLE h_moose_int (
  itemid bigint unsigned NOT NULL COMMENT 'TAG',
  clock  integer DEFAULT '0' NOT NULL,
  value  DOUBLE PRECISION DEFAULT '0.0000' NOT NULL,
  ns     integer DEFAULT '0' NOT NULL,
  ts     DATETIME(6) NOT NULL DEFAULT '1970-01-01 00:00:01',
  INDEX ts_idx (ts)
) ENGINE=MooseDB CHUNK_INTERVAL='1 DAY' RETENTION_PERIOD='6 DAYS' HOT_THRESHOLD='1 DAY';

CREATE TABLE h_moose_str (
  itemid varchar(20) NOT NULL COMMENT 'TAG',
  clock  integer DEFAULT '0' NOT NULL,
  value  DOUBLE PRECISION DEFAULT '0.0000' NOT NULL,
  ns     integer DEFAULT '0' NOT NULL,
  ts     DATETIME(6) NOT NULL DEFAULT '1970-01-01 00:00:01',
  INDEX ts_idx (ts)
) ENGINE=MooseDB CHUNK_INTERVAL='1 DAY' RETENTION_PERIOD='6 DAYS' HOT_THRESHOLD='1 DAY';

CREATE TABLE h_moose_v3 (
  itemid bigint unsigned NOT NULL COMMENT 'TAG',
  clock  integer DEFAULT '0' NOT NULL,
  value  DOUBLE PRECISION DEFAULT '0.0000' NOT NULL,
  ns     integer DEFAULT '0' NOT NULL,
  INDEX clock_idx (clock),
  INDEX item_clock (itemid, clock)
) ENGINE=MooseDB TIMESTAMP_COLUMN='clock' CHUNK_INTERVAL='1 DAY' RETENTION_PERIOD='6 DAYS' HOT_THRESHOLD='1 DAY';

DELIMITER //

-- Carga no formato do history syncer: 1 statement (autocommit) por passo de
-- tempo, com 1 valor de cada item (n_items linhas por statement), em ordem de
-- clock. Valores determinísticos f(itemid, clock), iguais em todas as tabelas:
--   itemid % 4 = 0  quase constante (muda 1x/dia)     -- ex.: disco total
--   itemid % 4 = 1  contador crescente                 -- ex.: bytes recebidos
--   demais          senóide + ruído, 4 casas decimais  -- ex.: CPU %
--   ns              pseudoaleatório 0..999999999 (como o Zabbix grava)
CREATE OR REPLACE PROCEDURE bench_load(IN tbl VARCHAR(64), IN itemid_expr VARCHAR(64), IN with_ts BOOL,
                                       IN c_start INT, IN n_steps INT, IN step INT, IN n_items INT)
BEGIN
  DECLARE k INT DEFAULT 0;
  SET @bench_sql = CONCAT(
    'INSERT INTO ', tbl, ' (itemid,clock,ns,value', IF(with_ts, ',ts', ''), ') ',
    'SELECT ', itemid_expr, ', c, ns, v',
    IF(with_ts, ', TIMESTAMP''1970-01-01 00:00:00'' + INTERVAL c SECOND + INTERVAL (ns DIV 1000) MICROSECOND', ''),
    ' FROM (SELECT i, c, CRC32(CONCAT(i,'':'',c)) % 1000000000 AS ns,',
    ' CASE i % 4 WHEN 0 THEN (i % 100) + (c DIV 86400) % 3',
    '            WHEN 1 THEN (c DIV 60) * (i % 7 + 1)',
    '            ELSE ROUND(50 + 30 * SIN(c / 3600 + i) + (CRC32(CONCAT(c,''/'',i)) % 200) / 100, 4) END AS v',
    ' FROM (SELECT 100000 + seq AS i, CAST(? AS SIGNED) AS c FROM seq_1_to_', n_items, ') g) x');
  PREPARE bench_stmt FROM @bench_sql;
  WHILE k < n_steps DO
    SET @bench_c = c_start + k * step;
    EXECUTE bench_stmt USING @bench_c;
    SET k = k + 1;
  END WHILE;
  DEALLOCATE PREPARE bench_stmt;
END //

-- "Último valor" de n itens exatamente como o frontend faz (CHistoryManager.php):
--   pk=1  getLastValuesFromSqlWithPk (usado quando history tem PK => InnoDB)
--   pk=0  getLastValuesFromSql, limit=1 (usado sem PK => MooseDB): MAX(clock) + SELECT por clock
--   pk=2  forma "nativa" MooseDB: ORDER BY ts DESC LIMIT 1 (índice temporal + pushdown de TAG)
-- itemid sempre entre aspas (zbx_dbstr). since = time() - history_period (24 h).
CREATE OR REPLACE PROCEDURE bench_lastvalue(IN tbl VARCHAR(64), IN pk INT, IN first_item INT, IN n INT, IN since INT)
BEGIN
  DECLARE k INT DEFAULT 0;
  WHILE k < n DO
    SET @bench_item = CAST(first_item + k AS CHAR);
    IF pk = 2 THEN
      SET @bench_sql = CONCAT('SELECT h.clock,h.ns,h.value INTO @lv_c,@lv_ns,@lv_v FROM ', tbl, ' h WHERE h.itemid=''',
                              @bench_item, ''' ORDER BY h.ts DESC LIMIT 1');
      PREPARE s1 FROM @bench_sql; EXECUTE s1; DEALLOCATE PREPARE s1;
    ELSEIF pk = 1 THEN
      SET @bench_sql = CONCAT('SELECT h.clock,h.ns,h.value INTO @lv_c,@lv_ns,@lv_v FROM ', tbl, ' h WHERE h.itemid=''',
                              @bench_item, ''' AND h.clock>', since, ' ORDER BY h.clock DESC,h.ns DESC LIMIT 1');
      PREPARE s1 FROM @bench_sql; EXECUTE s1; DEALLOCATE PREPARE s1;
    ELSE
      SET @bench_sql = CONCAT('SELECT MAX(h.clock) INTO @lv_c FROM ', tbl, ' h WHERE h.itemid=''',
                              @bench_item, ''' AND h.clock>', since);
      PREPARE s1 FROM @bench_sql; EXECUTE s1; DEALLOCATE PREPARE s1;
      SET @bench_sql = CONCAT('SELECT h.ns,h.value INTO @lv_ns,@lv_v FROM ', tbl, ' h WHERE h.itemid=''',
                              @bench_item, ''' AND h.clock=''', @lv_c, ''' ORDER BY h.ns DESC LIMIT 1');
      PREPARE s2 FROM @bench_sql; EXECUTE s2; DEALLOCATE PREPARE s2;
    END IF;
    SET k = k + 1;
  END WHILE;
END //

-- Housekeeper do Zabbix (housekeeper_server.c:housekeeping_history_and_trends):
-- 1 DELETE por item: delete from <t> where itemid=X and clock<cut (autocommit).
CREATE OR REPLACE PROCEDURE bench_hk_delete(IN tbl VARCHAR(64), IN first_item INT, IN n INT, IN cut INT)
BEGIN
  DECLARE k INT DEFAULT 0;
  SET @hk_deleted = 0;
  WHILE k < n DO
    SET @bench_sql = CONCAT('delete from ', tbl, ' where itemid=', first_item + k, ' and clock<', cut);
    PREPARE s1 FROM @bench_sql; EXECUTE s1;
    SET @hk_deleted = @hk_deleted + ROW_COUNT();
    DEALLOCATE PREPARE s1;
    SET k = k + 1;
  END WHILE;
END //

DELIMITER ;
