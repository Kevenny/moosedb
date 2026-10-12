-- Variante "adaptada v2" (gerada de adapted_history.sql): igual à v1, mas
-- itemid é VARCHAR(20) TAG em vez de BIGINT UNSIGNED TAG.
--
-- Motivo: o pushdown de TAG só existe para constantes string. O frontend
-- escreve itemid entre aspas (zbx_dbstr: gráficos, último valor) e passa a
-- ler só a série pedida; o server (value cache) e dbConditionInt (IN sem
-- aspas) continuam sem pushdown. O MariaDB compara VARCHAR x número como
-- DOUBLE, então resultados continuam corretos (itemid < 2^53).
--
-- Mesmas restrições da v1: sem PK, ts por trigger, housekeeper de histórico
-- desligado, expiração por RETENTION_PERIOD.
--
-- Uso (no banco do Zabbix, com o zabbix_server parado):
--   USE zabbix; SOURCE /lab/sql/adapted_history_vtag.sql;
-- As tabelas InnoDB originais ficam como <tabela>_innodb (reverter:
-- sql/revert_history.sql).

-- RETENTION_PERIOD='31 DAYS' = período padrão de histórico do Zabbix (hk_history).

CREATE TABLE history_moose (
  itemid varchar(20) NOT NULL COMMENT 'TAG',
  clock  integer DEFAULT '0' NOT NULL,
  value  DOUBLE PRECISION DEFAULT '0.0000' NOT NULL,
  ns     integer DEFAULT '0' NOT NULL,
  ts     DATETIME(6) NOT NULL DEFAULT '1970-01-01 00:00:01',
  INDEX ts_idx (ts)
) ENGINE=MooseDB CHUNK_INTERVAL='1 DAY' RETENTION_PERIOD='31 DAYS' HOT_THRESHOLD='1 DAY';

CREATE TABLE history_uint_moose (
  itemid varchar(20) NOT NULL COMMENT 'TAG',
  clock  integer DEFAULT '0' NOT NULL,
  value  bigint unsigned DEFAULT '0' NOT NULL,
  ns     integer DEFAULT '0' NOT NULL,
  ts     DATETIME(6) NOT NULL DEFAULT '1970-01-01 00:00:01',
  INDEX ts_idx (ts)
) ENGINE=MooseDB CHUNK_INTERVAL='1 DAY' RETENTION_PERIOD='31 DAYS' HOT_THRESHOLD='1 DAY';

CREATE TABLE history_str_moose (
  itemid varchar(20) NOT NULL COMMENT 'TAG',
  clock  integer DEFAULT '0' NOT NULL,
  value  varchar(255) DEFAULT '' NOT NULL,
  ns     integer DEFAULT '0' NOT NULL,
  ts     DATETIME(6) NOT NULL DEFAULT '1970-01-01 00:00:01',
  INDEX ts_idx (ts)
) ENGINE=MooseDB CHUNK_INTERVAL='1 DAY' RETENTION_PERIOD='31 DAYS' HOT_THRESHOLD='1 DAY';

CREATE TABLE history_text_moose (
  itemid varchar(20) NOT NULL COMMENT 'TAG',
  clock  integer DEFAULT '0' NOT NULL,
  value  text NOT NULL,
  ns     integer DEFAULT '0' NOT NULL,
  ts     DATETIME(6) NOT NULL DEFAULT '1970-01-01 00:00:01',
  INDEX ts_idx (ts)
) ENGINE=MooseDB CHUNK_INTERVAL='1 DAY' RETENTION_PERIOD='31 DAYS' HOT_THRESHOLD='1 DAY';

CREATE TABLE history_log_moose (
  itemid     varchar(20) NOT NULL COMMENT 'TAG',
  clock      integer DEFAULT '0' NOT NULL,
  timestamp  integer DEFAULT '0' NOT NULL,
  source     varchar(64) DEFAULT '' NOT NULL,
  severity   integer DEFAULT '0' NOT NULL,
  value      text NOT NULL,
  logeventid integer DEFAULT '0' NOT NULL,
  ns         integer DEFAULT '0' NOT NULL,
  ts         DATETIME(6) NOT NULL DEFAULT '1970-01-01 00:00:01',
  INDEX ts_idx (ts)
) ENGINE=MooseDB CHUNK_INTERVAL='1 DAY' RETENTION_PERIOD='31 DAYS' HOT_THRESHOLD='1 DAY';

CREATE TABLE history_bin_moose (
  itemid varchar(20) NOT NULL COMMENT 'TAG',
  clock  integer DEFAULT '0' NOT NULL,
  ns     integer DEFAULT '0' NOT NULL,
  value  longblob NOT NULL,
  ts     DATETIME(6) NOT NULL DEFAULT '1970-01-01 00:00:01',
  INDEX ts_idx (ts)
) ENGINE=MooseDB CHUNK_INTERVAL='1 DAY' RETENTION_PERIOD='31 DAYS' HOT_THRESHOLD='1 DAY';

-- ts derivado de clock+ns sem depender de time_zone (DATETIME = UTC na engine).
CREATE TRIGGER history_vtag_ts BEFORE INSERT ON history_moose FOR EACH ROW
  SET NEW.ts = TIMESTAMP'1970-01-01 00:00:00' + INTERVAL NEW.clock SECOND + INTERVAL (NEW.ns DIV 1000) MICROSECOND;
CREATE TRIGGER history_uint_vtag_ts BEFORE INSERT ON history_uint_moose FOR EACH ROW
  SET NEW.ts = TIMESTAMP'1970-01-01 00:00:00' + INTERVAL NEW.clock SECOND + INTERVAL (NEW.ns DIV 1000) MICROSECOND;
CREATE TRIGGER history_str_vtag_ts BEFORE INSERT ON history_str_moose FOR EACH ROW
  SET NEW.ts = TIMESTAMP'1970-01-01 00:00:00' + INTERVAL NEW.clock SECOND + INTERVAL (NEW.ns DIV 1000) MICROSECOND;
CREATE TRIGGER history_text_vtag_ts BEFORE INSERT ON history_text_moose FOR EACH ROW
  SET NEW.ts = TIMESTAMP'1970-01-01 00:00:00' + INTERVAL NEW.clock SECOND + INTERVAL (NEW.ns DIV 1000) MICROSECOND;
CREATE TRIGGER history_log_vtag_ts BEFORE INSERT ON history_log_moose FOR EACH ROW
  SET NEW.ts = TIMESTAMP'1970-01-01 00:00:00' + INTERVAL NEW.clock SECOND + INTERVAL (NEW.ns DIV 1000) MICROSECOND;
CREATE TRIGGER history_bin_vtag_ts BEFORE INSERT ON history_bin_moose FOR EACH ROW
  SET NEW.ts = TIMESTAMP'1970-01-01 00:00:00' + INTERVAL NEW.clock SECOND + INTERVAL (NEW.ns DIV 1000) MICROSECOND;

-- Troca atômica: oficiais viram <t>_innodb, MooseDB assume o nome oficial.
RENAME TABLE history      TO history_innodb,      history_moose      TO history,
             history_uint TO history_uint_innodb, history_uint_moose TO history_uint,
             history_str  TO history_str_innodb,  history_str_moose  TO history_str,
             history_text TO history_text_innodb, history_text_moose TO history_text,
             history_log  TO history_log_innodb,  history_log_moose  TO history_log,
             history_bin  TO history_bin_innodb,  history_bin_moose  TO history_bin;
