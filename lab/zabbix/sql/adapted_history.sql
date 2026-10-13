-- Variante "adaptada mínima" das tabelas de histórico do Zabbix 7.0 em MooseDB.
--
-- O que muda em relação ao esquema oficial (database/mysql/schema.sql):
--   * sem PRIMARY KEY (itemid,clock,ns)  — a engine não aceita PK/UNIQUE;
--   * itemid vira TAG (identidade da série);
--   * coluna extra ts DATETIME(6) NOT NULL + INDEX(ts) — eixo de tempo exigido
--     pela engine (clock INT continua existindo, com o mesmo nome e tipo);
--   * ts é preenchido por trigger BEFORE INSERT a partir de clock+ns
--     (independente de time_zone), porque o Zabbix só envia (itemid,clock,ns,value).
--
-- O Zabbix continua funcionando sem mudança de código (ver README), mas:
--   * consultas filtram por `clock`, não por `ts` => sem poda de chunks por tempo;
--   * itemid é BIGINT => sem pushdown de TAG (só strings têm pushdown);
--   * housekeeper de histórico PRECISA estar desligado (DELETE ... WHERE falha);
--     a expiração fica com RETENTION_PERIOD.
--
-- Uso (no banco do Zabbix, com o zabbix_server parado):
--   USE zabbix; SOURCE /lab/sql/adapted_history.sql;
-- As tabelas InnoDB originais ficam como <tabela>_innodb (reverter:
-- sql/revert_history.sql).

-- RETENTION_PERIOD='31 DAYS' = período padrão de histórico do Zabbix (hk_history).

CREATE TABLE history_moose (
  itemid bigint unsigned NOT NULL COMMENT 'TAG',
  clock  integer DEFAULT '0' NOT NULL,
  value  DOUBLE PRECISION DEFAULT '0.0000' NOT NULL,
  ns     integer DEFAULT '0' NOT NULL,
  ts     DATETIME(6) NOT NULL DEFAULT '1970-01-01 00:00:01',
  INDEX ts_idx (ts)
) ENGINE=MooseDB CHUNK_INTERVAL='1 DAY' RETENTION_PERIOD='31 DAYS' HOT_THRESHOLD='1 DAY';

CREATE TABLE history_uint_moose (
  itemid bigint unsigned NOT NULL COMMENT 'TAG',
  clock  integer DEFAULT '0' NOT NULL,
  value  bigint unsigned DEFAULT '0' NOT NULL,
  ns     integer DEFAULT '0' NOT NULL,
  ts     DATETIME(6) NOT NULL DEFAULT '1970-01-01 00:00:01',
  INDEX ts_idx (ts)
) ENGINE=MooseDB CHUNK_INTERVAL='1 DAY' RETENTION_PERIOD='31 DAYS' HOT_THRESHOLD='1 DAY';

CREATE TABLE history_str_moose (
  itemid bigint unsigned NOT NULL COMMENT 'TAG',
  clock  integer DEFAULT '0' NOT NULL,
  value  varchar(255) DEFAULT '' NOT NULL,
  ns     integer DEFAULT '0' NOT NULL,
  ts     DATETIME(6) NOT NULL DEFAULT '1970-01-01 00:00:01',
  INDEX ts_idx (ts)
) ENGINE=MooseDB CHUNK_INTERVAL='1 DAY' RETENTION_PERIOD='31 DAYS' HOT_THRESHOLD='1 DAY';

CREATE TABLE history_text_moose (
  itemid bigint unsigned NOT NULL COMMENT 'TAG',
  clock  integer DEFAULT '0' NOT NULL,
  value  text NOT NULL,
  ns     integer DEFAULT '0' NOT NULL,
  ts     DATETIME(6) NOT NULL DEFAULT '1970-01-01 00:00:01',
  INDEX ts_idx (ts)
) ENGINE=MooseDB CHUNK_INTERVAL='1 DAY' RETENTION_PERIOD='31 DAYS' HOT_THRESHOLD='1 DAY';

CREATE TABLE history_log_moose (
  itemid     bigint unsigned NOT NULL COMMENT 'TAG',
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
  itemid bigint unsigned NOT NULL COMMENT 'TAG',
  clock  integer DEFAULT '0' NOT NULL,
  ns     integer DEFAULT '0' NOT NULL,
  value  longblob NOT NULL,
  ts     DATETIME(6) NOT NULL DEFAULT '1970-01-01 00:00:01',
  INDEX ts_idx (ts)
) ENGINE=MooseDB CHUNK_INTERVAL='1 DAY' RETENTION_PERIOD='31 DAYS' HOT_THRESHOLD='1 DAY';

-- ts derivado de clock+ns sem depender de time_zone (DATETIME = UTC na engine).
CREATE TRIGGER history_moose_ts BEFORE INSERT ON history_moose FOR EACH ROW
  SET NEW.ts = TIMESTAMP'1970-01-01 00:00:00' + INTERVAL NEW.clock SECOND + INTERVAL (NEW.ns DIV 1000) MICROSECOND;
CREATE TRIGGER history_uint_moose_ts BEFORE INSERT ON history_uint_moose FOR EACH ROW
  SET NEW.ts = TIMESTAMP'1970-01-01 00:00:00' + INTERVAL NEW.clock SECOND + INTERVAL (NEW.ns DIV 1000) MICROSECOND;
CREATE TRIGGER history_str_moose_ts BEFORE INSERT ON history_str_moose FOR EACH ROW
  SET NEW.ts = TIMESTAMP'1970-01-01 00:00:00' + INTERVAL NEW.clock SECOND + INTERVAL (NEW.ns DIV 1000) MICROSECOND;
CREATE TRIGGER history_text_moose_ts BEFORE INSERT ON history_text_moose FOR EACH ROW
  SET NEW.ts = TIMESTAMP'1970-01-01 00:00:00' + INTERVAL NEW.clock SECOND + INTERVAL (NEW.ns DIV 1000) MICROSECOND;
CREATE TRIGGER history_log_moose_ts BEFORE INSERT ON history_log_moose FOR EACH ROW
  SET NEW.ts = TIMESTAMP'1970-01-01 00:00:00' + INTERVAL NEW.clock SECOND + INTERVAL (NEW.ns DIV 1000) MICROSECOND;
CREATE TRIGGER history_bin_moose_ts BEFORE INSERT ON history_bin_moose FOR EACH ROW
  SET NEW.ts = TIMESTAMP'1970-01-01 00:00:00' + INTERVAL NEW.clock SECOND + INTERVAL (NEW.ns DIV 1000) MICROSECOND;

-- Troca atômica: oficiais viram <t>_innodb, MooseDB assume o nome oficial.
RENAME TABLE history      TO history_innodb,      history_moose      TO history,
             history_uint TO history_uint_innodb, history_uint_moose TO history_uint,
             history_str  TO history_str_innodb,  history_str_moose  TO history_str,
             history_text TO history_text_innodb, history_text_moose TO history_text,
             history_log  TO history_log_innodb,  history_log_moose  TO history_log,
             history_bin  TO history_bin_innodb,  history_bin_moose  TO history_bin;
