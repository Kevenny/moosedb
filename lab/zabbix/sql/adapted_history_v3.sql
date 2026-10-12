-- Variante "adaptada v3" (recomendada após o pushdown numérico e o eixo inteiro):
--   * itemid BIGINT UNSIGNED TAG — IN/= sem aspas (server, dbConditionInt) e
--     com aspas (frontend) passam a podar séries;
--   * clock INT é o próprio eixo de tempo (TIMESTAMP_COLUMN='clock', segundos
--     epoch UTC) — sem coluna ts e sem trigger;
--   * ns continua coluna comum (ORDER BY clock,ns via filesort).
-- Restrições que permanecem: sem PRIMARY KEY (itemid,clock,ns) (MooseDB só
-- aceita índice simples na coluna de tempo), housekeeper de histórico
-- desligado, expiração por RETENTION_PERIOD.
--
-- Requer a engine com os itens 1 e 2 (TAG inteiro com pushdown, eixo inteiro).
-- Uso (no banco do Zabbix, com o zabbix_server parado):
--   USE zabbix; SOURCE /lab/sql/adapted_history_v3.sql;
-- As InnoDB originais ficam como <tabela>_innodb (reverter: sql/revert_history.sql).

CREATE TABLE history_moose (
  itemid bigint unsigned NOT NULL COMMENT 'TAG',
  clock  integer DEFAULT '0' NOT NULL,
  value  DOUBLE PRECISION DEFAULT '0.0000' NOT NULL,
  ns     integer DEFAULT '0' NOT NULL,
  INDEX clock_idx (clock)
) ENGINE=MooseDB TIMESTAMP_COLUMN='clock' CHUNK_INTERVAL='1 DAY' RETENTION_PERIOD='31 DAYS' HOT_THRESHOLD='1 DAY';

CREATE TABLE history_uint_moose (
  itemid bigint unsigned NOT NULL COMMENT 'TAG',
  clock  integer DEFAULT '0' NOT NULL,
  value  bigint unsigned DEFAULT '0' NOT NULL,
  ns     integer DEFAULT '0' NOT NULL,
  INDEX clock_idx (clock)
) ENGINE=MooseDB TIMESTAMP_COLUMN='clock' CHUNK_INTERVAL='1 DAY' RETENTION_PERIOD='31 DAYS' HOT_THRESHOLD='1 DAY';

CREATE TABLE history_str_moose (
  itemid bigint unsigned NOT NULL COMMENT 'TAG',
  clock  integer DEFAULT '0' NOT NULL,
  value  varchar(255) DEFAULT '' NOT NULL,
  ns     integer DEFAULT '0' NOT NULL,
  INDEX clock_idx (clock)
) ENGINE=MooseDB TIMESTAMP_COLUMN='clock' CHUNK_INTERVAL='1 DAY' RETENTION_PERIOD='31 DAYS' HOT_THRESHOLD='1 DAY';

CREATE TABLE history_text_moose (
  itemid bigint unsigned NOT NULL COMMENT 'TAG',
  clock  integer DEFAULT '0' NOT NULL,
  value  text NOT NULL,
  ns     integer DEFAULT '0' NOT NULL,
  INDEX clock_idx (clock)
) ENGINE=MooseDB TIMESTAMP_COLUMN='clock' CHUNK_INTERVAL='1 DAY' RETENTION_PERIOD='31 DAYS' HOT_THRESHOLD='1 DAY';

CREATE TABLE history_log_moose (
  itemid     bigint unsigned NOT NULL COMMENT 'TAG',
  clock      integer DEFAULT '0' NOT NULL,
  timestamp  integer DEFAULT '0' NOT NULL,
  source     varchar(64) DEFAULT '' NOT NULL,
  severity   integer DEFAULT '0' NOT NULL,
  value      text NOT NULL,
  logeventid integer DEFAULT '0' NOT NULL,
  ns         integer DEFAULT '0' NOT NULL,
  INDEX clock_idx (clock)
) ENGINE=MooseDB TIMESTAMP_COLUMN='clock' CHUNK_INTERVAL='1 DAY' RETENTION_PERIOD='31 DAYS' HOT_THRESHOLD='1 DAY';

CREATE TABLE history_bin_moose (
  itemid bigint unsigned NOT NULL COMMENT 'TAG',
  clock  integer DEFAULT '0' NOT NULL,
  ns     integer DEFAULT '0' NOT NULL,
  value  longblob NOT NULL,
  INDEX clock_idx (clock)
) ENGINE=MooseDB TIMESTAMP_COLUMN='clock' CHUNK_INTERVAL='1 DAY' RETENTION_PERIOD='31 DAYS' HOT_THRESHOLD='1 DAY';

-- Troca atômica: oficiais viram <t>_innodb, MooseDB assume o nome oficial.
RENAME TABLE history      TO history_innodb,      history_moose      TO history,
             history_uint TO history_uint_innodb, history_uint_moose TO history_uint,
             history_str  TO history_str_innodb,  history_str_moose  TO history_str,
             history_text TO history_text_innodb, history_text_moose TO history_text,
             history_log  TO history_log_innodb,  history_log_moose  TO history_log,
             history_bin  TO history_bin_innodb,  history_bin_moose  TO history_bin;
