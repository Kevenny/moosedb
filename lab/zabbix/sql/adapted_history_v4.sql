-- Variante "adaptada v4": o DDL OFICIAL do Zabbix 7.0 para history*, com PRIMARY KEY
-- (itemid,clock,ns) verificada de verdade pela engine (unicidade real: ER_DUP_ENTRY).
-- Únicas diferenças em relação ao DDL oficial:
--   * COMMENT 'TAG' em itemid (identidade da série);
--   * ENGINE=MooseDB TIMESTAMP_COLUMN='clock' (+ as opções de chunk/retenção do v3).
-- Não há INDEX (clock) nem INDEX (itemid, clock): a PK é o índice composto
-- (itemid, clock, ns) e também fornece o eixo de tempo. Ela serve:
--   * itemid=X AND clock BETWEEN/>/<= ... (ref/range), MAX(clock) por item;
--   * ORDER BY clock DESC, ns DESC LIMIT n sem filesort (getLastValuesFromSqlWithPk);
--   * itemid+clock+ns exatos (const/eq_ref em joins).
-- Limitações: REPLACE / INSERT ... ON DUPLICATE KEY UPDATE sobre chave existente
-- são recusados (a engine não tem UPDATE); o DELETE do housekeeper/history.clear funciona (tombstones);
-- a expiração por idade é mais barata por RETENTION_PERIOD.
--
-- Uso (no banco do Zabbix, com o zabbix_server parado):
--   USE zabbix; SOURCE /lab/sql/adapted_history_v4.sql;
-- As InnoDB originais ficam como <tabela>_innodb (reverter: sql/revert_history.sql).

CREATE TABLE history_moose (
  itemid                   bigint unsigned                           NOT NULL COMMENT 'TAG',
  clock                    integer         DEFAULT '0'               NOT NULL,
  value                    DOUBLE PRECISION DEFAULT '0.0000'         NOT NULL,
  ns                       integer         DEFAULT '0'               NOT NULL,
  PRIMARY KEY (itemid,clock,ns)
) ENGINE=MooseDB TIMESTAMP_COLUMN='clock' CHUNK_INTERVAL='1 DAY' RETENTION_PERIOD='31 DAYS' HOT_THRESHOLD='1 DAY';

CREATE TABLE history_uint_moose (
  itemid                   bigint unsigned                           NOT NULL COMMENT 'TAG',
  clock                    integer         DEFAULT '0'               NOT NULL,
  value                    bigint unsigned DEFAULT '0'               NOT NULL,
  ns                       integer         DEFAULT '0'               NOT NULL,
  PRIMARY KEY (itemid,clock,ns)
) ENGINE=MooseDB TIMESTAMP_COLUMN='clock' CHUNK_INTERVAL='1 DAY' RETENTION_PERIOD='31 DAYS' HOT_THRESHOLD='1 DAY';

CREATE TABLE history_str_moose (
  itemid                   bigint unsigned                           NOT NULL COMMENT 'TAG',
  clock                    integer         DEFAULT '0'               NOT NULL,
  value                    varchar(255)    DEFAULT ''                NOT NULL,
  ns                       integer         DEFAULT '0'               NOT NULL,
  PRIMARY KEY (itemid,clock,ns)
) ENGINE=MooseDB TIMESTAMP_COLUMN='clock' CHUNK_INTERVAL='1 DAY' RETENTION_PERIOD='31 DAYS' HOT_THRESHOLD='1 DAY';

CREATE TABLE history_text_moose (
  itemid                   bigint unsigned                           NOT NULL COMMENT 'TAG',
  clock                    integer         DEFAULT '0'               NOT NULL,
  value                    text                                      NOT NULL,
  ns                       integer         DEFAULT '0'               NOT NULL,
  PRIMARY KEY (itemid,clock,ns)
) ENGINE=MooseDB TIMESTAMP_COLUMN='clock' CHUNK_INTERVAL='1 DAY' RETENTION_PERIOD='31 DAYS' HOT_THRESHOLD='1 DAY';

CREATE TABLE history_log_moose (
  itemid                   bigint unsigned                           NOT NULL COMMENT 'TAG',
  clock                    integer         DEFAULT '0'               NOT NULL,
  timestamp                integer         DEFAULT '0'               NOT NULL,
  source                   varchar(64)     DEFAULT ''                NOT NULL,
  severity                 integer         DEFAULT '0'               NOT NULL,
  value                    text                                      NOT NULL,
  logeventid               integer         DEFAULT '0'               NOT NULL,
  ns                       integer         DEFAULT '0'               NOT NULL,
  PRIMARY KEY (itemid,clock,ns)
) ENGINE=MooseDB TIMESTAMP_COLUMN='clock' CHUNK_INTERVAL='1 DAY' RETENTION_PERIOD='31 DAYS' HOT_THRESHOLD='1 DAY';

CREATE TABLE history_bin_moose (
  itemid                   bigint unsigned                           NOT NULL COMMENT 'TAG',
  clock                    integer         DEFAULT '0'               NOT NULL,
  ns                       integer         DEFAULT '0'               NOT NULL,
  value                    longblob                                  NOT NULL,
  PRIMARY KEY (itemid,clock,ns)
) ENGINE=MooseDB TIMESTAMP_COLUMN='clock' CHUNK_INTERVAL='1 DAY' RETENTION_PERIOD='31 DAYS' HOT_THRESHOLD='1 DAY';

-- Troca atômica: oficiais viram <t>_innodb, MooseDB assume o nome oficial.
RENAME TABLE history      TO history_innodb,      history_moose      TO history,
             history_uint TO history_uint_innodb, history_uint_moose TO history_uint,
             history_str  TO history_str_innodb,  history_str_moose  TO history_str,
             history_text TO history_text_innodb, history_text_moose TO history_text,
             history_log  TO history_log_innodb,  history_log_moose  TO history_log,
             history_bin  TO history_bin_innodb,  history_bin_moose  TO history_bin;
