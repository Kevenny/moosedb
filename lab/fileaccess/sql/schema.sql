-- Esquemas MariaDB (banco fileacc). Mesmas colunas nas três tabelas; ver README §Suposições.
-- Collation utf8mb4_bin (padrão do servidor do lab) = comparação por bytes, igual ao ClickHouse.

-- InnoDB "típico": PK substituta + (ts), (host,ts), (process,ts) (~ os 2,1 GB de índice do relatório).
-- process é VARCHAR(63) e não 64: na MooseDB uma TAG VARCHAR(64) utf8mb4 (256 bytes de chave, > 255)
-- é rejeitada em índice composto como "index prefixes are not supported" (ver README, achados).
CREATE TABLE evento_innodb (
  id         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  ts         DATETIME(6)  NOT NULL,
  host       VARCHAR(32)  NOT NULL,
  process    VARCHAR(63)  NOT NULL,
  user_name  VARCHAR(32)  NOT NULL,
  folder     VARCHAR(255) NOT NULL,
  file       VARCHAR(512) NOT NULL,
  op         VARCHAR(8)   NOT NULL,
  bytes      BIGINT       NOT NULL,
  latency_us INT          NOT NULL,
  PRIMARY KEY (id),
  KEY ix_ts (ts),
  KEY ix_host_ts (host, ts),
  KEY ix_process_ts (process, ts)
) ENGINE=InnoDB;

-- Variante com índice em file (sustenta "quem acessa um arquivo"): custa ~2x de índice e de carga.
CREATE TABLE evento_innodb_f LIKE evento_innodb;
ALTER TABLE evento_innodb_f ADD KEY ix_file (file);

-- MooseDB (pedido): host e process TAG; folder/file NÃO são TAG (alta cardinalidade).
-- Índices: o do tempo (obrigatório) + compostos TAG(s)+tempo. Sem id (a engine não aceita AUTO_INCREMENT).
CREATE TABLE evento_moose (
  ts         DATETIME(6)  NOT NULL,
  host       VARCHAR(32)  NOT NULL COMMENT 'TAG',
  process    VARCHAR(63)  NOT NULL COMMENT 'TAG',
  user_name  VARCHAR(32)  NOT NULL,
  folder     VARCHAR(255) NOT NULL,
  file       VARCHAR(512) NOT NULL,
  op         VARCHAR(8)   NOT NULL,
  bytes      BIGINT       NOT NULL,
  latency_us INT          NOT NULL,
  INDEX ix_ts (ts),
  INDEX ix_host_ts (host, ts),
  INDEX ix_process_ts (process, ts),
  INDEX ix_host_process_ts (host, process, ts)
) ENGINE=MooseDB TIMESTAMP_COLUMN='ts' CHUNK_INTERVAL='1 DAY' COMPRESSION='ZSTD' HOT_THRESHOLD='1 DAY';

-- Variante de controle: só host como TAG (menos séries: ~2 mil em vez de ~70 mil).
CREATE TABLE evento_moose_h (
  ts         DATETIME(6)  NOT NULL,
  host       VARCHAR(32)  NOT NULL COMMENT 'TAG',
  process    VARCHAR(63)  NOT NULL,
  user_name  VARCHAR(32)  NOT NULL,
  folder     VARCHAR(255) NOT NULL,
  file       VARCHAR(512) NOT NULL,
  op         VARCHAR(8)   NOT NULL,
  bytes      BIGINT       NOT NULL,
  latency_us INT          NOT NULL,
  INDEX ix_ts (ts),
  INDEX ix_host_ts (host, ts)
) ENGINE=MooseDB TIMESTAMP_COLUMN='ts' CHUNK_INTERVAL='1 DAY' COMPRESSION='ZSTD' HOT_THRESHOLD='1 DAY';
