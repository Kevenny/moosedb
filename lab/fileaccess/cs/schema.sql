-- ColumnStore (contêiner fileacc-columnstore, banco fileacc). Mesmas colunas das tabelas de sql/schema.sql.
-- Sem índices: o ColumnStore não tem índice secundário; poda por extent map (min/max por extent de 8 M linhas)
-- e lê só as colunas da consulta. A carga em ordem de ts deixa o extent map de ts bem ordenado.
CREATE TABLE evento_cs (
  ts         DATETIME(6)  NOT NULL,
  host       VARCHAR(32)  NOT NULL,
  process    VARCHAR(63)  NOT NULL,
  user_name  VARCHAR(32)  NOT NULL,
  folder     VARCHAR(255) NOT NULL,
  file       VARCHAR(512) NOT NULL,
  op         VARCHAR(8)   NOT NULL,
  bytes      BIGINT       NOT NULL,
  latency_us INT          NOT NULL
) ENGINE=ColumnStore;
