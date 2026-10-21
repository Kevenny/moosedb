-- ClickHouse: MergeTree particionado por dia (poda de janela de 1 dia/1 hora) e ordenado por (host, ts).
-- LowCardinality em host/process/user/op; folder (50 mil) e file (2 M) ficam String.
CREATE DATABASE IF NOT EXISTS fileacc;
CREATE TABLE fileacc.evento (
  ts         DateTime64(6, 'UTC'),
  host       LowCardinality(String),
  process    LowCardinality(String),
  user_name  LowCardinality(String),
  folder     String,
  file       String,
  op         LowCardinality(String),
  bytes      Int64,
  latency_us Int32
) ENGINE = MergeTree
PARTITION BY toDate(ts)
ORDER BY (host, ts);
