#
# gbh_collation.test helper: builds table t (MooseDB, database test) and its
# InnoDB twin (database mirror) with the TAG column `tg` and the plain column
# `pc` in character set $cs and collation $coll, then runs the differential
# GROUP BY cases on them.
#
# In : $cs, $coll   character set / collation of tg and pc
#      $exact       1 = the collation is exact (equal strings have equal
#                   bytes): the key itself can be shown and compared
#
--disable_query_log
DROP TABLE IF EXISTS test.t;
DROP TABLE IF EXISTS mirror.t;
eval CREATE TABLE test.t (
  ts DATETIME NOT NULL,
  tg VARCHAR(16) CHARACTER SET $cs COLLATE $coll NOT NULL COMMENT 'TAG',
  rg VARCHAR(4) NOT NULL COMMENT 'TAG',
  pc VARCHAR(16) CHARACTER SET $cs COLLATE $coll,
  v  INT,
  INDEX(ts)
) ENGINE=MooseDB CHUNK_INTERVAL='1 HOUR';
# 13 spellings (case, accents, trailing space) x 3 regions: many series per value
INSERT INTO test.t SELECT '2024-06-01' + INTERVAL seq * 30 SECOND,
       ELT(1 + seq MOD 13, 'a','A','á','Á','b','B','a ','ab','AB','ß','ss','z','Z'),
       CONCAT('r', seq MOD 3),
       IF(seq MOD 17 = 0, NULL,
          ELT(1 + (seq DIV 3) MOD 13, 'a','A','á','Á','b','B','a ','ab','AB','ß','ss','z','Z')),
       IF(seq MOD 11 = 0, NULL, seq)
  FROM seq_0_to_1999;
FLUSH TABLES t;
INSERT INTO test.t SELECT '2024-06-02' + INTERVAL seq * 30 SECOND,
       ELT(1 + seq MOD 13, 'a','A','á','Á','b','B','a ','ab','AB','ß','ss','z','Z'),
       CONCAT('r', seq MOD 3),
       ELT(1 + (seq DIV 3) MOD 13, 'a','A','á','Á','b','B','a ','ab','AB','ß','ss','z','Z'),
       seq
  FROM seq_2000_to_2399;
eval CREATE TABLE mirror.t LIKE test.t;
ALTER TABLE mirror.t ENGINE=InnoDB;
INSERT INTO mirror.t SELECT * FROM test.t;
--enable_query_log

--echo ##### $coll (exact=$exact)
--let $label= $coll: TAG key (many series per value), key not shown
--let $q= SELECT COUNT(*) c, SUM(v) s, MIN(v) mn, MAX(v) mx, AVG(v) a FROM t GROUP BY tg ORDER BY tg
--source suite/moosedb/include/gbh_diff.inc

--let $label= $coll: plain VARCHAR key with NULLs
--let $q= SELECT COUNT(*) c, SUM(v) s, MIN(ts) t0, MAX(ts) t1 FROM t GROUP BY pc ORDER BY pc
--source suite/moosedb/include/gbh_diff.inc

--let $label= $coll: both keys, ordered by an aggregate then by the keys
--let $q= SELECT COUNT(*) c, SUM(v) s FROM t GROUP BY tg, pc ORDER BY c DESC, tg DESC, pc LIMIT 12 OFFSET 3
--source suite/moosedb/include/gbh_diff.inc

--let $label= $coll: TAG equality under the collation, grouped by the plain column
--let $q= SELECT COUNT(*) c, SUM(v) s FROM t WHERE tg = 'a' GROUP BY pc ORDER BY pc
--source suite/moosedb/include/gbh_diff.inc

--let $label= $coll: TAG IN under the collation, two TAG-derived keys
--let $q= SELECT COUNT(*) c FROM t WHERE tg IN ('B', 'ss') AND rg = 'r1' GROUP BY tg, pc ORDER BY tg, pc
--source suite/moosedb/include/gbh_diff.inc

--let $label= $coll: GROUP BY the region TAG (the value 'r*' is exact) with the string TAG as hidden key
--let $q= SELECT rg, COUNT(*) c, SUM(v) s FROM t GROUP BY rg, tg ORDER BY rg, tg LIMIT 9
--source suite/moosedb/include/gbh_diff.inc

--let $label= $coll: HAVING on the merged count
--let $q= SELECT COUNT(*) c FROM t GROUP BY tg HAVING c > 150 ORDER BY c, tg
--source suite/moosedb/include/gbh_diff.inc

if ($exact)
{
  --let $label= $coll: the key itself (exact collation)
  --let $q= SELECT tg, COUNT(*) c, SUM(v) s FROM t GROUP BY tg ORDER BY tg
  --source suite/moosedb/include/gbh_diff.inc

  --let $label= $coll: plain column key itself, DESC
  --let $q= SELECT pc, COUNT(*) c FROM t GROUP BY pc ORDER BY pc DESC
  --source suite/moosedb/include/gbh_diff.inc

  --let $label= $coll: both keys shown
  --let $q= SELECT tg, pc, COUNT(*) c FROM t GROUP BY tg, pc ORDER BY tg, pc LIMIT 15
  --source suite/moosedb/include/gbh_diff.inc
}

--echo # distinct groups: the engine merges exactly as the server does
--let $a= `SELECT COUNT(*) FROM (SELECT 1 FROM test.t GROUP BY tg) x`
--let $b= `SELECT COUNT(*) FROM (SELECT 1 FROM mirror.t GROUP BY tg) x`
if ($a != $b)
{
  --die the TAG groups of the two engines differ
}
DROP TABLE test.t;
DROP TABLE mirror.t;
