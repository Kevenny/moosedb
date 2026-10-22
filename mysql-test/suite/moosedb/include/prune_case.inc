#
# One pruning case: runs the query shape on the MooseDB table $mt and on its
# InnoDB mirror $it (include/mirror_cmp.inc, verdict SAME / DIFFERENT) and
# says whether the Moosedb_prune_* counters moved during the MooseDB run.
#
# In : $mt, $it, $sel, $tail   as mirror_cmp.inc
#      $expect                 1 = zone maps / Blooms must prune, 0 = must not
#
--disable_query_log
--let $p0= `SELECT SUM(VARIABLE_VALUE) FROM information_schema.GLOBAL_STATUS WHERE VARIABLE_NAME IN ('MOOSEDB_PRUNE_ZONE_PRUNED', 'MOOSEDB_PRUNE_BLOOM_PRUNED')`
--let $s0= `SELECT VARIABLE_VALUE FROM information_schema.GLOBAL_STATUS WHERE VARIABLE_NAME = 'MOOSEDB_PRUNE_ROWS_SKIPPED'`
--source suite/moosedb/include/mirror_cmp.inc
--disable_query_log
--let $p1= `SELECT SUM(VARIABLE_VALUE) FROM information_schema.GLOBAL_STATUS WHERE VARIABLE_NAME IN ('MOOSEDB_PRUNE_ZONE_PRUNED', 'MOOSEDB_PRUNE_BLOOM_PRUNED')`
--let $s1= `SELECT VARIABLE_VALUE FROM information_schema.GLOBAL_STATUS WHERE VARIABLE_NAME = 'MOOSEDB_PRUNE_ROWS_SKIPPED'`
--let $got= `SELECT ($p1 > $p0 AND $s1 > $s0)`
--let $ok= `SELECT IF($got = $expect, 'as expected', 'UNEXPECTED')`
--echo # pruned=$got expected=$expect: $ok
--enable_query_log
