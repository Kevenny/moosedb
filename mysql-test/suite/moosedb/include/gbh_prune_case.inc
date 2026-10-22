#
# One range-pruning case with the GROUP BY pushdown: the answer must be the
# same OFF / ON / InnoDB (include/gbh_diff.inc), and a separate ON session says
# whether zone maps pruned entries (Moosedb_prune_* moved).
#
# In : $q, $label, $refused   as gbh_diff.inc
#      $expect                1 = the ON run must prune, 0 = must not
#
--source suite/moosedb/include/gbh_diff.inc
--disable_query_log
--let $gp_p0= `SELECT SUM(VARIABLE_VALUE) FROM information_schema.GLOBAL_STATUS WHERE VARIABLE_NAME IN ('MOOSEDB_PRUNE_ZONE_PRUNED', 'MOOSEDB_PRUNE_BLOOM_PRUNED')`
--let $gp_s0= `SELECT VARIABLE_VALUE FROM information_schema.GLOBAL_STATUS WHERE VARIABLE_NAME = 'MOOSEDB_PRUNE_ROWS_SKIPPED'`
--exec $gd_cl --init-command="SET SESSION moosedb_group_by_pushdown=1" test -e "$q" > /dev/null 2>&1
--let $gp_p1= `SELECT SUM(VARIABLE_VALUE) FROM information_schema.GLOBAL_STATUS WHERE VARIABLE_NAME IN ('MOOSEDB_PRUNE_ZONE_PRUNED', 'MOOSEDB_PRUNE_BLOOM_PRUNED')`
--let $gp_s1= `SELECT VARIABLE_VALUE FROM information_schema.GLOBAL_STATUS WHERE VARIABLE_NAME = 'MOOSEDB_PRUNE_ROWS_SKIPPED'`
--let $gp_got= `SELECT ($gp_p1 > $gp_p0 AND $gp_s1 > $gp_s0)`
--let $gp_ok= `SELECT IF($gp_got = $expect, 'as expected', 'UNEXPECTED')`
--echo # pruned=$gp_got expected=$expect: $gp_ok
--enable_query_log
