#
# One composite-index query with a value predicate: compares the MooseDB
# answer with the InnoDB mirror's (include/cmp_query.inc, row order included)
# and says whether the entries were tested by the zone maps / Blooms.
#
# In : $q, $label, $mt, $it   as cmp_query.inc
#      $expect                1 = Moosedb_prune_entries_tested must grow, 0 = must not
#
--disable_query_log
--let $e0= `SELECT VARIABLE_VALUE FROM information_schema.GLOBAL_STATUS WHERE VARIABLE_NAME = 'MOOSEDB_PRUNE_ENTRIES_TESTED'`
--source suite/moosedb/include/cmp_query.inc
--disable_query_log
--let $e1= `SELECT VARIABLE_VALUE FROM information_schema.GLOBAL_STATUS WHERE VARIABLE_NAME = 'MOOSEDB_PRUNE_ENTRIES_TESTED'`
--let $got= `SELECT ($e1 > $e0)`
--let $ok= `SELECT IF($got = $expect, 'as expected', 'UNEXPECTED')`
--echo # tested=$got expected=$expect: $ok
--enable_query_log
