#
# gbh_diff.inc plus the shortcuts the ON run took: how many filters were sent to
# the core as exact value sets and whether the core did the top-N
# (status Moosedb_gbh_exact_filters / Moosedb_gbh_topn, deltas over the run).
#
--disable_query_log
--let $sc_e0= `SELECT VARIABLE_VALUE FROM information_schema.GLOBAL_STATUS WHERE VARIABLE_NAME = 'MOOSEDB_GBH_EXACT_FILTERS'`
--let $sc_t0= `SELECT VARIABLE_VALUE FROM information_schema.GLOBAL_STATUS WHERE VARIABLE_NAME = 'MOOSEDB_GBH_TOPN'`
--enable_query_log
--source suite/moosedb/include/gbh_diff.inc
--disable_query_log
--let $sc_e1= `SELECT VARIABLE_VALUE - $sc_e0 FROM information_schema.GLOBAL_STATUS WHERE VARIABLE_NAME = 'MOOSEDB_GBH_EXACT_FILTERS'`
--let $sc_t1= `SELECT VARIABLE_VALUE - $sc_t0 FROM information_schema.GLOBAL_STATUS WHERE VARIABLE_NAME = 'MOOSEDB_GBH_TOPN'`
--echo # shortcuts taken: exact filters=$sc_e1 core top-N=$sc_t1
--enable_query_log
