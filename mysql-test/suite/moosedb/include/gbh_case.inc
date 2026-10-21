#
# One GROUP BY pushdown scope case: runs the query on the MooseDB table (the
# session has moosedb_gbh_debug=ON, so the verdict note "gbh: ..." is printed
# with the rows), then compares the whole answer with the InnoDB mirror.
#
# In : $q      query text, {T} stands for the table, no double quotes
#      $label  text echoed above the case
#      $mt/$it MooseDB table / InnoDB mirror
#
--let $qm= `SELECT REPLACE("$q", '{T}', '$mt')`
--echo # $label
--echo $qm
--eval $qm
--let $show= 0
--let $status= 0
--let $label= same as InnoDB
--source suite/moosedb/include/cmp_query.inc
