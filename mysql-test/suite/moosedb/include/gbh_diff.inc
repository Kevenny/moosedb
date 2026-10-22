#
# Differential check of the GROUP BY pushdown: one statement is run in
# separate client sessions as
#   OFF     moosedb_group_by_pushdown=0 on the MooseDB table (test.g)
#   ON      moosedb_group_by_pushdown=1 on the MooseDB table
#   mirror  the same query on the InnoDB copy (database `mirror`)
# and the three complete outputs (rows, warnings, column metadata) must be
# identical. The status variable Moosedb_gbh_queries shows whether the engine
# computed the GROUP BY itself in each run.
#
# In : $q       statement(s); no double quotes, no dollar signs
#      $label   text echoed above the case
#      $pre     statements run first in the same session (may be empty)
#      $sorted  1 = no total ORDER BY: all three outputs are sorted as text
#      $either  1 = the engine may or may not run it (the server can answer by itself); the output must be the same
#      $nomaxlen  1 = compare without the Max_length of the column metadata (sticky)
#      $expect  1 (default) = the ON run must be done by the engine, 0 = refused
#
--disable_query_log
--echo # $label
--echo # $q
--let $gd_dir= $MYSQLTEST_VARDIR/tmp
--let $gd_cl= $MYSQL -uroot -S$MASTER_MYSOCK --table --column-type-info --show-warnings
--let $gd_post= cat
--let $gd_norm_ml=
# $nomaxlen (kept until reset by the test) = 1 drops the Max_length line of EVERY column of the result metadata (row data, names, types, flags, lengths are still compared; nothing narrower is possible with sed, and the bytes of PAD keys are checked by gbh_pad_scalar with cat -A). Reason: Max_length: a binary PAD SPACE key shows one value of its class ('a' or 'a  '), which one is the server's plan, and only the length of the metadata can tell.
if ($nomaxlen)
{
  --let $gd_norm_ml= -e '/^Max_length:/d'
}
# Only NOT_NULL and UNSIGNED of the column flags are compared: the others (BINARY, NUM, NO_DEFAULT_VALUE...) depend on whether the server sends a tmp-table field or the original item.
--let $gd_norm= sed -e '/^Flags:/{s/NOT_NULL/@N/g;s/UNSIGNED/@U/g;s/[A-Z_][A-Z_]*//g;s/@N/NOT_NULL/g;s/@U/UNSIGNED/g;s/  */ /g}'
if ($sorted)
{
  --let $gd_post= sort
}
--let $gd_c0= `SELECT VARIABLE_VALUE FROM information_schema.GLOBAL_STATUS WHERE VARIABLE_NAME = 'MOOSEDB_GBH_QUERIES'`
--exec $gd_cl --init-command="SET SESSION moosedb_group_by_pushdown=0" test -e "$pre $q"  2>&1 | $gd_norm $gd_norm_ml | $gd_post > $gd_dir/gbh_off.txt
--let $gd_c1= `SELECT VARIABLE_VALUE FROM information_schema.GLOBAL_STATUS WHERE VARIABLE_NAME = 'MOOSEDB_GBH_QUERIES'`
--exec $gd_cl --init-command="SET SESSION moosedb_group_by_pushdown=1" test -e "$pre $q"  2>&1 | $gd_norm $gd_norm_ml | $gd_post > $gd_dir/gbh_on.txt
--let $gd_c2= `SELECT VARIABLE_VALUE FROM information_schema.GLOBAL_STATUS WHERE VARIABLE_NAME = 'MOOSEDB_GBH_QUERIES'`
--exec $gd_cl mirror -e "$pre $q" 2>&1 | sed 's/`mirror`/`test`/' | $gd_norm $gd_norm_ml | $gd_post > $gd_dir/gbh_in.txt
--let $gd_off= `SELECT $gd_c1 - $gd_c0`
--let $gd_on= `SELECT $gd_c2 - $gd_c1`
--diff_files $gd_dir/gbh_off.txt $gd_dir/gbh_on.txt
--diff_files $gd_dir/gbh_off.txt $gd_dir/gbh_in.txt
if ($show)
{
  --cat_file $gd_dir/gbh_on.txt
}
if ($refused)
{
  if ($gd_on)
  {
    --die The engine took over a query that must be refused
  }
  --echo # same output with and without pushdown and on InnoDB; the engine did not run it
}
if (!$refused)
{
  if (!$gd_on)
  {
    # $either = 1: the server may fold the query itself (impossible WHERE, row count...): both are fine
    if (!$either)
    {
      --die The engine did not run the GROUP BY of this query
    }
    --echo # same output with and without pushdown and on InnoDB; the server answered by itself (the engine did not run it)
  }
  if ($gd_on)
  {
    --echo # same output with and without pushdown and on InnoDB; the engine ran the GROUP BY only with pushdown ON (runs: OFF=$gd_off ON=$gd_on)
  }
}
--let $refused= 0
--let $either= 0
--let $sorted= 0
--let $show= 0
--let $pre=
--enable_query_log
